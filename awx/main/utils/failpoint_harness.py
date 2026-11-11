"""The failpoint harness: the handler behind awx.main.utils.failpoints, and its control API.

MainConfig.ready() installs handle() when ``AWX_FAILPOINTS_ENABLED`` is true. Even then a
failpoint does nothing until a failpoint of that name is *armed*. Arming, releasing and inspecting
failpoints goes through a small Postgres table, because Postgres is the only store every
node in a cluster shares (Redis is node-local). That is what lets a test running anywhere
pause a dispatcher worker on node A, act on node B, then let A continue.

Typical use from a test or a shell::

    awx-manage failpoint arm heartbeat.start --action pause --match node=awx-1
    awx-manage failpoint wait heartbeat.start --fired
    ...do something elsewhere...
    awx-manage failpoint release heartbeat.start
    awx-manage failpoint release sweep.before_claim lost_instance.before_claim --in 2   # all resume together

Actions:
    pause   block until released or disarmed (``--timeout`` seconds, default 600)
    sleep   sleep for ``--seconds`` and continue
    raise   raise FailpointError at the call site, or ``--exception module.Class`` to raise
            a specific type (e.g. redis.exceptions.ConnectionError) the call site handles
    kill    SIGKILL the current process
    kill_parent  SIGKILL the parent process (for a dispatcher worker, the dispatcher)
    trigger  do nothing itself: the failpoint() call returns True, and a call site that asks
            (``if failpoint(...):``) takes its own forced branch

Matching:
    ``--match key=value`` must equal the call's context (all values compared as strings).
    ``node`` and ``pid`` are always present in the context.
    ``--nth N`` fires only on the Nth matching hit. ``--times N`` caps how often it fires.

This is a test facility. Never enable it in production.
"""

import importlib
import json
import logging
import os
import signal
import threading
import time

from django.conf import settings
from django.db import DatabaseError, connection, transaction

# REGISTRY, enabled and failpoint are re-exported so the harness tools need only this module.
from awx.main.utils.failpoints import REGISTRY, enabled, failpoint  # noqa: F401

logger = logging.getLogger('awx.main.utils.failpoints')

ACTIONS = ('pause', 'sleep', 'raise', 'kill', 'kill_parent', 'trigger')

TABLE = 'awx_failpoint'
HIT_TABLE = 'awx_failpoint_hit'
SNAPSHOT_TABLE = 'awx_failpoint_snapshot'

# How long an armed-set snapshot is trusted before it is re-read. Keeps the cost of a hot
# failpoint (callback.event) to a dict lookup between refreshes.
REFRESH_SECONDS = 1.0
PAUSE_POLL_SECONDS = 0.25


class FailpointError(Exception):
    """Raised at a failpoint armed with action=raise."""


_armed = {}
_armed_at = 0.0
_lock = threading.Lock()
_table_ready = False


def ensure_tables():
    """Create the control tables if they do not exist.

    A prototype convenience: a real version would ship a migration. Kept idempotent so any
    process, on any node, can be the first to touch them.
    """
    global _table_ready
    if _table_ready:
        return
    with connection.cursor() as cursor:
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {TABLE} (
                name text PRIMARY KEY,
                action text NOT NULL,
                arg jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                match jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                nth integer,
                times integer,
                hits integer NOT NULL DEFAULT 0,
                fired integer NOT NULL DEFAULT 0,
                released boolean NOT NULL DEFAULT false,
                armed_at timestamptz NOT NULL DEFAULT now()
            )'''
        )
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {HIT_TABLE} (
                id bigserial PRIMARY KEY,
                name text NOT NULL,
                node text NOT NULL,
                pid integer NOT NULL,
                ctx jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                fired boolean NOT NULL,
                action text,
                at timestamptz NOT NULL DEFAULT now()
            )'''
        )
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
                kind text NOT NULL,
                key text NOT NULL,
                data jsonb NOT NULL,
                node text NOT NULL,
                at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (kind, key)
            )'''
        )
        cursor.execute(f'ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS release_at double precision')
    _table_ready = True


def _load_armed():
    global _armed, _armed_at
    now = time.monotonic()
    if now - _armed_at < REFRESH_SECONDS:
        return _armed
    with _lock:
        if now - _armed_at < REFRESH_SECONDS:
            return _armed
        try:
            ensure_tables()
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT name, match FROM {TABLE}')
                _armed = {name: _json(match) for name, match in cursor.fetchall()}
        except Exception:
            logger.exception('Could not read armed failpoints; treating none as armed')
            _armed = {}
        _armed_at = now
    return _armed


def _json(value):
    """Decode a jsonb column. Django's psycopg setup hands jsonb back as text."""
    if isinstance(value, (bytes, str)):
        return json.loads(value) if value else {}
    return value or {}


def _matches(match, ctx):
    return all(str(ctx.get(k)) == str(v) for k, v in match.items())


def _record_hit(name, ctx):
    """Count this hit against the armed row and decide whether it fires.

    Returns the row (action, arg) when it fires, None otherwise. Done in one UPDATE so
    concurrent hits on different nodes cannot both claim the Nth hit.
    """
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                f'''
                UPDATE {TABLE} SET hits = hits + 1
                WHERE name = %s
                RETURNING action, arg, nth, times, hits, fired
                ''',
                [name],
            )
            row = cursor.fetchone()
            if row is None:
                return None
            action, arg, nth, times, hits, fired = row
            fire = (nth is None or hits == nth) and (times is None or fired < times)
            if fire:
                cursor.execute(f'UPDATE {TABLE} SET fired = fired + 1 WHERE name = %s', [name])
            cursor.execute(
                f'INSERT INTO {HIT_TABLE} (name, node, pid, ctx, fired, action) VALUES (%s, %s, %s, %s, %s, %s)',
                [name, ctx['node'], ctx['pid'], json.dumps(ctx, default=str), fire, action if fire else None],
            )
    return (action, _json(arg)) if fire else None


def _pause(name, arg):
    timeout = float(arg.get('timeout', 600))
    deadline = time.monotonic() + timeout
    logger.warning(f'Failpoint {name}: pausing pid={os.getpid()} until released (timeout {timeout}s)')
    while time.monotonic() < deadline:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT released, release_at, extract(epoch from clock_timestamp())::double precision FROM {TABLE} WHERE name = %s', [name])
                row = cursor.fetchone()
        except DatabaseError:
            # The database went away while held (a scenario restarting or cutting it off). Keep
            # holding and reconnect, so the caller resumes only when released.
            logger.warning(f'Failpoint {name}: database unavailable while paused pid={os.getpid()}; retrying')
            connection.close()
            time.sleep(1)
            continue
        if row is None or row[0]:
            if row is not None and row[1] is not None:
                _wait_until(name, row[1], row[2])
            logger.warning(f'Failpoint {name}: released pid={os.getpid()}')
            return
        time.sleep(PAUSE_POLL_SECONDS)
    logger.error(f'Failpoint {name}: pause timed out after {timeout}s, continuing')


def _wait_until(name, release_at, db_now):
    """A scheduled release: sleep until the database-clock instant every holder was given, then
    record how late this caller woke ('resumed' hit, ctx late_ms)."""
    time.sleep(max(0.0, release_at - db_now))
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT extract(epoch from clock_timestamp())::double precision')
            woke = cursor.fetchone()[0]
            ctx = {'node': settings.CLUSTER_HOST_ID, 'pid': os.getpid(), 'late_ms': round((woke - release_at) * 1000, 1), 'woke': woke}
            cursor.execute(
                f'INSERT INTO {HIT_TABLE} (name, node, pid, ctx, fired, action) VALUES (%s, %s, %s, %s, false, %s)',
                [name, ctx['node'], ctx['pid'], json.dumps(ctx), 'resumed'],
            )
    except Exception:
        logger.exception(f'Failpoint {name}: could not record the scheduled resume')


def _exception_class(path):
    """Resolve a dotted exception class path; FailpointError when none is given."""
    if not path:
        return FailpointError
    module, _, cls = path.rpartition('.')
    exc = getattr(importlib.import_module(module), cls)
    if not (isinstance(exc, type) and issubclass(exc, BaseException)):
        raise TypeError(f'{path} is not an exception class')
    return exc


def handle(name, ctx):
    """The failpoint handler: fire the armed failpoint ``name`` if ctx matches it.

    Returns True when it fired and did not raise or kill, so a call site can take a forced
    branch (``if failpoint(...):``); otherwise None.
    """
    armed = _load_armed()
    if name not in armed:
        return
    try:
        if not _matches(armed[name], ctx):
            return
    except Exception:
        logger.exception(f'Failpoint {name}: bad match {armed[name]!r}; not firing')
        return
    try:
        fired = _record_hit(name, ctx)
    except Exception:
        logger.exception(f'Failpoint {name}: could not record hit; not firing')
        return
    if not fired:
        return
    action, arg = fired
    logger.warning(f'Failpoint {name} fired: action={action} ctx={ctx}')
    if action == 'pause':
        _pause(name, arg)
    elif action == 'sleep':
        time.sleep(float(arg.get('seconds', 1)))
    elif action == 'raise':
        raise _exception_class(arg.get('exception'))(f'Failpoint {name} raised (ctx={ctx})')
    elif action == 'kill':
        os.kill(os.getpid(), signal.SIGKILL)
    elif action == 'kill_parent':
        os.kill(os.getppid(), signal.SIGKILL)
    return True


def record_snapshot(kind, key, data):
    """Keep a reference copy of some state for the invariants to compare against later.

    Inert unless failpoints are enabled. The first write for a (kind, key) wins, so a job's
    snapshot stays the one taken at job start even if adoption rebuilds the same state.
    Never raises: a test aid must not break the job it is observing.
    """
    if not enabled():
        return
    try:
        ensure_tables()
        with connection.cursor() as cursor:
            cursor.execute(
                f'INSERT INTO {SNAPSHOT_TABLE} (kind, key, data, node) VALUES (%s, %s, %s, %s) ON CONFLICT (kind, key) DO NOTHING',
                [kind, str(key), json.dumps(data, default=str), settings.CLUSTER_HOST_ID],
            )
    except Exception:
        logger.exception(f'Could not record {kind} snapshot for {key}')


def get_snapshot(kind, key):
    """Return the recorded snapshot data, or None if there is none."""
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT data FROM {SNAPSHOT_TABLE} WHERE kind = %s AND key = %s', [kind, str(key)])
        row = cursor.fetchone()
    return _json(row[0]) if row else None


# --- Control API, used by the management command and by tests ---------------------------


def arm(name, action, match=None, nth=None, times=None, **arg):
    if name not in REGISTRY:
        raise KeyError(f'Unregistered failpoint {name!r}')
    if action not in ACTIONS:
        raise ValueError(f'Unknown action {action!r}; expected one of {ACTIONS}')
    if arg.get('exception'):
        _exception_class(arg['exception'])  # fail at arm time, not at the call site
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(
            f'''
            INSERT INTO {TABLE} (name, action, arg, match, nth, times)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET action = EXCLUDED.action, arg = EXCLUDED.arg,
                match = EXCLUDED.match, nth = EXCLUDED.nth, times = EXCLUDED.times,
                hits = 0, fired = 0, released = false, release_at = NULL, armed_at = now()
            ''',
            [name, action, json.dumps(arg), json.dumps(match or {}), nth, times],
        )


def release(name):
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(f'UPDATE {TABLE} SET released = true WHERE name = %s', [name])
        return cursor.rowcount


def release_in(names, lead):
    """Release several failpoints with one UPDATE, every holder resuming at the same instant.

    The instant is ``lead`` seconds ahead on the database clock, which every node shares. now() is
    fixed for the statement, so every row gets the same instant (clock_timestamp() would not).
    Returns {'at': epoch seconds, 'rows': rows released}.
    """
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(
            f'UPDATE {TABLE} SET released = true, release_at = extract(epoch from now()) + %s WHERE name = ANY(%s) RETURNING release_at',
            [float(lead), list(names)],
        )
        rows = cursor.fetchall()
    return {'at': rows[0][0] if rows else None, 'rows': len(rows)}


def disarm(name=None):
    """Remove one failpoint, or all of them. Paused callers continue on their next poll."""
    ensure_tables()
    with connection.cursor() as cursor:
        if name:
            cursor.execute(f'DELETE FROM {TABLE} WHERE name = %s', [name])
        else:
            cursor.execute(f'DELETE FROM {TABLE}')
        return cursor.rowcount


def clear_hits():
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(f'DELETE FROM {HIT_TABLE}')


def armed_list():
    ensure_tables()
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT name, action, arg, match, nth, times, hits, fired, released, armed_at FROM {TABLE} ORDER BY name')
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    for row in rows:
        row['arg'], row['match'] = _json(row['arg']), _json(row['match'])
    return rows


def hits(name=None, fired_only=False, since_id=0):
    ensure_tables()
    sql = f'SELECT id, name, node, pid, ctx, fired, action, at FROM {HIT_TABLE} WHERE id > %s'
    params = [since_id]
    if name:
        sql += ' AND name = %s'
        params.append(name)
    if fired_only:
        sql += ' AND fired'
    sql += ' ORDER BY id'
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
    for row in rows:
        row['ctx'] = _json(row['ctx'])
    return rows


def wait_for_hit(name, fired_only=True, timeout=300, poll=0.5):
    """Block until the named failpoint has a (fired) hit. Returns the first one, or None."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = hits(name, fired_only=fired_only)
        if found:
            return found[0]
        time.sleep(poll)
    return None
