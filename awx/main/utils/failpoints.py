"""Failpoints: named spots in the code where a fault-injection harness can act.

``failpoint(name, **ctx)`` does nothing unless ``AWX_FAILPOINTS_ENABLED`` is true and a harness
installed a handler with ``set_handler``. The handler gets the name and the call's context,
with ``node`` and ``pid`` added, and decides what to do there: pause, sleep, raise, kill.
No handler ships with AWX, so in a normal install every call returns at once.

This is a test facility. Never enable it in production.
"""

import os

from django.conf import settings

# Every failpoint name must be registered here. A call with an unknown name raises when
# a handler is installed, so a typo in a test or in the code fails loudly instead of
# silently never firing.
REGISTRY = {
    'mesh.ignore_advertisement': 'inspect_execution_and_hop_nodes: a mesh advertisement from an execution or hop node (ctx: advertised). A fired hit skips it, so the node looks unseen.',
    'heartbeat.start': 'Start of cluster_node_heartbeat, before instance management. Pausing it makes this node miss heartbeats while staying alive.',
    'job.after_submit_before_unit_saved': 'Normal job path: work unit submitted to receptor, work_unit_id not yet saved on the job.',
    'job.stream_started': 'Normal job and adoption path: results stream opened, before the first event is read.',
    'callback.event': 'Every event handed to the callback (ctx: job_id, counter). Use with --match counter=N or --nth.',
    'callback.artifacts': 'RunnerCallback.artifacts_handler: start of the end-of-run artifacts processing, on a normal run and on an adoption (ctx: job_id).',
    'job.before_finalize': 'Normal job path: playbook finished, before the terminal status is saved.',
    'job.after_finalize_before_release': 'Normal job path: terminal status saved, before the work unit is released.',
    'adoption.after_snapshot': 'reattach_to_work_unit: dedup snapshot taken, before the replay starts.',
    'adoption.before_finalize': 'reattach_to_work_unit: stream finished, before hooks and the terminal status.',
    'adoption.after_finalize_before_release': 'reattach_to_work_unit: finalization done, before the work unit is released.',
    'callback_receiver.before_read': 'Callback receiver worker: about to pop its next event from Redis. A pause here leaves the backlog in Redis.',
    'callback_receiver.before_flush': 'Callback receiver worker: about to bulk insert its buffered events.',
    'events.stats_before_insert': 'Stats event: existing host summaries read, before the insert and host metrics update.',
    'events.stats_after_insert': 'Stats event: host summaries inserted (ctx: new, metric_hosts), before the host metrics update.',
    'adoption.unit_status': 'reattach_to_work_unit: about to query the work unit status. raise here reads as an unreachable unit.',
    'health_check.redis_ping': 'Instance.local_health_check: about to ping Redis. raise --exception redis.exceptions.ConnectionError marks this node unavailable.',
    'lost_instance.before_claim': 'Lost-instance path: job judged adoptable and capacity available, before the claim UPDATE.',
    'sweep.before_claim': 'Orphan sweep: capacity available, before the claim UPDATE.',
    'adoption.before_adoption_published': '_publish_adoption: the adoption task id is on the job, the task not yet published (a cancel here reaches no worker).',
    'adoption.after_claim': 'adopt_job_async: job claimed for this controller, before the capacity check and streaming.',
    'heartbeat.force_lost': 'Per peer in _heartbeat_instance_management (ctx: other). A fired trigger makes this heartbeat judge that peer lost, whatever its last_seen and its place on the mesh.',
    'adoption.before_task_id_saved': 'claim_for_adoption: the new celery_task_id is chosen, before the claim UPDATE writes it (a cancel here is addressed to the old owner).',
}


_handler = None


def set_handler(handler):
    """Install handler(name, ctx) as the harness behind every failpoint; None removes it."""
    global _handler
    _handler = handler


def enabled():
    return _handler is not None and bool(getattr(settings, 'AWX_FAILPOINTS_ENABLED', False))


def failpoint(name, **ctx):
    """Mark a spot where a test may inject a fault. Returns what the handler returns, else None."""
    if not enabled():
        return None
    if name not in REGISTRY:
        raise KeyError(f'Unregistered failpoint {name!r}; add it to awx.main.utils.failpoints.REGISTRY')
    return _handler(name, dict(ctx, node=settings.CLUSTER_HOST_ID, pid=os.getpid()))
