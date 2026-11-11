import pathlib
import re
from unittest import mock

import pytest

from awx.main.utils import failpoints


@pytest.fixture(autouse=True)
def restore_handler():
    # A harness may have installed its handler at startup; put it back after each test.
    installed = failpoints._handler
    yield
    failpoints.set_handler(installed)


@pytest.fixture
def handler():
    handler = mock.Mock(return_value=True)
    failpoints.set_handler(handler)
    return handler


def test_no_handler_is_inert_and_skips_registry_check(settings):
    failpoints.set_handler(None)
    settings.AWX_FAILPOINTS_ENABLED = True
    assert failpoints.failpoint('not.a.registered.name') is None


def test_handler_is_not_called_while_disabled(settings, handler):
    settings.AWX_FAILPOINTS_ENABLED = False
    assert failpoints.failpoint('heartbeat.start') is None
    handler.assert_not_called()


def test_handler_gets_the_context_with_node_and_pid(settings, handler):
    settings.AWX_FAILPOINTS_ENABLED = True
    settings.CLUSTER_HOST_ID = 'awx-1'

    assert failpoints.failpoint('job.before_finalize', job_id=7) is True

    name, ctx = handler.call_args[0]
    assert name == 'job.before_finalize'
    assert ctx['job_id'] == 7 and ctx['node'] == 'awx-1' and 'pid' in ctx


def test_enabled_rejects_unregistered_name(settings, handler):
    settings.AWX_FAILPOINTS_ENABLED = True
    with pytest.raises(KeyError, match='Unregistered failpoint'):
        failpoints.failpoint('not.a.registered.name')
    handler.assert_not_called()


def test_every_call_site_is_registered():
    """Every failpoint('...') literal in the source tree must be in REGISTRY, and vice versa."""
    root = pathlib.Path(failpoints.__file__).resolve().parents[2]
    used = set()
    for path in root.rglob('*.py'):
        if 'tests' in path.parts:
            continue
        used |= set(re.findall(r"\bfailpoint\('([a-z_.]+)'", path.read_text(errors='ignore')))
    assert used - set(failpoints.REGISTRY) == set()
    assert set(failpoints.REGISTRY) - used == set()
