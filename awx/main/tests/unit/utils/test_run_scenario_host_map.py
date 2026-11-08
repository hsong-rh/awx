"""The pass/fail decision of the failpoint harness's host-map scenario."""

import importlib.util
from pathlib import Path

import pytest

from awx.main.constants import HOST_RESULTS_UNMATCHED

_SCRIPT = Path(__file__).resolve().parents[5] / 'tools' / 'failpoints' / 'run_scenario.py'
_spec = importlib.util.spec_from_file_location('run_scenario', _SCRIPT)
run_scenario = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_scenario)

UNMATCHED = f'{HOST_RESULTS_UNMATCHED} (hosts were added or removed).'


def test_the_harness_looks_for_the_text_awx_writes():
    assert run_scenario.HOSTMAP_UNMATCHED in HOST_RESULTS_UNMATCHED


@pytest.mark.parametrize(
    'variant, adopt_via, status, explanation, ok',
    [
        ('changed', 'restart', 'failed', UNMATCHED, True),
        ('changed', 'peer', 'failed', UNMATCHED, True),
        ('changed', 'restart', 'successful', '', False),  # adopted with ids that may be wrong
        ('changed', 'restart', 'failed', 'Job terminated due to timeout', False),  # failed for another reason
        ('unchanged', 'restart', 'successful', '', True),
        ('unchanged', 'peer', 'successful', None, True),
        ('unchanged', 'restart', 'failed', UNMATCHED, False),  # failed a job whose inventory was untouched
        ('changed', 'none', 'successful', '', True),  # no failover, so nothing to fail
        ('changed', 'none', 'failed', UNMATCHED, False),
    ],
)
def test_outcome(variant, adopt_via, status, explanation, ok):
    assert run_scenario.hostmap_outcome(variant, adopt_via, status, explanation)['ok'] is ok
