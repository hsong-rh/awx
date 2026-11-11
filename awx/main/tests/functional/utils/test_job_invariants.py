from datetime import timedelta
from unittest import mock

import pytest

from awx.main.constants import HOST_RESULTS_UNMATCHED
from awx.main.models import HostMetric, Inventory, Job, JobEvent, JobHostSummary
from awx.main.utils import job_invariants


def _event(job, counter, event='runner_on_ok', host=None, **kw):
    return JobEvent.objects.create(
        job=job,
        counter=counter,
        event=event,
        job_created=job.created,
        host_name=host.name if host else '',
        host_id=host.id if host else None,
        **kw,
    )


@pytest.fixture
def hosts(inventory):
    return [inventory.hosts.create(name=f'h{i}') for i in range(1, 4)]


@pytest.fixture
def job(inventory):
    return Job.objects.create(inventory=inventory, status='successful')


def _snapshot(hosts):
    return {h.name: h.id for h in hosts}


_HOST_MAPS = {}


@pytest.fixture(autouse=True)
def host_map_snapshots():
    """The harness records the job-start host map (failpoints.record_snapshot); serve it from a dict."""
    _HOST_MAPS.clear()
    with mock.patch.object(job_invariants, 'get_snapshot', side_effect=lambda kind, key: _HOST_MAPS.get((kind, str(key)))):
        yield


def _set_host_map_snapshot(job, snap):
    _HOST_MAPS[('host_map', str(job.id))] = snap


def _fail_closed(job):
    job.status = 'failed'
    job.job_explanation = f'{HOST_RESULTS_UNMATCHED} (hosts were added or removed).'
    job.save(update_fields=['status', 'job_explanation'])


@pytest.mark.django_db
def test_host_ids_match_when_events_and_summaries_use_job_start_ids(job, hosts):
    for i, h in enumerate(hosts, start=1):
        _event(job, i, host=h)
        JobHostSummary.objects.create(job=job, host=h, host_name=h.name)
    _set_host_map_snapshot(job, _snapshot(hosts))
    result = job_invariants.check_host_ids_match_job_start(job)
    assert result['ok'], result


@pytest.mark.django_db
def test_host_ids_flag_replayed_events_with_rebuilt_map(job, hosts, inventory):
    """The host-map drift seen in adoption: a recreated host gets the new id. A deleted host's
    events may carry its job-start id and None, so only h3 is flagged."""
    snap = _snapshot(hosts)
    h1, h2, h3 = hosts
    _event(job, 1, host=h1)
    _event(job, 2, host=h2)
    h2.delete()
    JobEvent.objects.create(job=job, counter=3, event='runner_on_ok', job_created=job.created, host_name='h2', host_id=None)
    h3_old = h3.id
    h3.delete()
    h3_new = inventory.hosts.create(name='h3')
    _event(job, 4, host=h3_new)
    _set_host_map_snapshot(job, snap)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok']
    assert 'h2 [' not in result['detail'] and f'h3 [{h3_new.id}] != {h3_old}' in result['detail']


@pytest.mark.django_db
def test_host_ids_deleted_host_events_may_carry_the_job_start_id_or_none(job, hosts):
    """The callback receiver nulls a deleted host's id: events before the delete keep the id."""
    snap = _snapshot(hosts)
    h1, h2, _ = hosts
    _event(job, 1, host=h1)
    _event(job, 2, host=h2)
    h2.delete()
    JobEvent.objects.create(job=job, counter=3, event='runner_on_ok', job_created=job.created, host_name='h2', host_id=None)
    _set_host_map_snapshot(job, snap)
    assert job_invariants.check_host_ids_match_job_start(job)['ok']

    # A host that still exists must keep its job-start id.
    JobEvent.objects.create(job=job, counter=4, event='runner_on_ok', job_created=job.created, host_name='h1', host_id=None)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok'] and f'h1 [{h1.id}, None] != {h1.id}' in result['detail']


@pytest.mark.django_db
def test_host_ids_of_a_job_that_failed_closed_may_be_the_start_id_or_none_for_every_host(job, hosts):
    """Adoption saw a changed inventory: events before the controller was lost keep their ids,
    the replay carries none, and that holds for hosts that still exist."""
    h1, h2, _ = hosts
    _set_host_map_snapshot(job, _snapshot(hosts))
    _event(job, 1, host=h1)
    JobEvent.objects.create(job=job, counter=2, event='runner_on_ok', job_created=job.created, host_name='h1', host_id=None)
    assert not job_invariants.check_host_ids_match_job_start(job)['ok']  # not failed closed: strict

    _fail_closed(job)
    assert job_invariants.check_host_ids_match_job_start(job)['ok']


@pytest.mark.django_db
def test_a_job_that_failed_closed_still_may_not_carry_another_hosts_id(job, hosts, inventory):
    h1, h2, h3 = hosts
    _set_host_map_snapshot(job, _snapshot(hosts))
    _fail_closed(job)
    _event(job, 1, host=h3)
    JobEvent.objects.filter(job=job).update(host_name='h1')  # h1's events carrying h3's id
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok'] and f'h1 [{h3.id}] != {h1.id}' in result['detail']


@pytest.mark.django_db
def test_summaries_of_a_job_that_failed_closed_may_carry_no_host(job, hosts):
    h1, h2, _ = hosts
    _set_host_map_snapshot(job, _snapshot(hosts))
    JobHostSummary.objects.create(job=job, host=h1, host_name='h1')
    JobHostSummary.objects.create(job=job, host=None, host_name='h2')
    assert not job_invariants.check_host_ids_match_job_start(job)['ok']

    _fail_closed(job)
    assert job_invariants.check_host_ids_match_job_start(job)['ok']


@pytest.mark.django_db
def test_a_failed_job_with_another_explanation_is_checked_strictly(job, hosts):
    h1, _, _ = hosts
    _set_host_map_snapshot(job, _snapshot(hosts))
    JobEvent.objects.create(job=job, counter=1, event='runner_on_ok', job_created=job.created, host_name='h1', host_id=None)
    job.status = 'failed'
    job.job_explanation = 'Job terminated due to timeout'
    job.save(update_fields=['status', 'job_explanation'])
    assert not job_invariants.check_host_ids_match_job_start(job)['ok']


@pytest.mark.django_db
def test_host_summaries_expect_none_for_deleted_hosts(job, hosts, inventory):
    snap = _snapshot(hosts)
    h1, h2, _ = hosts
    h2.delete()
    replacement = inventory.hosts.create(name='h2')
    JobHostSummary.objects.create(job=job, host=h1, host_name='h1')
    JobHostSummary.objects.create(job=job, host=replacement, host_name='h2')
    _set_host_map_snapshot(job, snap)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok']
    assert f'summaries: h2 {replacement.id} != None' in result['detail']

    JobHostSummary.objects.filter(job=job, host_name='h2').update(host=None)
    assert job_invariants.check_host_ids_match_job_start(job)['ok']


@pytest.mark.django_db
def test_host_ids_without_snapshot_checks_consistency(job, hosts, inventory):
    """The harness recorded no job-start host map for this job."""
    h1 = hosts[0]
    _event(job, 1, host=h1)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert result['ok'] and 'consistency only' in result['detail']

    JobEvent.objects.create(job=job, counter=2, event='runner_on_ok', job_created=job.created, host_name='h1', host_id=None)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok'] and 'h1' in result['detail']


@pytest.mark.django_db
def test_playbook_events_complete(job):
    for i in range(1, 6):
        _event(job, i)
    _event(job, 6, event='playbook_on_stats')
    _event(job, 7, event='verbose')
    result = job_invariants.check_playbook_events_complete(job)
    assert result['ok'], result
    assert '6 of 6 stored' in result['detail']


@pytest.mark.django_db
def test_playbook_events_complete_flags_holes_before_final_event(job):
    """A harvest that lost the middle of the stream but kept the tail."""
    for i in (1, 2, 3, 9, 10):
        _event(job, i)
    _event(job, 11, event='playbook_on_stats')
    result = job_invariants.check_playbook_events_complete(job)
    assert not result['ok']
    assert '6 of 11 stored, 5 missing' in result['detail']


@pytest.mark.django_db
@pytest.mark.parametrize(
    'status, cancel_flag, ok',
    [('successful', False, False), ('failed', False, False), ('error', False, True), ('canceled', False, True), ('failed', True, True)],
)
def test_playbook_events_complete_without_final_event(job, status, cancel_flag, ok):
    """Output cut short with no gap: nothing after the cut was ever stored."""
    Job.objects.filter(pk=job.pk).update(status=status, cancel_flag=cancel_flag)
    job.refresh_from_db()
    for i in range(1, 17):
        _event(job, i)
    result = job_invariants.check_playbook_events_complete(job)
    assert result['ok'] is ok
    assert 'no playbook_on_stats event; 16 events stored' in result['detail']


@pytest.mark.django_db
def test_playbook_events_complete_counts_duplicates_once(job):
    """Without the (job, counter, job_created) unique constraint, as on devel, a counter can be stored
    twice and must count once. The constraint allows these rows because they differ by job_created, and
    the event queryset is not narrowed by job_created, as it is not for a job with unpartitioned events."""
    for i in (1, 2, 3):
        _event(job, i)
    for i in (2, 3):
        JobEvent.objects.create(job=job, counter=i, event='runner_on_ok', job_created=job.created + timedelta(microseconds=1))
    _event(job, 4, event='playbook_on_stats')
    with mock.patch.object(Job, 'get_event_queryset', lambda self: JobEvent.objects.filter(job_id=self.id)):
        result = job_invariants.check_playbook_events_complete(job)
    assert result['ok'], result
    assert '4 of 4 stored' in result['detail']


def _stats(job, counter, hosts, failures=(), dark=()):
    data = {
        'ok': {h.name: 2 for h in hosts if h.name not in dark},
        'processed': {h.name: 1 for h in hosts},
        'failures': {name: 1 for name in failures},
        'dark': {name: 1 for name in dark},
        'changed': {},
        'skipped': {},
    }
    return _event(job, counter, event='playbook_on_stats', event_data=data)


def _summaries(job, hosts, failures=(), dark=()):
    for h in hosts:
        f, d = int(h.name in failures), int(h.name in dark)
        JobHostSummary.objects.create(job=job, host=h, host_name=h.name, ok=0 if d else 2, processed=1, failures=f, dark=d, failed=bool(f or d))


@pytest.mark.django_db
def test_host_metrics_counted_once(job, hosts):
    _summaries(job, hosts, dark=('h3',))
    snap = {'h1': 4, 'h2': None, 'h3': 7}
    HostMetric.objects.create(hostname='h1', last_automation=job.created, automated_counter=5)
    HostMetric.objects.create(hostname='h2', last_automation=job.created, automated_counter=1)
    HostMetric.objects.create(hostname='h3', last_automation=job.created, automated_counter=7)
    with mock.patch.object(job_invariants, 'get_snapshot', return_value=snap):
        result = job_invariants.check_host_metrics_counted_once(job)
    assert result['ok'], result

    HostMetric.objects.filter(hostname='h1').update(automated_counter=6)
    with mock.patch.object(job_invariants, 'get_snapshot', return_value=snap):
        result = job_invariants.check_host_metrics_counted_once(job)
    assert not result['ok']
    assert result['detail'] == 'h1 +2 (want +1)'


@pytest.mark.django_db
def test_host_metrics_without_snapshot_is_not_checked(job, hosts):
    with mock.patch.object(job_invariants, 'get_snapshot', return_value=None):
        result = job_invariants.check_host_metrics_counted_once(job)
    assert result['ok'] and 'not checked' in result['detail']


@pytest.mark.django_db
def test_host_pointers_consistent(job, hosts, inventory):
    _stats(job, 5, hosts, failures=('h2',))
    _summaries(job, hosts, failures=('h2',))
    inventory.update_computed_fields()
    job.refresh_from_db()
    result = job_invariants.check_host_pointers(job)
    assert result['ok'], result
    assert 'hosts_with_active_failures=1' in result['detail']


@pytest.mark.django_db
def test_host_pointers_flag_stale_inventory_fields_and_summary_mismatch(job, hosts, inventory):
    _stats(job, 5, hosts, failures=('h2',))
    _summaries(job, hosts)  # h2's failure is missing from its summary
    inventory.update_computed_fields()
    job.refresh_from_db()
    result = job_invariants.check_host_pointers(job)
    assert not result['ok']
    assert 'h2 failures=0 != stats 1' in result['detail']

    JobHostSummary.objects.filter(job=job, host_name='h2').update(failures=1, failed=True)
    result = job_invariants.check_host_pointers(job)
    assert not result['ok']
    assert 'inventory hosts_with_active_failures=0 != computed 1' in result['detail']


@pytest.mark.django_db
def test_host_pointers_superseded_and_no_stats(job, hosts, inventory):
    _stats(job, 5, hosts)
    _summaries(job, hosts)
    later = Job.objects.create(inventory=inventory, status='successful')
    _summaries(later, hosts[:1])
    inventory.update_computed_fields()
    job.refresh_from_db()
    result = job_invariants.check_host_pointers(job)
    assert result['ok'], result
    assert 'superseded by later jobs: h1' in result['detail']

    no_stats = Job.objects.create(inventory=inventory, status='failed')
    result = job_invariants.check_host_pointers(no_stats)
    assert result['ok'] and 'no playbook_on_stats; 0 summaries' in result['detail']


def _lines(job, ranges):
    for counter, (start, end) in enumerate(ranges, start=1):
        _event(job, counter, start_line=start, end_line=end, stdout='x\n' * (end - start))


@pytest.mark.django_db
def test_stdout_lines_contiguous(job):
    _lines(job, [(0, 2), (2, 2), (2, 5), (5, 6)])
    result = job_invariants.check_stdout_lines_contiguous(job)
    assert result['ok'], result
    assert 'lines 0-6' in result['detail']


@pytest.mark.django_db
def test_stdout_lines_flag_gaps_and_overlaps(job):
    _lines(job, [(0, 2), (4, 6), (4, 6), (5, 7)])
    result = job_invariants.check_stdout_lines_contiguous(job)
    assert not result['ok']
    assert '1 gaps (lines 2-4 before counter 2)' in result['detail']
    assert '2 overlapping events' in result['detail']


@pytest.mark.django_db
def test_host_ids_constructed_inventory_summaries(organization, inventory, hosts):
    ci = Inventory.objects.create(name='constructed', organization=organization, kind='constructed')
    chosts = [ci.hosts.create(name=h.name, instance_id=str(h.id)) for h in hosts]
    job = Job.objects.create(inventory=ci, status='successful')
    snap = {h.name: h.id for h in chosts}
    for i, (c, h) in enumerate(zip(chosts, hosts), start=1):
        _event(job, i, host=c)
        JobHostSummary.objects.create(job=job, host=h, constructed_host=c, host_name=c.name)
    _set_host_map_snapshot(job, snap)
    result = job_invariants.check_host_ids_match_job_start(job)
    assert result['ok'], result

    JobHostSummary.objects.filter(job=job, host_name='h2').update(host=None, constructed_host=hosts[1])
    result = job_invariants.check_host_ids_match_job_start(job)
    assert not result['ok']
    assert f'h2 constructed_host_id {hosts[1].id} != {chosts[1].id}' in result['detail']
    assert f'h2 host_id None != {hosts[1].id}' in result['detail']


@pytest.mark.django_db
def test_constructed_summaries_of_a_job_that_failed_closed_may_carry_no_host(organization, inventory, hosts):
    ci = Inventory.objects.create(name='constructed', organization=organization, kind='constructed')
    chosts = [ci.hosts.create(name=h.name, instance_id=str(h.id)) for h in hosts]
    job = Job.objects.create(inventory=ci, status='successful')
    _set_host_map_snapshot(job, {h.name: h.id for h in chosts})
    JobHostSummary.objects.create(job=job, host=hosts[0], constructed_host=chosts[0], host_name='h1')
    JobHostSummary.objects.create(job=job, host=None, constructed_host=None, host_name='h2')
    assert not job_invariants.check_host_ids_match_job_start(job)['ok']

    _fail_closed(job)
    assert job_invariants.check_host_ids_match_job_start(job)['ok']


@pytest.mark.django_db
def test_playbook_events_complete_a_job_adoption_stopped_may_have_no_final_event(job):
    """Adoption cancels the unit of a job whose inventory changed, so the playbook never gets to its stats event."""
    for i in range(1, 6):
        _event(job, i)
    assert not job_invariants.check_playbook_events_complete(job)['ok']  # a successful job must have one

    _fail_closed(job)
    assert job_invariants.check_playbook_events_complete(job)['ok']

    job.job_explanation = 'Job terminated due to timeout'
    job.save(update_fields=['job_explanation'])
    assert not job_invariants.check_playbook_events_complete(job)['ok']  # any other failure still needs one
