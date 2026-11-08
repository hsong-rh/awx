"""Adoption rebuilds a job's host ids from the inventory only when the inventory is unchanged since the job started."""

from datetime import timedelta
from unittest import mock

import pytest

from django.db import connection
from django.utils.timezone import now

from awx.main.models import AdHocCommand, Inventory, InventorySource, InventoryUpdate, Job, ProjectUpdate
from awx.main.tasks.callback import RunnerCallback, inventory_change_since_start
from awx.main.tasks.receptor import _build_adoption_callback


@pytest.fixture
def inv(organization):
    inv = Inventory.objects.create(organization=organization, name='change-inv')
    for name in ('h1', 'h2', 'h3'):
        inv.hosts.create(name=name)
    return inv


@pytest.fixture
def job(inv):
    """A job that started after the hosts existed and saved their count."""
    return Job.objects.create(inventory=inv, status='running', started=now(), job_env={'host_count': 3})


def _sync(inv, created=None):
    source = InventorySource.objects.create(name='src', inventory=inv, source='ec2')
    update = InventoryUpdate.objects.create(inventory_source=source, source='ec2')
    if created:
        InventoryUpdate.objects.filter(pk=update.pk).update(created=created)
    return update


@pytest.mark.django_db
def test_an_unchanged_inventory_is_trusted(job):
    assert inventory_change_since_start(job) is None


@pytest.mark.django_db
def test_a_fact_write_or_a_job_in_the_same_inventory_changes_nothing(job, inv):
    inv.hosts.filter(name='h1').update(ansible_facts={'a': 1})
    Job.objects.create(inventory=inv, status='successful')
    assert inventory_change_since_start(job) is None


@pytest.mark.django_db
def test_a_host_added_after_start_is_a_change(job, inv):
    inv.hosts.create(name='h4')
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_host_deleted_after_start_is_a_change(job, inv):
    inv.hosts.get(name='h3').delete()
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_host_deleted_and_recreated_with_the_same_name_is_a_change(job, inv):
    """The count is the same; the new row is what gives it away."""
    inv.hosts.get(name='h3').delete()
    inv.hosts.create(name='h3')
    assert inv.hosts.count() == 3
    assert inventory_change_since_start(job)


@pytest.mark.django_db
@pytest.mark.parametrize('field, value', [('name', 'renamed'), ('enabled', False), ('description', 'edited')])
def test_an_edit_that_saves_the_whole_row_is_a_change(job, inv, field, value):
    host = inv.hosts.get(name='h1')
    setattr(host, field, value)
    host.save()
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_sync_is_a_change_even_when_it_changed_no_host(job, inv):
    _sync(inv)
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_sync_edit_made_with_update_fields_is_seen_through_the_sync(job, inv):
    """The importer renames with save(update_fields=...), which never moves Host.modified."""
    host = inv.hosts.get(name='h1')
    host.name = 'renamed'
    host.save(update_fields=['name'])
    assert inventory_change_since_start(job) is None  # modified alone cannot see it
    _sync(inv)
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_sync_that_ran_before_the_job_started_is_not_a_change(job, inv):
    _sync(inv, created=job.started - timedelta(minutes=1))
    assert inventory_change_since_start(job) is None


@pytest.mark.django_db
def test_a_sync_of_another_inventory_is_not_a_change(job, organization):
    _sync(Inventory.objects.create(organization=organization, name='other'))
    assert inventory_change_since_start(job) is None


@pytest.mark.django_db
@pytest.mark.parametrize('job_env', [None, {}, {'host_count': None}])
def test_a_job_without_a_saved_count_is_a_change(job, job_env):
    job.job_env = job_env
    assert 'no host count was saved' in inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_job_that_never_had_an_inventory_is_trusted():
    """It has no hosts to attribute, so there is nothing a change could get wrong."""
    job = Job.objects.create(status='running', started=now(), job_env={})
    callback = RunnerCallback()
    assert inventory_change_since_start(job) is None
    assert callback.populate_host_map_from_inventory(job) is None
    assert callback.host_map == {}


@pytest.mark.django_db
def test_a_deleted_inventory_is_a_change(job, inv):
    inv.delete()
    job.refresh_from_db()
    assert job.inventory_id is None
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_a_job_with_no_start_time_is_a_change(job):
    job.started = None
    assert inventory_change_since_start(job)


@pytest.mark.django_db
def test_the_map_is_rebuilt_from_an_unchanged_inventory(job, inv):
    callback = RunnerCallback()
    assert callback.populate_host_map_from_inventory(job) is None
    assert callback.host_map == {h.name: h.id for h in inv.hosts.all()}


@pytest.mark.django_db
def test_an_ad_hoc_command_is_checked_like_a_job(inv):
    command = AdHocCommand.objects.create(inventory=inv, status='running', started=now(), job_env={'host_count': 3})
    callback = RunnerCallback()
    assert callback.populate_host_map_from_inventory(command) is None
    assert len(callback.host_map) == 3
    inv.hosts.create(name='h4')
    callback = RunnerCallback()
    assert callback.populate_host_map_from_inventory(command)
    assert callback.host_map == {}


@pytest.mark.django_db
def test_a_changed_inventory_leaves_the_map_empty(job, inv):
    inv.hosts.create(name='h4')
    callback = RunnerCallback()
    assert callback.populate_host_map_from_inventory(job)
    assert callback.host_map == {}


@pytest.mark.django_db
def test_a_change_while_the_map_is_read_leaves_the_map_empty(job, inv):
    real = Inventory.get_script_data

    def read_then_delete_a_host(self, *args, **kwargs):
        data = real(self, *args, **kwargs)
        self.hosts.filter(name='h1').delete()
        return data

    callback = RunnerCallback()
    with mock.patch.object(Inventory, 'get_script_data', read_then_delete_a_host):
        assert callback.populate_host_map_from_inventory(job)
    assert callback.host_map == {}


@pytest.mark.django_db
def test_a_map_that_cannot_be_read_is_a_change(job):
    callback = RunnerCallback()
    with mock.patch.object(Inventory, 'get_script_data', side_effect=RuntimeError('db went away')):
        assert callback.populate_host_map_from_inventory(job) == 'the host map could not be read'
    assert callback.host_map == {}


@pytest.mark.django_db
@pytest.mark.parametrize('instance', [InventoryUpdate(), ProjectUpdate()])
def test_other_job_types_are_not_checked(instance):
    callback = RunnerCallback()
    assert callback.populate_host_map_from_inventory(instance) is None
    assert callback.host_map == {}


@pytest.mark.django_db
def test_the_adoption_callback_of_an_unchanged_inventory_has_the_map(job, inv):
    callback = _build_adoption_callback(job, 0, set())
    assert callback.inventory_change is None
    assert callback.host_map == {h.name: h.id for h in inv.hosts.all()}


@pytest.mark.django_db
def test_the_adoption_callback_of_a_changed_inventory_is_flagged_with_an_empty_map(job, inv, caplog):
    inv.hosts.create(name='h4')
    callback = _build_adoption_callback(job, 0, set())
    assert callback.inventory_change == 'hosts were added or removed'
    assert callback.host_map == {}
    assert f'Job {job.id} will not get host ids on replayed events: hosts were added or removed' in caplog.text


@pytest.mark.django_db
def test_the_adoption_callback_of_a_job_without_a_saved_count_is_flagged(job):
    job.job_env = {}
    job.save(update_fields=['job_env'])
    callback = _build_adoption_callback(job, 0, set())
    assert 'no host count was saved' in callback.inventory_change
    assert callback.host_map == {}


@pytest.mark.django_db
def test_the_adoption_callback_of_an_inventory_update_is_never_flagged(inv):
    source = InventorySource.objects.create(name='src', inventory=inv, source='ec2')
    update = InventoryUpdate.objects.create(inventory_source=source, source='ec2', status='running', started=now(), job_env={})
    callback = _build_adoption_callback(update, 0, set())
    assert callback.inventory_change is None
    assert callback.host_map == {}


# A smart inventory's host query uses DISTINCT ON, which sqlite lacks.
on_postgres = pytest.mark.skipif(connection.vendor != 'postgresql', reason='smart inventory hosts need PostgreSQL')


@pytest.fixture
def smart_job(inv, organization):
    """A job on a smart inventory whose members (a live host_filter query) are the 3 hosts of inv."""
    smart = Inventory.objects.create(name='smart', organization=organization, kind='smart', host_filter='name__startswith=h')
    return Job.objects.create(inventory=smart, status='running', started=now(), job_env={'host_count': smart.hosts.count()})


@on_postgres
@pytest.mark.django_db
def test_a_smart_inventory_job_is_trusted_while_its_members_are_unchanged(smart_job):
    assert smart_job.job_env['host_count'] == 3
    assert inventory_change_since_start(smart_job) is None


@on_postgres
@pytest.mark.django_db
@pytest.mark.parametrize('change', ['add', 'delete', 'edit', 'sync'])
def test_a_change_to_a_smart_inventorys_members_is_seen(smart_job, inv, change):
    if change == 'add':
        inv.hosts.create(name='h4')
    elif change == 'delete':
        inv.hosts.filter(name='h1').delete()
    elif change == 'edit':
        host = inv.hosts.get(name='h1')
        host.description = 'edited'
        host.save()
    else:
        _sync(inv)
    assert inventory_change_since_start(smart_job)


@pytest.mark.django_db
def test_a_sync_in_the_organization_is_a_change_for_a_smart_inventory(inv, organization):
    # The smart host query needs PostgreSQL, so its members are stood in for by inv's hosts.
    smart = Inventory.objects.create(name='smart', organization=organization, kind='smart', host_filter='name__startswith=h')
    job = Job.objects.create(inventory=smart, status='running', started=now(), job_env={'host_count': 3})
    with mock.patch.object(Inventory, 'hosts', new_callable=mock.PropertyMock, return_value=inv.hosts):
        assert inventory_change_since_start(job) is None
        _sync(inv)
        assert inventory_change_since_start(job) == 'the inventory was updated'


@pytest.mark.django_db
def test_a_sync_in_another_organization_is_not_a_change_for_a_smart_inventory(inv, organization, organization_factory):
    other = organization_factory('other').organization
    smart = Inventory.objects.create(name='smart', organization=organization, kind='smart', host_filter='name__startswith=h')
    job = Job.objects.create(inventory=smart, status='running', started=now(), job_env={'host_count': 3})
    _sync(Inventory.objects.create(name='elsewhere', organization=other))
    with mock.patch.object(Inventory, 'hosts', new_callable=mock.PropertyMock, return_value=inv.hosts):
        assert inventory_change_since_start(job) is None
