"""The host count a job records at start (the baseline adoption compares the inventory against)."""

from unittest import mock

import pytest

from awx.main.models import AdHocCommand, Inventory, InventorySource, InventoryUpdate, Job, ProjectUpdate
from awx.main.tasks.callback import RunnerCallback, needs_host_attribution
from awx.main.tasks.jobs import RunJob


@pytest.fixture
def inv(organization):
    inv = Inventory.objects.create(organization=organization, name='count-inv')
    for name in ('h1', 'h2', 'h3'):
        inv.hosts.create(name=name)
    return inv


@pytest.mark.django_db
@pytest.mark.parametrize('model', [Job, AdHocCommand])
def test_a_job_or_ad_hoc_command_with_an_inventory_records_the_count(inv, model):
    callback = RunnerCallback()
    callback.record_host_count(model.objects.create(inventory=inv))
    assert callback.host_count == 3


@pytest.mark.django_db
def test_the_count_includes_disabled_hosts(inv):
    inv.hosts.filter(name='h1').update(enabled=False)
    callback = RunnerCallback()
    callback.record_host_count(Job.objects.create(inventory=inv))
    assert callback.host_count == 3


@pytest.mark.django_db
@pytest.mark.parametrize('instance', [Job(), AdHocCommand(), InventoryUpdate(), ProjectUpdate()])
def test_nothing_is_recorded_without_an_inventory_or_for_other_job_types(instance):
    callback = RunnerCallback()
    callback.record_host_count(instance)
    assert callback.host_count is None


def test_needs_host_attribution_is_for_job_and_ad_hoc_command_with_an_inventory():
    assert needs_host_attribution(Job(inventory_id=1))
    assert needs_host_attribution(AdHocCommand(inventory_id=1))
    assert not needs_host_attribution(Job())
    assert not needs_host_attribution(InventoryUpdate(inventory_source=InventorySource(inventory_id=1)))
    assert not needs_host_attribution(ProjectUpdate())


@pytest.mark.django_db
def test_build_inventory_counts_before_it_reads_the_hosts(inv, tmp_path):
    """A host deleted while the inventory file is built stays in the file. Counting first makes the
    adoption count differ from the stored one; counting afterwards would hide the delete."""
    job = Job.objects.create(inventory=inv)
    task = RunJob()
    task.instance = job

    real = Inventory.get_script_data

    def delete_a_host_then_read(self, *args, **kwargs):
        data = real(self, *args, **kwargs)
        self.hosts.filter(name='h3').delete()
        return data

    for sub_dir in ('inventory',):
        (tmp_path / sub_dir).mkdir()
    with mock.patch.object(Inventory, 'get_script_data', delete_a_host_then_read):
        task.build_inventory(job, str(tmp_path))
    assert task.runner_callback.host_count == 3
    assert inv.hosts.count() == 2
