"""Failure scenarios about when a controller is really gone (write-up section 2).

Tests marked xfail reproduce a defect found on devel and assert the behavior it should
have. Each names its Jira bug; when the bug is fixed the test passes, strict xfail fails
the run, and the marker comes off.
"""

from unittest import mock

import pytest
from django.utils.timezone import now, timedelta

from awx.main.models import Instance, Job
from awx.main.tests.adoption_cluster import EXEC, GRACE, PEER, THIS, UNIT, configure, make_cluster, mesh, run_lost_instance_handling


@pytest.fixture(autouse=True)
def cluster_settings(settings):
    configure(settings)


@pytest.fixture
def peer_job():
    """A job controlled by ctrl-1 and running on receptor-1."""
    return Job.objects.create(controller_node=PEER, execution_node=EXEC, status='running', work_unit_id=UNIT, celery_task_id='peer-task')


@pytest.mark.xfail(strict=True, reason='AAP-96824: a controller that misses heartbeats is judged lost while it still runs its job')
@pytest.mark.django_db
def test_slow_controller_still_on_the_mesh_keeps_its_job(peer_job):
    # 2.1 Slow controller. ctrl-1 has missed two heartbeats but is still connected on the
    # receptor mesh and still streaming its job. ctrl-0 judges it lost from last_seen
    # alone: devel marks the live job failed, and cross-controller adoption takes it over
    # instead. Either way ctrl-1 loses a job it is still running.
    make_cluster()

    run_lost_instance_handling(mesh(PEER, EXEC))

    peer_job.refresh_from_db()
    assert peer_job.status == 'running'
    assert peer_job.controller_node == PEER


@pytest.mark.django_db
def test_controller_gone_from_the_mesh_has_its_job_adopted(peer_job):
    # The case the slow-controller test must not break: a controller that is silent and
    # gone from the mesh is lost. Its job ran on receptor-1, which is still up, so ctrl-0
    # takes it over rather than reporting it failed.
    make_cluster()

    assert run_lost_instance_handling(mesh(EXEC)) == [PEER]

    peer_job.refresh_from_db()
    assert peer_job.status == 'running'
    assert peer_job.controller_node == THIS


@pytest.mark.django_db
def test_slow_controller_job_sends_no_failed_notification(peer_job):
    # 5.15 Notification content (AAP-96848). On devel the reap of a live controller's job
    # sends a failed notification with no hosts; the job later ends successful and
    # notifies again. Taking the job over sends nothing; only its final status notifies.
    make_cluster()

    with mock.patch.object(Job, 'send_notification_templates') as send:
        run_lost_instance_handling(mesh(PEER, EXEC))

    assert mock.call('failed') not in send.call_args_list


@pytest.mark.xfail(strict=True, reason='gist 2.11, no Jira bug filed yet: an execution node still on the mesh is handled as lost and its live jobs reaped')
@pytest.mark.django_db
def test_execution_node_still_on_the_mesh_keeps_its_jobs():
    # 2.11 False lost-node report. receptor-1 missed its check-ins, so its last_seen is
    # stale, but ctrl-0 is still connected to it on the mesh and ctrl-0's own job is still
    # running there. The mesh gate defers only control nodes, so receptor-1 is handled as
    # lost and ctrl-0's live job is marked failed while its playbook runs on.
    make_cluster()
    Instance.objects.filter(hostname=EXEC).update(last_seen=now() - timedelta(seconds=GRACE + 600))
    job = Job.objects.create(controller_node=THIS, execution_node=EXEC, status='running', work_unit_id=UNIT, celery_task_id='job-task', started=now())

    run_lost_instance_handling(mesh(PEER, EXEC))

    job.refresh_from_db()
    assert job.status == 'running'
