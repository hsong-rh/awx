"""Decisions the heartbeat makes about lost instances and orphaned jobs.

Nothing in this module touches the database, receptor or the dispatcher. The heartbeat in
awx.main.tasks.system gathers state, asks these functions what to do, and carries out the
answer. Keeping the decisions separate lets them be tested with plain objects, without a
cluster, and gives cross-controller adoption (AAP-89602) one place to change each rule.

Jobs and instances are read only through attributes, so model instances and simple
stand-ins in tests both work.
"""

from enum import Enum

from django.utils.timezone import timedelta


class JobAction(str, Enum):
    SKIP = 'skip'
    ADOPT = 'adopt'
    REAP = 'reap'
    DEFER = 'defer'  # adoptable, but not here and not now; left for another controller
    REQUEUE = 'requeue'  # never reached an execution environment; back to pending


class LostInstanceAction(str, Enum):
    DEPROVISION = 'deprovision'
    MARK_OFFLINE = 'mark_offline'
    NONE = 'none'


def find_lost_instances(instance_list, this_hostname, ref_time):
    """Split peers into (lost, remaining). This instance is never lost here; it is judged separately."""
    lost = []
    remaining = []
    for inst in instance_list:
        if inst.hostname != this_hostname and inst.is_lost(ref_time=ref_time):
            lost.append(inst)
        else:
            remaining.append(inst)
    return lost, remaining


def live_controller_hostnames(instances, ref_time, grace_seconds):
    """Hostnames of the instances the orphan sweep must not take jobs from.

    An instance is live while it is ready or installed. It also stays live while unavailable
    if it checked in within grace_seconds of ref_time: one failed Redis ping marks a running
    controller unavailable with a recent last_seen, and sweeping its jobs then had the
    controller and its peers all stream them (AAP-96708). A controller that leaves on purpose
    back-dates its last_seen past the grace period, so its jobs are swept at once.
    """
    cutoff = ref_time - timedelta(seconds=grace_seconds)
    return {
        inst.hostname
        for inst in instances
        if inst.node_state in ('ready', 'installed') or (inst.node_state == 'unavailable' and inst.last_seen is not None and inst.last_seen >= cutoff)
    }


def gate_lost_instances(lost_instances, mesh_ready):
    """Return the lost instances that may be handled now.

    While the receptor mesh is re-establishing, a control node may only look lost, so its
    handling waits for a later heartbeat. Execution and hop nodes don't depend on mesh
    consensus and are handled either way.

    mesh_ready is a callable, asked only when something is lost.
    """
    if not lost_instances:
        return []
    if mesh_ready():
        return list(lost_instances)
    return [inst for inst in lost_instances if inst.node_type in ('execution', 'hop')]


def lost_instance_job_action(job, lost_hostname, slot_available):
    """What to do with a running job whose controller or execution node was lost.

    A job dispatched to receptor whose controller was lost is adopted if its playbook ran on
    another node, which outlived the controller. It is deferred instead when this controller
    has no room: it stays owned by the lost controller, so another controller's orphan sweep
    still finds it. A job that never reached an execution environment goes back to pending.
    Anything else is reaped.

    slot_available is a callable, asked only for a job that would be adopted.
    """
    if job.work_unit_id and job.controller_node == lost_hostname and job.execution_node != lost_hostname:
        return JobAction.ADOPT if slot_available() else JobAction.DEFER
    if not job.started and not job.work_unit_id:
        return JobAction.REQUEUE
    return JobAction.REAP


def lost_instance_disposition(inst, auto_deprovision):
    """What to do with the lost instance itself once its jobs are handled."""
    if auto_deprovision and inst.node_type == 'control':
        return LostInstanceAction.DEPROVISION
    if inst.node_state == 'ready':
        return LostInstanceAction.MARK_OFFLINE
    return LostInstanceAction.NONE


def adoption_slot_available(capacity, remaining_capacity, headroom_needed=0):
    """Is there room on this controller for one more adoption?

    An adoption is the one dispatcher task that occupies its worker for the whole remaining
    runtime of a job, because it streams that job's events. A controller inheriting a large
    instance's jobs would otherwise fill the pool with adoptions and starve the heartbeat,
    at which point the rest of the cluster declares *this* controller lost too and the
    cascade repeats. Jobs over the bound are simply not adopted this cycle: they stay running
    and claimed by us, so the next heartbeat re-queues them.

    Control capacity is that bound. It already prices exactly this cost — one
    AWX_CONTROL_NODE_TASK_IMPACT per running job we are the controller_node for — and an
    adopted job is controlled by us, so it is already in consumed_capacity. That makes the
    two adoption shapes fall out correctly without a second knob: re-adopting our own jobs
    after a restart is net zero, because those jobs counted against us before we died and
    still do, while inheriting a dead peer's jobs is genuinely new load and is throttled
    against the same number that bounds normally dispatched work. Operators tune it with the
    capacity_adjustment slider they already use.

    After a claim the job being considered is itself counted, so remaining_capacity == 0
    means it is the one that exactly fills us. That is the default, headroom_needed=0.

    Callers that ask *before* claiming pass headroom_needed=AWX_CONTROL_NODE_TASK_IMPACT,
    because their job is not in consumed_capacity yet and a controller sitting at exactly
    capacity would otherwise take one job too many. The lost-instance path is the one such
    caller: it has to decline rather than claim, so that a job it cannot hold stays owned by
    the dead controller and remains visible to another node's orphan sweep.

    Zero capacity means draining or not yet sized by the capacity job, not "no room";
    consumed_capacity is not meaningful against it, so it does not block adoption.
    """
    if not capacity:
        return True
    return remaining_capacity >= headroom_needed


def startup_job_action(job):
    """At startup, a running job this controller owns is adopted if it was dispatched to receptor, else reaped."""
    if job.work_unit_id:
        return JobAction.ADOPT
    return JobAction.REAP


def running_job_action(job, this_hostname, active_task_ids, ref_time, workflow_ctype_id, live_controller_hostnames):
    """On a periodic heartbeat, what to do with a job that may have been orphaned on this instance.

    A job is left alone unless it is running, belongs to this instance as controller or
    execution node, is not a workflow, started before ref_time, and no dispatcher task is
    tracking it. Such an orphan is adopted when this instance controls it and it was
    dispatched to receptor. One that another instance controls is left alone too: a live
    controller handles its own jobs, and a dispatched job of a dead controller is the orphan
    sweep's to adopt. Anything else is reaped.
    """
    if job.status != 'running':
        return JobAction.SKIP
    if this_hostname not in (job.controller_node, job.execution_node):
        return JobAction.SKIP
    if job.polymorphic_ctype_id == workflow_ctype_id:
        return JobAction.SKIP
    if active_task_ids and job.celery_task_id in active_task_ids:
        return JobAction.SKIP
    if ref_time and (job.started is None or job.started > ref_time):
        return JobAction.SKIP
    if job.work_unit_id and job.controller_node == this_hostname:
        return JobAction.ADOPT
    if job.controller_node != this_hostname and (job.controller_node in live_controller_hostnames or job.work_unit_id):
        return JobAction.SKIP
    return JobAction.REAP


def stall_budget_exhausted(stalled_since, last_event_time, ref_time, timeout_seconds):
    """True if an adoption that cannot make progress has been stalled too long to keep retrying.

    stalled_since is when this controller first recorded the stall, or None if it has no
    record; that is when the budget starts, not the job's last event. A playbook can run
    for longer than the timeout without emitting an event, and measuring from that event
    failed such a job on its first transient error after a controller restart (AAP-96829).
    An event saved after the stall began proves progress, so the newer of the two is used.
    With no record the budget is never exhausted: losing it only defers longer.
    """
    if stalled_since is None:
        return False
    if last_event_time and last_event_time > stalled_since:
        stalled_since = last_event_time
    return stalled_since < ref_time - timedelta(seconds=timeout_seconds)
