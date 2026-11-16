"""Fenced, resumable execution of simulation runs.

A worker must hold a lease to touch a run.  Every write carries the lease token
issued by the successful claim; a worker that lost its lease (crashed, partitioned,
or superseded by a takeover) is rejected instead of overwriting newer progress.
Claims are a single conditional UPDATE, so concurrent execution and duplicate job
delivery can only ever produce one current takeover holder.

Publishing is also a single conditional UPDATE after the versioned artifact file
has been written, so completion status, the API result, the result hash and the
result file always describe the same complete version.  Aggregation is derived
only from persisted replication rows, so an interrupted aggregation/commit is
idempotently re-run by the next holder.
"""
import uuid
from sqlalchemy import func, or_, update
from ..models import SimulationRun, RunReplication, RunEvent
from ..utils.clock import now, now_ts
from ..config.settings import RUN_LEASE_SECONDS
from .artifact_service import result_version, artifact_path
from .simulation_algorithm import aggregate_replicates


class LeaseLost(Exception):
    """The caller is no longer the current takeover holder."""


def _event(db, run_id, attempt, event, actor="", detail=""):
    db.add(RunEvent(run_id=run_id, attempt=attempt, event=event, actor=actor, detail=detail))


def _run_or_none(db, run_id):
    db.expire_all()
    return db.get(SimulationRun, run_id)


def _lease_free():
    return or_(
        SimulationRun.status=="queued",
        (SimulationRun.status=="running") & (func.coalesce(SimulationRun.lease_expires_at,0.)<now_ts()),
    )


def claim(db, run_id, worker_id, lease_seconds=None, actor="worker"):
    """Atomically claim a queued run or take over an expired running one.

    Returns the new lease token, or None when another holder is still current.
    """
    lease_seconds=lease_seconds or RUN_LEASE_SECONDS
    timestamp=now_ts();stamp=now()
    token=uuid.uuid4().hex
    result=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,_lease_free())
        .values(
            status="running",
            attempt=func.coalesce(SimulationRun.attempt,0)+1,
            lease_owner=worker_id,
            lease_token=token,
            lease_expires_at=timestamp+lease_seconds,
            started_at=func.coalesce(func.nullif(SimulationRun.started_at,""),stamp),
            progress_total=SimulationRun.replications,
            progress_updated_at=stamp,
            updated_at=stamp,
            error="",
        )
    )
    if result.rowcount!=1:
        db.rollback();return None
    run=_run_or_none(db,run_id)
    _event(db,run_id,run.attempt,"claimed" if run.attempt==1 else "taken_over",actor,run.lease_owner)
    db.commit()
    return token


def renew(db, run_id, token, lease_seconds=None):
    """Extend the current lease; returns False when the token is no longer current."""
    lease_seconds=lease_seconds or RUN_LEASE_SECONDS
    result=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(lease_expires_at=now_ts()+lease_seconds,updated_at=now())
    )
    if result.rowcount!=1:db.rollback();return False
    db.commit();return True


def persisted_replications(db, run_id):
    """Return persisted replication numbers (1-based) for a run."""
    return {value for (value,) in db.query(RunReplication.replication).filter(RunReplication.run_id==run_id).all()}


def load_replications(db, run_id):
    return db.query(RunReplication).filter(RunReplication.run_id==run_id).order_by(RunReplication.replication).all()


def record_replication(db, run_id, token, index, summary, sample_customers=None):
    """Durably save one independent replication, fenced by the lease token.

    Already persisted replications are never recomputed or overwritten: an
    existing ``(run_id, replication)`` row wins and the call is a no-op.
    """
    fenced=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(updated_at=now(),progress_updated_at=now(),lease_expires_at=now_ts()+RUN_LEASE_SECONDS)
    )
    if fenced.rowcount!=1:db.rollback();return False
    existing=db.query(RunReplication).filter_by(run_id=run_id,replication=index).first()
    if existing:
        db.commit();return True
    run=_run_or_none(db,run_id)
    db.add(RunReplication(run_id=run_id,replication=index,attempt=run.attempt,lease_token=token,summary=summary,sample_customers=sample_customers or []))
    db.flush()
    completed=db.query(func.count(RunReplication.id)).filter(RunReplication.run_id==run_id).scalar() or 0
    db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(progress_completed=completed,progress_total=SimulationRun.replications,progress_updated_at=now(),updated_at=now())
    )
    _event(db,run_id,run.attempt,"replication_saved","worker",f"R{index}")
    db.commit()
    return True


def publish_complete(db, run_id, token, build, writer, actor="worker"):
    """Publish a complete result version, including its artifact, exactly once.

    ``build(summaries, sample)`` assembles the result from persisted replications
    and raises when the set is incomplete, before any status change happens.
    ``writer(result, version)`` atomically persists the versioned artifact.
    """
    rows=load_replications(db,run_id)
    summaries=[row.summary for row in rows]
    sample=[]
    for row in rows:
        if row.sample_customers:sample=row.sample_customers;break
    result=build(summaries,sample)
    version=result_version(result)
    path=writer(result,version)
    stamp=now()
    published=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(
            status="completed",
            result=result,
            result_version=version,
            artifact_path=str(path),
            completed_at=stamp,
            updated_at=stamp,
            lease_expires_at=0.,
            progress_completed=len(summaries),
            progress_total=SimulationRun.replications,
            progress_updated_at=stamp,
            error="",
        )
    )
    if published.rowcount!=1:
        # A superseded holder must not change the run or its referenced artifact.
        db.rollback();return False
    run=_run_or_none(db,run_id)
    _event(db,run_id,run.attempt,"completed",actor,f"version={version}")
    db.commit()
    return True


def mark_failed(db, run_id, token, error, actor="worker"):
    """Fail a run only while the caller still holds its lease."""
    result=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(status="failed",error=error,lease_expires_at=0.,updated_at=now(),progress_updated_at=now())
    )
    if result.rowcount!=1:db.rollback();return False
    run=_run_or_none(db,run_id)
    _event(db,run_id,run.attempt,"failed",actor,error[:500])
    db.commit()
    return True


def release(db, run_id, token, reason="", actor="worker", retry_delay=5.):
    """Give up a claim without failing the run.

    The run stays ``running`` with no holder and its lease pushed ``retry_delay``
    seconds into the future, so any worker can resume the missing replications
    after the short backoff (this also avoids a hot retry loop).
    """
    result=db.execute(
        update(SimulationRun)
        .where(SimulationRun.id==run_id,SimulationRun.lease_token==token,SimulationRun.status=="running")
        .values(lease_owner="",lease_expires_at=now_ts()+max(0.,retry_delay),updated_at=now())
    )
    if result.rowcount!=1:db.rollback();return False
    run=_run_or_none(db,run_id)
    _event(db,run_id,run.attempt,"released",actor,reason[:500])
    db.commit()
    return True


def reclaimable_ids(db, limit=50):
    """Runs a worker should pick up: queued jobs and expired running jobs."""
    return [
        value for (value,) in db.query(SimulationRun.id)
        .filter(_lease_free())
        .order_by(SimulationRun.id)
        .limit(limit)
        .all()
    ]


def requeue(db, run_id, actor="", force=False):
    """Re-enqueue a run for takeover without resetting persisted progress.

    Only allowed while no live takeover holder exists, unless ``force`` revokes
    the current holder's token (the revoked holder's later writes are fenced out).
    Failed runs may also be retried: their already persisted replications are
    kept, so recovery never recomputes work that is already on disk.
    """
    timestamp=now_ts();stamp=now()
    conditions=[
        SimulationRun.id==run_id,
        SimulationRun.status.in_(("queued","running","failed")),
    ]
    if not force:
        conditions.append(or_(SimulationRun.status!="running",func.coalesce(SimulationRun.lease_expires_at,0.)<timestamp))
    result=db.execute(
        update(SimulationRun)
        .where(*conditions)
        .values(status="queued",lease_owner="",lease_token="",lease_expires_at=0.,error="",updated_at=stamp,progress_updated_at=stamp)
    )
    if result.rowcount!=1:db.rollback();return False
    run=_run_or_none(db,run_id)
    _event(db,run_id,run.attempt or 0,"requeued",actor,"强制接管" if force else "")
    db.commit()
    return True


def run_events(db, run_id):
    return db.query(RunEvent).filter(RunEvent.run_id==run_id).order_by(RunEvent.id).all()
