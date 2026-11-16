"""Resumable simulation worker.

The Redis list is only a wake-up hint.  After every wake-up the worker also scans
the database for queued runs and for running runs whose lease has expired, so a
run interrupted by a crash, restart or lost worker is taken over automatically
instead of staying "running" forever or needing a wasteful resubmit.

Each independent replication is persisted as soon as it is computed.  A takeover
skips replications already on disk and only computes the missing ones, using the
same root SeedSequence and per-replication spawn keys as an uninterrupted run.
All progress and result writes are fenced by the lease token, so a late progress
message or result from a superseded holder can never overwrite newer work.
"""
import threading,time,traceback,uuid
from pathlib import Path
from .config.database import Base,engine,SessionLocal
from .config.migrate import migrate_schema
from .config.settings import ARTIFACT_DIR,RUN_LEASE_SECONDS
from .models import SimulationRun
from .services.run_service import queue
from .services.comparison_service import build_result, simulate_one
from .services.run_lease_service import claim,renew,persisted_replications,record_replication,publish_complete,mark_failed,release,reclaimable_ids
from .services.artifact_service import write_artifact

WORKER_ID=f"worker-{uuid.uuid4().hex[:8]}"
QUEUE_KEY="queue:jobs"
ARTIFACT_ROOT=Path(ARTIFACT_DIR)
# Renew several times per lease window so a long replication never appears dead.
HEARTBEAT_SECONDS=max(1.,RUN_LEASE_SECONDS/3)
_active_lock=threading.Lock()
_active={"run_id":None,"token":""}


def heartbeat():
    try:
        (ARTIFACT_ROOT/"heartbeat").write_text(str(time.time()))
    except OSError:
        pass


def _keep_alive():
    """Independently renew the active lease and refresh the worker heartbeat."""
    while True:
        heartbeat()
        with _active_lock:
            run_id,token=_active["run_id"],_active["token"]
        if run_id and token:
            try:
                with SessionLocal() as db:renew(db,run_id,token)
            except Exception:
                pass
        time.sleep(HEARTBEAT_SECONDS)


def _build(run):
    def build(summaries,sample):
        return build_result(run.input_snapshot,run.customers,run.warmup,run.replications,run.seed,run.confidence,summaries,sample)
    return build


def _writer(run):
    def write(result,version):
        return write_artifact(run.id,version,result,directory=ARTIFACT_ROOT)
    return write


def execute(run_id,worker_id=WORKER_ID):
    """Claim a run and drive it to completion; returns the outcome string."""
    heartbeat()
    with SessionLocal() as db:
        try:
            token=claim(db,run_id,worker_id)
        except Exception:
            traceback.print_exc();db.rollback();return "retry"
        if not token:return "not_claimed"
        _active.update(run_id=run_id,token=token)
        run=db.get(SimulationRun,run_id)
        try:
            done=persisted_replications(db,run_id)
            if (run.progress_completed or 0)!=len(done):
                db.query(SimulationRun).filter_by(id=run_id).update({"progress_completed":len(done),"progress_total":run.replications})
                db.commit()
            for index in range(1,run.replications+1):
                heartbeat()
                if index in done:continue
                if not renew(db,run_id,token):return "lease_lost"
                summary,sample=simulate_one(run.input_snapshot,run.customers,run.warmup,run.replications,run.seed,index-1)
                if not record_replication(db,run_id,token,index,summary,sample):return "lease_lost"
                done.add(index)
                heartbeat()
            if len(done)!=run.replications:
                release(db,run_id,token,"独立重复未齐全，等待重试")
                return "retry"
            if not publish_complete(db,run_id,token,_build(run),_writer(run)):return "lease_lost"
            return "completed"
        except Exception as e:
            traceback.print_exc()
            # A persistence/aggregation error may be transient.  Give up the lease
            # so the next worker retries the missing replications; only a
            # deterministic computation error (a ValueError) is a real failure.
            if isinstance(e,ValueError):mark_failed(db,run_id,token,str(e));return "failed"
            release(db,run_id,token,f"可恢复错误，等待重试：{e}")
            return "retry"
        finally:
            _active.update(run_id=None,token="")


def main():
    Base.metadata.create_all(engine);migrate_schema();ARTIFACT_ROOT.mkdir(exist_ok=True)
    threading.Thread(target=_keep_alive,daemon=True).start()
    while True:
        heartbeat()
        job=queue.blpop(QUEUE_KEY,timeout=2)
        candidates=set()
        if job:
            try:candidates.add(int(job[1]))
            except (TypeError,ValueError):pass
        with SessionLocal() as db:candidates.update(reclaimable_ids(db))
        for run_id in sorted(candidates):execute(run_id)


if __name__=="__main__":
    main()
