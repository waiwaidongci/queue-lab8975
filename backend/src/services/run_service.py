import csv,io
from fastapi import HTTPException
from redis import Redis
from sqlalchemy.exc import IntegrityError
from ..config.settings import REDIS_URL
from ..models import SimulationRun,ServiceScenario,ArrivalProfile,QueueStudy,CapacityNote
from ..utils.serialize import record
from .erlang_algorithm import erlang_c
from .audit_service import log
from .run_lease_service import run_events,requeue
from .comparison_service import input_hash
from ..utils.clock import now_ts
from .artifact_service import result_version,artifact_path,verify_artifact,write_artifact
from ..config.settings import ARTIFACT_DIR

queue=Redis.from_url(REDIS_URL,decode_responses=True)
PRIVATE_FIELDS={"lease_token"}


def public_record(row):
    """Expose run state without internal fencing secrets."""
    return {k:v for k,v in record(row).items() if k not in PRIVATE_FIELDS}


def list_runs(db):return [public_record(r) for r in db.query(SimulationRun).order_by(SimulationRun.id.desc())]


def get_run(db,rid):
    row=db.get(SimulationRun,rid)
    if not row:raise HTTPException(404,"运行不存在")
    return row


def completion_state(row):
    """Describe whether a completed run is a complete, verifiable result version.

    Runs completed before resumable execution have a result but no version hash;
    they stay viewable and exportable.  A run completed by the resumable worker
    carries ``result_version`` and must have all its replications persisted and
    its artifact matching that version.
    """
    if row.status!="completed":return {"complete":False,"version":row.result_version or "","artifact_ok":False,"reason":"运行尚未完成"}
    version=row.result_version or ""
    if not version:
        return {"complete":True,"version":"","artifact_ok":True,"reason":"旧版完成运行，按原结果文件导出"}
    replications=(row.result or {}).get("replications") or []
    complete=len(replications)==row.replications and sorted(x.get("replication") for x in replications)==list(range(1,row.replications+1))
    path=row.artifact_path or str(artifact_path(row.id,version,ARTIFACT_DIR))
    artifact_ok=verify_artifact(path,version)
    reason="结果版本完整" if complete and artifact_ok else "完成结果缺少重复或结果文件不一致"
    return {"complete":complete and artifact_ok,"version":version,"artifact_ok":artifact_ok,"reason":reason,"artifact_path":path}


def run_detail(db,row,user=None):
    data=public_record(row)
    state=completion_state(row)
    data["completion"]=state
    completed=row.progress_completed
    if not completed and row.status=="completed":
        completed=len((row.result or {}).get("replications") or [])
    data["progress"]={
        "completed":completed or 0,
        "total":row.progress_total or row.replications,
        "updated_at":row.progress_updated_at or "",
    }
    data["lease"]={"owner":row.lease_owner or "","expires_at":row.lease_expires_at or 0.,"attempt":row.attempt or 0}
    data["events"]=[record(e) for e in run_events(db,row.id)]
    return data


def create_snapshot(db,scenario):
    profile=db.get(ArrivalProfile,scenario.profile_id);study=db.get(QueueStudy,profile.study_id)
    return {"study":record(study),"profile":record(profile),"scenario":record(scenario),"time_unit":"minutes","rate_unit":"requests/minute"}


def create_run(db,data,user):
    scenario=db.get(ServiceScenario,data.scenario_id)
    if not scenario:raise HTTPException(404,"服务情景不存在")
    snapshot=create_snapshot(db,scenario)
    try:erlang_c(snapshot['profile']['arrival_rate'],1/scenario.mean_service_minutes,scenario.servers)
    except ValueError as e:raise HTTPException(422,str(e))
    if data.customers-data.warmup<100 or data.customers*data.replications>250000:raise HTTPException(422,"至少保留100顾客，且单任务总顾客不超过250000")
    fingerprint_hash=input_hash(snapshot,data.customers,data.warmup,data.replications,data.seed,data.confidence)
    existing=(
        db.query(SimulationRun)
        .filter(SimulationRun.status.in_(("queued","running","failed")),SimulationRun.input_hash_fingerprint==fingerprint_hash)
        .order_by(SimulationRun.id.desc())
        .first()
    )
    if existing:
        # Absorb accidental duplicate submissions of an interrupted or queued
        # experiment instead of queueing a wasteful second copy.  The existing
        # run keeps its persisted replications and can be taken over.
        if existing.status=="failed":
            requeue(db,existing.id,user.email)
            try:queue.rpush("queue:jobs",existing.id)
            except Exception:pass
        data=public_record(existing);data["deduplicated"]=True
        return data
    row=SimulationRun(**data.model_dump(),input_snapshot=snapshot,owner=user.name,progress_total=data.replications,input_hash_fingerprint=fingerprint_hash)
    db.add(row)
    try:db.flush()
    except IntegrityError:
        # A concurrent identical submission won the unique active-run guard;
        # reuse it instead of queueing a duplicate computation.
        db.rollback()
        existing=db.query(SimulationRun).filter(SimulationRun.input_hash_fingerprint==fingerprint_hash,SimulationRun.status.in_(("queued","running"))).order_by(SimulationRun.id.desc()).first()
        if existing:
            data=public_record(existing);data["deduplicated"]=True
            return data
        raise HTTPException(409,"相同参数的试验正在提交，请刷新后查看")
    log(db,user.email,"simulation.enqueue",row.id);db.commit()
    try:queue.rpush("queue:jobs",row.id)
    except Exception:
        row.status="failed";row.error="队列不可用，请稍后点击接管续算";db.commit();raise HTTPException(503,row.error)
    return public_record(row)


def resume_run(db,rid,user,force=False):
    """Explicitly re-enqueue an interrupted or failed run for takeover.

    Resubmitting is never needed: persisted replications stay and only missing
    ones are computed.  A live lease holder is respected; if one is current the
    request is refused instead of creating a second executor.  An admin may force
    a takeover of a hung holder; the old holder's token is revoked, so its late
    progress and results are fenced out.
    """
    row=get_run(db,rid)
    if row.status=="completed":raise HTTPException(409,"运行已完成，无需续算")
    if force:
        if user.role!="admin":raise HTTPException(403,"只有运营研究负责人可以强制接管")
    elif row.status=="running" and (row.lease_expires_at or 0.)>now_ts() and row.lease_owner:
        raise HTTPException(409,"运行仍由当前接管者执行，不能重复接管")
    if not requeue(db,rid,user.email,force=force):raise HTTPException(409,"运行状态已变化，请刷新后重试")
    log(db,user.email,"simulation.resume" if not force else "simulation.force_resume",rid);db.commit()
    try:queue.rpush("queue:jobs",rid)
    except Exception:pass
    return public_record(get_run(db,rid))


def export_run(db,rid):
    row=get_run(db,rid)
    if row.status!="completed" or not db.query(CapacityNote).filter_by(run_id=rid,status="approved").first():raise HTTPException(403,"仅获批的完成运行可导出")
    state=completion_state(row)
    if not state["complete"]:raise HTTPException(409,"该运行的结果版本不完整，不能导出："+state["reason"])
    if state["version"]:
        expected=result_version(row.result)
        if expected!=state["version"]:raise HTTPException(409,"接口结果与结果版本哈希不一致，不能导出")
    out=io.StringIO();w=csv.writer(out);w.writerow(['replication','mean_wait_minutes','mean_system_minutes','wait_probability','busy_fraction_window','window_minutes','retained_customers','arrival_spawn_key','service_spawn_key','analytic_wait_minutes','input_hash'])
    for x in row.result['replications']:w.writerow([x[k] for k in ['replication','mean_wait_minutes','mean_system_minutes','wait_probability','busy_fraction_window','window_minutes','retained_customers','arrival_spawn_key','service_spawn_key']]+[row.result['analytical']['mean_wait_minutes'],row.result['input_hash']])
    return out.getvalue()
