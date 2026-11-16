import {useEffect,useState} from 'react';import {getRun,resumeRun} from '../api/runs';import {Run} from '../types/domain';import {RunStatus,RunEvent} from '../constants/enums';import {useAuth} from '../stores/auth';import {time} from '../utils/format';

const liveStatuses=new Set(['queued','running']);

export function RunActivity({runId,live,initial,onResume}:{runId:number;live:boolean;initial?:Run;onResume?:(run:Run)=>void}){
  const {user}=useAuth();
  const [run,setRun]=useState<Run|undefined>(initial),[error,setError]=useState(''),[busy,setBusy]=useState(false);
  useEffect(()=>{
    let cancelled=false;
    async function load(){try{const detail=await getRun(runId);if(!cancelled)setRun(detail)}catch(e){if(!cancelled)setError((e as Error).message)}}
    setRun(undefined);setError('');load();
    if(!live)return()=>{cancelled=true};
    const timer=setInterval(load,2000);
    return()=>{cancelled=true;clearInterval(timer)};
  },[runId,live]);
  if(!run)return error?<p className="notice">{error}</p>:null;
  const completed=run.progress?.completed??run.progress_completed??0,total=run.progress?.total||run.replications||1;
  const events=run.events||[],takeovers=events.filter(e=>e.event==='taken_over').length;
  const expired=(run.lease?.expires_at||0)*1000<Date.now()||!run.lease?.owner;
  const canResume=(user?.role==='admin'||user?.role==='analyst')&&(run.status==='failed'||(run.status==='running'&&expired)||run.status==='queued');
  const canForce=user?.role==='admin'&&run.status==='running'&&!expired;
  async function resume(force=false){
    setBusy(true);setError('');
    try{const next=await resumeRun(runId,force);setRun(next);onResume?.(next)}catch(e){setError((e as Error).message)}finally{setBusy(false)}
  }
  return <div className="run-activity">
    <div className="progress-head"><span className="eyebrow">EXECUTION PROGRESS / 接管经过</span><span className="mono">{completed} / {total} 独立重复</span></div>
    <div className="progress-track" role="progressbar" aria-valuemin={0} aria-valuemax={total} aria-valuenow={completed} aria-label="独立重复完成进度"><i style={{width:Math.min(100,Math.round(completed/total*100))+'%'}}/></div>
    <div className="progress-meta">
      <span>第 {run.attempt||1} 次执行 · {RunStatus[run.status as keyof typeof RunStatus]||run.status}{run.lease?.owner?` · 接管者 ${run.lease.owner}`:''}</span>
      {run.status==='running'&&expired&&<b className="warning">接管者失联，系统将自动接管续算</b>}
      {run.status==='completed'&&run.completion?.complete&&<b className="safe">完成版本 {run.result_version?run.result_version.slice(0,16):'旧版结果'}</b>}
      {run.status==='completed'&&run.completion&&!run.completion.complete&&<b className="warning">{run.completion.reason}</b>}
      {run.status==='failed'&&<b className="warning">已保存 {completed} 个重复，续算不会重算</b>}
    </div>
    {takeovers>0&&<p className="caption">已自动接管 {takeovers} 次；已落盘重复不重算，随机子流与最终统计与不中断运行一致。</p>}
    {liveStatuses.has(run.status)&&!expired&&<p className="caption">本页每 2 秒刷新进度；中断后新接管者从已落盘重复继续。</p>}
    {error&&<p className="notice">{error}</p>}
    {canResume&&<div className="reclaim-bar"><span>已保存的独立重复会保留，只补算缺失部分。</span>{run.status!=='queued'&&<button className="outline" disabled={busy} onClick={()=>resume()}>{busy?'提交中…':'立即接管续算'}</button>}</div>}
    {canForce&&<div className="reclaim-bar"><span>若接管者已失联但租约未到期，可强制接管；旧执行者的迟到进度会被拒绝。</span><button className="outline" disabled={busy} onClick={()=>resume(true)}>{busy?'提交中…':'强制接管续算'}</button></div>}
    {events.length>0&&<ol className="takeover-log">{events.slice(-8).map(e=><li key={e.id}><span className="mono">{time(e.created_at)}</span><b>{RunEvent[e.event as keyof typeof RunEvent]||e.event}</b>{e.detail&&<code>{e.detail}</code>}<small>第{e.attempt}次 · {e.actor}</small></li>)}</ol>}
  </div>
}
