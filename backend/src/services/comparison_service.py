"""Pure result assembly shared by one-shot and resumable execution paths.

``build_result`` is the only place that turns persisted per-replication summaries
into the published result document, so an interrupted-then-resumed run and an
uninterrupted run produce identical statistics, spawn keys and input hash.
"""
from .simulation_algorithm import simulate_replication, aggregate_replicates, _validate_run_parameters
from .erlang_algorithm import erlang_c
from ..utils.fingerprint import fingerprint

METHOD_VERSION="erlangc-fcfs-independent-t-v1"

def input_hash(snapshot,customers,warmup,replications,seed,confidence):
    return fingerprint({"method":METHOD_VERSION,"snapshot":snapshot,"customers":customers,"warmup":warmup,"replications":replications,"seed":seed,"confidence":confidence})

def analytical_result(snapshot):
    s=snapshot['scenario'];p=snapshot['profile']
    return erlang_c(p['arrival_rate'],1/s['mean_service_minutes'],s['servers'])

def build_result(snapshot,customers,warmup,replications,seed,confidence,replication_summaries,sample_customers):
    """Assemble a complete result version; refuses an incomplete replication set."""
    analytic=analytical_result(snapshot)
    result=aggregate_replicates(analytic,replication_summaries,sample_customers,customers,warmup,replications,seed,confidence)
    result['input_snapshot']=snapshot
    result['input_hash']=input_hash(snapshot,customers,warmup,replications,seed,confidence)
    result['method_version']=METHOD_VERSION
    return result

def simulate_one(snapshot,customers,warmup,replications,seed,index):
    """Compute one replication exactly as an uninterrupted run would."""
    _validate_run_parameters(customers,warmup,replications,seed)
    s=snapshot['scenario'];p=snapshot['profile']
    return simulate_replication(p['arrival_rate'],1/s['mean_service_minutes'],s['servers'],customers,warmup,replications,seed,index)

def calculate_run(snapshot,customers,warmup,replications,seed,confidence):
    """One-shot path kept for seeding and non-resumable callers.

    It computes each replication through exactly the same helper and assembles
    the document through exactly the same builder as the resumable worker, so
    both paths are guaranteed to agree.
    """
    _validate_run_parameters(customers,warmup,replications,seed)
    summaries=[];sample=[]
    for index in range(replications):
        summary,rep_sample=simulate_one(snapshot,customers,warmup,replications,seed,index)
        summaries.append(summary)
        if index==0:sample=rep_sample
    return build_result(snapshot,customers,warmup,replications,seed,confidence,summaries,sample)
