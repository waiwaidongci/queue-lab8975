from sqlalchemy import ForeignKey,Index,JSON,text
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now

class SimulationRun(Base):
    __tablename__="simulation_runs"
    # At most one queued/running copy of an identical input; partial index so
    # completed archives with the same input are still allowed, and rows without
    # a fingerprint (legacy/manual) are never constrained.
    __table_args__=(Index(
        "run_active_input_unique","input_hash_fingerprint",unique=True,
        postgresql_where=text("input_hash_fingerprint<>'' AND status IN ('queued','running')"),
        sqlite_where=text("input_hash_fingerprint<>'' AND status IN ('queued','running')"),
    ),)
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    scenario_id:Mapped[int]=mapped_column(ForeignKey("service_scenarios.id"))
    customers:Mapped[int]
    warmup:Mapped[int]
    replications:Mapped[int]
    seed:Mapped[int]
    confidence:Mapped[float]
    input_snapshot:Mapped[dict]=mapped_column(JSON)
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(default="")
    owner:Mapped[str]
    created_at:Mapped[str]=mapped_column(default=now)
    # Resumable execution: claims are fenced by lease_token so only the current
    # takeover holder may persist progress or publish a completed result.
    attempt:Mapped[int]=mapped_column(default=0)
    lease_owner:Mapped[str]=mapped_column(default="")
    lease_token:Mapped[str]=mapped_column(default="")
    lease_expires_at:Mapped[float]=mapped_column(default=0.)
    progress_completed:Mapped[int]=mapped_column(default=0)
    progress_total:Mapped[int]=mapped_column(default=0)
    progress_updated_at:Mapped[str]=mapped_column(default="")
    started_at:Mapped[str]=mapped_column(default="")
    updated_at:Mapped[str]=mapped_column(default="")
    completed_at:Mapped[str]=mapped_column(default="")
    result_version:Mapped[str]=mapped_column(default="")
    artifact_path:Mapped[str]=mapped_column(default="")
    input_hash_fingerprint:Mapped[str]=mapped_column(default="")
