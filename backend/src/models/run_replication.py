from sqlalchemy import ForeignKey,Integer,String,JSON,UniqueConstraint
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now

class RunReplication(Base):
    """One durably persisted independent replication of a simulation run.

    Rows are keyed by ``(run_id, replication)`` so a resumed attempt can skip work
    already on disk.  ``lease_token`` records which takeover holder wrote the row;
    writes from a superseded holder are rejected by the compare-and-set helpers in
    ``run_lease_service`` rather than silently overwriting newer progress.
    """
    __tablename__="run_replications"
    __table_args__=(UniqueConstraint("run_id","replication",name="run_replication_unique"),)
    id:Mapped[int]=mapped_column(primary_key=True)
    run_id:Mapped[int]=mapped_column(ForeignKey("simulation_runs.id",ondelete="CASCADE"))
    replication:Mapped[int]=mapped_column(Integer)
    attempt:Mapped[int]=mapped_column(default=0)
    lease_token:Mapped[str]=mapped_column(String(64),default="")
    summary:Mapped[dict]=mapped_column(JSON)
    sample_customers:Mapped[list]=mapped_column(JSON,default=list)
    created_at:Mapped[str]=mapped_column(default=now)
