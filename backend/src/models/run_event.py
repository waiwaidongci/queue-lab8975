from sqlalchemy import ForeignKey,Integer,String
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now

class RunEvent(Base):
    """Takeover / progress history for a simulation run (append-only)."""
    __tablename__="run_events"
    id:Mapped[int]=mapped_column(primary_key=True)
    run_id:Mapped[int]=mapped_column(ForeignKey("simulation_runs.id",ondelete="CASCADE"))
    attempt:Mapped[int]=mapped_column(default=0)
    event:Mapped[str]=mapped_column(String(40))
    actor:Mapped[str]=mapped_column(default="")
    detail:Mapped[str]=mapped_column(default="")
    created_at:Mapped[str]=mapped_column(default=now)
