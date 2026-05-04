"""
Run Log
Stores history of every pipeline run in the database.
Used by the dashboard and logs page.
"""

from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Session
import os


class Base(DeclarativeBase):
    pass


class RunLog(Base):
    __tablename__ = "_datatap_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String, nullable=False, index=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    records_fetched = Column(Integer, default=0)
    records_written = Column(Integer, default=0)
    success = Column(Boolean, default=False)
    error = Column(Text, nullable=True)


def init_log_db(engine) -> None:
    """Create the run log table if it doesn't exist."""
    Base.metadata.create_all(engine)


def save_run(engine, result) -> None:
    """Persist a RunResult to the run log table."""
    with Session(engine) as session:
        log = RunLog(
            source_name=result.source_name,
            started_at=result.started_at,
            finished_at=result.finished_at,
            records_fetched=result.records_fetched,
            records_written=result.records_written,
            success=result.success,
            error=result.error,
        )
        session.add(log)
        session.commit()


def get_runs(engine, source_name: str | None = None, limit: int = 50) -> list[RunLog]:
    """Fetch recent run logs, optionally filtered by source."""
    with Session(engine) as session:
        q = session.query(RunLog).order_by(RunLog.started_at.desc())
        if source_name:
            q = q.filter(RunLog.source_name == source_name)
        return q.limit(limit).all()


def get_source_stats(engine) -> dict[str, dict]:
    """
    Return per-source summary stats for the dashboard.
    {source_name: {last_run, total_rows, last_success, run_count}}
    """
    with Session(engine) as session:
        runs = session.query(RunLog).order_by(RunLog.started_at.desc()).all()

    stats: dict[str, dict] = {}
    for run in runs:
        name = run.source_name
        if name not in stats:
            stats[name] = {
                "last_run": run.started_at,
                "last_success": run.started_at if run.success else None,  # datetime or None
                "total_written": run.records_written or 0,
                "run_count": 1,
                "last_error": run.error if not run.success else None,
            }
        else:
            stats[name]["run_count"] += 1
            stats[name]["total_written"] += run.records_written or 0
            # Only overwrite last_success if this run succeeded and it's more recent
            if run.success and (stats[name]["last_success"] is None or run.started_at > stats[name]["last_success"]):
                stats[name]["last_success"] = run.started_at

    return stats
