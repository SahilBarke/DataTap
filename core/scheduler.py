"""
Scheduler
Registers one background job per config using APScheduler.
Each job runs the full pipeline and saves results to the run log.
"""

from __future__ import annotations
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from core.config_loader import SourceConfig
from core.pipeline import run_pipeline
from core.run_log import save_run
from sqlalchemy.engine import Engine

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


def _make_job(config: SourceConfig, engine: Engine):
    def job():
        print(f"[scheduler] Running job for '{config.name}'")
        result = run_pipeline(config, engine)
        save_run(engine, result)

    return job


def register_source(config: SourceConfig, engine: Engine) -> None:
    """Register or replace a scheduled job for a source config."""
    scheduler = get_scheduler()
    job_id = f"datatap_{config.name}"

    # Remove existing job if present (allows re-registration after config change)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    scheduler.add_job(
        _make_job(config, engine),
        trigger=IntervalTrigger(minutes=config.schedule.interval_mins),
        id=job_id,
        name=config.name,
        replace_existing=True,
    )
    print(
        f"[scheduler] Registered '{config.name}' every {config.schedule.interval_mins} min"
    )


def start_scheduler(configs: list[SourceConfig], engine: Engine) -> None:
    """Register all configs and start the scheduler."""
    scheduler = get_scheduler()
    for config in configs:
        register_source(config, engine)
    if not scheduler.running:
        scheduler.start()
        print(f"[scheduler] Started with {len(configs)} job(s)")


def stop_scheduler() -> None:
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
