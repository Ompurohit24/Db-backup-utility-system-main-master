"""Lightweight APScheduler wrapper for scheduled backups.

The scheduler is started during FastAPI lifespan startup and used by the
schedule service to add/update/remove jobs without duplicating boilerplate.
"""

from __future__ import annotations

from typing import Any, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.logging_setup import setup_logging
from app.logger import get_logger


scheduler = AsyncIOScheduler()
log = get_logger("scheduler")


def _start_if_needed() -> None:
    if not scheduler.running:
        scheduler.start()
        log.info("Scheduler started")


async def scheduler_startup() -> None:
    setup_logging()
    _start_if_needed()


async def scheduler_shutdown() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")


def upsert_job(
    job_id: str,
    trigger: CronTrigger,
    func: Callable[..., Any],
    *,
    kwargs: dict | None = None,
) -> None:
    _start_if_needed()
    scheduler.add_job(
        func,
        trigger=trigger,
        id=job_id,
        replace_existing=True,
        kwargs=kwargs or {},
    )
    log.info("Scheduled job %s with cron=%s", job_id, trigger)


def remove_job(job_id: str) -> None:
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        log.info("Removed scheduled job %s", job_id)


def get_next_run(job_id: str):
    job = scheduler.get_job(job_id)
    return job.next_run_time if job else None

