"""Service layer for scheduled backups using APScheduler and Appwrite Tables."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger
from appwrite.query import Query
from appwrite.exception import AppwriteException

from app.config import (
    DATABASE_ID,
    BACKUP_SCHEDULES_COLLECTION_ID,
    DEFAULT_TIMEZONE,
)
from app.core.appwrite_client import tables
from app.logger import get_logger
from app.services import backup_service
from app.services import notification_service
from app.services.database_service import get_user_database
from app.utils.dependencies import _is_admin_user
from app.utils.appwrite_normalize import normalize_row, normalize_row_collection
from app.utils.scheduler import get_next_run, remove_job, scheduler_startup, upsert_job


log = get_logger("scheduler")


async def _notify_schedule_user(
    user_id: str,
    event_type: str,
    level: str,
    title: str,
    message: str,
) -> None:
    """Best-effort schedule notification helper."""
    try:
        await notification_service.create_notification(
            user_id=user_id,
            event_type=event_type,
            level=level,
            title=title,
            message=message,
        )
    except Exception:
        pass


def _parse_weekday_token(token: str) -> int | None:
    token = str(token or "").strip().lower()
    names = {
        "mon": 0,
        "tue": 1,
        "wed": 2,
        "thu": 3,
        "fri": 4,
        "sat": 5,
        "sun": 6,
    }
    if token in names:
        return names[token]
    try:
        # Cron often uses 0/7=Sunday, 1=Monday ... 6=Saturday.
        value = int(token)
        if value in (0, 7):
            return 6
        if 1 <= value <= 6:
            return value - 1
    except Exception:
        return None
    return None


def _derive_schedule_status(doc: dict, next_run_time: datetime | None) -> str:
    if not bool(doc.get("enabled", False)):
        return "Completed"

    tz_name = _normalize_timezone(doc.get("timezone"))
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)

    frequency = str(doc.get("frequency") or "").lower()
    cron_parts = str(doc.get("cron_expression") or "").split()

    # For daily/weekly schedules, evaluate this cycle's configured time directly.
    if frequency in {"daily", "weekly"} and len(cron_parts) == 5:
        try:
            minute = int(cron_parts[0])
            hour = int(cron_parts[1])
        except Exception:
            minute = hour = None

        if minute is not None and hour is not None:
            if frequency == "daily":
                cycle_time = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return "Active" if now_local < cycle_time else "Completed"

            weekday = _parse_weekday_token(cron_parts[4])
            if weekday is not None:
                current_weekday = now_local.weekday()
                if current_weekday < weekday:
                    return "Active"
                if current_weekday > weekday:
                    return "Completed"
                cycle_time = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
                return "Active" if now_local < cycle_time else "Completed"

    now_utc = datetime.now(timezone.utc)
    if next_run_time is not None:
        if next_run_time.tzinfo is None:
            next_run_time = next_run_time.replace(tzinfo=timezone.utc)
        return "Active" if next_run_time > now_utc else "Completed"
    return "Completed"


def _build_cron_expression(
    *, frequency: str, time_str: Optional[str], day_of_week: Optional[str], cron_expression: Optional[str]
) -> str:
    if frequency == "daily":
        hour, minute = map(int, (time_str or "00:00").split(":"))
        return f"{minute} {hour} * * *"
    if frequency == "weekly":
        hour, minute = map(int, (time_str or "00:00").split(":"))
        dow = (day_of_week or "sun").lower()
        return f"{minute} {hour} * * {dow}"
    if frequency == "cron" and cron_expression:
        return cron_expression
    raise ValueError("Invalid schedule parameters")


def _normalize_timezone(tz_str: Optional[str]) -> str:
    tz = tz_str or DEFAULT_TIMEZONE
    try:
        ZoneInfo(tz)
        return tz
    except Exception:
        log.warning("Invalid timezone '%s', falling back to %s", tz, DEFAULT_TIMEZONE)
        return DEFAULT_TIMEZONE


def _cron_trigger(expression: str, tz: Optional[str]) -> CronTrigger:
    safe_tz = _normalize_timezone(tz)
    return CronTrigger.from_crontab(expression, timezone=ZoneInfo(safe_tz))


async def _run_scheduled_backup(schedule_row: dict) -> None:
    try:
        result = await backup_service.trigger_backup(
            db_config_id=schedule_row["db_config_id"],
            user_id=schedule_row["user_id"],
            role="system",
            ip_address=None,
            device_info="scheduler",
        )
        if result.get("_success", True):
            log.info("Scheduled backup finished schedule_id=%s", schedule_row.get("$id"))
            return

        await _notify_schedule_user(
            user_id=str(schedule_row.get("user_id") or ""),
            event_type="scheduled_backup_failed",
            level="error",
            title="Scheduled Backup Failed",
            message=(
                f"Scheduled backup failed for schedule '{schedule_row.get('$id', '')}': "
                f"{result.get('_result_message', 'Unknown error')}"
            ),
        )
        log.error(
            "Scheduled backup reported failure schedule_id=%s message=%s",
            schedule_row.get("$id"),
            result.get("_result_message", "Unknown error"),
        )
    except Exception as exc:  # pragma: no cover - defensive
        await _notify_schedule_user(
            user_id=str(schedule_row.get("user_id") or ""),
            event_type="scheduled_backup_failed",
            level="error",
            title="Scheduled Backup Failed",
            message=(
                f"Scheduled backup failed for schedule '{schedule_row.get('$id', '')}': {exc}"
            ),
        )
        log.error(
            "Scheduled backup failed schedule_id=%s db_config_id=%s error=%s",
            schedule_row.get("$id"),
            schedule_row.get("db_config_id"),
            exc,
        )


async def create_schedule(
    *,
    user_id: str,
    frequency: str,
    db_config_id: str,
    time_str: Optional[str],
    day_of_week: Optional[str],
    cron_expression: Optional[str],
    timezone_str: Optional[str],
    enabled: bool,
    description: Optional[str],
) -> dict:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        raise RuntimeError("BACKUP_SCHEDULES_COLLECTION_ID is not configured")

    # Ensure the DB config belongs to the user.
    db_doc = await get_user_database(db_config_id)
    if not db_doc or db_doc.get("user_id") != user_id:
        raise PermissionError("Database configuration not found for this user")

    cron_expr = _build_cron_expression(
        frequency=frequency,
        time_str=time_str,
        day_of_week=day_of_week,
        cron_expression=cron_expression,
    )
    tz = _normalize_timezone(timezone_str or DEFAULT_TIMEZONE)

    row = await asyncio.to_thread(
        tables.create_row,
        database_id=DATABASE_ID,
        table_id=BACKUP_SCHEDULES_COLLECTION_ID,
        row_id="unique()",
        data={
            "user_id": user_id,
            "db_config_id": db_config_id,
            "frequency": frequency,
            "cron_expression": cron_expr,
            "timezone": tz,
            "enabled": enabled,
            "description": description or "",
        },
    )

    schedule_row = normalize_row(row)
    
    # Fetch the complete row again to ensure we get Appwrite-generated created_at and updated_at
    created_schedule_id = schedule_row.get("$id")
    if created_schedule_id:
        try:
            fetched_row = await asyncio.to_thread(
                tables.get_row,
                database_id=DATABASE_ID,
                table_id=BACKUP_SCHEDULES_COLLECTION_ID,
                row_id=created_schedule_id,
            )
            schedule_row = normalize_row(fetched_row)
        except Exception as exc:
            log.warning("Failed to fetch created schedule for timestamps: %s", exc)
    
    if enabled:
        _register_job(schedule_row)

    await _notify_schedule_user(
        user_id=user_id,
        event_type="schedule_created",
        level="success",
        title="Schedule Created",
        message=(
            f"Backup schedule created ({frequency}, {cron_expr}, {tz})"
            + (" and enabled." if enabled else " and disabled.")
        ),
    )
    return _to_schedule_out(schedule_row)


async def list_schedules(user_id: str) -> list[dict]:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        return []

    try:
        result = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            queries=[Query.equal("user_id", user_id)],
        )
    except AppwriteException as exc:
        if "Attribute not found in schema: user_id" not in str(exc):
            raise
        result = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            queries=[Query.equal("owner_user_id", user_id)],
        )
    # Query.order_desc("createdAt")
    collection = normalize_row_collection(result)
    return [_to_schedule_out(row) for row in collection.get("rows", [])]


async def list_admin_schedules(*, limit: int = 50, offset: int = 0) -> list[dict]:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        return []

    result = await asyncio.to_thread(
        tables.list_rows,
        database_id=DATABASE_ID,
        table_id=BACKUP_SCHEDULES_COLLECTION_ID,
        queries=[Query.limit(limit), Query.offset(offset)],
    )
    collection = normalize_row_collection(result)
    rows = collection.get("rows", [])
    if not rows:
        return []

    user_ids = {str(row.get("user_id") or "") for row in rows if row.get("user_id")}
    admin_map = {
        user_id: is_admin
        for user_id, is_admin in zip(
            user_ids,
            await asyncio.gather(*[_is_admin_user(user_id) for user_id in user_ids]),
        )
    }

    return [
        _to_schedule_out(row)
        for row in rows
        if admin_map.get(str(row.get("user_id") or ""), False)
    ]


async def delete_schedule(schedule_id: str, user_id: str) -> None:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        return

    try:
        row = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            row_id=schedule_id,
        )
        doc = normalize_row(row)
        if doc.get("user_id") != user_id:
            raise PermissionError("Not allowed")
    except Exception:
        # Swallow if not found/forbidden to mirror idempotent delete.
        return

    await asyncio.to_thread(
        tables.delete_row,
        database_id=DATABASE_ID,
        table_id=BACKUP_SCHEDULES_COLLECTION_ID,
        row_id=schedule_id,
    )
    remove_job(schedule_id)

    await _notify_schedule_user(
        user_id=user_id,
        event_type="schedule_deleted",
        level="warning",
        title="Schedule Deleted",
        message=f"Backup schedule '{schedule_id}' deleted successfully.",
    )


async def delete_schedule_admin(schedule_id: str) -> None:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        return

    try:
        await asyncio.to_thread(
            tables.delete_row,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            row_id=schedule_id,
        )
    except Exception:
        # Swallow if not found/forbidden to mirror idempotent delete.
        return

    remove_job(schedule_id)


async def toggle_schedule(schedule_id: str, user_id: str, enabled: bool) -> dict:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        raise RuntimeError("BACKUP_SCHEDULES_COLLECTION_ID is not configured")

    row = await asyncio.to_thread(
        tables.get_row,
        database_id=DATABASE_ID,
        table_id=BACKUP_SCHEDULES_COLLECTION_ID,
        row_id=schedule_id,
    )
    doc = normalize_row(row)
    if doc.get("user_id") != user_id:
        raise PermissionError("Not allowed")

    updated = await asyncio.to_thread(
        tables.update_row,
        database_id=DATABASE_ID,
        table_id=BACKUP_SCHEDULES_COLLECTION_ID,
        row_id=schedule_id,
        data={"enabled": enabled},
    )
    schedule_row = normalize_row(updated)
    
    # Fetch the complete row again to ensure we get Appwrite-generated created_at and updated_at
    try:
        fetched_row = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            row_id=schedule_id,
        )
        schedule_row = normalize_row(fetched_row)
    except Exception as exc:
        log.warning("Failed to fetch updated schedule for timestamps: %s", exc)
    
    if enabled:
        _register_job(schedule_row)
    else:
        remove_job(schedule_id)

    await _notify_schedule_user(
        user_id=user_id,
        event_type="schedule_updated",
        level="info",
        title="Schedule Updated",
        message=(
            f"Backup schedule '{schedule_id}' "
            + ("enabled." if enabled else "disabled.")
        ),
    )
    return _to_schedule_out(schedule_row)


async def load_active_schedules() -> None:
    if not BACKUP_SCHEDULES_COLLECTION_ID:
        log.warning("Skipping scheduler bootstrap; BACKUP_SCHEDULES_COLLECTION_ID missing")
        return

    await scheduler_startup()
    try:
        result = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUP_SCHEDULES_COLLECTION_ID,
            queries=[Query.equal("enabled", True), Query.limit(200)],
        )
    except AppwriteException as exc:
        # If the collection lacks the 'enabled' attribute or schema differs, skip bootstrapping.
        log.warning("Skipping schedule bootstrap due to schema/query issue: %s", exc)
        return

    collection = normalize_row_collection(result)
    for row in collection.get("rows", []):
        try:
            _register_job(row)
        except Exception as exc:  # pragma: no cover - bootstrap resilience
            log.error(
                "Failed to register schedule_id=%s db_config_id=%s error=%s",
                row.get("$id"),
                row.get("db_config_id"),
                exc,
            )


def _register_job(schedule_row: dict) -> None:
    if not schedule_row.get("enabled"):
        return

    trigger = _cron_trigger(
        schedule_row.get("cron_expression", "0 2 * * *"),
        schedule_row.get("timezone") or DEFAULT_TIMEZONE,
    )

    upsert_job(
        schedule_row["$id"],
        trigger,
        _run_scheduled_backup,
        kwargs={"schedule_row": schedule_row},
    )


def _to_schedule_out(doc: dict) -> dict:
    next_run_time = get_next_run(doc.get("$id"))
    status = _derive_schedule_status(doc, next_run_time)

    if next_run_time is not None and next_run_time.tzinfo is None:
        # Normalize naive datetimes for consistent API serialization.
        next_run_time = next_run_time.replace(tzinfo=timezone.utc)

    return {
        "schedule_id": doc.get("$id", ""),
        "user_id": doc.get("user_id", ""),
        "db_config_id": doc.get("db_config_id", ""),
        "frequency": doc.get("frequency", ""),
        "cron_expression": doc.get("cron_expression", ""),
        "timezone": _normalize_timezone(doc.get("timezone")),
        "enabled": bool(doc.get("enabled", False)),
        "status": status,
        "description": doc.get("description") or None,
        "next_run_time": next_run_time,
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }


