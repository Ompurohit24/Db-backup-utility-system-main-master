import asyncio
import uuid
from typing import Optional

from appwrite.exception import AppwriteException
from appwrite.query import Query

from app.config import DATABASE_ID, NOTIFICATIONS_COLLECTION_ID
from app.core.appwrite_client import tables
from app.utils.appwrite_normalize import normalize_row, normalize_row_collection


def _enabled() -> bool:
    return bool(DATABASE_ID and NOTIFICATIONS_COLLECTION_ID)


async def create_notification(
    user_id: str,
    event_type: str,
    title: str,
    message: str,
    level: str = "info",
    resource_id: str = "",
) -> Optional[dict]:
    """Persist a user notification. Best effort: returns None when disabled or on failure."""
    if not _enabled() or not user_id:
        return None

    notification_id = uuid.uuid4().hex

    data = {
        "notification_id": notification_id,
        "user_id": str(user_id),
        "event_type": str(event_type or "event")[:100],
        "level": str(level or "info")[:20],
        "title": str(title or "Notification")[:255],
        "message": str(message or "")[:2048],
        "is_read": False,
    }

    try:
        row = await asyncio.to_thread(
            tables.create_row,
            database_id=DATABASE_ID,
            table_id=NOTIFICATIONS_COLLECTION_ID,
            row_id=notification_id,
            data=data,
        )
        return normalize_row(row)
    except AppwriteException as exc:
        message = str(exc)

        # Some deployments use a reduced schema (no level column).
        if ('Unknown attribute: "level"' in message) or ('Unknown attribute: level' in message):
            fallback_data = {
                "notification_id": notification_id,
                "user_id": str(user_id),
                "event_type": str(event_type or "event")[:100],
                "title": str(title or "Notification")[:255],
                "message": str(message or "")[:2048],
                "is_read": False,
            }
            try:
                row = await asyncio.to_thread(
                    tables.create_row,
                    database_id=DATABASE_ID,
                    table_id=NOTIFICATIONS_COLLECTION_ID,
                    row_id=notification_id,
                    data=fallback_data,
                )
                return normalize_row(row)
            except Exception:
                return None
        return None
    except Exception:
        return None


async def list_notifications(
    user_id: str,
    user_email: str = "",
    limit: int = 25,
    offset: int = 0,
    unread_only: bool = False,
) -> dict:
    if not _enabled():
        return {"rows": [], "total": 0}

    user_id_norm = str(user_id or "").strip()
    user_email_norm = str(user_email or "").strip().lower()
    owner_values = [v for v in [user_id_norm, user_email_norm] if v]

    def _to_scalar(value) -> str:
        if value is None:
            return ""
        if isinstance(value, dict):
            for key in ("$id", "id", "user_id", "userId", "email"):
                nested = value.get(key)
                if nested is not None and str(nested).strip():
                    return str(nested).strip()
            return ""
        return str(value).strip()

    def _row_user_values(row: dict) -> list[str]:
        values: list[str] = []

        for key in ("user_id", "owner_user_id", "userId", "created_by", "createdBy", "email"):
            scalar = _to_scalar(row.get(key))
            if scalar:
                values.append(scalar)

        # Heuristic fallback for schema variants.
        for key, value in row.items():
            key_norm = str(key).strip().lower()
            if "user" in key_norm or key_norm.endswith("email"):
                scalar = _to_scalar(value)
                if scalar:
                    values.append(scalar)

        return list(dict.fromkeys(values))

    def _is_owner_match(row: dict) -> bool:
        row_values = _row_user_values(row)
        for value in row_values:
            if value == user_id_norm:
                return True
            if user_email_norm and value.lower() == user_email_norm:
                return True
        return False

    async def _list_rows_with_order_fallback(base_queries: list):
        """Prefer ordered results; retry without order if Appwrite rejects it."""
        try:
            return await asyncio.to_thread(
                tables.list_rows,
                database_id=DATABASE_ID,
                table_id=NOTIFICATIONS_COLLECTION_ID,
                queries=[*base_queries, Query.order_desc("$createdAt")],
            )
        except AppwriteException as exc:
            if "Invalid query" not in str(exc):
                raise
            return await asyncio.to_thread(
                tables.list_rows,
                database_id=DATABASE_ID,
                table_id=NOTIFICATIONS_COLLECTION_ID,
                queries=base_queries,
            )

    async def _try_scoped_query(owner_attr: str) -> Optional[dict]:
        for owner_value in owner_values:
            scoped_queries = [Query.equal(owner_attr, owner_value), Query.limit(limit), Query.offset(offset)]
            if unread_only:
                scoped_queries.append(Query.equal("is_read", False))

            try:
                response = await _list_rows_with_order_fallback(scoped_queries)
                collection = normalize_row_collection(response)
                rows = [row for row in collection.get("rows", []) if _is_owner_match(row)]
                if rows:
                    rows.sort(
                        key=lambda row: str(row.get("$createdAt") or row.get("created_at") or ""),
                        reverse=True,
                    )
                    return {"rows": rows, "total": len(rows)}
            except AppwriteException as exc:
                if f"Attribute not found in schema: {owner_attr}" in str(exc):
                    return None
            except Exception:
                continue

        return None

    # Preferred fast path: server-side filtered queries.
    for owner_attr in ("user_id", "owner_user_id", "userId", "email"):
        scoped = await _try_scoped_query(owner_attr)
        if scoped and (scoped.get("rows") or scoped.get("total", 0) > 0):
            return scoped

    # Fallback path: scan recent rows and filter in memory (safe, no cross-user leak).
    matched_rows: list[dict] = []
    scan_offset = 0
    page_size = 100
    max_pages = 10

    for _ in range(max_pages):
        queries = [Query.limit(page_size), Query.offset(scan_offset)]
        try:
            response = await _list_rows_with_order_fallback(queries)
        except Exception:
            break

        collection = normalize_row_collection(response)
        page_rows = collection.get("rows", [])
        if not page_rows:
            break

        for row in page_rows:
            if not _is_owner_match(row):
                continue
            if unread_only and bool(row.get("is_read", False)):
                continue
            matched_rows.append(row)

        if len(page_rows) < page_size:
            break
        scan_offset += page_size

    total = len(matched_rows)
    matched_rows.sort(
        key=lambda row: str(row.get("$createdAt") or row.get("created_at") or ""),
        reverse=True,
    )
    start = max(0, int(offset))
    end = start + max(0, int(limit))
    return {"rows": matched_rows[start:end], "total": total}


async def get_notification(notification_id: str) -> Optional[dict]:
    if not _enabled():
        return None

    try:
        row = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=NOTIFICATIONS_COLLECTION_ID,
            row_id=notification_id,
        )
        return normalize_row(row)
    except Exception:
        return None


async def mark_notification_as_read(notification_id: str) -> bool:
    if not _enabled():
        return False

    try:
        await asyncio.to_thread(
            tables.update_row,
            database_id=DATABASE_ID,
            table_id=NOTIFICATIONS_COLLECTION_ID,
            row_id=notification_id,
            data={"is_read": True},
        )
        return True
    except Exception:
        return False


async def mark_all_notifications_as_read(user_id: str, user_email: str = "") -> int:
    if not _enabled():
        return 0

    updated = 0
    offset = 0
    page_size = 100

    while True:
        page = await list_notifications(
            user_id=user_id,
            user_email=user_email,
            limit=page_size,
            offset=offset,
            unread_only=True,
        )
        rows = page.get("rows", [])
        if not rows:
            break

        for row in rows:
            if await mark_notification_as_read(str(row.get("$id", ""))):
                updated += 1

        if len(rows) < page_size:
            break
        offset += page_size

    return updated

