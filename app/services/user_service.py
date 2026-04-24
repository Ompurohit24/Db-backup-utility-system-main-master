import asyncio
from datetime import datetime, timezone
from typing import Optional

from appwrite.exception import AppwriteException
from appwrite.query import Query

from app.core.appwrite_client import tables   # ← new TablesDB API
from app.config import DATABASE_ID, USER_COLLECTION_ID
from app.utils.appwrite_normalize import normalize_row, normalize_row_collection


# Unknown-column fallback is only safe for truly optional legacy fields.
_REMOVABLE_UNKNOWN_ATTRIBUTES = {
    "password_hash",
    "phone",
    "bio",
    "created_at",
    "updated_at",
}


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _strip_unknown_attribute(data: dict, exc: Exception) -> dict:
    """Remove unknown attribute key from payload for schema-compat writes."""
    message = str(exc)
    marker = "Unknown attribute:"
    if marker not in message:
        return data

    key = message.split(marker, 1)[1].strip().strip('"\'')
    if key not in _REMOVABLE_UNKNOWN_ATTRIBUTES:
        raise ValueError(
            f"Missing required Appwrite column '{key}' in table '{USER_COLLECTION_ID}'. "
            "Please add this column in Appwrite schema."
        )

    if key in data:
        data = dict(data)
        data.pop(key, None)
    return data


async def _resolve_profile_row_id(user_id: str) -> str:
    """Return actual row id for a profile, supporting legacy rows where $id != user_id."""
    # Preferred path: row id is user id.
    try:
        await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=USER_COLLECTION_ID,
            row_id=user_id,
        )
        return user_id
    except Exception:
        pass

    # Fallback: lookup by user_id column.
    result = await asyncio.to_thread(
        tables.list_rows,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        queries=[Query.equal("user_id", user_id), Query.limit(1)],
    )
    rows = normalize_row_collection(result).get("rows", [])
    if not rows:
        return user_id
    return str(rows[0].get("$id") or user_id)


async def _create_row_with_fallback(row_id: str, data: dict) -> dict:
    payload = dict(data)
    for _ in range(4):
        try:
            response = await asyncio.to_thread(
                tables.create_row,
                database_id=DATABASE_ID,
                table_id=USER_COLLECTION_ID,
                row_id=row_id,
                data=payload,
            )
            return normalize_row(response)
        except AppwriteException as exc:
            reduced = _strip_unknown_attribute(payload, exc)
            if reduced == payload:
                raise
            payload = reduced

    response = await asyncio.to_thread(
        tables.create_row,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        row_id=row_id,
        data=payload,
    )
    return normalize_row(response)


async def _update_row_with_fallback(user_id: str, data: dict) -> dict:
    row_id = await _resolve_profile_row_id(user_id)
    payload = dict(data)
    for _ in range(4):
        try:
            response = await asyncio.to_thread(
                tables.update_row,
                database_id=DATABASE_ID,
                table_id=USER_COLLECTION_ID,
                row_id=row_id,
                data=payload,
            )
            return normalize_row(response)
        except AppwriteException as exc:
            reduced = _strip_unknown_attribute(payload, exc)
            if reduced == payload:
                raise
            payload = reduced

    response = await asyncio.to_thread(
        tables.update_row,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        row_id=row_id,
        data=payload,
    )
    return normalize_row(response)


def _normalized_role(value: str | None) -> str:
    role = str(value or "user").strip().lower()
    return "admin" if role == "admin" else "user"


def _normalized_status(value: str | None) -> str:
    status = str(value or "active").strip().lower()
    return "suspended" if status == "suspended" else "active"


async def create_user_profile(
    user_id: str,
    email: str,
    name: str,
    password_hash: str = "",
    phone: Optional[str] = None,
    bio: Optional[str] = None,
    role: str = "user",
    status: str = "active",
    is_active: bool = True,
) -> dict:
    """Create a new user profile row in the Appwrite database."""
    now = datetime.now(timezone.utc).isoformat()
    normalized_email = normalize_email(email)
    data = {
        "user_id": user_id,
        "email": normalized_email,
        "name": name,
        "password_hash": password_hash,
        "phone": phone or "",
        "bio": bio or "",
        "role": _normalized_role(role),
        "status": _normalized_status(status),
        "is_active": bool(is_active),
        "created_at": now,
        "updated_at": now,
    }
    return await _create_row_with_fallback(
        row_id=user_id,
        data=data,
    )


async def get_user_profile(user_id: str) -> Optional[dict]:
    """Fetch a single user profile by user_id."""
    try:
        row_id = await _resolve_profile_row_id(user_id)
        response = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=USER_COLLECTION_ID,
            row_id=row_id,
        )
        normalized = normalize_row(response)
        return normalized or None
    except Exception:
        return None


async def get_user_profile_by_email(email: str) -> Optional[dict]:
    """Fetch a user profile by email."""
    normalized_email = normalize_email(email)
    result = await asyncio.to_thread(
        tables.list_rows,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        queries=[Query.equal("email", normalized_email), Query.limit(1)],
    )
    rows = normalize_row_collection(result).get("rows", [])
    if rows:
        return rows[0]

    # Compatibility fallback for older rows with mixed-case emails.
    offset = 0
    page_size = 100
    while True:
        page = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=USER_COLLECTION_ID,
            queries=[Query.limit(page_size), Query.offset(offset)],
        )
        page_rows = normalize_row_collection(page).get("rows", [])
        if not page_rows:
            break

        for row in page_rows:
            if normalize_email(str(row.get("email", ""))) == normalized_email:
                return row

        if len(page_rows) < page_size:
            break
        offset += page_size

    return None


async def update_user_profile(
    user_id: str,
    name: Optional[str] = None,
    phone: Optional[str] = None,
    bio: Optional[str] = None,
    role: Optional[str] = None,
    status: Optional[str] = None,
    is_active: Optional[bool] = None,
) -> dict:
    """Update an existing user profile row."""
    data: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if name is not None:
        data["name"] = name
    if phone is not None:
        data["phone"] = phone
    if bio is not None:
        data["bio"] = bio
    if role is not None:
        data["role"] = _normalized_role(role)
    if status is not None:
        data["status"] = _normalized_status(status)
    if is_active is not None:
        data["is_active"] = bool(is_active)

    return await _update_row_with_fallback(user_id=user_id, data=data)


async def delete_user_profile(user_id: str) -> None:
    """Delete a user profile row from the database."""
    row_id = await _resolve_profile_row_id(user_id)
    await asyncio.to_thread(
        tables.delete_row,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        row_id=row_id,
    )


async def list_user_profiles(limit: int = 25, offset: int = 0) -> dict:
    """List user profile rows with pagination."""
    response = await asyncio.to_thread(
        tables.list_rows,
        database_id=DATABASE_ID,
        table_id=USER_COLLECTION_ID,
        queries=[
            Query.limit(limit),
            Query.offset(offset),
        ],
    )
    return normalize_row_collection(response)


async def set_user_role(user_id: str, role: str) -> dict:
    return await update_user_profile(user_id=user_id, role=_normalized_role(role))


async def set_user_status(user_id: str, status: str) -> dict:
    normalized_status = _normalized_status(status)
    return await update_user_profile(
        user_id=user_id,
        status=normalized_status,
        is_active=(normalized_status == "active"),
    )

