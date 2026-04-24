"""
Service layer: test external DB connections and persist configs in Appwrite.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from cryptography.fernet import InvalidToken
from appwrite.exception import AppwriteException
from appwrite.query import Query

from app.core.appwrite_client import tables   # ← new TablesDB API
from app.config import DATABASE_ID, USER_DATABASES_COLLECTION_ID
from app.utils.appwrite_normalize import normalize_row, normalize_row_collection
from app.utils.db_connector import test_connection, ConnectionTestResult
from app.utils.encryption import encrypt, decrypt


# ── Test connection ──────────────────────────────────────────────────

async def test_user_database(
    database_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
) -> ConnectionTestResult:
    """
    Attempt a real connection to the external database.
    Returns a ConnectionTestResult (success / failure + server version).
    """
    return await test_connection(
        database_type=database_type,
        host=host,
        port=port,
        database_name=database_name,
        username=username,
        password=password,
    )


# ── Save config to Appwrite ─────────────────────────────────────────

async def save_database_config(
    user_id: str,
    database_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
) -> dict:
    """
    Store the database configuration in the Appwrite
    'user_databases' table after a successful connection test.
    """
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "user_id": user_id,
        "database_type": database_type,
        "host": host,
        "port": port,
        "database_name": database_name,
        "username": username,
        "password": encrypt(password),          # ← AES-encrypted
        "status": "connected",
        "created_at": now,
        "updated_at": now,
    }
    response = await asyncio.to_thread(
        tables.create_row,
        database_id=DATABASE_ID,
        table_id=USER_DATABASES_COLLECTION_ID,
        row_id="unique()",
        data=data,
    )
    return normalize_row(response)


# ── CRUD helpers (list / get / delete) ───────────────────────────────

async def list_user_databases(
    user_id: str, limit: int = 25, offset: int = 0,
) -> dict:
    """List all saved database configs belonging to a user."""
    base_queries = [Query.limit(limit), Query.offset(offset)]

    try:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=USER_DATABASES_COLLECTION_ID,
            queries=[Query.equal("user_id", user_id), *base_queries],
        )
    except AppwriteException as exc:
        # Legacy deployments may still use owner_user_id.
        if "Attribute not found in schema: user_id" not in str(exc):
            raise
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=USER_DATABASES_COLLECTION_ID,
            queries=[Query.equal("owner_user_id", user_id), *base_queries],
        )

    return normalize_row_collection(response)


async def list_all_databases(limit: int = 50, offset: int = 0) -> dict:
    """List saved database configs across all users (admin use)."""
    response = await asyncio.to_thread(
        tables.list_rows,
        database_id=DATABASE_ID,
        table_id=USER_DATABASES_COLLECTION_ID,
        queries=[
            Query.limit(limit),
            Query.offset(offset),
            Query.order_desc("created_at"),
        ],
    )
    return normalize_row_collection(response)


async def get_user_database(document_id: str) -> Optional[dict]:
    """Fetch a single saved database config by row ID."""
    try:
        response = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=USER_DATABASES_COLLECTION_ID,
            row_id=document_id,
        )
        return normalize_row(response)
    except Exception:
        return None


async def _patch_database_row(document_id: str, data: dict) -> None:
    """Best-effort compatibility patch for legacy rows."""
    if not data:
        return
    try:
        await asyncio.to_thread(
            tables.update_row,
            database_id=DATABASE_ID,
            table_id=USER_DATABASES_COLLECTION_ID,
            row_id=document_id,
            data=data,
        )
    except Exception:
        # Do not block backup flow if compatibility patch fails.
        pass


async def get_user_database_decrypted(document_id: str) -> Optional[dict]:
    """Fetch a database config and return a usable plain password for backup."""
    doc = await get_user_database(document_id)
    if not doc:
        return None

    raw_password = doc.get("password", "")
    if not raw_password:
        return doc

    try:
        doc["password"] = decrypt(raw_password)
        return doc
    except InvalidToken:
        # Legacy rows may contain plain-text passwords from older app versions.
        if raw_password.startswith("gAAAA"):
            raise RuntimeError(
                "Saved database password cannot be decrypted. Verify ENCRYPTION_KEY."
            )

        doc["password"] = raw_password
        patch_data = {
            "password": encrypt(raw_password),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        await _patch_database_row(document_id, patch_data)
    return doc


async def delete_user_database(document_id: str) -> None:
    """Delete a saved database config from Appwrite."""
    await asyncio.to_thread(
        tables.delete_row,
        database_id=DATABASE_ID,
        table_id=USER_DATABASES_COLLECTION_ID,
        row_id=document_id,
    )
