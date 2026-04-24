"""
Service layer: trigger backups and persist backup metadata in Appwrite.
"""

import asyncio
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from appwrite.exception import AppwriteException
from appwrite.query import Query
from appwrite.input_file import InputFile

from app.core.appwrite_client import tables, storage
from app.config import (
    DATABASE_ID,
    BACKUPS_COLLECTION_ID,
    RESTORES_COLLECTION_ID,
    APPWRITE_STORAGE_BUCKET_ID,
)
from app.logger import get_logger
from app.services.database_service import get_user_database_decrypted
from app.services import log_service
#from app.services import notification_service
import app.services.notification_service as notification_service
from app.services.metadata_service import update_metadata_async
from app.utils.appwrite_normalize import normalize_row, normalize_row_collection
from app.utils.backup_engine import run_backup, BackupResult, run_restore
from app.utils.compression import gzip_compress, gzip_decompress, is_gzip_name
from app.utils.ownership import get_owner_user_id
from app.utils.file_encryption import encrypt_file, decrypt_bytes
from app.utils.key_manager import get_backup_key_optional


# Ensure new backup attributes exist (for deployments created before compression fields were added).
_backup_attrs_ensured = False
_GZIP_MAGIC = b"\x1f\x8b"

# Scoped loggers
backup_logger = get_logger("backup")
restore_logger = get_logger("restore")
error_logger = get_logger("error")


async def _notify_user(
    user_id: str,
    event_type: str,
    level: str,
    title: str,
    message: str,
    resource_id: str = "",
) -> None:
    """Best-effort user notification emitter."""
    try:
        await notification_service.create_notification(
            user_id=user_id,
            event_type=event_type,
            level=level,
            title=title,
            message=message,
            resource_id=resource_id,
        )
    except Exception:
        pass


def _resolve_compression(stored: str | None, file_name: str) -> str:
    """Fallback compression inference for older records missing metadata."""
    if stored:
        return stored
    name = (file_name or "").lower()
    if name.endswith(".gz") or name.endswith(".gz.enc"):
        return "gzip"
    return "none"


def _is_unknown_attribute_error(exc: Exception, field_name: str) -> bool:
    text = str(exc).lower()
    return "unknown attribute" in text and field_name.lower() in text


async def _ensure_backup_attributes():
    global _backup_attrs_ensured
    if _backup_attrs_ensured or not BACKUPS_COLLECTION_ID:
        return

    def _create_string(key: str, size: int):
        try:
            try:
                # Newer SDKs: text columns generally don't require size.
                tables.create_text_column(
                    database_id=DATABASE_ID,
                    table_id=BACKUPS_COLLECTION_ID,
                    key=key,
                    required=False,
                )
            except TypeError:
                # Backward compatibility for SDK variants that still accept size.
                tables.create_text_column(
                    database_id=DATABASE_ID,
                    table_id=BACKUPS_COLLECTION_ID,
                    key=key,
                    size=size,
                    required=False,
                )
            except AttributeError:
                # Backward compatibility for older Appwrite SDKs.
                tables.create_string_column(
                    database_id=DATABASE_ID,
                    table_id=BACKUPS_COLLECTION_ID,
                    key=key,
                    size=size,
                    required=False,
                )
        except Exception as e:
            if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                raise

    # Best-effort; run in a worker thread to avoid blocking the event loop.
    try:
        await asyncio.to_thread(_create_string, "original_file_name", 512)
        await asyncio.to_thread(_create_string, "original_file_size", 50)
        await asyncio.to_thread(_create_string, "compression", 50)
        await asyncio.to_thread(_create_string, "encryption", 50)
        await asyncio.to_thread(_create_string, "backup_type", 50)
        await asyncio.to_thread(_create_string, "base_backup_id", 255)
        await asyncio.to_thread(_create_string, "duration_seconds", 50)
        _backup_attrs_ensured = True
    except Exception as e:
        # Do not fail the request if attribute creation has permission issues.
        backup_logger.warning("Could not auto-ensure backup attributes: %s", e)


def _looks_like_gzip(data: bytes) -> bool:
    return bool(data) and len(data) >= 2 and data[:2] == _GZIP_MAGIC


async def _safe_remove_file(path: str, retries: int = 5, delay_seconds: float = 0.15) -> None:
    """Delete a local file with retries to handle transient Windows file locks."""
    if not path:
        return
    for attempt in range(retries):
        try:
            os.remove(path)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            if attempt == retries - 1:
                return
            await asyncio.sleep(delay_seconds)
        except OSError:
            if attempt == retries - 1:
                return
            await asyncio.sleep(delay_seconds)


async def _upload_backup_file_with_retries(file_path: str, retries: int = 3) -> dict:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            upload = await asyncio.to_thread(
                storage.create_file,
                bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                file_id="unique()",
                file=InputFile.from_path(file_path),
            )
            if hasattr(upload, "to_dict"):
                upload = upload.to_dict()
            if hasattr(upload, "model_dump"):
                upload = upload.model_dump(by_alias=True)
            return upload if isinstance(upload, dict) else {}
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    raise last_error or RuntimeError("Storage upload failed")


async def _get_last_successful_backup(user_id: str, db_config_id: str) -> Optional[dict]:
    """Return latest successful backup row for a database config, if available."""
    base_queries = [
        Query.equal("db_config_id", db_config_id),
        Query.equal("status", "success"),
        Query.limit(1),
    ]

    try:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=[Query.equal("user_id", user_id), *base_queries, Query.order_desc("created_at")],
        )
    except AppwriteException as exc:
        if "Attribute not found in schema: user_id" in str(exc):
            try:
                response = await asyncio.to_thread(
                    tables.list_rows,
                    database_id=DATABASE_ID,
                    table_id=BACKUPS_COLLECTION_ID,
                    queries=[
                        Query.equal("owner_user_id", user_id),
                        *base_queries,
                        Query.order_desc("created_at"),
                    ],
                )
            except AppwriteException:
                response = await asyncio.to_thread(
                    tables.list_rows,
                    database_id=DATABASE_ID,
                    table_id=BACKUPS_COLLECTION_ID,
                    queries=[Query.equal("owner_user_id", user_id), *base_queries],
                )
        else:
            response = await asyncio.to_thread(
                tables.list_rows,
                database_id=DATABASE_ID,
                table_id=BACKUPS_COLLECTION_ID,
                queries=[Query.equal("user_id", user_id), *base_queries],
            )
    except Exception:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=[Query.equal("user_id", user_id), *base_queries],
        )

    rows = normalize_row_collection(response).get("rows", [])
    return rows[0] if rows else None


async def _resolve_backup_type(
    requested_type: str,
    *,
    user_id: str,
    db_config_id: str,
) -> tuple[str, Optional[dict], str]:
    """Resolve requested mode into effective backup type and base backup context."""
    mode = (requested_type or "auto").strip().lower()
    if mode not in {"auto", "full", "incremental"}:
        raise ValueError("backup_type must be one of: auto, full, incremental")

    if mode == "full":
        return "full", None, ""

    last_success = await _get_last_successful_backup(user_id=user_id, db_config_id=db_config_id)
    if last_success:
        return "incremental", last_success, ""

    # First backup (or no successful prior backup) cannot be incremental.
    return "full", None, "No previous successful backup found; full backup was created."


# ── Trigger a backup ─────────────────────────────────────────────────

async def trigger_backup(
    db_config_id: str,
    user_id: str,
    backup_type: str = "auto",
    role: str = "user",
    ip_address: str | None = None,
    device_info: str | None = None,
) -> dict:
    """
    1. Load the saved DB config (with decrypted password).
    2. Run the backup engine.
    3. Persist backup metadata in Appwrite 'backups' table.
    4. Return the backup metadata row.
    """

    start_dt = datetime.now(timezone.utc)
    log_id: str = ""

    backup_logger.info(
        "Backup started user_id=%s db_config_id=%s", user_id, db_config_id
    )

    # Step 1 – ensure collection schema is compatible (best-effort for older deployments)
    await _ensure_backup_attributes()

    # Step 2 – load config
    doc = await get_user_database_decrypted(db_config_id)
    if not doc:
        raise ValueError(f"Database config '{db_config_id}' not found.")
    if get_owner_user_id(doc) != user_id:
        raise PermissionError("Access denied to this database configuration.")

    backup_logger.info(
        "Backup connecting database user_id=%s db_config_id=%s database=%s",
        user_id,
        db_config_id,
        doc.get("database_name", ""),
    )
    await _notify_user(
        user_id=user_id,
        event_type="backup_started",
        level="info",
        title="Backup Started",
        message=f"Backup started for database '{doc.get('database_name', '')}'.",
        resource_id=db_config_id,
    )

    effective_backup_type, base_backup, backup_type_note = await _resolve_backup_type(
        backup_type,
        user_id=user_id,
        db_config_id=db_config_id,
    )
    base_backup_id = (base_backup or {}).get("$id", "")

    try:
        log_row = await log_service.create_log_entry(
            user_id=user_id,
            role=role,
            operation_type="backup",
            status="started",
            database_name=doc["database_name"],
            db_config_id=db_config_id,
            start_time=start_dt.isoformat(),
            ip_address=ip_address,
            device_info=device_info,
        )
        log_id = log_row.get("$id", "") if log_row else ""
    except Exception:
        # best effort logging
        log_id = ""

    # Step 3 – run backup
    try:
        result: BackupResult = await run_backup(
            database_type=doc["database_type"],
            host=doc["host"],
            port=doc["port"],
            database_name=doc["database_name"],
            username=doc["username"],
            password=doc["password"],
        )
    except Exception as e:
        error_logger.exception(
            "Backup failed during engine run user_id=%s db_config_id=%s database=%s",
            user_id,
            db_config_id,
            doc.get("database_name", ""),
        )
        await _notify_user(
            user_id=user_id,
            event_type="backup_failed",
            level="error",
            title="Backup Failed",
            message=f"Backup failed for database '{doc.get('database_name', '')}': {e}",
            resource_id=db_config_id,
        )
        end_dt = datetime.now(timezone.utc)
        if log_id:
            try:
                await log_service.update_log_entry(
                    log_id,
                    status="failed",
                    end_time=end_dt.isoformat(),
                    duration=(end_dt - start_dt).total_seconds(),
                    error_message=str(e),
                    db_config_id=db_config_id,
                    database_name=doc.get("database_name", ""),
                )
            except Exception:
                pass
        raise

    # Compress backup before upload (gzip)
    compression_error = None
    if result.success and result.file_path:
        try:
            source_path = Path(result.file_path)
            original_size = result.file_size or source_path.stat().st_size
            original_name = result.file_name or source_path.name

            compressed_path = await asyncio.to_thread(gzip_compress, source_path)
            compressed_size = compressed_path.stat().st_size

            # remove uncompressed source to save space
            await _safe_remove_file(str(source_path))

            result.file_path = str(compressed_path)
            result.file_name = compressed_path.name
            result.file_size = compressed_size
            result.compression = "gzip"
            result.original_file_name = original_name
            result.original_file_size = original_size
        except Exception as e:
            compression_error = str(e)
            result.compression = "none"
            result.original_file_name = result.original_file_name or result.file_name
            result.original_file_size = result.original_file_size or result.file_size

    # Encrypt compressed backup before upload
    encryption_error = None
    if result.success and result.file_path:
        key = get_backup_key_optional()
        if not key:
            encryption_error = "Missing backup encryption key"
            result.success = False
            result.message = "Backup generated but encryption key is not configured."
        else:
            try:
                source_path = Path(result.file_path)
                if not source_path.exists():
                    raise FileNotFoundError(f"Backup file does not exist: {source_path}")
                encrypted_path = source_path.with_suffix(source_path.suffix + ".enc")
                await asyncio.to_thread(encrypt_file, source_path, encrypted_path, key)

                await _safe_remove_file(str(source_path))

                result.file_path = str(encrypted_path)
                result.file_name = encrypted_path.name
                result.file_size = encrypted_path.stat().st_size
                result.encryption = "aes-256-gcm"
                backup_logger.info(
                    "Backup file encrypted successfully user_id=%s db_config_id=%s file=%s",
                    user_id,
                    db_config_id,
                    result.file_name,
                )
            except Exception as e:
                encryption_error = str(e)
                result.success = False
                result.message = f"Backup generated but encryption failed: {e}"
                error_logger.error(
                    "Backup encryption failed user_id=%s db_config_id=%s error=%s",
                    user_id,
                    db_config_id,
                    e,
                )

    file_id = ""
    storage_bucket = ""
    if result.success and result.file_path and APPWRITE_STORAGE_BUCKET_ID:
        try:
            upload = await _upload_backup_file_with_retries(result.file_path)
            file_id = upload.get("$id", "")
            storage_bucket = APPWRITE_STORAGE_BUCKET_ID

            # Remove local temp file after successful upload to storage.
            await _safe_remove_file(result.file_path)
            result.file_path = ""
        except Exception as e:
            result.success = False
            result.message = f"Backup generated but storage upload failed: {e}"
            error_logger.error(
                "Backup upload failed user_id=%s db_config_id=%s file=%s error=%s",
                user_id,
                db_config_id,
                result.file_name,
                e,
            )

    if compression_error:
        result.message = f"{result.message} (Compression skipped: {compression_error})"
    if encryption_error:
        result.message = f"{result.message} (Encryption issue: {encryption_error})"
    if backup_type_note:
        result.message = f"{result.message} ({backup_type_note})"

    if not getattr(result, "encryption", None):
        result.encryption = "aes-256-gcm" if not encryption_error and result.success else "none"

    end_dt = datetime.now(timezone.utc)
    now = end_dt.isoformat()
    duration_seconds = (end_dt - start_dt).total_seconds()

    # Step 4 – persist metadata
    data = {
        "db_config_id":  db_config_id,
        "user_id":       user_id,
        "database_type": doc["database_type"],
        "database_name": doc["database_name"],
        "file_name":     result.file_name or "",
        "file_path":     result.file_path or "",
        "file_id":       file_id,
        "storage_bucket": storage_bucket,
        "file_size":     str(result.file_size) if result.file_size is not None else "0",
        "original_file_name": result.original_file_name or "",
        "original_file_size": str(result.original_file_size) if result.original_file_size is not None else "0",
        "compression":   result.compression or "none",
        "encryption":    getattr(result, "encryption", "none"),
        "backup_type":   effective_backup_type,
        "base_backup_id": base_backup_id,
        "duration_seconds": str(round(duration_seconds, 3)),
        "status":        "success" if result.success else "failed",
        "error_message": "" if result.success else result.message,
        "created_at":    now,
    }

    try:
        backup_row = await asyncio.to_thread(
            tables.create_row,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            row_id="unique()",
            data=data,
        )
    except Exception as e:
        # Hotfix for older tables that don't yet have duration_seconds.
        if _is_unknown_attribute_error(e, "duration_seconds"):
            backup_logger.warning(
                "Missing duration_seconds column in backups table; retrying without it"
            )
            data_without_duration = dict(data)
            data_without_duration.pop("duration_seconds", None)
            backup_row = await asyncio.to_thread(
                tables.create_row,
                database_id=DATABASE_ID,
                table_id=BACKUPS_COLLECTION_ID,
                row_id="unique()",
                data=data_without_duration,
            )
        else:
            raise
    backup_row = normalize_row(backup_row)

    backup_logger.info(
        "Backup completed user_id=%s db_config_id=%s backup_id=%s status=%s file=%s size=%s",
        user_id,
        db_config_id,
        backup_row.get("$id", ""),
        backup_row.get("status", ""),
        backup_row.get("file_name", ""),
        backup_row.get("file_size", ""),
    )

    if log_id:
        try:
            await log_service.update_log_entry(
                log_id,
                status="success" if backup_row.get("status") == "success" else "failed",
                end_time=end_dt.isoformat(),
                duration=(end_dt - start_dt).total_seconds(),
                file_name=backup_row.get("file_name", ""),
                file_size=int(backup_row.get("file_size", 0) or 0),
                error_message=backup_row.get("error_message", ""),
                db_config_id=db_config_id,
                database_name=doc.get("database_name", ""),
                backup_id=backup_row.get("$id", ""),
            )
        except Exception:
            pass

    try:
        await update_metadata_async(
            db_config_id=db_config_id,
            backup_type=effective_backup_type,
            file_id=file_id or backup_row.get("$id", ""),
            file_name=result.file_name or "",
            status="success" if result.success else "failed",
            error_message=result.message if not result.success else None,
        )
    except Exception:
        pass

    if backup_row.get("status") == "success":
        await _notify_user(
            user_id=user_id,
            event_type="backup_completed",
            level="success",
            title="Backup Completed",
            message=(
                f"Backup completed for '{doc.get('database_name', '')}' "
                f"({backup_row.get('file_name', '')})."
            ),
            resource_id=str(backup_row.get("$id") or ""),
        )
    else:
        await _notify_user(
            user_id=user_id,
            event_type="backup_failed",
            level="error",
            title="Backup Failed",
            message=(
                f"Backup failed for '{doc.get('database_name', '')}': "
                f"{backup_row.get('error_message', result.message)}"
            ),
            resource_id=str(backup_row.get("$id") or ""),
        )

    # Attach success/error message so routes can surface it
    backup_row["_result_message"] = result.message
    backup_row["_success"] = result.success
    backup_row["backup_type"] = backup_row.get("backup_type", effective_backup_type)
    backup_row["base_backup_id"] = backup_row.get("base_backup_id", base_backup_id)

    return backup_row


# ── List backups ──────────────────────────────────────────────────────

async def list_backups(
    user_id: str,
    db_config_id: Optional[str] = None,
    limit: int = 25,
    offset: int = 0,
) -> dict:
    """
    List backup records for a user, optionally filtered by db_config_id.
    """
    base_queries = [Query.limit(limit), Query.offset(offset), Query.order_desc("created_at")]
    if db_config_id:
        base_queries.insert(0, Query.equal("db_config_id", db_config_id))

    try:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=[Query.equal("user_id", user_id), *base_queries],
        )
    except AppwriteException as exc:
        if "Attribute not found in schema: user_id" not in str(exc):
            raise
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=[Query.equal("owner_user_id", user_id), *base_queries],
        )

    return normalize_row_collection(response)


async def list_all_backups(limit: int = 50, offset: int = 0) -> dict:
    """List backup records across all users (admin use)."""
    base_queries = [Query.limit(limit), Query.offset(offset)]
    try:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=[*base_queries, Query.order_desc("created_at")],
        )
    except AppwriteException:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            queries=base_queries,
        )

    return normalize_row_collection(response)


# ── Get single backup ─────────────────────────────────────────────────

async def get_backup(backup_id: str) -> Optional[dict]:
    """Fetch a single backup record by row ID."""
    try:
        row = await asyncio.to_thread(
            tables.get_row,
            database_id=DATABASE_ID,
            table_id=BACKUPS_COLLECTION_ID,
            row_id=backup_id,
        )
        return normalize_row(row)
    except Exception:
        return None


# ── Delete backup ─────────────────────────────────────────────────────

async def delete_backup(backup_id: str, delete_file: bool = False) -> None:
    """
    Delete a backup record from Appwrite.
    If delete_file=True, also remove the backup file from disk.
    """
    if delete_file:
        doc = await get_backup(backup_id)
        if doc and doc.get("file_id") and doc.get("storage_bucket"):
            try:
                await asyncio.to_thread(
                    storage.delete_file,
                    bucket_id=doc["storage_bucket"],
                    file_id=doc["file_id"],
                )
            except Exception:
                pass

        if doc and doc.get("file_path"):
            try:
                os.remove(doc["file_path"])
            except FileNotFoundError:
                pass

    await asyncio.to_thread(
        tables.delete_row,
        database_id=DATABASE_ID,
        table_id=BACKUPS_COLLECTION_ID,
        row_id=backup_id,
    )


async def get_backup_file_bytes(doc: dict) -> bytes:
    """Return backup file content from Appwrite Storage or local fallback."""
    data: bytes = b""
    if doc.get("file_id") and doc.get("storage_bucket"):
        data = await asyncio.to_thread(
            storage.get_file_download,
            bucket_id=doc["storage_bucket"],
            file_id=doc["file_id"],
        )
    else:
        file_path = doc.get("file_path", "")
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError("Backup file not found.")

        with open(file_path, "rb") as f:
            data = f.read()

    # Decrypt if needed
    if doc.get("encryption") == "aes-256-gcm":
        key = get_backup_key_optional()
        if not key:
            raise RuntimeError("Backup encryption key is not configured for decryption.")
        try:
            restore_logger.info(
                "Decryption started for restore backup_id=%s", doc.get("$id", "")
            )
            data = await asyncio.to_thread(decrypt_bytes, data, key)

            restore_logger.info(
                "Decryption completed for restore backup_id=%s", doc.get("$id", "")
            )
        except Exception as e:
            error_msg = str(e).lower() if str(e) else ""
            error_logger.error(
                "Decryption failed backup_id=%s error=%s", 
                doc.get("$id", ""), 
                str(e),
                exc_info=True
            )
            
            # Provide helpful error messages
            if not error_msg or error_msg.strip() == "":
                raise RuntimeError("Decryption failed - file may be corrupted or encryption key may be incorrect.") from e
            elif "too small" in error_msg:
                raise RuntimeError("Backup file is corrupted or incomplete (too small).") from e
            elif "authentication" in error_msg or "tag" in error_msg:
                raise RuntimeError("Backup file authentication failed - file may be corrupted, or encryption key may be incorrect.") from e
            elif "no such file" in error_msg or "not found" in error_msg:
                raise RuntimeError("Backup file not found or inaccessible.") from e
            else:
                raise RuntimeError(f"Decryption failed: {error_msg}") from e

    return data


# ── Restore helpers ───────────────────────────────────────────────────


async def _write_temp_file(file_name: str, data: bytes) -> str:
    """Persist bytes to a temp file and return its path."""
    suffix = os.path.splitext(file_name)[1] if file_name else ""
    fd, path = tempfile.mkstemp(suffix=suffix, prefix="restore_")
    with os.fdopen(fd, "wb") as tmp:
        tmp.write(data)
    return path


async def _prepare_restore_file(
    temp_path: str,
    file_name: str,
    compression: str,
    original_file_name: Optional[str] = None,
    file_bytes: Optional[bytes] = None,
) -> tuple[str, str, Optional[str]]:
    """
    Decompress the temp file if needed and return (path, file_name, extra_path_for_cleanup).
    """
    compression = (compression or "none").lower()
    working_path = temp_path
    working_name = file_name
    decompressed_path: Optional[str] = None

    if compression == "gzip" or is_gzip_name(file_name) or _looks_like_gzip(file_bytes or b""):
        target_name = original_file_name or os.path.splitext(file_name)[0]
        decompressed = await asyncio.to_thread(
            gzip_decompress,
            Path(temp_path),
            target_name,
        )
        working_path = str(decompressed)
        working_name = target_name or decompressed.name
        decompressed_path = str(decompressed)

    return working_path, working_name, decompressed_path


async def restore_backup_from_record(
    backup_id: str,
    user_id: str,
    role: str = "user",
    ip_address: str | None = None,
    device_info: str | None = None,
) -> dict:
    """Restore a backup referenced by its stored record."""
    start_dt = datetime.now(timezone.utc)
    log_id = ""

    restore_logger.info(
        "Restore-from-record started user_id=%s backup_id=%s",
        user_id,
        backup_id,
    )

    doc = await get_backup(backup_id)
    if not doc:
        restore_logger.warning(
            "Restore aborted: backup not found user_id=%s backup_id=%s",
            user_id,
            backup_id,
        )
        return {"success": False, "message": "Backup not found."}

    if get_owner_user_id(doc) != user_id:
        restore_logger.warning(
            "Restore aborted: access denied user_id=%s backup_id=%s",
            user_id,
            backup_id,
        )
        return {"success": False, "message": "Access denied."}

    db_config = await get_user_database_decrypted(doc.get("db_config_id", ""))
    if not db_config or get_owner_user_id(db_config) != user_id:
        restore_logger.warning(
            "Restore aborted: db config not found user_id=%s backup_id=%s db_config_id=%s",
            user_id,
            backup_id,
            doc.get("db_config_id", ""),
        )
        return {"success": False, "message": "Database config not found or not accessible."}

    await _notify_user(
        user_id=user_id,
        event_type="restore_started",
        level="info",
        title="Restore Started",
        message=f"Restore started for database '{db_config.get('database_name', '')}'.",
        resource_id=backup_id,
    )

    try:
        log_row = await log_service.create_log_entry(
            user_id=user_id,
            role=role,
            operation_type="restore",
            status="started",
            database_name=doc.get("database_name", ""),
            file_name=doc.get("file_name", ""),
            db_config_id=db_config.get("$id") or db_config.get("db_config_id", ""),
            backup_id=backup_id,
            start_time=start_dt.isoformat(),
            ip_address=ip_address,
            device_info=device_info,
        )
        log_id = log_row.get("$id", "") if log_row else ""
    except Exception:
        log_id = ""

    try:
        data = await get_backup_file_bytes(doc)
    except FileNotFoundError as e:
        error_logger.error(
            "Restore failed: backup file missing user_id=%s backup_id=%s error=%s",
            user_id,
            backup_id,
            e,
        )
        end_dt = datetime.now(timezone.utc)
        if log_id:
            try:
                await log_service.update_log_entry(
                    log_id,
                    status="failed",
                    end_time=end_dt.isoformat(),
                    duration=(end_dt - start_dt).total_seconds(),
                    error_message=str(e),
                )
            except Exception:
                pass
        await _notify_user(
            user_id=user_id,
            event_type="restore_failed",
            level="error",
            title="Restore Failed",
            message=f"Restore failed: {e}",
            resource_id=backup_id,
        )
        raise

    temp_path = await _write_temp_file(doc.get("file_name", "backup"), data)
    compression = _resolve_compression(doc.get("compression"), doc.get("file_name", ""))
    working_path, working_name, decompressed_path = await _prepare_restore_file(
        temp_path=temp_path,
        file_name=doc.get("file_name", "backup"),
        compression=compression,
        original_file_name=doc.get("original_file_name") or doc.get("file_name", ""),
        file_bytes=data,
    )

    result = await run_restore(
        database_type=db_config["database_type"],
        host=db_config["host"],
        port=int(db_config["port"]),
        database_name=db_config["database_name"],
        username=db_config["username"],
        password=db_config["password"],
        file_path=working_path,
        file_name=working_name,
    )

    if not result.success:
        error_logger.error(
            "Restore failed user_id=%s backup_id=%s db_config_id=%s message=%s",
            user_id,
            backup_id,
            db_config.get("$id") or db_config.get("db_config_id", ""),
            result.message,
        )

    for path in [working_path if decompressed_path else None, temp_path]:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

    restore_row_id = await _record_restore(
        user_id=user_id,
        db_config_id=db_config.get("$id") or db_config.get("db_config_id", ""),
        backup_id=backup_id,
        file_name=doc.get("file_name", ""),
        source="record",
        status="success" if result.success else "failed",
        message=result.message,
    )

    end_dt = datetime.now(timezone.utc)
    restore_logger.info(
        "Restore-from-record completed user_id=%s backup_id=%s status=%s message=%s",
        user_id,
        backup_id,
        "success" if result.success else "failed",
        result.message,
    )
    if log_id:
        try:
            await log_service.update_log_entry(
                log_id,
                status="success" if result.success else "failed",
                end_time=end_dt.isoformat(),
                duration=(end_dt - start_dt).total_seconds(),
                file_name=doc.get("file_name", ""),
                error_message=result.message if not result.success else "",
                db_config_id=db_config.get("$id") or db_config.get("db_config_id", ""),
                backup_id=backup_id,
                restore_id=restore_row_id or "",
            )
        except Exception:
            pass

    await _notify_user(
        user_id=user_id,
        event_type="restore_completed" if result.success else "restore_failed",
        level="success" if result.success else "error",
        title="Restore Completed" if result.success else "Restore Failed",
        message=(
            f"Restore completed for '{db_config.get('database_name', '')}'."
            if result.success
            else f"Restore failed for '{db_config.get('database_name', '')}': {result.message}"
        ),
        resource_id=backup_id,
    )

    return {"success": result.success, "message": result.message}


async def restore_backup_from_upload(
    db_config_id: str,
    user_id: str,
    upload_file,
    role: str = "user",
    ip_address: str | None = None,
    device_info: str | None = None,
) -> dict:
    """Restore from a user-provided backup upload."""
    start_dt = datetime.now(timezone.utc)
    log_id = ""

    restore_logger.info(
        "Restore-from-upload started user_id=%s db_config_id=%s filename=%s",
        user_id,
        db_config_id,
        getattr(upload_file, "filename", ""),
    )

    db_config = await get_user_database_decrypted(db_config_id)
    if not db_config or get_owner_user_id(db_config) != user_id:
        restore_logger.warning(
            "Restore aborted: db config not found user_id=%s db_config_id=%s",
            user_id,
            db_config_id,
        )
        return {"success": False, "message": "Database config not found or not accessible."}

    await _notify_user(
        user_id=user_id,
        event_type="restore_started",
        level="info",
        title="Restore Started",
        message=f"Restore started for database '{db_config.get('database_name', '')}'.",
        resource_id=db_config_id,
    )

    try:
        log_row = await log_service.create_log_entry(
            user_id=user_id,
            role=role,
            operation_type="restore",
            status="started",
            database_name=db_config.get("database_name", ""),
            db_config_id=db_config_id,
            start_time=start_dt.isoformat(),
            ip_address=ip_address,
            device_info=device_info,
        )
        log_id = log_row.get("$id", "") if log_row else ""
    except Exception:
        log_id = ""

    file_bytes = await upload_file.read()
    if not file_bytes:
        restore_logger.warning(
            "Restore aborted: uploaded file empty user_id=%s db_config_id=%s",
            user_id,
            db_config_id,
        )
        await _notify_user(
            user_id=user_id,
            event_type="restore_failed",
            level="error",
            title="Restore Failed",
            message="Restore failed: uploaded file is empty.",
            resource_id=db_config_id,
        )
        return {"success": False, "message": "Uploaded file is empty."}

    # Handle encrypted uploads (e.g., files downloaded from storage ending with .enc)
    original_name = upload_file.filename or "upload"
    encrypted_upload = original_name.lower().endswith(".enc")
    processed_name = original_name[:-4] if encrypted_upload else original_name

    if encrypted_upload:
        key = get_backup_key_optional()
        if not key:
            await _notify_user(
                user_id=user_id,
                event_type="restore_failed",
                level="error",
                title="Restore Failed",
                message="Restore failed: encryption key is not configured.",
                resource_id=db_config_id,
            )
            return {"success": False, "message": "Encryption key not configured for restore."}

        try:
            restore_logger.info(
                "Decryption started for uploaded file user_id=%s db_config_id=%s file=%s",
                user_id,
                db_config_id,
                original_name,
            )

            # Decrypt bytes directly without temp files
            file_bytes = await asyncio.to_thread(decrypt_bytes, file_bytes, key)

            restore_logger.info(
                "Decryption completed for uploaded file user_id=%s db_config_id=%s file=%s",
                user_id,
                db_config_id,
                original_name,
            )

        except Exception as e:
            error_msg = str(e).lower() if str(e) else ""
            error_logger.error(
                "Decryption failed for uploaded file user_id=%s db_config_id=%s file=%s error=%s",
                user_id,
                db_config_id,
                original_name,
                str(e),
                exc_info=True
            )

            # Provide helpful error messages based on the error type
            if not error_msg or error_msg.strip() == "":
                detail = "File may be corrupted or encryption key may be incorrect."
            elif "too small" in error_msg:
                detail = "File is corrupted or incomplete (too small)."
            elif "authentication" in error_msg or "tag" in error_msg:
                detail = "File authentication failed - file may be corrupted or encryption key may be incorrect."
            elif "no such file" in error_msg or "not found" in error_msg:
                detail = "File not found or inaccessible."
            else:
                detail = f"{error_msg}. File may be corrupted or encryption key incorrect."

            await _notify_user(
                user_id=user_id,
                event_type="restore_failed",
                level="error",
                title="Restore Failed",
                message=f"Restore failed during decryption: {detail}",
                resource_id=db_config_id,
            )

            return {"success": False, "message": detail}

    temp_path = await _write_temp_file(processed_name or "upload", file_bytes)
    compression = _resolve_compression(None, processed_name)
    original_name_guess = processed_name
    if compression == "gzip":
        original_name_guess = os.path.splitext(processed_name or "")[0]
    working_path, working_name, decompressed_path = await _prepare_restore_file(
        temp_path=temp_path,
        file_name=processed_name or "upload",
        compression=compression,
        original_file_name=original_name_guess,
        file_bytes=file_bytes,
    )

    result = await run_restore(
        database_type=db_config["database_type"],
        host=db_config["host"],
        port=int(db_config["port"]),
        database_name=db_config["database_name"],
        username=db_config["username"],
        password=db_config["password"],
        file_path=working_path,
        file_name=working_name,
    )

    if not result.success:
        error_logger.error(
            "Restore-from-upload failed user_id=%s db_config_id=%s filename=%s message=%s",
            user_id,
            db_config_id,
            getattr(upload_file, "filename", ""),
            result.message,
        )

    for path in [working_path if decompressed_path else None, temp_path]:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

    restore_row_id = await _record_restore(
        user_id=user_id,
        db_config_id=db_config_id,
        backup_id="",
        file_name=upload_file.filename,
        source="upload",
        status="success" if result.success else "failed",
        message=result.message,
    )

    end_dt = datetime.now(timezone.utc)
    restore_logger.info(
        "Restore-from-upload completed user_id=%s db_config_id=%s status=%s message=%s",
        user_id,
        db_config_id,
        "success" if result.success else "failed",
        result.message,
    )
    if log_id:
        try:
            await log_service.update_log_entry(
                log_id,
                status="success" if result.success else "failed",
                end_time=end_dt.isoformat(),
                duration=(end_dt - start_dt).total_seconds(),
                file_name=upload_file.filename,
                error_message=result.message if not result.success else "",
                db_config_id=db_config_id,
                restore_id=restore_row_id or "",
            )
        except Exception:
            pass

    await _notify_user(
        user_id=user_id,
        event_type="restore_completed" if result.success else "restore_failed",
        level="success" if result.success else "error",
        title="Restore Completed" if result.success else "Restore Failed",
        message=(
            f"Restore completed for '{db_config.get('database_name', '')}'."
            if result.success
            else f"Restore failed for '{db_config.get('database_name', '')}': {result.message}"
        ),
        resource_id=db_config_id,
    )


    return {"success": result.success, "message": result.message}


async def _record_restore(
    user_id: str,
    db_config_id: str,
    backup_id: str,
    file_name: str,
    source: str,
    status: str,
    message: str,
) -> Optional[str]:
    """Persist a restore attempt to Appwrite if configured."""
    if not RESTORES_COLLECTION_ID:
        return None

    data = {
        "user_id": user_id,
        "db_config_id": db_config_id,
        "backup_id": backup_id,
        "file_name": file_name or "",
        "source": source,
        "status": status,
        "message": message[:2048] if message else "",
    }

    try:
        row = await asyncio.to_thread(
            tables.create_row,
            database_id=DATABASE_ID,
            table_id=RESTORES_COLLECTION_ID,
            row_id="unique()",
            data=data,
        )
        normalized = normalize_row(row)
        return normalized.get("$id") if normalized else None
    except Exception:
        # Best-effort logging; do not block restore on logging failure
        return None


async def list_all_restores(limit: int = 50, offset: int = 0) -> dict:
    """List restore records across all users (admin use)."""
    if not RESTORES_COLLECTION_ID:
        return {"rows": [], "total": 0}

    base_queries = [Query.limit(limit), Query.offset(offset)]
    try:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=RESTORES_COLLECTION_ID,
            queries=[*base_queries, Query.order_desc("created_at")],
        )
    except AppwriteException:
        response = await asyncio.to_thread(
            tables.list_rows,
            database_id=DATABASE_ID,
            table_id=RESTORES_COLLECTION_ID,
            queries=base_queries,
        )

    return normalize_row_collection(response)

