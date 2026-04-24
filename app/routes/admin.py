import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query, Request, UploadFile, File, Form
from fastapi.responses import JSONResponse, Response

from app.core.appwrite_client import users
from app.schemas.admin import (
    AdminBackupRecord,
    AdminDatabaseRecord,
    AdminRestoreRecord,
    AdminStorageMonitoringResponse,
    AdminUserCreateRequest,
    AdminUserRecord,
    AdminUserRoleUpdateRequest,
    AdminUserStatusUpdateRequest,
    AdminUserUpdateRequest,
)
from app.schemas.backup import TriggerBackupResponse
from app.schemas.schedule import ScheduleCreate, ScheduleOut
from app.services import database_service, backup_service, schedule_service, storage_service, user_service
from app.utils.password import prehash_for_appwrite
from app.utils.dependencies import require_admin_user

router = APIRouter(prefix="/admin", tags=["Admin"])


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _safe_float_or_none(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return None


def _format_storage_label(total_size: int) -> str:
    if total_size >= 1024 ** 3:
        return f"{round(total_size / (1024 ** 3), 2)} GB"
    return f"{round(total_size / (1024 ** 2), 2)} MB"


def _parse_iso_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def _collect_all_databases() -> list[dict]:
    db_rows = []
    db_offset = 0
    db_limit = 200
    while True:
        dbs = await database_service.list_all_databases(limit=db_limit, offset=db_offset)
        page = dbs.get("rows", dbs.get("documents", []))
        if not page:
            break
        db_rows.extend(page)
        if len(page) < db_limit:
            break
        db_offset += db_limit
    return db_rows


async def _collect_all_backups() -> list[dict]:
    backup_rows = []
    backup_offset = 0
    backup_limit = 200
    while True:
        backups = await backup_service.list_all_backups(limit=backup_limit, offset=backup_offset)
        page = backups.get("rows", backups.get("documents", []))
        if not page:
            break
        backup_rows.extend(page)
        if len(page) < backup_limit:
            break
        backup_offset += backup_limit
    return backup_rows


async def _build_storage_monitoring(backup_rows: list[dict], db_rows: list[dict]) -> dict:
    now = datetime.now(timezone.utc)
    recent_from = now - timedelta(days=7)
    previous_from = now - timedelta(days=14)

    total_used_bytes = 0
    recent_total_bytes = 0
    previous_total_bytes = 0

    db_lookup = {
        str(row.get("$id") or row.get("document_id") or ""): row
        for row in db_rows
    }
    grouped: dict[str, dict] = {}
    successful_backup_count = 0

    for row in backup_rows:
        if str(row.get("status") or "").lower() != "success":
            continue

        size = _safe_int(row.get("file_size"), 0)
        successful_backup_count += 1
        total_used_bytes += size

        db_config_id = str(row.get("db_config_id") or "")
        bucket = grouped.setdefault(
            db_config_id,
            {
                "db_config_id": db_config_id,
                "database_name": str((db_lookup.get(db_config_id) or {}).get("database_name") or row.get("database_name") or ""),
                "user_id": str((db_lookup.get(db_config_id) or {}).get("user_id") or row.get("user_id") or ""),
                "backup_count": 0,
                "storage_used_bytes": 0,
                "recent_7d": 0,
                "previous_7d": 0,
            },
        )
        bucket["backup_count"] += 1
        bucket["storage_used_bytes"] += size

        created_at = _parse_iso_datetime(row.get("created_at") or row.get("$createdAt"))
        if created_at:
            if created_at >= recent_from:
                recent_total_bytes += size
                bucket["recent_7d"] += size
            elif previous_from <= created_at < recent_from:
                previous_total_bytes += size
                bucket["previous_7d"] += size

    def _growth_rate(recent: int, previous: int) -> float | None:
        if previous <= 0:
            return 100.0 if recent > 0 else 0.0
        return round(((recent - previous) / previous) * 100, 2)

    database_storage_usage = []
    for item in grouped.values():
        used = item["storage_used_bytes"]
        count = item["backup_count"]
        avg = used / count if count else 0
        database_storage_usage.append(
            {
                "db_config_id": item["db_config_id"],
                "database_name": item["database_name"],
                "user_id": item["user_id"],
                "backup_count": count,
                "storage_used_bytes": used,
                "storage_used_mb": round(used / (1024 ** 2), 2),
                "storage_used_gb": round(used / (1024 ** 3), 4),
                "average_backup_size_bytes": round(avg, 2),
                "average_backup_size_mb": round(avg / (1024 ** 2), 2),
                "growth_rate_percent_7d": _growth_rate(item["recent_7d"], item["previous_7d"]),
                "growth_delta_bytes_7d": item["recent_7d"] - item["previous_7d"],
            }
        )

    database_storage_usage.sort(key=lambda x: x["storage_used_bytes"], reverse=True)

    total_capacity_bytes, quota_source = await storage_service.get_total_storage_capacity_bytes()
    storage_available_bytes = None
    if isinstance(total_capacity_bytes, int):
        storage_available_bytes = max(total_capacity_bytes - total_used_bytes, 0)

    avg_backup_size_bytes = (total_used_bytes / successful_backup_count) if successful_backup_count else 0

    return {
        "total_appwrite_storage_bytes": total_capacity_bytes,
        "total_appwrite_storage": storage_service.format_storage_size(total_capacity_bytes) if isinstance(total_capacity_bytes, int) else "Unknown",
        "total_storage_used_bytes": total_used_bytes,
        "total_storage_used": storage_service.format_storage_size(total_used_bytes),
        "storage_available_bytes": storage_available_bytes,
        "storage_available": storage_service.format_storage_size(storage_available_bytes) if isinstance(storage_available_bytes, int) else "Unknown",
        "average_backup_size_bytes": round(avg_backup_size_bytes, 2),
        "average_backup_size_mb": round(avg_backup_size_bytes / (1024 ** 2), 2),
        "growth_rate_percent_7d": _growth_rate(recent_total_bytes, previous_total_bytes),
        "growth_delta_bytes_7d": recent_total_bytes - previous_total_bytes,
        "quota_available": isinstance(total_capacity_bytes, int),
        "quota_source": quota_source,
        "database_storage_usage": database_storage_usage,
    }


def _to_admin_backup_record(row: dict) -> AdminBackupRecord:
    return AdminBackupRecord(
        backup_id=str(row.get("$id") or row.get("backup_id") or ""),
        db_config_id=str(row.get("db_config_id") or ""),
        user_id=str(row.get("user_id") or ""),
        database_type=str(row.get("database_type") or ""),
        database_name=str(row.get("database_name") or ""),
        file_name=str(row.get("file_name") or ""),
        file_size=_safe_int(row.get("file_size"), 0),
        status=str(row.get("status") or ""),
        compression=str(row.get("compression") or "none"),
        encryption=str(row.get("encryption") or "none"),
        duration_seconds=_safe_float_or_none(
            row.get("duration_seconds", row.get("duration"))
        ),
        created_at=str(row.get("created_at") or row.get("$createdAt") or ""),
    )


def _to_admin_restore_record(row: dict) -> AdminRestoreRecord:
    return AdminRestoreRecord(
        restore_id=str(row.get("$id") or row.get("restore_id") or ""),
        user_id=str(row.get("user_id") or ""),
        db_config_id=str(row.get("db_config_id") or ""),
        backup_id=str(row.get("backup_id") or ""),
        file_name=str(row.get("file_name") or ""),
        source=str(row.get("source") or ""),
        status=str(row.get("status") or ""),
        message=str(row.get("message") or ""),
        created_at=str(row.get("created_at") or row.get("$createdAt") or ""),
    )


def _to_admin_user_record(row: dict, assigned_databases: int = 0) -> AdminUserRecord:
    status = str(row.get("status") or "").strip().lower()
    if status not in {"active", "suspended"}:
        status = "active" if bool(row.get("is_active", True)) else "suspended"

    role = str(row.get("role") or "user").strip().lower()
    if role not in {"admin", "user"}:
        role = "user"

    return AdminUserRecord(
        user_id=str(row.get("user_id") or row.get("$id") or ""),
        email=str(row.get("email") or ""),
        name=str(row.get("name") or ""),
        role=role,
        status=status,
        is_active=bool(row.get("is_active", True) and status != "suspended"),
        phone=str(row.get("phone") or ""),
        bio=str(row.get("bio") or ""),
        assigned_databases=max(int(assigned_databases or 0), 0),
        created_at=str(row.get("created_at") or row.get("$createdAt") or ""),
        updated_at=str(row.get("updated_at") or row.get("$updatedAt") or ""),
    )


@router.get("/users", response_model=list[AdminUserRecord])
async def admin_list_users(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        result = await user_service.list_user_profiles(limit=limit, offset=offset)
        rows = result.get("rows", result.get("documents", []))

        database_counts: Counter[str] = Counter()
        db_rows = await _collect_all_databases()
        for db_row in db_rows:
            owner = str(db_row.get("user_id") or "").strip()
            if owner:
                database_counts[owner] += 1

        return [
            _to_admin_user_record(
                row,
                assigned_databases=database_counts.get(str(row.get("user_id") or row.get("$id") or ""), 0),
            )
            for row in rows
        ]
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/users/{user_id}", response_model=AdminUserRecord)
async def admin_get_user(
    user_id: str,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        row = await user_service.get_user_profile(user_id)
        if not row:
            return JSONResponse(status_code=404, content={"error": "User profile not found"})

        assigned_databases = 0
        try:
            db_result = await database_service.list_user_databases(user_id=user_id, limit=1, offset=0)
            assigned_databases = int(db_result.get("total", len(db_result.get("rows", []))))
        except Exception:
            assigned_databases = 0

        return _to_admin_user_record(row, assigned_databases=assigned_databases)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/users", response_model=AdminUserRecord)
async def admin_create_user(
    payload: AdminUserCreateRequest,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        normalized_email = user_service.normalize_email(str(payload.email))
        safe_password = prehash_for_appwrite(payload.password)

        auth_user = await asyncio.to_thread(
            users.create,
            user_id="unique()",
            email=normalized_email,
            password=safe_password,
            name=payload.name,
        )
        auth_user_data = auth_user.to_dict() if hasattr(auth_user, "to_dict") else auth_user
        created_user_id = str((auth_user_data or {}).get("$id") or "")
        if not created_user_id:
            return JSONResponse(status_code=500, content={"error": "Failed to create auth user"})

        profile = await user_service.create_user_profile(
            user_id=created_user_id,
            email=normalized_email,
            name=payload.name,
            phone=payload.phone,
            bio=payload.bio,
            role=payload.role,
            status=payload.status,
            is_active=(payload.status == "active"),
        )
        return _to_admin_user_record(profile)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.put("/users/{user_id}", response_model=AdminUserRecord)
async def admin_update_user(
    user_id: str,
    payload: AdminUserUpdateRequest,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        existing = await user_service.get_user_profile(user_id)
        if not existing:
            return JSONResponse(status_code=404, content={"error": "User profile not found"})

        updated = await user_service.update_user_profile(
            user_id=user_id,
            name=payload.name,
            phone=payload.phone,
            bio=payload.bio,
        )
        return _to_admin_user_record(updated)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.patch("/users/{user_id}", response_model=AdminUserRecord)
async def admin_edit_user(
    user_id: str,
    payload: AdminUserUpdateRequest,
    current_user: dict = Depends(require_admin_user),
):
    return await admin_update_user(user_id=user_id, payload=payload, current_user=current_user)


@router.patch("/users/{user_id}/role", response_model=AdminUserRecord)
async def admin_update_user_role(
    user_id: str,
    payload: AdminUserRoleUpdateRequest,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        existing = await user_service.get_user_profile(user_id)
        if not existing:
            return JSONResponse(status_code=404, content={"error": "User profile not found"})

        updated = await user_service.set_user_role(user_id=user_id, role=payload.role)
        return _to_admin_user_record(updated)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.patch("/users/{user_id}/status", response_model=AdminUserRecord)
async def admin_update_user_status(
    user_id: str,
    payload: AdminUserStatusUpdateRequest,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        existing = await user_service.get_user_profile(user_id)
        if not existing:
            return JSONResponse(status_code=404, content={"error": "User profile not found"})

        updated = await user_service.set_user_status(user_id=user_id, status=payload.status)
        return _to_admin_user_record(updated)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.delete("/users/{user_id}")
async def admin_delete_user(
    user_id: str,
    delete_auth: bool = Query(default=True, description="Also delete user from Appwrite Auth"),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        existing = await user_service.get_user_profile(user_id)
        if not existing:
            return JSONResponse(status_code=404, content={"error": "User profile not found"})

        await user_service.delete_user_profile(user_id)

        if delete_auth:
            try:
                await asyncio.to_thread(users.delete, user_id=user_id)
            except Exception:
                # Keep profile deletion idempotent even if auth user is missing.
                pass

        return {
            "message": "User deleted successfully",
            "user_id": user_id,
            "auth_deleted": bool(delete_auth),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/databases", response_model=list[AdminDatabaseRecord])
async def list_all_databases(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        result = await database_service.list_all_databases(limit=limit, offset=offset)
        rows = result.get("rows", result.get("documents", []))
        return [
            AdminDatabaseRecord(
                document_id=str(row.get("$id") or row.get("document_id") or ""),
                user_id=str(row.get("user_id") or ""),
                database_type=str(row.get("database_type") or ""),
                host=str(row.get("host") or ""),
                port=_safe_int(row.get("port"), 0),
                database_name=str(row.get("database_name") or ""),
                username=str(row.get("username") or ""),
                status=str(row.get("status") or "connected"),
                created_at=str(row.get("created_at") or row.get("$createdAt") or ""),
                updated_at=str(row.get("updated_at") or row.get("$updatedAt") or ""),
            )
            for row in rows
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/backups", response_model=list[AdminBackupRecord])
async def list_all_backups(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        result = await backup_service.list_all_backups(limit=limit, offset=offset)
        rows = result.get("rows", result.get("documents", []))
        return [_to_admin_backup_record(row) for row in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/databases/{document_id}/backup", response_model=TriggerBackupResponse)
async def admin_trigger_backup_for_database(
    document_id: str,
    request: Request,
    backup_type: str = Query(default="auto", description="auto | full | incremental"),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        db_doc = await database_service.get_user_database(document_id)
        if not db_doc:
            return JSONResponse(status_code=404, content={"error": "Database config not found"})

        owner_user_id = str(db_doc.get("user_id") or "")
        if not owner_user_id:
            return JSONResponse(status_code=400, content={"error": "Database owner user_id is missing"})

        doc = await backup_service.trigger_backup(
            db_config_id=document_id,
            user_id=owner_user_id,
            backup_type=backup_type,
            role="admin",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )

        if not doc.get("_success", True):
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Backup failed",
                    "detail": doc.get("_result_message", "Unknown error"),
                    "backup_id": doc.get("$id", ""),
                },
            )

        return TriggerBackupResponse(
            backup_id=doc["$id"],
            success=True,
            message=doc.get("_result_message", "Backup completed."),
            database_type=doc["database_type"],
            database_name=doc["database_name"],
            file_name=doc["file_name"],
            file_size=int(doc.get("file_size", 0)),
            compression=doc.get("compression", "none"),
            original_file_name=doc.get("original_file_name", ""),
            original_file_size=int(doc.get("original_file_size", 0) or 0),
            backup_type=doc.get("backup_type", "full"),
            base_backup_id=doc.get("base_backup_id") or None,
            status=doc["status"],
            created_at=doc["created_at"],
        )
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/backups/{backup_id}/download")
async def admin_download_backup(
    backup_id: str,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        doc = await backup_service.get_backup(backup_id)
        if not doc:
            return JSONResponse(status_code=404, content={"error": "Backup not found"})

        data = await backup_service.get_backup_file_bytes(doc)
        file_name = doc.get("file_name", "backup")
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
        )
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "Backup file not found."})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.delete("/backups/{backup_id}")
async def admin_delete_backup(
    backup_id: str,
    delete_file: bool = Query(default=False, description="Also delete file from Appwrite storage/local disk"),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        doc = await backup_service.get_backup(backup_id)
        if not doc:
            return JSONResponse(status_code=404, content={"error": "Backup not found"})

        await backup_service.delete_backup(backup_id, delete_file=delete_file)
        return {
            "message": "Backup record deleted successfully.",
            "backup_id": backup_id,
            "file_deleted": delete_file,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.delete("/databases/{document_id}")
async def admin_delete_database(
    document_id: str,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        doc = await database_service.get_user_database(document_id)
        if not doc:
            return JSONResponse(status_code=404, content={"error": "Database config not found"})

        await database_service.delete_user_database(document_id)
        return {
            "message": "Database configuration deleted successfully",
            "document_id": document_id,
            "user_id": str(doc.get("user_id") or ""),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/schedules", response_model=ScheduleOut)
async def admin_create_schedule(
    payload: ScheduleCreate,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        db_doc = await database_service.get_user_database(payload.db_config_id)
        if not db_doc:
            return JSONResponse(status_code=404, content={"error": "Database config not found"})

        owner_user_id = str(db_doc.get("user_id") or "")
        if not owner_user_id:
            return JSONResponse(status_code=400, content={"error": "Database owner user_id is missing"})

        return await schedule_service.create_schedule(
            user_id=owner_user_id,
            frequency=payload.frequency,
            db_config_id=payload.db_config_id,
            time_str=payload.time,
            day_of_week=payload.day_of_week,
            cron_expression=payload.cron_expression,
            timezone_str=payload.timezone,
            enabled=payload.enabled,
            description=payload.description,
        )
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/schedules", response_model=list[ScheduleOut])
async def admin_list_admin_schedules(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        return await schedule_service.list_admin_schedules(limit=limit, offset=offset)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.delete("/schedules/{schedule_id}")
async def admin_delete_schedule(
    schedule_id: str,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        await schedule_service.delete_schedule_admin(schedule_id)
        return {"message": "Schedule removed", "schedule_id": schedule_id}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/restores", response_model=list[AdminRestoreRecord])
async def list_all_restores(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        result = await backup_service.list_all_restores(limit=limit, offset=offset)
        rows = result.get("rows", result.get("documents", []))
        return [_to_admin_restore_record(row) for row in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/backups/{backup_id}/restore", tags=["Restore"])
async def admin_restore_backup_from_record(
    backup_id: str,
    request: Request,
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        backup_doc = await backup_service.get_backup(backup_id)
        if not backup_doc:
            return JSONResponse(status_code=404, content={"error": "Backup not found"})

        owner_user_id = str(backup_doc.get("user_id") or "")
        if not owner_user_id:
            return JSONResponse(status_code=400, content={"error": "Backup owner user_id is missing"})

        result = await backup_service.restore_backup_from_record(
            backup_id=backup_id,
            user_id=owner_user_id,
            role="admin",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )
        status = 200 if result.get("success") else 400
        return JSONResponse(status_code=status, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/backups/restore/upload", tags=["Restore"])
async def admin_restore_backup_from_upload(
    request: Request,
    db_config_id: str = Form(..., description="Target database config id"),
    file: UploadFile = File(..., description="Backup file to restore"),
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        db_doc = await database_service.get_user_database(db_config_id)
        if not db_doc:
            return JSONResponse(status_code=404, content={"error": "Database config not found"})

        owner_user_id = str(db_doc.get("user_id") or "")
        if not owner_user_id:
            return JSONResponse(status_code=400, content={"error": "Database owner user_id is missing"})

        result = await backup_service.restore_backup_from_upload(
            db_config_id=db_config_id,
            user_id=owner_user_id,
            upload_file=file,
            role="admin",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )
        status = 200 if result.get("success") else 400
        return JSONResponse(status_code=status, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/dashboard")
async def admin_dashboard(
    current_user: dict = Depends(require_admin_user),
):
    del current_user

    try:
        db_rows = await _collect_all_databases()
        backup_rows = await _collect_all_backups()

        # Counts
        total_databases = len(db_rows)
        total_backups = len(backup_rows)

        failed_backups = len([
            b for b in backup_rows
            if b.get("status", "").lower() == "failed"
        ])

        success_backups = len([
            b for b in backup_rows
            if b.get("status", "").lower() == "success"
        ])

        running_jobs = len([
            b for b in backup_rows
            if b.get("status", "").lower() == "running"
        ])

        # Success %
        success_rate = 0
        if total_backups > 0:
            success_rate = round((success_backups / total_backups) * 100)

        # Storage used across all users + per-user breakdown.
        total_size = 0
        user_storage: dict[str, dict] = {}
        for row in backup_rows:
            user_id = str(row.get("user_id", "") or "unknown")
            size = _safe_int(row.get("file_size", 0))
            total_size += size

            bucket = user_storage.setdefault(user_id, {"user_id": user_id, "backup_count": 0, "storage_used_bytes": 0})
            bucket["backup_count"] += 1
            bucket["storage_used_bytes"] += size

        storage_gb = round(total_size / (1024 ** 3), 2)
        user_storage_usage = []
        for item in user_storage.values():
            bytes_used = item["storage_used_bytes"]
            user_storage_usage.append(
                {
                    "user_id": item["user_id"],
                    "backup_count": item["backup_count"],
                    "storage_used_bytes": bytes_used,
                    "storage_used_mb": round(bytes_used / (1024 ** 2), 2),
                    "storage_used_gb": round(bytes_used / (1024 ** 3), 4),
                }
            )
        user_storage_usage.sort(key=lambda x: x["storage_used_bytes"], reverse=True)
        storage_monitoring = await _build_storage_monitoring(backup_rows=backup_rows, db_rows=db_rows)

        return {
            "total_databases": total_databases,
            "total_backups": total_backups,
            "success_rate": success_rate,
            "failed_backups": failed_backups,
            "storage_used": _format_storage_label(total_size),
            "storage_used_bytes": total_size,
            "storage_used_mb": round(total_size / (1024 ** 2), 2),
            "active_jobs": running_jobs,
            "user_storage_usage": user_storage_usage,
            "storage_monitoring": storage_monitoring,
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@router.get("/storage/monitoring", response_model=AdminStorageMonitoringResponse)
async def admin_storage_monitoring(
    current_user: dict = Depends(require_admin_user),
):
    del current_user
    try:
        db_rows = await _collect_all_databases()
        backup_rows = await _collect_all_backups()
        return await _build_storage_monitoring(backup_rows=backup_rows, db_rows=db_rows)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


