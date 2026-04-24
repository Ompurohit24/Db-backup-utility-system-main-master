"""
Backup routes.

POST   /databases/{document_id}/backup  →  trigger a backup for a saved DB config
GET    /databases/{document_id}/backups →  list all backups for a saved DB config
GET    /backups                         →  list ALL backups for the current user
GET    /backups/{backup_id}             →  get metadata for a single backup
DELETE /backups/{backup_id}             →  delete backup record (+ file optionally)
GET    /backups/{backup_id}/download    →  download the backup file
"""

from fastapi import APIRouter, Depends, Query as QParam, UploadFile, File, Form
from fastapi.responses import JSONResponse, Response
from fastapi import Request

from app.schemas.backup import TriggerBackupResponse, BackupRecord
from app.services import backup_service
from app.services import notification_service
from app.utils.dependencies import get_current_user
from app.utils.ownership import get_owner_user_id

router = APIRouter(tags=["Backups"])


# ── POST /databases/{document_id}/backup ─────────────────────────────

@router.post(
    "/databases/{document_id}/backup",
    response_model=TriggerBackupResponse,
)
async def trigger_backup(
    document_id: str,
    request: Request,
    backup_type: str = QParam(default="auto", description="auto | full | incremental"),
    current_user: dict = Depends(get_current_user),
):
    """
    Trigger a backup for a saved database configuration.

    - Connects to the external database using the stored (encrypted) credentials.
    - Dumps the full database to a file in the **DB/** folder.
    - Saves backup metadata to Appwrite.
    - Returns backup details including file name and size.
    """
    try:
        doc = await backup_service.trigger_backup(
            db_config_id=document_id,
            user_id=current_user["user_id"],
            backup_type=backup_type,
            role="user",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )

        if not doc.get("_success", True):
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Backup failed",
                    "detail": doc.get("_result_message", "Unknown error"),
                    "backup_id": doc["$id"],
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


# ── GET /databases/{document_id}/backups ─────────────────────────────

@router.get(
    "/databases/{document_id}/backups",
    response_model=list[BackupRecord],
)
async def list_backups_for_database(
    document_id: str,
    limit: int = QParam(default=25, ge=1, le=100),
    offset: int = QParam(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    """List all backup records for a specific saved database configuration."""
    try:
        result = await backup_service.list_backups(
            user_id=current_user["user_id"],
            db_config_id=document_id,
            limit=limit,
            offset=offset,
        )
        rows = result.get("rows", result.get("documents", []))
        return [_to_backup_record(b) for b in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /backups ──────────────────────────────────────────────────────

@router.get("/backups", response_model=list[BackupRecord])
async def list_all_backups(
    limit: int = QParam(default=25, ge=1, le=100),
    offset: int = QParam(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    """List ALL backup records for the current user across all databases."""
    try:
        result = await backup_service.list_backups(
            user_id=current_user["user_id"],
            limit=limit,
            offset=offset,
        )
        rows = result.get("rows", result.get("documents", []))
        return [_to_backup_record(b) for b in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /backups/{backup_id} ──────────────────────────────────────────

@router.get("/backups/{backup_id}", response_model=BackupRecord)
async def get_backup(
    backup_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Get metadata for a single backup record."""
    try:
        doc = await backup_service.get_backup(backup_id)
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Backup not found"}
            )
        if get_owner_user_id(doc) != current_user["user_id"]:
            return JSONResponse(
                status_code=403, content={"error": "Access denied"}
            )
        return _to_backup_record(doc)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── DELETE /backups/{backup_id} ───────────────────────────────────────

@router.delete("/backups/{backup_id}")
async def delete_backup(
    backup_id: str,
    delete_file: bool = QParam(
        default=False,
        description="Also delete the backup file from disk",
    ),
    current_user: dict = Depends(get_current_user),
):
    """
    Delete a backup record from Appwrite.
    Pass `?delete_file=true` to also remove the backup file from the server disk.
    """
    try:
        doc = await backup_service.get_backup(backup_id)
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Backup not found"}
            )
        if get_owner_user_id(doc) != current_user["user_id"]:
            return JSONResponse(
                status_code=403, content={"error": "Access denied"}
            )
        await backup_service.delete_backup(backup_id, delete_file=delete_file)
        await notification_service.create_notification(
            user_id=current_user["user_id"],
            event_type="backup_deleted",
            level="warning",
            title="Backup Deleted",
            message=f"Backup '{doc.get('file_name', backup_id)}' deleted successfully.",
        )
        return {"message": "Backup record deleted successfully."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── POST /backups/{backup_id}/restore (restore from stored record) ────


@router.post("/backups/{backup_id}/restore", tags=["Restore"])
async def restore_backup_from_record(
    backup_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    try:
        result = await backup_service.restore_backup_from_record(
            backup_id=backup_id,
            user_id=current_user["user_id"],
            role="user",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )
        status = 200 if result.get("success") else 400
        return JSONResponse(status_code=status, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── POST /backups/restore/upload (restore from uploaded file) ─────────


@router.post("/backups/restore/upload", tags=["Restore"])
async def restore_backup_from_upload(
    request: Request,
    db_config_id: str = Form(..., description="Target database config id"),
    file: UploadFile = File(..., description="Backup file to restore (.sql or .json)"),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = await backup_service.restore_backup_from_upload(
            db_config_id=db_config_id,
            user_id=current_user["user_id"],
            upload_file=file,
            role="user",
            ip_address=request.client.host if request.client else None,
            device_info=request.headers.get("user-agent", ""),
        )
        status = 200 if result.get("success") else 400
        return JSONResponse(status_code=status, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /backups/{backup_id}/download ────────────────────────────────

@router.get("/backups/{backup_id}/download")
async def download_backup(
    backup_id: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Download the raw backup file for a given backup record.
    Returns the .sql or .json file as an attachment.
    """
    try:
        doc = await backup_service.get_backup(backup_id)
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Backup not found"}
            )
        if get_owner_user_id(doc) != current_user["user_id"]:
            return JSONResponse(
                status_code=403, content={"error": "Access denied"}
            )

        data = await backup_service.get_backup_file_bytes(doc)
        file_name = doc.get("file_name", "backup")
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{file_name}"',
            },
        )
    except FileNotFoundError:
        return JSONResponse(
            status_code=404,
            content={"error": "Backup file not found."},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── Helper ────────────────────────────────────────────────────────────

# def _to_backup_record(doc: dict) -> BackupRecord:
#     return BackupRecord(
#         backup_id=doc["$id"],
#         db_config_id=doc["db_config_id"],
#         owner_user_id=get_owner_user_id(doc),
#         user_id=get_owner_user_id(doc),
#         database_type=doc["database_type"],
#         database_name=doc["database_name"],
#         file_name=doc["file_name"],
#         file_path=doc.get("file_path", ""),
#         file_id=doc.get("file_id", ""),
#         storage_bucket=doc.get("storage_bucket", ""),
#         file_size=int(doc.get("file_size", 0)),
#         compression=doc.get("compression", "none"),
#         original_file_name=doc.get("original_file_name", ""),
#         original_file_size=int(doc.get("original_file_size", 0) or 0),
#         backup_type=doc.get("backup_type", "full"),
#         base_backup_id=doc.get("base_backup_id") or None,
#         status=doc["status"],
#         error_message=doc.get("error_message") or None,
#         created_at=doc["created_at"],
#     )

def _safe_str(value, default=""):
    if value is None:
        return default
    return str(value)


def _safe_float_or_none(value):
    if value is None or value == "":
        return None
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return None

def _to_backup_record(doc: dict) -> BackupRecord:
    return BackupRecord(
        backup_id=_safe_str(doc.get("$id")),
        db_config_id=_safe_str(doc.get("db_config_id")),
        owner_user_id=_safe_str(get_owner_user_id(doc)),
        user_id=_safe_str(get_owner_user_id(doc)),

        database_type=_safe_str(doc.get("database_type"), "unknown"),
        database_name=_safe_str(doc.get("database_name"), "Unknown DB"),

        file_name=_safe_str(doc.get("file_name"), "backup.sql"),
        file_path=_safe_str(doc.get("file_path")),
        file_id=_safe_str(doc.get("file_id")),
        storage_bucket=_safe_str(doc.get("storage_bucket")),

        file_size=int(doc.get("file_size", 0) or 0),
        duration_seconds=_safe_float_or_none(
            doc.get("duration_seconds", doc.get("duration"))
        ),

        compression=_safe_str(doc.get("compression"), "none"),

        original_file_name=_safe_str(doc.get("original_file_name")),
        original_file_size=int(doc.get("original_file_size", 0) or 0),

        backup_type=_safe_str(doc.get("backup_type"), "full"),
        base_backup_id=_safe_str(doc.get("base_backup_id")) or None,

        status=_safe_str(doc.get("status"), "Success"),
        error_message=_safe_str(doc.get("error_message")) or None,

        created_at=_safe_str(doc.get("created_at")),
    )