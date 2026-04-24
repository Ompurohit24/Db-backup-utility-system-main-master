"""File backup API endpoints."""
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.schemas.file_backup import (
    FileBackupRequest,
    FileBackupResponse,
    FileRestoreRequest,
    FileRestoreResponse,
    FileBackupRecord,
)
from app.services import file_backup_service
from app.utils.dependencies import get_current_user

router = APIRouter(prefix="/file-backups", tags=["File Backups"])


@router.get("/ping")
async def ping():
    return {"status": "ok"}


@router.post("/backup", response_model=FileBackupResponse)
async def start_backup(payload: FileBackupRequest, current_user: dict = Depends(get_current_user)):
    try:
        result = file_backup_service.create_backup(
            source_path=payload.source_path,
            destination_dir=payload.destination_dir,
        )
        status_code = 200 if result.get("success") else 400
        return JSONResponse(status_code=status_code, content=result)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@router.post("/restore", response_model=FileRestoreResponse)
async def restore_backup(payload: FileRestoreRequest, current_user: dict = Depends(get_current_user)):
    try:
        result = file_backup_service.restore_backup(
            backup_file=payload.backup_file,
            target_path=payload.target_path,
        )
        status_code = 200 if result.get("success") else 400
        return JSONResponse(status_code=status_code, content=result)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@router.get("", response_model=list[FileBackupRecord])
async def list_backups(current_user: dict = Depends(get_current_user)):
    records = file_backup_service.list_backups()
    return [FileBackupRecord(**r) for r in records]


@router.delete("/{file_name}")
async def delete_backup(file_name: str, current_user: dict = Depends(get_current_user)):
    try:
        result = file_backup_service.delete_backup(file_name)
        status_code = 200 if result.get("success") else 404
        return JSONResponse(status_code=status_code, content=result)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

