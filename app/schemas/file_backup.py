"""Pydantic schemas for file/folder backup operations."""
from pydantic import BaseModel, Field
from typing import Optional


class FileBackupRequest(BaseModel):
    source_path: str = Field(..., description="Absolute or relative path to file/folder to back up")
    destination_dir: Optional[str] = Field(None, description="Optional destination directory for backups")


class FileBackupResponse(BaseModel):
    success: bool
    message: str
    backup_file: Optional[str] = None
    size_bytes: Optional[int] = None
    duration_seconds: Optional[float] = None


class FileRestoreRequest(BaseModel):
    backup_file: str = Field(..., description="Path to the backup archive to restore")
    target_path: str = Field(..., description="Destination directory to extract into")


class FileRestoreResponse(BaseModel):
    success: bool
    message: str
    restored_to: Optional[str] = None
    duration_seconds: Optional[float] = None


class FileBackupRecord(BaseModel):
    file_name: str
    full_path: str
    size_bytes: int
    modified_at: float

