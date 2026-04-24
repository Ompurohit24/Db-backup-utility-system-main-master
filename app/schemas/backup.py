"""
Pydantic schemas for the backup API endpoints.
"""

from pydantic import BaseModel
from typing import Optional


class TriggerBackupResponse(BaseModel):
    """
    Returned after POST /databases/{document_id}/backup

    Success example:
    {
        "backup_id": "abc123",
        "success": true,
        "message": "MySQL backup of 'my_db' completed successfully.",
        "database_type": "mysql",
        "database_name": "my_db",
        "file_name": "mysql_my_db_20260314_120000.sql",
        "file_size": 204800,
        "status": "success",
        "created_at": "2026-03-14T12:00:00+00:00"
    }
    """
    backup_id: str
    success: bool
    message: str
    database_type: str
    database_name: str
    file_name: str
    file_size: int
    compression: str
    original_file_name: str
    original_file_size: int
    backup_type: str = "full"
    base_backup_id: Optional[str] = None
    status: str
    created_at: str


class BackupRecord(BaseModel):
    """
    A single backup entry returned by the list / get endpoints.
    """
    backup_id: str
    db_config_id: str
    owner_user_id: str
    user_id: str = ""
    database_type: str
    database_name: str
    file_name: str
    file_path: str
    file_id: str = ""
    storage_bucket: str = ""
    file_size: int
    duration_seconds: Optional[float] = None
    compression: str = "none"
    original_file_name: str = ""
    original_file_size: int = 0
    backup_type: str = "full"
    base_backup_id: Optional[str] = None
    status: str
    error_message: Optional[str] = None
    created_at: str

