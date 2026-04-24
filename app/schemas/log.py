"""
Pydantic models for backup/restore operation logs.
"""
from typing import Optional, Literal
from pydantic import BaseModel, Field


class LogRecord(BaseModel):
    log_id: str = Field(..., description="Log row id")
    user_id: str
    role: str
    operation_type: Literal["backup", "restore"]
    status: Literal["started", "success", "failed"]
    database_name: Optional[str] = None
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    start_time: str
    end_time: Optional[str] = None
    duration: Optional[float] = None
    error_message: Optional[str] = None
    ip_address: Optional[str] = None
    device_info: Optional[str] = None
    db_config_id: Optional[str] = None
    backup_id: Optional[str] = None
    restore_id: Optional[str] = None
    created_at: str


class LogCreateRequest(BaseModel):
    operation_type: Literal["backup", "restore"]
    status: Literal["started", "success", "failed"] = "started"
    database_name: Optional[str] = None
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration: Optional[float] = None
    error_message: Optional[str] = None
    ip_address: Optional[str] = None
    device_info: Optional[str] = None
    role: str = "user"
    db_config_id: Optional[str] = None
    backup_id: Optional[str] = None
    restore_id: Optional[str] = None


class LogUpdateRequest(BaseModel):
    status: Optional[Literal["started", "success", "failed"]] = None
    database_name: Optional[str] = None
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration: Optional[float] = None
    error_message: Optional[str] = None
    ip_address: Optional[str] = None
    device_info: Optional[str] = None
    role: Optional[str] = None
    db_config_id: Optional[str] = None
    backup_id: Optional[str] = None
    restore_id: Optional[str] = None


class LogListResponse(BaseModel):
    total: int
    logs: list[LogRecord]


class LogSummaryResponse(BaseModel):
    total: int
    by_status: dict
    by_operation: dict
    average_duration_seconds: float | None

