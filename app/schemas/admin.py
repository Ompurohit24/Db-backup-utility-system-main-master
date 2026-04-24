from typing import Literal, Optional

from pydantic import BaseModel, EmailStr, Field


class AdminDatabaseRecord(BaseModel):
    document_id: str
    user_id: str
    database_type: str
    host: str
    port: int
    database_name: str
    username: str
    status: str
    created_at: str = ""
    updated_at: str = ""


class AdminBackupRecord(BaseModel):
    backup_id: str
    db_config_id: str
    user_id: str
    database_type: str
    database_name: str
    file_name: str
    file_size: int
    status: str
    compression: str = "none"
    encryption: str = "none"
    duration_seconds: float | None = None
    created_at: str = ""


class AdminRestoreRecord(BaseModel):
    restore_id: str
    user_id: str
    db_config_id: str
    backup_id: str = ""
    file_name: str = ""
    source: str = ""
    status: str = ""
    message: str = ""
    created_at: str = ""


UserRole = Literal["admin", "user"]
UserStatus = Literal["active", "suspended"]


class AdminUserRecord(BaseModel):
    user_id: str
    email: EmailStr
    name: str
    role: UserRole = "user"
    status: UserStatus = "active"
    is_active: bool = True
    phone: str = ""
    bio: str = ""
    assigned_databases: int = 0
    created_at: str = ""
    updated_at: str = ""


class AdminUserCreateRequest(BaseModel):
    email: EmailStr
    name: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=8)
    role: UserRole = "user"
    status: UserStatus = "active"
    phone: Optional[str] = None
    bio: Optional[str] = None


class AdminUserUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    bio: Optional[str] = None


class AdminUserRoleUpdateRequest(BaseModel):
    role: UserRole


class AdminUserStatusUpdateRequest(BaseModel):
    status: UserStatus


class AdminDatabaseStorageMetrics(BaseModel):
    db_config_id: str
    database_name: str = ""
    user_id: str = ""
    backup_count: int = 0
    storage_used_bytes: int = 0
    storage_used_mb: float = 0
    storage_used_gb: float = 0
    average_backup_size_bytes: float = 0
    average_backup_size_mb: float = 0
    growth_rate_percent_7d: float | None = None
    growth_delta_bytes_7d: int = 0


class AdminStorageMonitoringResponse(BaseModel):
    total_appwrite_storage_bytes: int | None = None
    total_appwrite_storage: str = "Unknown"
    total_storage_used_bytes: int = 0
    total_storage_used: str = "0 MB"
    storage_available_bytes: int | None = None
    storage_available: str = "Unknown"
    average_backup_size_bytes: float = 0
    average_backup_size_mb: float = 0
    growth_rate_percent_7d: float | None = None
    growth_delta_bytes_7d: int = 0
    quota_available: bool = False
    quota_source: str = "unknown"
    database_storage_usage: list[AdminDatabaseStorageMetrics] = Field(default_factory=list)


