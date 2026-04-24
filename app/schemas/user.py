from pydantic import BaseModel
from typing import Optional


class CreateUserProfile(BaseModel):
    """Schema for creating a user profile document in the database."""
    phone: Optional[str] = None
    bio: Optional[str] = None


class UpdateUserProfile(BaseModel):
    """Schema for updating a user profile document."""
    name: Optional[str] = None
    phone: Optional[str] = None
    bio: Optional[str] = None


class UserProfileResponse(BaseModel):
    """Schema for returning user profile data."""
    user_id: str
    email: str
    name: str
    phone: Optional[str] = None
    bio: Optional[str] = None
    is_active: bool = True
    created_at: str = ""
    updated_at: str = ""


class UserDashboardDatabase(BaseModel):
    document_id: str
    database_type: str
    host: str
    port: int
    database_name: str
    username: str
    status: str = "connected"
    created_at: str = ""


class UserDashboardBackup(BaseModel):
    backup_id: str
    db_config_id: str
    database_type: str
    database_name: str
    file_name: str
    file_size: int
    file_size_mb: float = 0
    status: str
    created_at: str


class UserDashboardResponse(BaseModel):
    user_id: str
    total_databases: int
    total_backups: int
    last_backup_time: str | None = None
    storage_used_bytes: int
    storage_used_mb: float
    filtered_total_backups: int = 0
    filtered_last_backup_time: str | None = None
    filtered_storage_used_bytes: int = 0
    filtered_storage_used_mb: float = 0
    my_databases: list[UserDashboardDatabase]
    my_backups: list[UserDashboardBackup]


