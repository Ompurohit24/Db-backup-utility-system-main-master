from typing import Literal, Optional

from pydantic import BaseModel, Field


NotificationLevel = Literal["info", "success", "warning", "error"]


class NotificationRecord(BaseModel):
    notification_id: str
    user_id: str
    event_type: str
    level: NotificationLevel
    title: str
    message: str
    is_read: bool
    resource_id: Optional[str] = None
    created_at: str


class NotificationListResponse(BaseModel):
    total: int = Field(default=0, ge=0)
    unread_count: int = Field(default=0, ge=0)
    notifications: list[NotificationRecord]


class NotificationMarkReadResponse(BaseModel):
    success: bool
    notification_id: str


class NotificationMarkAllReadResponse(BaseModel):
    success: bool
    updated_count: int

