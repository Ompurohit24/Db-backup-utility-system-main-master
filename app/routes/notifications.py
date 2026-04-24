from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from app.schemas.notification import (
    NotificationListResponse,
    NotificationMarkAllReadResponse,
    NotificationMarkReadResponse,
    NotificationRecord,
)
from app.services import notification_service
from app.utils.dependencies import get_current_user
from app.utils.ownership import get_owner_user_id

router = APIRouter(prefix="/notifications", tags=["Notifications"])


def _normalize_level(value: str) -> str:
    level = str(value or "info").strip().lower()
    if level in {"info", "success", "warning", "error"}:
        return level
    return "info"


def _to_notification_record(row: dict) -> NotificationRecord:
    return NotificationRecord(
        notification_id=str(row.get("notification_id") or row.get("$id") or ""),
        user_id=str(get_owner_user_id(row) or row.get("user_id") or ""),
        event_type=str(row.get("event_type") or "event"),
        level=_normalize_level(str(row.get("level") or "info")),
        title=str(row.get("title") or "Notification"),
        message=str(row.get("message") or ""),
        is_read=bool(row.get("is_read", False)),
        resource_id=str(row.get("resource_id") or "") or None,
        created_at=str(row.get("$createdAt") or row.get("created_at") or ""),
    )


@router.get("", response_model=NotificationListResponse)
async def list_my_notifications(
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    unread_only: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = await notification_service.list_notifications(
            user_id=current_user["user_id"],
            user_email=current_user.get("email", ""),
            limit=limit,
            offset=offset,
            unread_only=unread_only,
        )
        rows = result.get("rows", [])
        unread_count = sum(1 for item in rows if not bool(item.get("is_read", False)))
        return NotificationListResponse(
            total=int(result.get("total", len(rows)) or 0),
            unread_count=unread_count,
            notifications=[_to_notification_record(row) for row in rows],
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.patch("/{notification_id}/read", response_model=NotificationMarkReadResponse)
async def mark_notification_read(
    notification_id: str,
    current_user: dict = Depends(get_current_user),
):
    try:
        row = await notification_service.get_notification(notification_id)
        if not row:
            return JSONResponse(status_code=404, content={"error": "Notification not found"})

        if str(get_owner_user_id(row) or row.get("user_id") or "") != current_user["user_id"]:
            return JSONResponse(status_code=403, content={"error": "Access denied"})

        success = await notification_service.mark_notification_as_read(notification_id)
        if not success:
            return JSONResponse(status_code=500, content={"error": "Could not update notification"})

        return NotificationMarkReadResponse(success=True, notification_id=notification_id)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.patch("/read-all", response_model=NotificationMarkAllReadResponse)
async def mark_all_my_notifications_read(
    current_user: dict = Depends(get_current_user),
):
    try:
        updated = await notification_service.mark_all_notifications_as_read(
            current_user["user_id"],
            current_user.get("email", ""),
        )
        return NotificationMarkAllReadResponse(success=True, updated_count=updated)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

