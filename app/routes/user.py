from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from datetime import datetime, timezone

from app.schemas.user import (
    CreateUserProfile,
    UpdateUserProfile,
    UserProfileResponse,
    UserDashboardResponse,
    UserDashboardDatabase,
    UserDashboardBackup,
)
from app.services import user_service, database_service, backup_service
from app.utils.dependencies import get_current_user

router = APIRouter(prefix="/users", tags=["Users"])


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _match_backup_filters(
    row: dict,
    status_filter: str | None,
    start_dt: datetime | None,
    end_dt: datetime | None,
) -> bool:
    if status_filter:
        row_status = str(row.get("status", "")).strip().lower()
        if row_status != status_filter:
            return False

    if start_dt or end_dt:
        created_at = str(row.get("created_at", "")).strip()
        if not created_at:
            return False
        try:
            row_dt = _parse_iso_datetime(created_at)
        except ValueError:
            return False
        if row_dt is None:
            return False
        if start_dt and row_dt < start_dt:
            return False
        if end_dt and row_dt > end_dt:
            return False

    return True


@router.get("/dashboard", response_model=UserDashboardResponse)
async def my_dashboard(
    backup_limit: int = Query(default=25, ge=1, le=200),
    backup_offset: int = Query(default=0, ge=0),
    backup_status: str | None = Query(default=None, description="Filter by backup status (e.g. success, failed, running)"),
    start_date: str | None = Query(default=None, description="Filter backups from this ISO datetime (inclusive)"),
    end_date: str | None = Query(default=None, description="Filter backups up to this ISO datetime (inclusive)"),
    db_limit: int = Query(default=100, ge=1, le=200),
    db_offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    """Return dashboard data scoped to the current user; metrics follow backup filters when provided."""
    try:
        user_id = current_user["user_id"]
        status_filter = (backup_status or "").strip().lower() or None

        try:
            start_dt = _parse_iso_datetime(start_date)
            end_dt = _parse_iso_datetime(end_date)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid date format. Use ISO format like 2026-04-14T10:00:00Z"},
            )

        if start_dt and end_dt and start_dt > end_dt:
            return JSONResponse(
                status_code=400,
                content={"error": "start_date cannot be later than end_date"},
            )

        db_result = await database_service.list_user_databases(
            user_id=user_id,
            limit=db_limit,
            offset=db_offset,
        )
        db_rows = db_result.get("rows", db_result.get("documents", []))

        backup_result = await backup_service.list_backups(
            user_id=user_id,
            limit=backup_limit,
            offset=backup_offset,
        )
        backup_rows = backup_result.get("rows", backup_result.get("documents", []))
        backup_rows = [
            row
            for row in backup_rows
            if _match_backup_filters(row, status_filter, start_dt, end_dt)
        ]

        # Aggregate all-backup metrics and filtered metrics in one pass.
        total_backups = 0
        storage_used_bytes = 0
        last_backup_time = None

        filtered_total_backups = 0
        filtered_storage_used_bytes = 0
        metric_offset = 0
        metric_limit = 100
        filtered_last_backup_time = None

        while True:
            metric_result = await backup_service.list_backups(
                user_id=user_id,
                limit=metric_limit,
                offset=metric_offset,
            )
            metric_rows = metric_result.get("rows", metric_result.get("documents", []))
            if not metric_rows:
                break

            total_backups += len(metric_rows)
            storage_used_bytes += sum(_safe_int(row.get("file_size", 0)) for row in metric_rows)
            if last_backup_time is None and metric_rows:
                last_backup_time = metric_rows[0].get("created_at")

            filtered_metric_rows = [
                row
                for row in metric_rows
                if _match_backup_filters(row, status_filter, start_dt, end_dt)
            ]

            if filtered_last_backup_time is None and filtered_metric_rows:
                filtered_last_backup_time = filtered_metric_rows[0].get("created_at")

            filtered_total_backups += len(filtered_metric_rows)
            filtered_storage_used_bytes += sum(_safe_int(row.get("file_size", 0)) for row in filtered_metric_rows)

            if len(metric_rows) < metric_limit:
                break
            metric_offset += metric_limit

        return UserDashboardResponse(
            user_id=user_id,
            total_databases=len(db_rows),
            total_backups=total_backups,
            last_backup_time=last_backup_time,
            storage_used_bytes=storage_used_bytes,
            storage_used_mb=round(storage_used_bytes / (1024 * 1024), 2),
            filtered_total_backups=filtered_total_backups,
            filtered_last_backup_time=filtered_last_backup_time,
            filtered_storage_used_bytes=filtered_storage_used_bytes,
            filtered_storage_used_mb=round(filtered_storage_used_bytes / (1024 * 1024), 2),
            my_databases=[
                UserDashboardDatabase(
                    document_id=str(doc.get("$id", "")),
                    database_type=str(doc.get("database_type", "")),
                    host=str(doc.get("host", "")),
                    port=_safe_int(doc.get("port", 0)),
                    database_name=str(doc.get("database_name", "")),
                    username=str(doc.get("username", "")),
                    status=str(doc.get("status", "connected")),
                    created_at=str(doc.get("created_at", "")),
                )
                for doc in db_rows
            ],
            my_backups=[
                UserDashboardBackup(
                    backup_id=str(doc.get("$id", "")),
                    db_config_id=str(doc.get("db_config_id", "")),
                    database_type=str(doc.get("database_type", "")),
                    database_name=str(doc.get("database_name", "")),
                    file_name=str(doc.get("file_name", "")),
                    file_size=_safe_int(doc.get("file_size", 0)),
                    file_size_mb=round(_safe_int(doc.get("file_size", 0)) / (1024 * 1024), 2),
                    status=str(doc.get("status", "")),
                    created_at=str(doc.get("created_at", "")),
                )
                for doc in backup_rows
            ],
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/profile", response_model=UserProfileResponse)
async def create_profile(
    payload: CreateUserProfile,
    current_user: dict = Depends(get_current_user),
):
    """Create a database profile for the currently authenticated user."""
    try:
        # Check if profile already exists
        existing = await user_service.get_user_profile(current_user["user_id"])
        if existing:
            return JSONResponse(
                status_code=409,
                content={"error": "Profile already exists for this user"},
            )

        doc = await user_service.create_user_profile(
            user_id=current_user["user_id"],
            email=current_user["email"],
            name=current_user["name"],
            phone=payload.phone,
            bio=payload.bio,
        )
        return UserProfileResponse(
            user_id=doc["user_id"],
            email=doc["email"],
            name=doc["name"],
            phone=doc.get("phone"),
            bio=doc.get("bio"),
            is_active=doc.get("is_active", True),
            created_at=doc.get("created_at", ""),
            updated_at=doc.get("updated_at", ""),
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/profile", response_model=UserProfileResponse)
async def get_my_profile(current_user: dict = Depends(get_current_user)):
    """Fetch the current user's database profile."""
    try:
        doc = await user_service.get_user_profile(current_user["user_id"])
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Profile not found"}
            )
        return UserProfileResponse(
            user_id=doc["user_id"],
            email=doc["email"],
            name=doc["name"],
            phone=doc.get("phone"),
            bio=doc.get("bio"),
            is_active=doc.get("is_active", True),
            created_at=doc.get("created_at", ""),
            updated_at=doc.get("updated_at", ""),
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.put("/profile", response_model=UserProfileResponse)
async def update_my_profile(
    payload: UpdateUserProfile,
    current_user: dict = Depends(get_current_user),
):
    """Update the current user's database profile."""
    try:
        doc = await user_service.update_user_profile(
            user_id=current_user["user_id"],
            name=payload.name,
            phone=payload.phone,
            bio=payload.bio,
        )
        return UserProfileResponse(
            user_id=doc["user_id"],
            email=doc["email"],
            name=doc["name"],
            phone=doc.get("phone"),
            bio=doc.get("bio"),
            is_active=doc.get("is_active", True),
            created_at=doc.get("created_at", ""),
            updated_at=doc.get("updated_at", ""),
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.delete("/profile")
async def delete_my_profile(current_user: dict = Depends(get_current_user)):
    """Delete the current user's database profile."""
    try:
        await user_service.delete_user_profile(current_user["user_id"])
        return {"message": "Profile deleted successfully"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/", response_model=list[UserProfileResponse])
async def list_profiles(
    limit: int = 25,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """List all user profiles (protected)."""
    try:
        result = await user_service.list_user_profiles(limit=limit, offset=offset)
        return [
            UserProfileResponse(
                user_id=doc["user_id"],
                email=doc["email"],
                name=doc["name"],
                phone=doc.get("phone"),
                bio=doc.get("bio"),
                is_active=doc.get("is_active", True),
                created_at=doc.get("created_at", ""),
                updated_at=doc.get("updated_at", ""),
            )
            for doc in result.get("rows", result.get("documents", []))
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

