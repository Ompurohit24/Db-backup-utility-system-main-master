from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.schemas.schedule import ScheduleCreate, ScheduleOut, ScheduleToggle
from app.services import schedule_service
from app.utils.dependencies import get_current_user


router = APIRouter(prefix="/schedules", tags=["Schedules"])


@router.post("", response_model=ScheduleOut)
async def create_schedule(payload: ScheduleCreate, current_user: dict = Depends(get_current_user)):
    try:
        return await schedule_service.create_schedule(
            user_id=current_user["user_id"],
            frequency=payload.frequency,
            db_config_id=payload.db_config_id,
            time_str=payload.time,
            day_of_week=payload.day_of_week,
            cron_expression=payload.cron_expression,
            timezone_str=payload.timezone,
            enabled=payload.enabled,
            description=payload.description,
        )
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("", response_model=list[ScheduleOut])
async def list_user_schedules(current_user: dict = Depends(get_current_user)):
    try:
        return await schedule_service.list_schedules(current_user["user_id"])
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.delete("/{schedule_id}")
async def delete_schedule(schedule_id: str, current_user: dict = Depends(get_current_user)):
    try:
        await schedule_service.delete_schedule(schedule_id, current_user["user_id"])
        return {"message": "Schedule removed"}
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.patch("/{schedule_id}/enabled", response_model=ScheduleOut)
async def toggle_schedule(
    schedule_id: str,
    payload: ScheduleToggle,
    current_user: dict = Depends(get_current_user),
):
    try:
        return await schedule_service.toggle_schedule(schedule_id, current_user["user_id"], payload.enabled)
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

