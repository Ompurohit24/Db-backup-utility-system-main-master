from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, validator


class ScheduleCreate(BaseModel):
    frequency: Literal["daily", "weekly", "cron"]
    db_config_id: str = Field(..., description="Database configuration to back up")
    time: Optional[str] = Field(
        None,
        description="HH:MM in 24h format (required for daily/weekly)",
        examples=["02:00"],
    )
    day_of_week: Optional[str] = Field(
        None,
        description="Day for weekly schedule (mon-sun)",
        examples=["sun"],
    )
    cron_expression: Optional[str] = Field(
        None,
        description="Custom cron expression (required for frequency=cron)",
        examples=["0 */6 * * *"],
    )
    timezone: str = Field(default="UTC", description="Timezone for schedule")
    enabled: bool = Field(default=True)
    description: Optional[str] = Field(default=None)

    @validator("time")
    def _validate_time(cls, value, values):
        freq = values.get("frequency")
        if freq in {"daily", "weekly"}:
            if not value:
                raise ValueError("time is required for daily/weekly schedules (HH:MM)")
            parts = value.split(":")
            if len(parts) != 2:
                raise ValueError("time must be HH:MM in 24h format")
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("time must be within 00:00-23:59")
        return value

    @validator("day_of_week")
    def _validate_day(cls, value, values):
        freq = values.get("frequency")
        if freq == "weekly" and not value:
            raise ValueError("day_of_week is required for weekly schedules")
        return value

    @validator("cron_expression")
    def _validate_cron(cls, value, values):
        if values.get("frequency") == "cron" and not value:
            raise ValueError("cron_expression is required for frequency=cron")
        return value


class ScheduleOut(BaseModel):
    schedule_id: str
    user_id: str
    db_config_id: str
    frequency: str
    cron_expression: str
    timezone: str
    enabled: bool
    status: str
    description: Optional[str] = None
    next_run_time: Optional[datetime] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None

    class Config:
        from_attributes = True


class ScheduleToggle(BaseModel):
    enabled: bool

