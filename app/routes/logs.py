from fastapi import APIRouter, Depends
from app.routes.auth import get_current_user
from app.routes.admin import require_admin_user

router = APIRouter(tags=["Logs"])


# USER LOGS
@router.get("/logs/my")
async def my_logs(current_user: dict = Depends(get_current_user)):

    user_id = current_user["user_id"]
    logs = []

    for file in ["logs/backup.log", "logs/restore.log", "logs/error.log"]:
        try:
            with open(file, "r", encoding="utf-8") as f:
                for line in f.readlines():
                    if f"user_id={user_id}" in line:
                        logs.append(line.strip())
        except:
            pass

    return {"logs": logs[-100:]}


# ADMIN LOGS
@router.get("/admin/logs")
async def all_logs(current_user: dict = Depends(require_admin_user)):

    logs = []

    for file in [
        "logs/app.log",
        "logs/backup.log",
        "logs/restore.log"
    ]:
        try:
            with open(file, "r", encoding="utf-8") as f:
                logs.extend(
                    [
                        line.strip()
                        for line in f.readlines()
                        if "| ERROR |" not in line
                    ]
                )
        except:
            pass

    return {"logs": logs[-300:]}