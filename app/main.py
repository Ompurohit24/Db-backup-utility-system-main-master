from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.logging_setup import setup_logging
from app.routes.auth import router as auth_router
from app.routes.user import router as user_router
from app.routes.database import router as database_router
from app.routes.backup import router as backup_router
from app.routes.schedule import router as schedule_router
from app.routes.admin import router as admin_router
from app.routes.notifications import router as notifications_router
from app.services.schedule_service import load_active_schedules
from app.utils.scheduler import scheduler_shutdown, scheduler_startup
from fastapi.middleware.cors import CORSMiddleware
from app.routes.logs import router as logs_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    await scheduler_startup()
    await load_active_schedules()
    yield
    await scheduler_shutdown()


app = FastAPI(title="Database Backup Utility", version="1.0", lifespan=lifespan)
app.include_router(logs_router)
app.include_router(auth_router)
app.include_router(user_router)
app.include_router(database_router)
app.include_router(backup_router)
app.include_router(schedule_router)
app.include_router(admin_router)
app.include_router(notifications_router)


@app.get("/")
def home():
    return {"message": "Database Backup Utility API Running"}

# ✅ Allowed frontend URLs
origins = [
    "https://www.vaultdb.live",
    "http://localhost:5500",
    "http://127.0.0.1:5500"
]

# ✅ Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],   # allow all HTTP methods
    allow_headers=["*"],   # allow all headers
)
