"""
Database connection routes.

POST /test-connection  →  test creds, if OK store in Appwrite, else return error
GET  /databases        →  list saved databases for current user
GET  /databases/{id}   →  get one saved database config
DELETE /databases/{id} →  remove a saved database config
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.schemas.database import (
    TestConnectionRequest,
    TestConnectionResponse,
    DatabaseConfigResponse,
)
from app.services import database_service
from app.services import notification_service
from app.utils.dependencies import get_current_user
from app.utils.ownership import get_owner_user_id

router = APIRouter(tags=["Database Connections"])


# ── POST /test-connection ────────────────────────────────────────────

@router.post("/test-connection", response_model=TestConnectionResponse)
async def test_connection(
    payload: TestConnectionRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    1. Accept database credentials from the user.
    2. Attempt to connect to the database.
    3. If successful → save config to Appwrite 'user_databases' collection.
    4. If failed    → return error message (nothing is saved).

    Example request body:
    {
        "database_type": "mysql",
        "host": "localhost",
        "port": 3306,
        "database_name": "my_app_db",
        "username": "root",
        "password": "secret"
    }
    """
    try:
        # ── Step 1: Test the connection ──────────────────────────────
        result = await database_service.test_user_database(
            database_type=payload.database_type,
            host=payload.host,
            port=payload.port,
            database_name=payload.database_name,
            username=payload.username,
            password=payload.password,
        )

        # ── Step 2: Connection FAILED → return error ─────────────────
        if not result.success:
            await notification_service.create_notification(
                user_id=current_user["user_id"],
                event_type="database_connection_failed",
                level="error",
                title="Database Connection Failed",
                message=f"Could not connect to '{payload.database_name}': {result.message}",
            )
            return TestConnectionResponse(
                success=False,
                message=result.message,
                database_type=payload.database_type,
                host=payload.host,
                port=payload.port,
                database_name=payload.database_name,
                server_version=None,
                document_id=None,
            )

        # ── Step 3: Connection OK → save config in Appwrite ─────────
        doc = await database_service.save_database_config(
            user_id=current_user["user_id"],
            database_type=payload.database_type,
            host=payload.host,
            port=payload.port,
            database_name=payload.database_name,
            username=payload.username,
            password=payload.password,
        )

        await notification_service.create_notification(
            user_id=current_user["user_id"],
            event_type="database_connected",
            level="success",
            title="Database Connected",
            message=f"Database '{payload.database_name}' connected and saved successfully.",
            resource_id=str(doc.get("$id") or ""),
        )

        return TestConnectionResponse(
            success=True,
            message="Connection successful. Database configuration saved.",
            database_type=payload.database_type,
            host=payload.host,
            port=payload.port,
            database_name=payload.database_name,
            server_version=result.server_version,
            document_id=doc["$id"],
        )

    except Exception as e:
        await notification_service.create_notification(
            user_id=current_user.get("user_id", ""),
            event_type="database_connection_error",
            level="error",
            title="Database Connection Error",
            message=f"Unexpected error while connecting database: {e}",
        )
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /databases ───────────────────────────────────────────────────

@router.get("/databases", response_model=list[DatabaseConfigResponse])
async def list_databases(
    limit: int = 25,
    offset: int = 0,
    current_user: dict = Depends(get_current_user),
):
    """List all saved database configurations for the current user."""
    try:
        result = await database_service.list_user_databases(
            user_id=current_user["user_id"],
            limit=limit,
            offset=offset,
        )
        return [
            {
                "document_id": doc["$id"],
                "owner_user_id": get_owner_user_id(doc),
                "user_id": get_owner_user_id(doc),
                "database_type": doc["database_type"],
                "host": doc["host"],
                "port": doc["port"],
                "database_name": doc["database_name"],
                "username": doc["username"],
                "status": doc.get("status", "connected"),
                "created_at": doc.get("created_at", ""),
                "updated_at": doc.get("updated_at", ""),
            }
            for doc in result.get("rows", result.get("documents", []))
        ]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /databases/{document_id} ─────────────────────────────────────

@router.get("/databases/{document_id}", response_model=DatabaseConfigResponse)
async def get_database(
    document_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Get a single saved database configuration."""
    try:
        doc = await database_service.get_user_database(document_id)
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Database config not found"}
            )
        if get_owner_user_id(doc) != current_user["user_id"]:
            return JSONResponse(
                status_code=403, content={"error": "Access denied"}
            )
        return {
            "document_id": doc["$id"],
            "owner_user_id": get_owner_user_id(doc),
            "user_id": get_owner_user_id(doc),
            "database_type": doc["database_type"],
            "host": doc["host"],
            "port": doc["port"],
            "database_name": doc["database_name"],
            "username": doc["username"],
            "status": doc.get("status", "connected"),
            "created_at": doc.get("created_at", ""),
            "updated_at": doc.get("updated_at", ""),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── DELETE /databases/{document_id} ──────────────────────────────────

@router.delete("/databases/{document_id}")
async def delete_database(
    document_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Remove a saved database configuration."""
    try:
        doc = await database_service.get_user_database(document_id)
        if not doc:
            return JSONResponse(
                status_code=404, content={"error": "Database config not found"}
            )
        if get_owner_user_id(doc) != current_user["user_id"]:
            return JSONResponse(
                status_code=403, content={"error": "Access denied"}
            )
        await database_service.delete_user_database(document_id)
        return {"message": "Database configuration deleted successfully"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
