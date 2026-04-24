from pydantic import BaseModel, field_validator
from typing import Optional

SUPPORTED_DB_TYPES = ["mysql", "postgresql", "mongodb"]

DEFAULT_PORTS = {
    "mysql": 3306,
    "postgresql": 5432,
    "mongodb": 27017,
}


class TestConnectionRequest(BaseModel):
    """
    Request body for POST /test-connection.

    Example:
    {
        "database_type": "mysql",
        "host": "localhost",
        "port": 3306,
        "database_name": "my_app_db",
        "username": "root",
        "password": "secret"
    }
    """
    database_type: str          # mysql | postgresql | mongodb
    host: str
    port: Optional[int] = None  # auto-filled from database_type if omitted
    database_name: str
    username: str
    password: str

    @field_validator("database_type")
    @classmethod
    def validate_database_type(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in SUPPORTED_DB_TYPES:
            raise ValueError(
                f"database_type must be one of {SUPPORTED_DB_TYPES}"
            )
        return v

    @field_validator("port", mode="before")
    @classmethod
    def set_default_port(cls, v, info):
        if v is None:
            db_type = info.data.get("database_type", "").strip().lower()
            return DEFAULT_PORTS.get(db_type)
        return v


class TestConnectionResponse(BaseModel):
    """
    Response for POST /test-connection.

    Success example:
    {
        "success": true,
        "message": "Connection successful. Database saved.",
        "database_type": "mysql",
        "host": "localhost",
        "port": 3306,
        "database_name": "my_app_db",
        "server_version": "8.0.35",
        "document_id": "664a1f..."
    }

    Failure example:
    {
        "success": false,
        "message": "Access denied for user 'root'@'localhost'",
        "database_type": "mysql",
        "host": "localhost",
        "port": 3306,
        "database_name": "my_app_db",
        "server_version": null,
        "document_id": null
    }
    """
    success: bool
    message: str
    database_type: str
    host: str
    port: int
    database_name: str
    server_version: Optional[str] = None
    document_id: Optional[str] = None


class DatabaseConfigResponse(BaseModel):
    """Returned when listing / fetching saved database configs (password hidden)."""
    document_id: str
    owner_user_id: str
    user_id: str = ""
    database_type: str
    host: str
    port: int
    database_name: str
    username: str
    status: str
    created_at: str = ""
    updated_at: str = ""
