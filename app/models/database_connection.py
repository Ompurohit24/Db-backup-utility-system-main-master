from pydantic import BaseModel


class UserDatabaseDocument(BaseModel):
    """
    Represents a user's external database configuration
    stored in the Appwrite 'user_databases' collection.
    """
    owner_user_id: str
    user_id: str  # legacy alias kept for existing rows
    database_type: str          # mysql | postgresql | mongodb
    host: str
    port: int
    database_name: str
    username: str
    password: str
    status: str = "connected"   # connected | failed
    created_at: str = ""
    updated_at: str = ""
