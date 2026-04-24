from pydantic import BaseModel, EmailStr
from typing import Optional


class UserDocument(BaseModel):
    """Represents a user document stored in the Appwrite database."""
    user_id: str
    email: EmailStr
    name: str
    password_hash: str = ""
    role: str = "user"
    status: str = "active"
    phone: Optional[str] = None
    bio: Optional[str] = None
    is_active: bool = True
    created_at: str = ""
    updated_at: str = ""

