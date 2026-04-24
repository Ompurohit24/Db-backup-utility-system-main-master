import asyncio

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import ADMIN_USER_IDS
from app.core.appwrite_client import users
from app.services import user_service
from app.utils.jwt_handler import decode_access_token

security = HTTPBearer()


def _admin_user_ids() -> set[str]:
    return {item.strip() for item in ADMIN_USER_IDS.split(",") if item.strip()}


async def _is_admin_user(user_id: str) -> bool:
    # Fast path: env-based static admin list
    if user_id in _admin_user_ids():
        return True

    # If profile table has role field and it's set to admin, allow access.
    try:
        profile = await user_service.get_user_profile(user_id)
        role = str((profile or {}).get("role", "")).strip().lower()
        if role == "admin":
            return True
    except Exception:
        pass

    # Appwrite Auth user metadata fallback: labels includes 'admin' OR prefs.role == 'admin'.
    try:
        auth_user = await asyncio.to_thread(users.get, user_id=user_id)
        if hasattr(auth_user, "to_dict"):
            auth_user = auth_user.to_dict()
        labels = auth_user.get("labels", []) if isinstance(auth_user, dict) else []
        if any(str(label).strip().lower() == "admin" for label in labels):
            return True

        prefs = auth_user.get("prefs", {}) if isinstance(auth_user, dict) else {}
        if str((prefs or {}).get("role", "")).strip().lower() == "admin":
            return True
    except Exception:
        pass

    return False


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    """
    Dependency that extracts and validates the JWT token from the
    Authorization: Bearer <token> header.
    Returns the token payload (user_id, email, name).
    """
    token = str(credentials.credentials or "").strip().strip('"').strip("'")
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()

    payload = decode_access_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id: str | None = payload.get("sub") or payload.get("user_id")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {
        "user_id": user_id,
        "email": payload.get("email", ""),
        "name": payload.get("name", ""),
    }


async def require_admin_user(current_user: dict = Depends(get_current_user)) -> dict:
    """Allow access only to admin users (profile/auth metadata or ADMIN_USER_IDS fallback)."""
    if not await _is_admin_user(current_user.get("user_id", "")):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


