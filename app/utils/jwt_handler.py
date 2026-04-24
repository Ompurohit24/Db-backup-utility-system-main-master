from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from app.config import JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRATION_MINUTES


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=JWT_EXPIRATION_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict | None:
    """Decode and verify a JWT access token. Returns payload or None if invalid."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        return None

