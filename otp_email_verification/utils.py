from __future__ import annotations

import secrets
import os
from datetime import datetime, timedelta, timezone

DEFAULT_OTP_EXPIRY_SECONDS = int(os.getenv("OTP_EXPIRY_SECONDS", "300"))

# In-memory OTP store: {email: {otp: str, expires_at: datetime}}
# NOTE: This is suitable for single-process development only.
# In production, use Redis or another shared cache with TTL.
_otp_store: dict[str, dict[str, datetime | str]] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def generate_otp(length: int = 6) -> str:
    if length <= 0:
        raise ValueError("OTP length must be positive")
    return "".join(str(secrets.randbelow(10)) for _ in range(length))


def cleanup_expired_otps() -> None:
    now = _now_utc()
    expired_emails = [
        email
        for email, payload in _otp_store.items()
        if isinstance(payload.get("expires_at"), datetime) and payload["expires_at"] <= now
    ]
    for email in expired_emails:
        _otp_store.pop(email, None)


def store_otp(email: str, otp: str, expiry_seconds: int = DEFAULT_OTP_EXPIRY_SECONDS) -> datetime:
    expires_at = _now_utc() + timedelta(seconds=expiry_seconds)
    _otp_store[email] = {
        "otp": otp,
        "expires_at": expires_at,
    }
    return expires_at


def verify_otp(email: str, submitted_otp: str) -> tuple[bool, str]:
    cleanup_expired_otps()

    payload = _otp_store.get(email)
    if not payload:
        return False, "not_found"

    expected = str(payload.get("otp") or "")
    expires_at = payload.get("expires_at")

    if not isinstance(expires_at, datetime) or expires_at <= _now_utc():
        _otp_store.pop(email, None)
        return False, "expired"

    if not secrets.compare_digest(expected, submitted_otp):
        return False, "invalid"

    # One-time use OTP: remove after successful verification.
    _otp_store.pop(email, None)
    return True, "verified"


def get_store_size() -> int:
    """Used by the smoke test and diagnostics."""
    cleanup_expired_otps()
    return len(_otp_store)

