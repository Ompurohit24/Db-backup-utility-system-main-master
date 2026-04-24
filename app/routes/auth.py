from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from fastapi_mail import FastMail, MessageSchema, MessageType
from fastapi.responses import JSONResponse
from appwrite.exception import AppwriteException
from app.core.appwrite_client import users
from app.schemas.auth import (
    LoginRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
    VerifyRegistrationOTPRequest,
)
from app.utils.jwt_handler import create_access_token
from app.utils.password import prehash_for_appwrite, hash_password, verify_password
from app.utils.dependencies import get_current_user
from app.services import user_service
from otp_email_verification.config import MAIL_CONFIG
from otp_email_verification.utils import DEFAULT_OTP_EXPIRY_SECONDS, generate_otp, store_otp, verify_otp
import asyncio


router = APIRouter(prefix="/auth", tags=["Authentication"])
mail_client = FastMail(MAIL_CONFIG)

# In-memory pending registrations for OTP verification flow.
# Use Redis in production for shared/process-safe storage with TTL.
_pending_registrations: dict[str, dict[str, datetime | str]] = {}


def _normalized_email(email: str) -> str:
    return (email or "").strip().lower()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cleanup_pending_registrations() -> None:
    now = _now_utc()
    expired = [
        email
        for email, payload in _pending_registrations.items()
        if isinstance(payload.get("expires_at"), datetime) and payload["expires_at"] <= now
    ]
    for email in expired:
        _pending_registrations.pop(email, None)


def _store_pending_registration(email: str, name: str, password: str) -> None:
    _pending_registrations[email] = {
        "name": name,
        "password": password,
        "expires_at": _now_utc() + timedelta(seconds=DEFAULT_OTP_EXPIRY_SECONDS),
    }


async def _send_registration_otp_email(email: str, otp: str) -> None:
    message = MessageSchema(
        subject="Verify your account - OTP",
        recipients=[email],
        body=(
            f"Your verification OTP is <b>{otp}</b>.<br>"
            "This code expires in 5 minutes."
        ),
        subtype=MessageType.html,
    )
    await mail_client.send_message(message)




def _normalize_appwrite_error(exc: Exception) -> tuple[int, str]:
    if not isinstance(exc, AppwriteException):
        return 500, str(exc)

    message = str(exc.message or "")
    lower_msg = message.lower()
    if "already" in lower_msg and "verif" in lower_msg:
        return 409, "Email is already verified"
    if "expired" in lower_msg or "invalid" in lower_msg or "secret" in lower_msg:
        return 400, "Invalid or expired token/link"
    if "not found" in lower_msg:
        return 404, "User not found"
    return int(exc.code or 400), message or "Appwrite request failed"

@router.post("/register", response_model=dict)
async def register(payload: RegisterRequest):
    """Start registration by sending OTP. User is created only after OTP verification."""
    try:
        normalized_email = _normalized_email(payload.email)

        _cleanup_pending_registrations()

        # Do not allow registration if profile already exists.
        existing_profile = await user_service.get_user_profile_by_email(normalized_email)
        if existing_profile:
            return JSONResponse(
                status_code=409,
                content={"detail": "User already exists. Please login or reset password."},
            )

        otp = generate_otp()
        store_otp(normalized_email, otp)
        _store_pending_registration(normalized_email, payload.name, payload.password)

        try:
            await _send_registration_otp_email(normalized_email, otp)
        except Exception as email_error:
            _pending_registrations.pop(normalized_email, None)
            return JSONResponse(status_code=500, content={"detail": f"Email sending failed: {email_error}"})

        return {
            "message": "OTP sent to email. Verify OTP to complete registration.",
            "email": normalized_email,
            "otp_expires_in_seconds": DEFAULT_OTP_EXPIRY_SECONDS,
        }

    except AppwriteException as e:
        status_code, message = _normalize_appwrite_error(e)
        return JSONResponse(status_code=status_code, content={"detail": message})
    except Exception as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})


@router.post("/verify-registration-otp", response_model=TokenResponse)
async def verify_registration_otp(payload: VerifyRegistrationOTPRequest):
    """Complete registration after OTP verification and return login token."""
    try:
        normalized_email = _normalized_email(payload.email)
        _cleanup_pending_registrations()

        verified, reason = verify_otp(normalized_email, payload.otp)
        if not verified:
            if reason == "expired":
                return JSONResponse(status_code=410, content={"error": "OTP expired"})
            if reason == "invalid":
                return JSONResponse(status_code=400, content={"error": "Invalid OTP"})
            return JSONResponse(status_code=404, content={"error": "OTP not found for this email"})

        pending = _pending_registrations.get(normalized_email)
        if not pending:
            return JSONResponse(status_code=404, content={"error": "Registration request expired. Please register again."})

        name = str(pending.get("name") or "").strip()
        password = str(pending.get("password") or "")
        if not name or not password:
            _pending_registrations.pop(normalized_email, None)
            return JSONResponse(status_code=400, content={"error": "Registration data is invalid. Please register again."})

        safe_pw = prehash_for_appwrite(password)
        user = await asyncio.to_thread(
            users.create,
            user_id="unique()",
            email=normalized_email,
            password=safe_pw,
            name=name,
        )
        user_data = user.to_dict() if hasattr(user, "to_dict") else user

        profile = await user_service.create_user_profile(
            user_id=user_data["$id"],
            email=normalized_email,
            name=user_data["name"],
            password_hash=hash_password(password),
        )

        _pending_registrations.pop(normalized_email, None)

        user_id = str(profile.get("user_id") or user_data["$id"])
        token = create_access_token(
            data={
                "sub": user_id,
                "user_id": user_id,
                "email": profile.get("email", normalized_email),
                "name": profile.get("name", user_data.get("name", "")),
            }
        )

        return TokenResponse(
            access_token=token,
            user_id=user_id,
            name=profile.get("name", user_data.get("name", "")),
            email=profile.get("email", normalized_email),
        )
    except AppwriteException as e:
        status_code, message = _normalize_appwrite_error(e)
        return JSONResponse(status_code=status_code, content={"error": message})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/login", response_model=TokenResponse)
async def login(payload: LoginRequest):
    """Legacy JWT login using local profile password_hash."""
    try:
        normalized_email = _normalized_email(payload.email)
        profile = await user_service.get_user_profile_by_email(normalized_email)

        if not profile:
            return JSONResponse(status_code=401, content={"error": "Invalid email or password"})

        stored_hash = str(profile.get("password_hash", "") or "")
        if not stored_hash or not verify_password(payload.password, stored_hash):
            return JSONResponse(status_code=401, content={"error": "Invalid email or password"})

        profile_status = str(profile.get("status", "active") or "active").strip().lower()
        if profile_status == "suspended" or not profile.get("is_active", True):
            return JSONResponse(status_code=403, content={"error": "Account is suspended"})

        user_id = str(profile.get("user_id") or profile.get("$id") or "")
        token = create_access_token(
            data={
                "sub": user_id,
                "user_id": user_id,
                "email": profile.get("email", ""),
                "name": profile.get("name", ""),
            }
        )

        return TokenResponse(
            access_token=token,
            user_id=user_id,
            name=profile.get("name", ""),
            email=profile.get("email", ""),
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})



@router.get("/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    """Return the currently authenticated user's info from the token."""
    return UserResponse(
        user_id=current_user["user_id"],
        name=current_user["name"],
        email=current_user["email"],
    )


@router.get("/user/{user_id}")
async def get_user(user_id: str, current_user: dict = Depends(get_current_user)):
    """Get a user by ID (protected)."""
    try:
        user = await asyncio.to_thread(users.get, user_id)
        return user
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/users")
async def list_users(current_user: dict = Depends(get_current_user)):
    """List all users (protected)."""
    try:
        result = await asyncio.to_thread(users.list)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
