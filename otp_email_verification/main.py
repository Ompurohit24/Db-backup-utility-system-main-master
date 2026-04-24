from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi_mail import FastMail, MessageSchema, MessageType

from .config import MAIL_CONFIG
from .models import MessageResponse, SendOTPRequest, VerifyOTPRequest
from .utils import generate_otp, store_otp, verify_otp

app = FastAPI(title="OTP Email Verification API", version="1.0.0")
mail_client = FastMail(MAIL_CONFIG)


async def _send_otp_email(email: str, otp: str) -> None:
    message = MessageSchema(
        subject="Your OTP Code",
        recipients=[email],
        body=(
            f"Your OTP is <b>{otp}</b>.<br>"
            "It expires in 5 minutes.<br><br>"
            "If you did not request this code, ignore this email."
        ),
        subtype=MessageType.html,
    )
    await mail_client.send_message(message)


@app.post("/send-otp/", response_model=MessageResponse)
async def send_otp(
    payload: SendOTPRequest,
    background_tasks: BackgroundTasks,
    send_in_background: bool = False,
):
    """
    Generate and send a 6-digit OTP.

    We currently send immediately to surface SMTP errors to the client.
    For production scale, you can queue this in a worker or Redis-backed task queue.
    """
    email = payload.email
    otp = generate_otp()
    store_otp(email=email, otp=otp)

    if send_in_background:
        background_tasks.add_task(_send_otp_email, email, otp)
        return MessageResponse(success=True, message="OTP queued for email delivery")

    try:
        await _send_otp_email(email=email, otp=otp)
    except Exception as exc:
        # Keep endpoint error explicit when SMTP delivery fails.
        raise HTTPException(status_code=500, detail=f"Email sending failed: {exc}") from exc


    return MessageResponse(success=True, message="OTP sent successfully")


@app.post("/verify-otp/", response_model=MessageResponse)
async def verify_otp_endpoint(payload: VerifyOTPRequest):
    is_valid, reason = verify_otp(email=payload.email, submitted_otp=payload.otp)

    if is_valid:
        return MessageResponse(success=True, message="OTP verified successfully")

    if reason == "expired":
        raise HTTPException(status_code=410, detail="OTP expired")

    if reason == "invalid":
        raise HTTPException(status_code=400, detail="Invalid OTP")

    raise HTTPException(status_code=404, detail="OTP not found for this email")


@app.get("/")
async def health_check() -> dict[str, str]:
    return {"message": "OTP Email Verification API is running"}

