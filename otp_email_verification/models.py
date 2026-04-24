from pydantic import BaseModel, EmailStr, Field


class SendOTPRequest(BaseModel):
    email: EmailStr


class VerifyOTPRequest(BaseModel):
    email: EmailStr
    otp: str = Field(..., pattern=r"^\d{6}$", description="6-digit numeric OTP")


class MessageResponse(BaseModel):
    success: bool
    message: str

