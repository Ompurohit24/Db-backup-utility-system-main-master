from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class VerifyRegistrationOTPRequest(BaseModel):
    email: EmailStr
    otp: str = Field(..., pattern=r"^\d{6}$")




class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    name: str
    email: str


class UserResponse(BaseModel):
    user_id: str
    name: str
    email: str



