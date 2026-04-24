import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi_mail import ConnectionConfig

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


MAIL_USERNAME = _required_env("MAIL_USERNAME")
MAIL_PASSWORD = _required_env("MAIL_PASSWORD")
MAIL_FROM = _required_env("MAIL_FROM")
MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.gmail.com")
MAIL_STARTTLS = _to_bool(os.getenv("MAIL_STARTTLS"), default=True)
MAIL_SSL_TLS = _to_bool(os.getenv("MAIL_SSL_TLS"), default=False)
MAIL_FROM_NAME = os.getenv("MAIL_FROM_NAME", "OTP Verification")

OTP_EXPIRY_SECONDS = int(os.getenv("OTP_EXPIRY_SECONDS", "300"))

MAIL_CONFIG = ConnectionConfig(
    MAIL_USERNAME=MAIL_USERNAME,
    MAIL_PASSWORD=MAIL_PASSWORD,
    MAIL_FROM=MAIL_FROM,
    MAIL_PORT=MAIL_PORT,
    MAIL_SERVER=MAIL_SERVER,
    MAIL_STARTTLS=MAIL_STARTTLS,
    MAIL_SSL_TLS=MAIL_SSL_TLS,
    MAIL_FROM_NAME=MAIL_FROM_NAME,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True,
)

