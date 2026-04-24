Set-Location "H:\DB_Backup_Utility_System\Db-backup-utility-system-main"
.\.venv\Scripts\pip.exe install -r requirements.txtSet-Location "H:\DB_Backup_Utility_System\Db-backup-utility-system-main"
.\.venv\Scripts\pip.exe install -r requirements.txt# OTP Email Verification (FastAPI + FastAPI-Mail)

This module provides a complete OTP email verification flow:
- Generate 6-digit OTP
- Send OTP over SMTP (Gmail app password friendly)
- Store OTP in memory with 5-minute expiry
- Verify OTP

## Files
- `otp_email_verification/main.py` - FastAPI routes
- `otp_email_verification/config.py` - SMTP/FastAPI-Mail configuration
- `otp_email_verification/models.py` - Pydantic request/response schemas
- `otp_email_verification/utils.py` - OTP generation/storage/verification logic
- `otp_email_verification/smoke_test.py` - tiny OTP logic smoke test

## Environment Variables
Add these to your project `.env`:

```env
MAIL_USERNAME=your_gmail@gmail.com
MAIL_PASSWORD=your_gmail_app_password
MAIL_FROM=your_gmail@gmail.com
MAIL_PORT=587
MAIL_SERVER=smtp.gmail.com
MAIL_STARTTLS=true
MAIL_SSL_TLS=false
MAIL_FROM_NAME=OTP Verification
OTP_EXPIRY_SECONDS=300
```

> For Gmail, use an **App Password** (not your normal account password).

## Run
```bash
uvicorn otp_email_verification.main:app --reload
```

## Endpoints
### `POST /send-otp/`
Request body:

```json
{
  "email": "user@example.com"
}
```

Optional query flag:
- `send_in_background=true` queues email with `BackgroundTasks`

### `POST /verify-otp/`
Request body:

```json
{
  "email": "user@example.com",
  "otp": "123456"
}
```

## Quick IDE Testing
If you use the JetBrains HTTP client, run requests from:
- `otp_email_verification/test_otp.http`

## Production Note
Current OTP storage uses an in-memory dictionary and is process-local.
Use Redis (or another shared cache with TTL) in production deployments.

