"""Small local smoke test for OTP logic (no SMTP required)."""

from otp_email_verification.utils import generate_otp, get_store_size, store_otp, verify_otp


def run() -> None:
    email = "smoke@example.com"
    otp = generate_otp()
    assert len(otp) == 6 and otp.isdigit()

    store_otp(email, otp, expiry_seconds=60)
    assert get_store_size() >= 1

    ok, reason = verify_otp(email, otp)
    assert ok is True and reason == "verified"

    ok2, reason2 = verify_otp(email, otp)
    assert ok2 is False and reason2 == "not_found"

    print("smoke-test-ok")


if __name__ == "__main__":
    run()

