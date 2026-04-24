"""
AES encryption / decryption for sensitive fields (e.g. DB passwords).

Uses Fernet (symmetric AES-128-CBC + HMAC-SHA256) from the `cryptography` lib.
The key is loaded once from ENCRYPTION_KEY in .env — never hard-code it.
"""

from cryptography.fernet import Fernet
from app.config import ENCRYPTION_KEY

if not ENCRYPTION_KEY:
    raise RuntimeError(
        "ENCRYPTION_KEY is not set in .env. "
        "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    )

_fernet = Fernet(ENCRYPTION_KEY.encode("utf-8"))


def encrypt(plain_text: str) -> str:
    """Encrypt a plain-text string → URL-safe base64 cipher text."""
    return _fernet.encrypt(plain_text.encode("utf-8")).decode("utf-8")


def decrypt(cipher_text: str) -> str:
    """Decrypt a Fernet cipher text → original plain-text string."""
    return _fernet.decrypt(cipher_text.encode("utf-8")).decode("utf-8")

