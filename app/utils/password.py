import hashlib
import base64

import bcrypt


def _prehash(plain_password: str) -> bytes:
    """
    SHA-256 pre-hash → base64 encode → bytes.

    bcrypt only accepts up to 72 bytes of input.  By running the password
    through SHA-256 first we:
      • allow passwords of ANY length (no 72-byte error)
      • always feed exactly 44 base64-chars (32 raw bytes) into bcrypt
      • preserve full entropy of the original password
    This is the same technique used by Dropbox, 1Password, etc.
    """
    sha256_digest = hashlib.sha256(plain_password.encode("utf-8")).digest()
    return base64.b64encode(sha256_digest)  # 44 bytes, always < 72


def hash_password(plain_password: str) -> str:
    """Hash a plain-text password using SHA-256 + bcrypt."""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(_prehash(plain_password), salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain-text password against the stored SHA-256 + bcrypt hash."""
    try:
        return bcrypt.checkpw(
            _prehash(plain_password),
            hashed_password.encode("utf-8"),
        )
    except Exception:
        return False


def prehash_for_appwrite(plain_password: str) -> str:
    """
    Return a safe ≤72-byte derivative of any-length password.
    Used when passing a password to Appwrite's users.create(),
    which enforces bcrypt's 72-byte limit internally.
    """
    return _prehash(plain_password).decode("ascii")


