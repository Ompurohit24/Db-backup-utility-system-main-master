"""Key management helpers for backup file encryption.

The backup encryption key must be provided via environment variable
``BACKUP_ENCRYPTION_KEY`` as a base64-encoded 32-byte value (AES-256).
Generate one with:

    python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
"""

import base64
from typing import Optional

from app.config import BACKUP_ENCRYPTION_KEY, ENCRYPTION_KEY


def load_backup_key() -> bytes:
    # Prefer dedicated BACKUP_ENCRYPTION_KEY; fall back to ENCRYPTION_KEY for compatibility.
    key_str = BACKUP_ENCRYPTION_KEY or ENCRYPTION_KEY
    # key_str = BACKUP_ENCRYPTION_KEY
    if not key_str:
        raise RuntimeError(
            "BACKUP_ENCRYPTION_KEY (or ENCRYPTION_KEY) is not set. Add it to .env or secrets."
        )

    try:
        key_bytes = base64.urlsafe_b64decode(key_str)
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError("Backup key must be base64-encoded 32 bytes.") from exc

    if len(key_bytes) != 32:
        raise RuntimeError("Backup key must decode to 32 bytes (AES-256).")

    return key_bytes


def get_backup_key_optional() -> Optional[bytes]:
    try:
        return load_backup_key()
    except Exception:
        return None



