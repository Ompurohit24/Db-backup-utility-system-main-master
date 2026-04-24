"""
Ownership helpers for user-scoped resources.

`user_id` is the canonical field. `owner_user_id` is kept as a fallback for
older rows that used a different attribute name.
"""


def get_owner_user_id(doc: dict) -> str:
    """Return the owner id from a row, supporting legacy records."""
    return doc.get("user_id") or doc.get("owner_user_id") or ""

