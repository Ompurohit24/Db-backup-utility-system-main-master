"""Helpers to turn Appwrite Tables rows into plain dicts.

Appwrite's Python SDK returns Pydantic models (Row / RowList). Downstream code
often expects dictionaries, so we normalize here.
"""

from typing import Any

from appwrite.models.row import Row
from appwrite.models.row_list import RowList


def normalize_row(row: Any) -> dict:
    """Return a flattened dict for any Appwrite row-like object."""
    if row is None:
        return {}
    if isinstance(row, Row):
        raw = row.to_dict()
    elif isinstance(row, dict):
        raw = dict(row)
    elif hasattr(row, "to_dict"):
        raw = row.to_dict()
    elif hasattr(row, "model_dump"):
        raw = row.model_dump(by_alias=True)
    else:
        return {}

    data = raw.pop("data", None)
    if isinstance(data, dict):
        raw.update(data)
    return raw


def normalize_row_collection(result: Any) -> dict:
    """Convert RowList/dict responses into a dict with a plain rows list."""
    if isinstance(result, RowList):
        rows = result.rows or []
        payload: dict[str, Any] = {"total": getattr(result, "total", len(rows))}
    elif isinstance(result, dict):
        rows = result.get("rows", result.get("documents", [])) or []
        payload = {k: v for k, v in result.items() if k not in {"rows", "documents"}}
    else:
        rows, payload = [], {}

    payload["rows"] = [normalize_row(row) for row in rows]
    return payload

