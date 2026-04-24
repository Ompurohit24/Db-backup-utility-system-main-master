"""Utility helpers for gzip compression/decompression used by backups."""

from __future__ import annotations

import gzip
import shutil
from pathlib import Path
from typing import Optional


def is_gzip_name(name: str) -> bool:
    """Lightweight extension check for gzip files."""
    return name.lower().endswith(".gz") if name else False


def gzip_compress(source: Path) -> Path:
    """Compress a file to `<name>.<ext>.gz` and return the new path."""
    dest = source.with_suffix(source.suffix + ".gz")
    with source.open("rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return dest


def gzip_decompress(source: Path, target_name: Optional[str] = None) -> Path:
    """Decompress a gzip file to `target_name` (or strip the .gz suffix)."""
    if target_name:
        dest = source.parent / target_name
    else:
        # Strip only the final .gz suffix
        dest = source.with_suffix("")

    with gzip.open(source, "rb") as f_in, dest.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    return dest

