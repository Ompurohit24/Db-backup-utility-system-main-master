"""File-level AES-256-GCM encryption helpers with streaming support.

Encrypts files using a single nonce and authentication tag:
    output = nonce (12 bytes) || ciphertext || tag (16 bytes)

Decryption reverses the process and validates the tag before returning bytes.
"""

import secrets
from pathlib import Path
from typing import BinaryIO

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidTag

CHUNK_SIZE = 1024 * 1024  # 1 MB chunks to limit memory usage
NONCE_SIZE = 12
TAG_SIZE = 16


def _aes_gcm_encrypt_stream(in_file: BinaryIO, out_file: BinaryIO, key: bytes) -> None:
    nonce = secrets.token_bytes(NONCE_SIZE)
    out_file.write(nonce)

    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(nonce),
        backend=default_backend(),
    ).encryptor()

    while True:
        chunk = in_file.read(CHUNK_SIZE)
        if not chunk:
            break
        out_file.write(encryptor.update(chunk))

    encryptor.finalize()
    out_file.write(encryptor.tag)


def _aes_gcm_decrypt_stream(in_file: BinaryIO, out_file: BinaryIO, key: bytes) -> None:
    nonce = in_file.read(NONCE_SIZE)
    if len(nonce) != NONCE_SIZE:
        raise ValueError("Encrypted data missing nonce")

    data = in_file.read()
    if len(data) < TAG_SIZE:
        raise ValueError("Encrypted data missing authentication tag")

    ciphertext, tag = data[:-TAG_SIZE], data[-TAG_SIZE:]

    decryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(nonce, tag),
        backend=default_backend(),
    ).decryptor()

    offset = 0
    total = len(ciphertext)
    while offset < total:
        next_offset = min(offset + CHUNK_SIZE, total)
        out_file.write(decryptor.update(ciphertext[offset:next_offset]))
        offset = next_offset

    try:
        decryptor.finalize()
    except InvalidTag as exc:
        raise ValueError(
            "Authentication failed - file may be corrupted, or encryption key may be incorrect."
        ) from exc


def encrypt_file(source_path: Path, dest_path: Path, key: bytes) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(source_path, "rb") as src, open(dest_path, "wb") as dst:
        _aes_gcm_encrypt_stream(src, dst, key)
    return dest_path


def decrypt_file(source_path: Path, dest_path: Path, key: bytes) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(source_path, "rb") as src, open(dest_path, "wb") as dst:
        _aes_gcm_decrypt_stream(src, dst, key)
    return dest_path


def decrypt_bytes(data: bytes, key: bytes) -> bytes:
    if len(data) < NONCE_SIZE + TAG_SIZE:
        raise ValueError("Encrypted payload too small")

    nonce = data[:NONCE_SIZE]
    tag = data[-TAG_SIZE:]
    ciphertext = data[NONCE_SIZE:-TAG_SIZE]

    decryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(nonce, tag),
        backend=default_backend(),
    ).decryptor()

    plaintext_chunks = []
    offset = 0
    total = len(ciphertext)
    while offset < total:
        next_offset = min(offset + CHUNK_SIZE, total)
        plaintext_chunks.append(decryptor.update(ciphertext[offset:next_offset]))
        offset = next_offset

    try:
        decryptor.finalize()
    except InvalidTag as exc:
        raise ValueError(
            "Authentication failed - file may be corrupted, or encryption key may be incorrect."
        ) from exc
    
    return b"".join(plaintext_chunks)


def encrypt_bytes(data: bytes, key: bytes) -> bytes:
    """
    Encrypt bytes directly using AES-256-GCM.
    
    Returns: nonce (12 bytes) || ciphertext || tag (16 bytes)
    """
    nonce = secrets.token_bytes(NONCE_SIZE)
    
    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(nonce),
        backend=default_backend(),
    ).encryptor()
    
    ciphertext_chunks = []
    offset = 0
    total = len(data)
    
    while offset < total:
        next_offset = min(offset + CHUNK_SIZE, total)
        ciphertext_chunks.append(encryptor.update(data[offset:next_offset]))
        offset = next_offset
    
    encryptor.finalize()
    
    # Return: nonce || ciphertext || tag
    return nonce + b"".join(ciphertext_chunks) + encryptor.tag
