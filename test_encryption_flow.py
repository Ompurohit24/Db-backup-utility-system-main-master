"""
Test script to verify encryption/decryption flow for backup files.
This script tests the complete cycle of encrypting and decrypting files.
"""

import asyncio
import base64
import os
import tempfile
from pathlib import Path

from app.utils.file_encryption import encrypt_file, decrypt_file, encrypt_bytes, decrypt_bytes
from app.utils.key_manager import load_backup_key, get_backup_key_optional


def test_encryption_with_bytes():
    """Test encryption/decryption with bytes directly."""
    print("\n=== Testing Encryption/Decryption with Bytes ===")
    
    # Get key
    key = get_backup_key_optional()
    if not key:
        print("ERROR: Encryption key not configured. Set BACKUP_ENCRYPTION_KEY in .env")
        return False
    
    print(f"✓ Key loaded: {len(key)} bytes (AES-{len(key)*8})")
    
    # Test data
    original_data = b"SELECT * FROM users; -- Test backup data"
    print(f"✓ Original data: {len(original_data)} bytes")
    
    # Encrypt
    try:
        encrypted_data = encrypt_bytes(original_data, key)
        print(f"✓ Encrypted data: {len(encrypted_data)} bytes")
        
        # Verify structure: nonce (12) + ciphertext + tag (16)
        if len(encrypted_data) >= 28:  # Min: 12 + 0 + 16
            print(f"  - Nonce: {encrypted_data[:12].hex()}")
            print(f"  - Ciphertext: {len(encrypted_data)-28} bytes")
            print(f"  - Tag: {encrypted_data[-16:].hex()}")
        else:
            print(f"ERROR: Encrypted data too small: {len(encrypted_data)} bytes")
            return False
    except Exception as e:
        print(f"ERROR: Encryption failed: {e}")
        return False
    
    # Decrypt
    try:
        decrypted_data = decrypt_bytes(encrypted_data, key)
        print(f"✓ Decrypted data: {len(decrypted_data)} bytes")
    except Exception as e:
        print(f"ERROR: Decryption failed: {e}")
        return False
    
    # Verify
    if decrypted_data == original_data:
        print("✓ Data matches! Encryption/decryption cycle successful.")
        return True
    else:
        print(f"ERROR: Data mismatch!")
        print(f"  Expected: {original_data}")
        print(f"  Got: {decrypted_data}")
        return False


def test_encryption_with_files():
    """Test encryption/decryption with actual files."""
    print("\n=== Testing Encryption/Decryption with Files ===")
    
    # Get key
    key = get_backup_key_optional()
    if not key:
        print("ERROR: Encryption key not configured. Set BACKUP_ENCRYPTION_KEY in .env")
        return False
    
    print(f"✓ Key loaded: {len(key)} bytes (AES-{len(key)*8})")
    
    # Create test file
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        original_file = tmpdir / "test.sql"
        encrypted_file = tmpdir / "test.sql.enc"
        decrypted_file = tmpdir / "test_restored.sql"
        
        # Write test data
        test_data = b"SELECT * FROM users;\nSELECT * FROM products;\n" * 100
        original_file.write_bytes(test_data)
        print(f"✓ Original file created: {original_file.stat().st_size} bytes")
        
        # Encrypt
        try:
            encrypt_file(original_file, encrypted_file, key)
            print(f"✓ File encrypted: {encrypted_file.stat().st_size} bytes")
        except Exception as e:
            print(f"ERROR: Encryption failed: {e}")
            return False
        
        # Decrypt
        try:
            decrypt_file(encrypted_file, decrypted_file, key)
            print(f"✓ File decrypted: {decrypted_file.stat().st_size} bytes")
        except Exception as e:
            print(f"ERROR: Decryption failed: {e}")
            return False
        
        # Verify
        restored_data = decrypted_file.read_bytes()
        if restored_data == test_data:
            print("✓ File content matches! File encryption/decryption cycle successful.")
            return True
        else:
            print(f"ERROR: File content mismatch!")
            print(f"  Expected: {len(test_data)} bytes")
            print(f"  Got: {len(restored_data)} bytes")
            return False


def test_key_configuration():
    """Test key configuration and loading."""
    print("\n=== Testing Key Configuration ===")
    
    backup_key_env = os.getenv("BACKUP_ENCRYPTION_KEY")
    encryption_key_env = os.getenv("ENCRYPTION_KEY")
    
    print(f"BACKUP_ENCRYPTION_KEY: {'Set' if backup_key_env else 'Not set'}")
    print(f"ENCRYPTION_KEY: {'Set' if encryption_key_env else 'Not set'}")
    
    try:
        key = load_backup_key()
        print(f"✓ Key successfully loaded: {len(key)} bytes")
        print(f"  Base64: {base64.urlsafe_b64encode(key).decode()[:50]}...")
        
        if len(key) == 32:
            print("✓ Key is correct length for AES-256")
            return True
        else:
            print(f"ERROR: Key is {len(key)} bytes, expected 32 bytes for AES-256")
            return False
    except RuntimeError as e:
        print(f"ERROR: {e}")
        return False


def test_corrupted_data():
    """Test handling of corrupted encrypted data."""
    print("\n=== Testing Corrupted Data Handling ===")
    
    key = get_backup_key_optional()
    if not key:
        print("ERROR: Encryption key not configured.")
        return False
    
    # Test 1: Data too small
    try:
        decrypt_bytes(b"short", key)
        print("ERROR: Should have raised ValueError for small data")
        return False
    except ValueError as e:
        if "too small" in str(e).lower():
            print(f"✓ Correctly rejected small data: {e}")
        else:
            print(f"ERROR: Wrong error message: {e}")
            return False
    
    # Test 2: Corrupted tag
    original_data = b"Test data"
    encrypted_data = encrypt_bytes(original_data, key)
    corrupted_data = bytearray(encrypted_data)
    corrupted_data[-1] ^= 0xFF  # Flip bits in tag
    
    try:
        decrypt_bytes(bytes(corrupted_data), key)
        print("ERROR: Should have raised error for corrupted tag")
        return False
    except ValueError as e:
        if "authentication tag" in str(e).lower():
            print(f"✓ Correctly rejected corrupted tag: {e}")
            return True
        else:
            print(f"ERROR: Wrong error message: {e}")
            return False


async def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("ENCRYPTION/DECRYPTION FLOW TEST")
    print("="*60)
    
    results = {
        "Key Configuration": test_key_configuration(),
        "Bytes Encryption": test_encryption_with_bytes(),
        "File Encryption": test_encryption_with_files(),
        "Corrupted Data Handling": test_corrupted_data(),
    }
    
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")
    
    all_passed = all(results.values())
    print("\n" + ("="*60))
    if all_passed:
        print("✓ ALL TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED - Check configuration and code")
    print("="*60)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)

