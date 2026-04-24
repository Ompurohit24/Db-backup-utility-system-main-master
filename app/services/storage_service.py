"""
Storage Service: Handles backup file storage and retrieval from Appwrite Storage.
Manages file uploads, downloads, and metadata tracking.
"""

import asyncio
import os
import tempfile
from typing import Optional, Tuple

from appwrite.input_file import InputFile

from app.config import APPWRITE_STORAGE_BUCKET_ID, APPWRITE_TOTAL_STORAGE_BYTES
from app.core.appwrite_client import storage
from app.logger import get_logger
from app.utils.compression import gzip_compress, gzip_decompress
from app.utils.file_encryption import encrypt_file, decrypt_bytes
from app.utils.key_manager import get_backup_key_optional

logger = get_logger("storage")


def _safe_int(value) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def format_storage_size(bytes_value: Optional[int]) -> str:
    """Human-readable storage label used by dashboard/monitoring APIs."""
    value = _safe_int(bytes_value) or 0
    if value >= 1024 ** 3:
        return f"{round(value / (1024 ** 3), 2)} GB"
    return f"{round(value / (1024 ** 2), 2)} MB"


class StorageService:
    """Manages backup file storage in Appwrite Storage."""
    
    @staticmethod
    async def upload_backup_file(
        file_content: str,
        file_name: str,
        compress: bool = True,
        encrypt: bool = True,
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Upload a backup file to Appwrite Storage with optional compression and encryption.
        
        Args:
            file_content: Backup content as string
            file_name: Name for the backup file (without extension)
            compress: Whether to compress with gzip
            encrypt: Whether to encrypt with Fernet
        
        Returns:
            Tuple of (file_id, storage_path, file_size) or (None, None, None) on error
        """
        if not APPWRITE_STORAGE_BUCKET_ID:
            logger.error("APPWRITE_STORAGE_BUCKET_ID not configured")
            return None, None, None
        
        temp_file = None
        try:
            # Convert content to bytes
            file_bytes = file_content.encode("utf-8")
            logger.info("Starting upload: filename=%s, size=%d bytes", file_name, len(file_bytes))
            
            # Compress if requested
            if compress:
                file_bytes = await asyncio.to_thread(gzip_compress, file_bytes)
                file_name_final = f"{file_name}.gz"
                logger.info("Compressed file: %s (%d bytes)", file_name_final, len(file_bytes))
            else:
                file_name_final = file_name
            
            # Encrypt if requested and key available
            if encrypt:
                encryption_key = get_backup_key_optional()
                if encryption_key:
                    file_bytes = await asyncio.to_thread(
                        encrypt_file, file_bytes, encryption_key
                    )
                    file_name_final = f"{file_name_final}.enc"
                    logger.info("Encrypted file: %s (%d bytes)", file_name_final, len(file_bytes))
                else:
                    logger.warning("Encryption requested but key not configured")
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as tmp:
                tmp.write(file_bytes)
                temp_file = tmp.name
            
            # Upload to Appwrite Storage
            def _upload_file():
                file_obj = InputFile.from_path(temp_file, file_name_final)
                result = storage.create_file(
                    bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                    file_id="unique()",
                    file=file_obj,
                )
                return result
            
            result = await asyncio.to_thread(_upload_file)
            
            # Extract file ID and size
            file_id = result.get("$id") if hasattr(result, "$id") else result.get("$id")
            file_size = result.size if hasattr(result, "size") else result.get("size")
            
            logger.info(
                "File uploaded successfully: file_id=%s, size=%d bytes, filename=%s",
                file_id, file_size, file_name_final
            )
            
            return file_id, file_name_final, file_size
        
        except Exception as e:
            logger.error("Failed to upload file: %s", e)
            return None, None, None
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                    logger.debug("Cleaned up temporary file")
                except Exception as e:
                    logger.warning("Failed to clean up temporary file: %s", e)
    
    @staticmethod
    async def download_backup_file(
        file_id: str,
        decompress: bool = True,
        decrypt: bool = True,
    ) -> Optional[str]:
        """
        Download a backup file from Appwrite Storage with optional decompression and decryption.
        
        Args:
            file_id: Appwrite file ID
            decompress: Whether to decompress if gzip
            decrypt: Whether to decrypt if encrypted
        
        Returns:
            File content as string or None on error
        """
        if not APPWRITE_STORAGE_BUCKET_ID:
            logger.error("APPWRITE_STORAGE_BUCKET_ID not configured")
            return None
        
        try:
            logger.info("Starting download: file_id=%s", file_id)
            
            # Download file
            def _download_file():
                return storage.get_file_download(
                    bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                    file_id=file_id,
                )
            
            file_bytes = await asyncio.to_thread(_download_file)
            logger.info("Downloaded file: %d bytes", len(file_bytes))
            
            # Decrypt if needed
            if decrypt:
                encryption_key = get_backup_key_optional()
                if encryption_key and file_id.endswith(".enc"):
                    file_bytes = await asyncio.to_thread(
                        decrypt_bytes, file_bytes, encryption_key
                    )
                    logger.info("Decrypted file: %d bytes", len(file_bytes))
            
            # Decompress if needed
            if decompress and file_id.endswith(".gz"):
                file_bytes = await asyncio.to_thread(gzip_decompress, file_bytes)
                logger.info("Decompressed file: %d bytes", len(file_bytes))
            
            # Convert bytes to string
            content = file_bytes.decode("utf-8")
            logger.info("File downloaded and processed successfully")
            
            return content
        
        except Exception as e:
            logger.error("Failed to download file: %s", e)
            return None
    
    @staticmethod
    async def delete_backup_file(file_id: str) -> bool:
        """
        Delete a backup file from Appwrite Storage.
        
        Args:
            file_id: Appwrite file ID
        
        Returns:
            True if successful
        """
        if not APPWRITE_STORAGE_BUCKET_ID:
            logger.error("APPWRITE_STORAGE_BUCKET_ID not configured")
            return False
        
        try:
            def _delete_file():
                storage.delete_file(
                    bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                    file_id=file_id,
                )
            
            await asyncio.to_thread(_delete_file)
            logger.info("File deleted successfully: file_id=%s", file_id)
            return True
        
        except Exception as e:
            logger.error("Failed to delete file: %s", e)
            return False
    
    @staticmethod
    async def get_file_info(file_id: str) -> Optional[dict]:
        """
        Get information about a stored file.
        
        Args:
            file_id: Appwrite file ID
        
        Returns:
            File metadata dictionary or None
        """
        if not APPWRITE_STORAGE_BUCKET_ID:
            logger.error("APPWRITE_STORAGE_BUCKET_ID not configured")
            return None
        
        try:
            def _get_file():
                return storage.get_file(
                    bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                    file_id=file_id,
                )
            
            result = await asyncio.to_thread(_get_file)

            # file_info = {
            #     "file_id": result.get($id) if hasattr(result, "$id") else result.get("$id"),
            #     "file_name": result.name if hasattr(result, "name") else result.get("name"),
            #     "file_size": result.size if hasattr(result, "size") else result.get("size"),
            #     "created_at": result.created_at if hasattr(result, "created_at") else result.get("created_at"),
            #     "updated_at": result.updated_at if hasattr(result, "updated_at") else result.get("updated_at"),
            # }

            file_info = {
                "file_id": result.get("$id") if isinstance(result, dict) else getattr(result, "$id", None),
                "file_name": result.name if hasattr(result, "name") else result.get("name"),
                "file_size": result.size if hasattr(result, "size") else result.get("size"),
                "created_at": result.created_at if hasattr(result, "created_at") else result.get("created_at"),
                "updated_at": result.updated_at if hasattr(result, "updated_at") else result.get("updated_at"),
            }
            
            logger.info("Retrieved file info: %s", file_info)
            return file_info
        
        except Exception as e:
            logger.error("Failed to get file info: %s", e)
            return None
    
    @staticmethod
    async def list_backup_files(
        limit: int = 100,
        offset: int = 0,
    ) -> Optional[list]:
        """
        List backup files in storage.
        
        Args:
            limit: Maximum number of files to return
            offset: Offset for pagination
        
        Returns:
            List of file metadata dictionaries
        """
        if not APPWRITE_STORAGE_BUCKET_ID:
            logger.error("APPWRITE_STORAGE_BUCKET_ID not configured")
            return None
        
        try:
            def _list_files():
                return storage.list_files(
                    bucket_id=APPWRITE_STORAGE_BUCKET_ID,
                    limit=limit,
                    offset=offset,
                )
            
            result = await asyncio.to_thread(_list_files)
            
            files = []
            if hasattr(result, "files"):
                for f in result.files:
                    files.append({
            "file_id": getattr(f, "$id", None) if not isinstance(f, dict) else f.get("$id"),
            "file_name": f.name if hasattr(f, "name") else f.get("name"),
            "file_size": f.size if hasattr(f, "size") else f.get("size"),
            "created_at": f.created_at if hasattr(f, "created_at") else f.get("created_at"),
        })
            elif isinstance(result, dict) and "files" in result:
                files = result["files"]
            
            logger.info("Listed %d files", len(files))
            return files

        # files_list.append({
        #     "file_id": getattr(f, "$id", None) if not isinstance(f, dict) else f.get("$id"),
        #     "file_name": f.name if hasattr(f, "name") else f.get("name"),
        #     "file_size": f.size if hasattr(f, "size") else f.get("size"),
        #     "created_at": f.created_at if hasattr(f, "created_at") else f.get("created_at"),
        # })
        
        except Exception as e:
            logger.error("Failed to list files: %s", e)
            return None


async def get_total_storage_capacity_bytes() -> tuple[Optional[int], str]:
    """
    Returns (capacity_bytes, source) where source is one of: env, appwrite, unknown.
    """
    env_value = _safe_int(APPWRITE_TOTAL_STORAGE_BYTES)
    if env_value and env_value > 0:
        return env_value, "env"

    if not APPWRITE_STORAGE_BUCKET_ID:
        return None, "unknown"

    # Best effort: Appwrite SDK versions differ; inspect known bucket fields if available.
    try:
        if hasattr(storage, "get_bucket"):
            bucket = await asyncio.to_thread(
                storage.get_bucket,
                bucket_id=APPWRITE_STORAGE_BUCKET_ID,
            )
            data = bucket if isinstance(bucket, dict) else getattr(bucket, "_data", {})
            if not isinstance(data, dict):
                data = {}

            for key in (
                "maximumBucketSize",
                "maximum_bucket_size",
                "maxBucketSize",
                "max_bucket_size",
            ):
                val = _safe_int(data.get(key))
                if val and val > 0:
                    return val, "appwrite"
    except Exception:
        return None, "unknown"

    return None, "unknown"


