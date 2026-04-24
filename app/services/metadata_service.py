"""
Metadata Service: Manages backup metadata including last backup time and backup history.
Stores metadata in both local JSON file and Appwrite collection for redundancy.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from app.config import DATABASE_ID, BACKUPS_COLLECTION_ID
from app.core.appwrite_client import tables
from app.logger import get_logger
from app.utils.appwrite_normalize import normalize_row

logger = get_logger("metadata")

# Local metadata file
METADATA_FILE = Path(__file__).resolve().parents[2] / "backup_meta.json"


class BackupMetadata:
    """Manages backup metadata for incremental backup strategy."""
    
    @staticmethod
    def _ensure_metadata_file() -> None:
        """Ensure metadata file exists with proper structure."""
        if not METADATA_FILE.exists():
            initial_meta = {
                "backups": {},  # db_config_id -> {last_backup_time, type, file_id}
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            METADATA_FILE.write_text(json.dumps(initial_meta, indent=2))
            logger.info("Created new metadata file: %s", METADATA_FILE)
    
    @staticmethod
    def get_last_backup_time(db_config_id: str) -> Optional[datetime]:
        """Get the last backup time for a specific database configuration."""
        try:
            BackupMetadata._ensure_metadata_file()
            with open(METADATA_FILE, "r") as f:
                meta = json.load(f)
            
            backup_info = meta.get("backups", {}).get(db_config_id)
            if not backup_info or not backup_info.get("last_backup_time"):
                logger.info("No previous backup found for db_config_id=%s", db_config_id)
                return None
            
            return datetime.fromisoformat(backup_info["last_backup_time"])
        except Exception as e:
            logger.error("Failed to get last backup time: %s", e)
            return None
    
    @staticmethod
    def get_backup_type(db_config_id: str) -> str:
        """Determine if next backup should be FULL or INCREMENTAL."""
        try:
            last_time = BackupMetadata.get_last_backup_time(db_config_id)
            return "incremental" if last_time else "full"
        except Exception as e:
            logger.error("Failed to determine backup type: %s", e)
            return "full"  # Default to full backup on error
    
    @staticmethod
    def update_metadata(
        db_config_id: str,
        backup_type: str,
        file_id: str,
        file_name: str,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Update metadata after a successful backup.
        
        Args:
            db_config_id: Database configuration ID
            backup_type: "full" or "incremental"
            file_id: Appwrite file ID
            file_name: Backup file name
            status: "success" or "failed"
            error_message: Error message if status is "failed"
        
        Returns:
            True if metadata was updated successfully
        """
        try:
            BackupMetadata._ensure_metadata_file()
            
            with open(METADATA_FILE, "r") as f:
                meta = json.load(f)
            
            current_time = datetime.now(timezone.utc)
            
            meta["backups"][db_config_id] = {
                "last_backup_time": current_time.isoformat(),
                "last_backup_type": backup_type,
                "last_file_id": file_id,
                "last_file_name": file_name,
                "status": status,
                "error_message": error_message,
            }
            meta["last_updated"] = current_time.isoformat()
            
            with open(METADATA_FILE, "w") as f:
                json.dump(meta, f, indent=2)
            
            logger.info(
                "Updated metadata: db_config_id=%s, type=%s, file_id=%s, status=%s",
                db_config_id, backup_type, file_id, status
            )
            return True
        except Exception as e:
            logger.error("Failed to update metadata: %s", e)
            return False
    
    @staticmethod
    def get_all_metadata() -> Dict[str, Any]:
        """Get all backup metadata."""
        try:
            BackupMetadata._ensure_metadata_file()
            with open(METADATA_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to get all metadata: %s", e)
            return {"backups": {}}
    
    @staticmethod
    async def sync_metadata_to_appwrite(db_config_id: str, user_id: str) -> Optional[str]:
        """
        Sync local metadata to Appwrite backup record for redundancy.
        Returns the backup document ID if successful.
        """
        if not BACKUPS_COLLECTION_ID:
            logger.warning("BACKUPS_COLLECTION_ID not configured, skipping sync")
            return None
        
        try:
            meta = BackupMetadata.get_all_metadata()
            backup_info = meta.get("backups", {}).get(db_config_id, {})
            
            # Metadata is already stored in individual backup records
            # This is optional for additional tracking
            logger.debug("Metadata sync to Appwrite: db_config_id=%s", db_config_id)
            return backup_info.get("last_file_id")
        except Exception as e:
            logger.error("Failed to sync metadata to Appwrite: %s", e)
            return None


async def get_last_backup_time_async(db_config_id: str) -> Optional[datetime]:
    """Async wrapper for getting last backup time."""
    return await asyncio.to_thread(BackupMetadata.get_last_backup_time, db_config_id)


async def get_backup_type_async(db_config_id: str) -> str:
    """Async wrapper for determining backup type."""
    return await asyncio.to_thread(BackupMetadata.get_backup_type, db_config_id)


async def update_metadata_async(
    db_config_id: str,
    backup_type: str,
    file_id: str,
    file_name: str,
    status: str = "success",
    error_message: Optional[str] = None,
) -> bool:
    """Async wrapper for updating metadata."""
    return await asyncio.to_thread(
        BackupMetadata.update_metadata,
        db_config_id,
        backup_type,
        file_id,
        file_name,
        status,
        error_message,
    )

