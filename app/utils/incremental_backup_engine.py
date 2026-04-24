"""
Incremental Backup Engine: Implements timestamp-based incremental backup strategy.
Supports both full and incremental backups with soft delete handling.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict

from app.logger import get_logger

logger = get_logger("incremental_backup")


@dataclass
class BackupMetadataInfo:
    """Metadata for incremental backup."""
    type: str  # "full" or "incremental"
    timestamp: str  # ISO 8601 datetime
    database_name: str
    db_config_id: str
    record_count: int
    deleted_count: int
    backup_start_time: str
    backup_end_time: str


class IncrementalBackupEngine:
    """Handles incremental backup operations with timestamp-based change detection."""
    
    @staticmethod
    def create_full_backup(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a full backup containing all records.
        
        Args:
            records: List of all database records
        
        Returns:
            Backup data dictionary
        """
        return {
            "type": "full",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_records": len(records),
            "data": records,
        }
    
    @staticmethod
    def create_incremental_backup(
        new_records: List[Dict[str, Any]],
        updated_records: List[Dict[str, Any]],
        deleted_records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Create an incremental backup with new, updated, and deleted records.
        
        Args:
            new_records: Records created since last backup
            updated_records: Records modified since last backup
            deleted_records: Records deleted (soft deleted) since last backup
        
        Returns:
            Backup data dictionary
        """
        # Combine new and updated records
        changed_records = new_records + updated_records
        
        return {
            "type": "incremental",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_changed": len(changed_records),
            "total_deleted": len(deleted_records),
            "data": {
                "new_and_updated": changed_records,
                "deleted": [
                    {
                        "id": rec.get("$id") or rec.get("id"),
                        "deleted_at": rec.get("deleted_at"),
                    }
                    for rec in deleted_records
                ],
            },
        }
    
    @staticmethod
    def detect_changes(
        records: List[Dict[str, Any]],
        last_backup_time: datetime,
    ) -> tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Detect new, updated, and deleted records based on timestamps.
        
        Args:
            records: All current records from database
            last_backup_time: Timestamp of last successful backup
        
        Returns:
            Tuple of (new_records, updated_records, deleted_records)
        """
        new_records = []
        updated_records = []
        deleted_records = []
        
        for record in records:
            # Parse timestamps
            created_at_str = record.get("created_at") or record.get("$createdAt")
            updated_at_str = record.get("updated_at") or record.get("$updatedAt")
            deleted_at_str = record.get("deleted_at")
            is_deleted = record.get("is_deleted", False)
            
            try:
                created_at = datetime.fromisoformat(created_at_str) if created_at_str else None
                updated_at = datetime.fromisoformat(updated_at_str) if updated_at_str else None
                deleted_at = datetime.fromisoformat(deleted_at_str) if deleted_at_str else None
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid timestamp format for record %s: created=%s, updated=%s, deleted=%s",
                    record.get("$id"), created_at_str, updated_at_str, deleted_at_str
                )
                continue
            
            # Check if record is deleted
            if is_deleted and deleted_at and deleted_at > last_backup_time:
                deleted_records.append(record)
                logger.debug(
                    "Detected deleted record: id=%s, deleted_at=%s",
                    record.get("$id"), deleted_at
                )
            # Check if record was created after last backup
            elif created_at and created_at > last_backup_time:
                new_records.append(record)
                logger.debug(
                    "Detected new record: id=%s, created_at=%s",
                    record.get("$id"), created_at
                )
            # Check if record was updated after last backup
            elif updated_at and updated_at > last_backup_time:
                updated_records.append(record)
                logger.debug(
                    "Detected updated record: id=%s, updated_at=%s",
                    record.get("$id"), updated_at
                )
        
        logger.info(
            "Change detection complete: new=%d, updated=%d, deleted=%d",
            len(new_records), len(updated_records), len(deleted_records)
        )
        
        return new_records, updated_records, deleted_records
    
    @staticmethod
    def validate_backup_integrity(backup_data: Dict[str, Any]) -> bool:
        """
        Validate backup integrity before storage.
        
        Args:
            backup_data: Backup data dictionary
        
        Returns:
            True if backup is valid
        """
        try:
            # Check required fields
            if "type" not in backup_data:
                logger.error("Missing 'type' in backup data")
                return False
            
            if backup_data["type"] not in ("full", "incremental"):
                logger.error("Invalid backup type: %s", backup_data["type"])
                return False
            
            if "timestamp" not in backup_data:
                logger.error("Missing 'timestamp' in backup data")
                return False
            
            if "data" not in backup_data:
                logger.error("Missing 'data' in backup data")
                return False
            
            # Try to serialize to JSON to ensure it's valid
            json.dumps(backup_data)
            
            logger.info("Backup integrity validation passed")
            return True
        except Exception as e:
            logger.error("Backup integrity validation failed: %s", e)
            return False
    
    @staticmethod
    def serialize_backup(backup_data: Dict[str, Any]) -> str:
        """
        Serialize backup data to JSON string.
        
        Args:
            backup_data: Backup data dictionary
        
        Returns:
            JSON string representation
        """
        try:
            json_str = json.dumps(backup_data, indent=2, default=str)
            logger.debug("Backup serialized successfully: %d bytes", len(json_str))
            return json_str
        except Exception as e:
            logger.error("Failed to serialize backup: %s", e)
            raise
    
    @staticmethod
    def deserialize_backup(json_str: str) -> Dict[str, Any]:
        """
        Deserialize backup from JSON string.
        
        Args:
            json_str: JSON string representation
        
        Returns:
            Backup data dictionary
        """
        try:
            backup_data = json.loads(json_str)
            logger.debug("Backup deserialized successfully")
            return backup_data
        except Exception as e:
            logger.error("Failed to deserialize backup: %s", e)
            raise
    
    @staticmethod
    def get_backup_summary(backup_data: Dict[str, Any]) -> str:
        """
        Get a human-readable summary of backup.
        
        Args:
            backup_data: Backup data dictionary
        
        Returns:
            Summary string
        """
        backup_type = backup_data.get("type", "unknown")
        timestamp = backup_data.get("timestamp", "unknown")
        
        if backup_type == "full":
            record_count = backup_data.get("total_records", 0)
            return f"Full Backup at {timestamp}: {record_count} records"
        else:
            changed = backup_data.get("total_changed", 0)
            deleted = backup_data.get("total_deleted", 0)
            return f"Incremental Backup at {timestamp}: {changed} changed, {deleted} deleted"


def is_empty_incremental(backup_data: Dict[str, Any]) -> bool:
    """
    Check if incremental backup is empty (no changes).
    
    Args:
        backup_data: Backup data dictionary
    
    Returns:
        True if incremental backup has no changes
    """
    if backup_data.get("type") != "incremental":
        return False
    
    changed = backup_data.get("total_changed", 0)
    deleted = backup_data.get("total_deleted", 0)
    
    return changed == 0 and deleted == 0

