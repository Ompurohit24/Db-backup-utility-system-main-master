"""
Incremental Restore Service: Handles restoration from full and incremental backups.
Implements chronological application of incremental backups.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from app.logger import get_logger
from app.utils.incremental_backup_engine import IncrementalBackupEngine

logger = get_logger("incremental_restore")


class IncrementalRestoreService:
    """Orchestrates incremental restore operations."""
    
    @staticmethod
    def validate_restore_data(restore_data: Dict[str, Any]) -> bool:
        """
        Validate restore data before restoration.
        
        Args:
            restore_data: Backup data to restore
        
        Returns:
            True if valid
        """
        try:
            if "type" not in restore_data:
                logger.error("Missing 'type' in restore data")
                return False
            
            if restore_data["type"] not in ("full", "incremental"):
                logger.error("Invalid restore type: %s", restore_data["type"])
                return False
            
            if "data" not in restore_data:
                logger.error("Missing 'data' in restore data")
                return False
            
            logger.info("Restore data validation passed")
            return True
        except Exception as e:
            logger.error("Restore data validation failed: %s", e)
            return False
    
    @staticmethod
    def extract_records_from_full_backup(backup_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract records from a full backup.
        
        Args:
            backup_data: Full backup data
        
        Returns:
            List of records to restore
        """
        if backup_data.get("type") != "full":
            raise ValueError("Expected full backup type")
        
        records = backup_data.get("data", [])
        logger.info("Extracted %d records from full backup", len(records))
        return records
    
    @staticmethod
    def extract_changes_from_incremental(
        backup_data: Dict[str, Any],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Extract changes from an incremental backup.
        
        Args:
            backup_data: Incremental backup data
        
        Returns:
            Tuple of (changed_records, deleted_ids)
        """
        if backup_data.get("type") != "incremental":
            raise ValueError("Expected incremental backup type")
        
        data = backup_data.get("data", {})
        changed_records = data.get("new_and_updated", [])
        deleted_list = data.get("deleted", [])
        deleted_ids = [d.get("id") or d.get("$id") for d in deleted_list]
        
        logger.info(
            "Extracted %d changes and %d deletions from incremental backup",
            len(changed_records), len(deleted_ids)
        )
        
        return changed_records, deleted_ids
    
    @staticmethod
    def apply_full_backup(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply a full backup (replace all records).
        
        Args:
            records: Records from full backup
        
        Returns:
            Dictionary with restored state
        """
        state = {
            "records": {rec.get("$id") or rec.get("id"): rec for rec in records},
            "record_ids": [rec.get("$id") or rec.get("id") for rec in records],
        }
        logger.info("Applied full backup with %d records", len(records))
        return state
    
    @staticmethod
    def apply_incremental_backup(
        current_state: Dict[str, Any],
        changed_records: List[Dict[str, Any]],
        deleted_ids: List[str],
    ) -> Dict[str, Any]:
        """
        Apply incremental backup to current state.
        
        Args:
            current_state: Current restored state
            changed_records: New and updated records
            deleted_ids: IDs of deleted records
        
        Returns:
            Updated state
        """
        records_dict = current_state.get("records", {})
        record_ids = current_state.get("record_ids", [])
        
        # Apply changed records
        for record in changed_records:
            record_id = record.get("$id") or record.get("id")
            records_dict[record_id] = record
            
            if record_id not in record_ids:
                record_ids.append(record_id)
            
            logger.debug("Updated/added record: %s", record_id)
        
        # Apply deletions (soft delete)
        for deleted_id in deleted_ids:
            if deleted_id in records_dict:
                # Mark as deleted or remove
                records_dict[deleted_id]["is_deleted"] = True
                # Optionally keep deleted records with is_deleted flag
                # Or remove them entirely:
                # del records_dict[deleted_id]
                # record_ids.remove(deleted_id)
                logger.debug("Marked record as deleted: %s", deleted_id)
        
        state = {
            "records": records_dict,
            "record_ids": record_ids,
        }
        logger.info(
            "Applied incremental backup: %d changes, %d deletions",
            len(changed_records), len(deleted_ids)
        )
        return state
    
    @staticmethod
    def prepare_records_for_restore(
        state: Dict[str, Any],
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Prepare final list of records for restoration.
        
        Args:
            state: Final restored state
            include_deleted: Whether to include soft-deleted records
        
        Returns:
            List of records ready for restoration
        """
        records_dict = state.get("records", {})
        records = []
        
        for record in records_dict.values():
            if not include_deleted and record.get("is_deleted"):
                continue
            records.append(record)
        
        logger.info("Prepared %d records for restoration", len(records))
        return records
    
    @staticmethod
    def get_restore_summary(state: Dict[str, Any], include_deleted: bool = False) -> str:
        """
        Get a summary of restored state.
        
        Args:
            state: Final restored state
            include_deleted: Whether to count deleted records
        
        Returns:
            Summary string
        """
        records_dict = state.get("records", {})
        active_count = 0
        deleted_count = 0
        
        for record in records_dict.values():
            if record.get("is_deleted"):
                deleted_count += 1
            else:
                active_count += 1
        
        summary = f"Active: {active_count}, Deleted: {deleted_count}"
        return summary


def merge_backups(
    backups: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Merge multiple backups chronologically.
    First backup must be full, subsequent must be incremental.
    
    Args:
        backups: List of backups in chronological order
    
    Returns:
        Merged backup state
    """
    if not backups:
        raise ValueError("No backups provided")
    
    # First backup should be full
    if backups[0].get("type") != "full":
        logger.warning("First backup is not full type, treating as full")
    
    # Apply full backup first
    state = IncrementalRestoreService.apply_full_backup(
        IncrementalRestoreService.extract_records_from_full_backup(backups[0])
    )
    logger.info("Applied base full backup")
    
    # Apply incremental backups in order
    for i, backup in enumerate(backups[1:], 1):
        if backup.get("type") != "incremental":
            logger.warning("Backup %d is not incremental type, skipping", i)
            continue
        
        changed_records, deleted_ids = (
            IncrementalRestoreService.extract_changes_from_incremental(backup)
        )
        state = IncrementalRestoreService.apply_incremental_backup(
            state, changed_records, deleted_ids
        )
        logger.info("Applied incremental backup %d", i)
    
    logger.info("Backup merge complete: %s", 
                IncrementalRestoreService.get_restore_summary(state))
    
    return state

