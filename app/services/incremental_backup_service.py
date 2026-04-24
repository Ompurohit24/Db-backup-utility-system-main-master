"""
Incremental Backup Service: Main service for incremental backup operations.
Orchestrates metadata tracking, change detection, and backup creation.
"""

import asyncio
from typing import List, Dict, Any

from app.core.appwrite_client import tables
from appwrite.query import Query
from app.logger import get_logger
from app.services.metadata_service import (
    get_last_backup_time_async,
    get_backup_type_async,
)
from app.utils.incremental_backup_engine import (
    IncrementalBackupEngine,
    is_empty_incremental,
)

logger = get_logger("incremental")


async def _list_all_table_rows(database_id: str, table_id: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """Fetch all table rows using paginated TablesDB list_rows."""
    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        result = await asyncio.to_thread(
            tables.list_rows,
            database_id=database_id,
            table_id=table_id,
            queries=[Query.limit(page_size), Query.offset(offset)],
        )

        page = result.get("rows", result.get("documents", [])) if isinstance(result, dict) else []
        rows.extend(page)

        if len(page) < page_size:
            break
        offset += page_size

    return rows


class IncrementalBackupService:
    """Orchestrates incremental backup operations."""
    
    @staticmethod
    async def prepare_backup_for_table(
        database_id: str,
        table_id: str,
        db_config_id: str,
    ) -> Dict[str, Any]:
        """
        Prepare backup data for a specific table (Appwrite Tables).
        
        Args:
            database_id: Appwrite database ID
            table_id: Appwrite table ID
            db_config_id: User's database configuration ID
        
        Returns:
            Backup data dictionary (full or incremental)
        """
        try:
            # Determine backup type
            backup_type = await get_backup_type_async(db_config_id)
            logger.info("Preparing %s backup for table_id=%s", backup_type, table_id)
            
            # Fetch records based on backup type
            if backup_type == "full":
                backup_data = await _prepare_full_backup(database_id, table_id)
            else:
                backup_data = await _prepare_incremental_backup(
                    database_id, table_id, db_config_id
                )
            
            # Validate backup integrity
            if not IncrementalBackupEngine.validate_backup_integrity(backup_data):
                raise ValueError("Backup integrity validation failed")
            
            logger.info(
                "Backup prepared: type=%s, %s",
                backup_data.get("type"),
                IncrementalBackupEngine.get_backup_summary(backup_data)
            )
            
            return backup_data
        except Exception as e:
            logger.error("Failed to prepare backup: %s", e)
            raise
    
    @staticmethod
    async def prepare_backup_for_external_db(
        records: List[Dict[str, Any]],
        db_config_id: str,
    ) -> Dict[str, Any]:
        """
        Prepare backup data for external database (MySQL, PostgreSQL, MongoDB).
        
        Args:
            records: List of records from external database
            db_config_id: User's database configuration ID
        
        Returns:
            Backup data dictionary (full or incremental)
        """
        try:
            # Determine backup type
            backup_type = await get_backup_type_async(db_config_id)
            logger.info("Preparing %s backup for external database", backup_type)
            
            if backup_type == "full":
                backup_data = IncrementalBackupEngine.create_full_backup(records)
            else:
                # Get last backup time
                last_backup_time = await get_last_backup_time_async(db_config_id)
                if not last_backup_time:
                    logger.warning("No last backup time found, falling back to full backup")
                    backup_data = IncrementalBackupEngine.create_full_backup(records)
                else:
                    # Detect changes
                    new_records, updated_records, deleted_records = (
                        IncrementalBackupEngine.detect_changes(records, last_backup_time)
                    )
                    
                    backup_data = IncrementalBackupEngine.create_incremental_backup(
                        new_records, updated_records, deleted_records
                    )
                    
                    # Log if incremental backup is empty
                    if is_empty_incremental(backup_data):
                        logger.info("Incremental backup is empty (no changes)")
            
            # Validate backup integrity
            if not IncrementalBackupEngine.validate_backup_integrity(backup_data):
                raise ValueError("Backup integrity validation failed")
            
            logger.info(
                "Backup prepared: type=%s, %s",
                backup_data.get("type"),
                IncrementalBackupEngine.get_backup_summary(backup_data)
            )
            
            return backup_data
        except Exception as e:
            logger.error("Failed to prepare backup: %s", e)
            raise


async def _prepare_full_backup(database_id: str, table_id: str) -> Dict[str, Any]:
    """Prepare a full backup by fetching all records from a table."""
    try:
        records = await _list_all_table_rows(database_id, table_id)
        
        logger.info("Full backup: fetched %d records from table", len(records))
        return IncrementalBackupEngine.create_full_backup(records)
    except Exception as e:
        logger.error("Failed to prepare full backup: %s", e)
        raise


async def _prepare_incremental_backup(
    database_id: str,
    table_id: str,
    db_config_id: str,
) -> Dict[str, Any]:
    """Prepare an incremental backup by detecting changes since last backup."""
    try:
        # Get last backup time
        last_backup_time = await get_last_backup_time_async(db_config_id)
        if not last_backup_time:
            logger.warning("No last backup time, falling back to full backup")
            return await _prepare_full_backup(database_id, table_id)
        
        records = await _list_all_table_rows(database_id, table_id)
        
        # Detect changes
        new_records, updated_records, deleted_records = (
            IncrementalBackupEngine.detect_changes(records, last_backup_time)
        )
        
        logger.info(
            "Incremental backup: new=%d, updated=%d, deleted=%d",
            len(new_records), len(updated_records), len(deleted_records)
        )
        
        return IncrementalBackupEngine.create_incremental_backup(
            new_records, updated_records, deleted_records
        )
    except Exception as e:
        logger.error("Failed to prepare incremental backup: %s", e)
        raise

