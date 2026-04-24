"""
One-time setup script.
Creates the 'user_databases' and 'users' collections (and their attributes)
in Appwrite.

Usage:
    python setup_collections.py
"""

from app.core.appwrite_client import databases, storage
from app.config import (
    DATABASE_ID,
    USER_DATABASES_COLLECTION_ID,
    USER_COLLECTION_ID,
    BACKUPS_COLLECTION_ID,
    APPWRITE_STORAGE_BUCKET_ID,
    RESTORES_COLLECTION_ID,
    LOGS_COLLECTION_ID,
    NOTIFICATIONS_COLLECTION_ID,
)
import time

DB_COLLECTION_ID = USER_DATABASES_COLLECTION_ID  # "user_databases"


def create_collection(collection_id: str, name: str):
    """Create a collection by ID and name."""
    try:
        databases.create_collection(
            database_id=DATABASE_ID,
            collection_id=collection_id,
            name=name,
        )
        print(f"✅  Collection '{collection_id}' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️  Collection '{collection_id}' already exists. Skipping.")
        else:
            print(f"❌  Error creating collection: {e}")
            raise


def create_string_attrs(collection_id: str, attrs: list):
    """Create string attributes on a collection."""
    for key, size, required in attrs:
        try:
            databases.create_string_attribute(
                database_id=DATABASE_ID,
                collection_id=collection_id,
                key=key,
                size=size,
                required=required,
            )
            print(f"  ✅  String attribute '{key}' created.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ℹ️  Attribute '{key}' already exists. Skipping.")
            else:
                print(f"  ❌  Error creating '{key}': {e}")


def setup_user_databases_collection():
    """Set up the user_databases collection."""
    print(f"\n🔧  Setting up collection: '{DB_COLLECTION_ID}'")
    create_collection(DB_COLLECTION_ID, "User Databases")

    string_attrs = [
        ("owner_user_id", 255, False),
        ("user_id",       255, True),
        ("database_type", 50,  True),
        ("host",          255, True),
        ("database_name", 255, True),
        ("username",      255, True),
        ("password",      512, True),
        ("status",        50,  True),
        ("created_at",    50,  True),
        ("updated_at",    50,  True),
    ]
    create_string_attrs(DB_COLLECTION_ID, string_attrs)

    # Integer attribute: port
    try:
        databases.create_integer_attribute(
            database_id=DATABASE_ID,
            collection_id=DB_COLLECTION_ID,
            key="port",
            required=True,
            min=1,
            max=65535,
        )
        print("  ✅  Integer attribute 'port' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  ℹ️  Attribute 'port' already exists. Skipping.")
        else:
            print(f"  ❌  Error creating 'port': {e}")


def setup_users_collection():
    """Set up the users collection with password_hash support."""
    print(f"\n🔧  Setting up collection: '{USER_COLLECTION_ID}'")
    create_collection(USER_COLLECTION_ID, "Users")

    string_attrs = [
        ("user_id",       255,  True),
        ("email",         255,  True),
        ("name",          255,  True),
        ("role",          50,   False),
        ("status",        50,   False),
        ("password_hash", 512,  True),
        ("phone",         50,   False),
        ("bio",           1024, False),
        ("created_at",    50,   True),
        ("updated_at",    50,   True),
    ]
    create_string_attrs(USER_COLLECTION_ID, string_attrs)

    # Boolean attribute: is_active
    try:
        databases.create_boolean_attribute(
            database_id=DATABASE_ID,
            collection_id=USER_COLLECTION_ID,
            key="is_active",
            required=False,
            default=True,
        )
        print("  ✅  Boolean attribute 'is_active' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  ℹ️  Attribute 'is_active' already exists. Skipping.")
        else:
            print(f"  ❌  Error creating 'is_active': {e}")


def setup_backups_collection():
    """Set up the backups collection for storing backup metadata."""
    print(f"\n🔧  Setting up collection: '{BACKUPS_COLLECTION_ID}'")
    create_collection(BACKUPS_COLLECTION_ID, "Backups")

    string_attrs = [
        ("db_config_id",  255,  True),
        ("owner_user_id", 255,  False),
        ("user_id",       255,  True),
        ("database_type", 50,   True),
        ("database_name", 255,  True),
        ("file_name",     512,  True),
        ("file_path",     1024, True),
        ("file_id",       255,  False),
        ("storage_bucket",255,  False),
        ("file_size",     50,   True),   # stored as string
        ("original_file_name", 512, False),
        ("original_file_size", 50,  False),
        ("compression",   50,   False),
        ("encryption",    50,   False),
        ("backup_type",   50,   False),
        ("base_backup_id", 255, False),
        ("duration_seconds", 50, False),
        ("status",        50,   True),   # success | failed
        ("error_message", 2048, False),
        ("created_at",    50,   True),
    ]
    create_string_attrs(BACKUPS_COLLECTION_ID, string_attrs)


def setup_restores_collection():
    """Set up the restores collection to log restore operations."""
    if not RESTORES_COLLECTION_ID:
        print("⚠️  RESTORES_COLLECTION_ID not set. Skipping restores collection setup.")
        return

    print(f"\n🔧  Setting up collection: '{RESTORES_COLLECTION_ID}'")
    create_collection(RESTORES_COLLECTION_ID, "Restores")

    string_attrs = [
        ("user_id",       255, True),
        ("db_config_id",  255, True),
        ("backup_id",     255, False),
        ("file_name",     512, False),
        ("source",        50,  True),   # record | upload
        ("status",        50,  True),   # success | failed
        ("message",       2048, False),
        ("created_at",    50,  True),
    ]
    create_string_attrs(RESTORES_COLLECTION_ID, string_attrs)


def setup_logs_collection():
    """Set up the logs collection for backup/restore audit events."""
    if not LOGS_COLLECTION_ID:
        print("⚠️  LOGS_COLLECTION_ID not set. Skipping logs collection setup.")
        return

    print(f"\n🔧  Setting up collection: '{LOGS_COLLECTION_ID}'")
    create_collection(LOGS_COLLECTION_ID, "BackupRestoreLogs")

    string_attrs = [
        ("user_id",          255, True),
        ("role",             50,  False),
        ("operation_type",   50,  True),   # backup | restore
        ("status",           50,  True),   # started | success | failed
        ("database_name",    255, False),
        ("file_name",        512, False),
        ("file_size",        50,  False),
        ("start_time",       64,  True),
        ("end_time",         64,  False),
        ("duration",         32,  False),
        ("error_message",    2048, False),
        ("ip_address",       64,  False),
        ("device_info",      1024, False),
        ("db_config_id",     255, False),
        ("backup_id",        255, False),
        ("restore_id",       255, False),
        ("created_at",       64,  True),
    ]
    create_string_attrs(LOGS_COLLECTION_ID, string_attrs)


def setup_notifications_collection():
    """Set up the notifications collection for user notification feed."""
    if not NOTIFICATIONS_COLLECTION_ID:
        print("⚠️  NOTIFICATIONS_COLLECTION_ID not set. Skipping notifications collection setup.")
        return

    print(f"\n🔧  Setting up collection: '{NOTIFICATIONS_COLLECTION_ID}'")
    create_collection(NOTIFICATIONS_COLLECTION_ID, "Notifications")

    string_attrs = [
        ("user_id", 255, True),
        ("event_type", 100, True),
        ("level", 20, True),
        ("title", 255, True),
        ("message", 2048, True),
        ("resource_id", 255, False),
    ]
    create_string_attrs(NOTIFICATIONS_COLLECTION_ID, string_attrs)

    try:
        databases.create_boolean_attribute(
            database_id=DATABASE_ID,
            collection_id=NOTIFICATIONS_COLLECTION_ID,
            key="is_read",
            required=False,
            default=False,
        )
        print("  ✅  Boolean attribute 'is_read' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  ℹ️  Attribute 'is_read' already exists. Skipping.")
        else:
            print(f"  ❌  Error creating 'is_read': {e}")


def setup_storage_bucket():
    """Set up the Appwrite storage bucket for backup files."""
    if not APPWRITE_STORAGE_BUCKET_ID:
        print("\n⚠️  APPWRITE_STORAGE_BUCKET_ID not set. Skipping storage bucket setup.")
        return

    print(f"\n🔧  Setting up storage bucket: '{APPWRITE_STORAGE_BUCKET_ID}'")
    try:
        storage.get_bucket(bucket_id=APPWRITE_STORAGE_BUCKET_ID)
        print(f"ℹ️  Bucket '{APPWRITE_STORAGE_BUCKET_ID}' already exists. Skipping.")
    except Exception:
        storage.create_bucket(
            bucket_id=APPWRITE_STORAGE_BUCKET_ID,
            name="Database Backups",
            file_security=True,
            enabled=True,
            maximum_file_size=50 * 1024 * 1024,
            allowed_file_extensions=["sql", "json", "gz", "zip"],
            encryption=True,
            antivirus=True,
        )
        print(f"✅  Bucket '{APPWRITE_STORAGE_BUCKET_ID}' created.")


def main():
    print(f"    Database ID: {DATABASE_ID}\n")

    setup_users_collection()
    setup_user_databases_collection()
    setup_backups_collection()
    setup_restores_collection()
    setup_logs_collection()
    setup_notifications_collection()
    setup_storage_bucket()

    print("\n⏳  Waiting 3 seconds for attributes to become available...")
    time.sleep(3)

    print("\n🎉  Done! All collections are ready.\n")


if __name__ == "__main__":
    main()

