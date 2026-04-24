from dotenv import load_dotenv
import os

load_dotenv()


def _to_bool(value: str | None, default: bool = False) -> bool:
	if value is None:
		return default
	return str(value).strip().lower() in {"1", "true", "yes", "on"}

APPWRITE_ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY = os.getenv("APPWRITE_API_KEY")
APPWRITE_STORAGE_BUCKET_ID = os.getenv("APPWRITE_STORAGE_BUCKET_ID")
APPWRITE_TOTAL_STORAGE_BYTES = os.getenv("APPWRITE_TOTAL_STORAGE_BYTES")
DATABASE_ID = os.getenv("DATABASE_ID")
COLLECTION_ID = os.getenv("COLLECTION_ID")
USER_COLLECTION_ID = os.getenv("USER_COLLECTION_ID")
USER_DATABASES_COLLECTION_ID = os.getenv("USER_DATABASES_COLLECTION_ID")
BACKUPS_COLLECTION_ID = os.getenv("BACKUPS_COLLECTION_ID")
RESTORES_COLLECTION_ID = os.getenv("RESTORES_COLLECTION_ID")
LOGS_COLLECTION_ID = os.getenv("LOGS_COLLECTION_ID")
NOTIFICATIONS_COLLECTION_ID = os.getenv("NOTIFICATIONS_COLLECTION_ID")
BACKUP_SCHEDULES_COLLECTION_ID = os.getenv("BACKUP_SCHEDULES_COLLECTION_ID")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Asia/Kolkata")
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "69db9c1ebb14cfdb64e5")

# JWT Settings
JWT_SECRET = os.getenv("JWT_SECRET", "wertyuiop;lkjhgfdertyuikjhgdrtyuikbvtyuknbvfr67ikfr")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRATION_MINUTES = int(os.getenv("JWT_EXPIRATION_MINUTES", "120"))

# Encryption key for database connection passwords (Fernet AES)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

# Encryption key for backup files (base64-encoded 32 bytes for AES-256)
BACKUP_ENCRYPTION_KEY = os.getenv("BACKUP_ENCRYPTION_KEY")

# Frontend route used by Appwrite email verification links.


