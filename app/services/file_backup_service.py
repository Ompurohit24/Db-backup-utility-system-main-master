"""File backup service disabled."""


def create_backup(*args, **kwargs):
    return {"success": False, "message": "File backup disabled"}


def restore_backup(*args, **kwargs):
    return {"success": False, "message": "File restore disabled"}


def list_backups(*args, **kwargs):
    return []


def delete_backup(*args, **kwargs):
    return {"success": False, "message": "File backup delete disabled"}


def scheduled_backup(*args, **kwargs):
    return None
