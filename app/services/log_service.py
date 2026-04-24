"""Logging disabled placeholder implementations."""


async def create_log_entry(*args, **kwargs):
    return {}


async def get_log(*args, **kwargs):
    return None


async def update_log_entry(*args, **kwargs):
    return None


async def list_logs(*args, **kwargs):
    return {"rows": [], "total": 0}


async def list_logs_for_user(*args, **kwargs):
    return {"rows": [], "total": 0}



async def export_logs(*args, **kwargs):
    return "text/plain", ""


async def summarize_logs(*args, **kwargs):
    return {"total": 0, "by_status": {}, "by_operation": {}, "average_duration_seconds": None}

