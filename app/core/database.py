from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from app.core.appwrite_client import tables   # ← new TablesDB API
from app.config import DATABASE_ID, COLLECTION_ID
from app.utils.dependencies import get_current_user
import asyncio

router = APIRouter(tags=["Database"])

@router.get("/data")
async def add_database(name: str, current_user: dict = Depends(get_current_user)):
    try:
        response = await asyncio.to_thread(
            tables.create_row,
            database_id=DATABASE_ID,
            table_id=COLLECTION_ID,
            row_id="unique()",
            data={
                "name": name,
                "status": "active"
            }
        )
        return response
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
