from fastapi import APIRouter
from fastapi.responses import FileResponse

from pathlib import Path

path_current = Path(__file__)
path_dashboard = path_current.parent.parent.parent / "templates" / "dashboard.html"

router = APIRouter()

@router.get("/")
async def dashboard() -> FileResponse:
    try:
        return FileResponse(path_dashboard)
    except FileNotFoundError:
        return {"error": "Dashboard file not found" }
