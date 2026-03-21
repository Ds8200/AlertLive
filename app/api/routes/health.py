from fastapi import APIRouter

from app.services.broadcaster import manager

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {"status": "ok", "connected_clients": manager.count}
