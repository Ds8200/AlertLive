import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.alert_store import store
from app.services.broadcaster import manager

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)

    # Replay the last hour of alerts so the client is immediately up-to-date.
    # Alerts are delivered oldest → newest, matching natural arrival order.
    # A sentinel {"type": "history_end"} is sent after replay so the client
    # knows subsequent messages are live and should trigger nearby alerts.
    recent = store.get_recent()
    for alert in recent:
        await ws.send_text(json.dumps(alert.to_dict(), ensure_ascii=False))
    await ws.send_text(json.dumps({"type": "history_end"}))
    if recent:
        print(f"[ws] Replayed {len(recent)} historical alert(s) to new client")

    try:
        while True:
            await ws.receive_text()  # keep connection alive, ignore incoming messages
    except WebSocketDisconnect:
        manager.disconnect(ws)
