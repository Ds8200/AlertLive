import asyncio

from fastapi import WebSocket


class ConnectionManager:
    """
    Manages active WebSocket connections and broadcasts messages to all clients.

    Scale path: replace the in-memory set with a Redis pub/sub channel
    to support multiple server instances behind a load balancer.
    """

    def __init__(self):
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)
        print(f"[broadcaster] Client connected  — total: {self.count}")

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)
        print(f"[broadcaster] Client disconnected — total: {self.count}")

    async def  broadcast(self, message: str) -> None:
        if not self._clients:
            return

        results = await asyncio.gather(
            *[client.send_text(message) for client in self._clients.copy()],
            return_exceptions=True,
        )

        # prune dead connections
        dead = {
            client
            for client, result in zip(list(self._clients.copy()), results)
            if isinstance(result, Exception)
        }
        self._clients -= dead

    @property
    def count(self) -> int:
        return len(self._clients)


manager = ConnectionManager()
