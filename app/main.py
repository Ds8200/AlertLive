import asyncio
import sys
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI

from app.api.routes import health, ws, dashboard
from app.core.config import settings
from app.core.poller import poll_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_loop())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="RocketIL Dashboard", lifespan=lifespan)

app.include_router(ws.router)
app.include_router(health.router)
app.include_router(dashboard.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, reload=True)
