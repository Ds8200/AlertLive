import httpx

from app.core.config import settings


async def fetch_alerts(since_ts: int) -> list[dict]:
    url = f"{settings.api_url}?since={since_ts}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json().get("alerts", [])
    except httpx.RequestError as e:
        print(f"[fetcher] Request failed: {e}")
        return []
    except Exception as e:
        print(f"[fetcher] Unexpected error: {e}")
        return []
