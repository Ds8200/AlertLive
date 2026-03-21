import asyncio
import json
import time

from app.core.config import settings
from app.services.alert_store import store
from app.services.broadcaster import manager
from app.services.fetcher import fetch_alerts
from app.services.processor import process_alerts


async def poll_loop() -> None:
    last_ts = int(time.time() * 1000)
    print("[poller] Started")

    while True:
        try:
            raw_alerts = await fetch_alerts(last_ts)

            if raw_alerts:
                alerts = process_alerts(raw_alerts)

                for alert in alerts:
                    store.upsert(alert)  # keep latest version in history
                    payload = json.dumps(alert.to_dict(), ensure_ascii=False)
                    await manager.broadcast(payload)

                last_ts = int(max(a.timestamp.timestamp() * 1000 for a in alerts)) + 1
                print(f"[poller] Sent {len(alerts)} alert(s) to {manager.count} client(s) | store size: {store.size}")

        except asyncio.CancelledError:
            print("[poller] Stopped")
            break
        except Exception as e:
            print(f"[poller] Unexpected error: {e}")

        await asyncio.sleep(settings.poll_interval)
