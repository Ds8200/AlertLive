from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models.alert import Alert


class AlertStore:
    """
    In-memory store that keeps the **latest version** of each alert
    received within the last `max_age_seconds` seconds (default: 1 hour).

    Deduplication key  : oref_city
    Retention strategy : alerts older than the TTL are rejected on write
                         and pruned from the store on every read.

    Scale path: replace this class with a Redis-backed implementation
    (HSET keyed by oref_city, EXPIREAT per entry) without touching callers.
    """

    def __init__(self, max_age_seconds: int = 3600) -> None:
        self._store: dict[str, Alert] = {}   # oref_city → latest Alert
        self._max_age = max_age_seconds

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    def upsert(self, alert: Alert) -> bool:
        """
        Insert or replace an alert.
        Alerts outside the retention window are rejected immediately so they
        are never stored or broadcast to connected clients.
        Replacement happens only when the incoming alert is **strictly newer**,
        so out-of-order or duplicate deliveries never overwrite fresher data.

        Returns True if the alert was new or updated, False otherwise.
        """
        cutoff = datetime.now(tz=timezone.utc).timestamp() - self._max_age
        if alert.timestamp.timestamp() < cutoff:
            return False  # too old — discard before storing or broadcasting

        existing = self._store.get(alert.oref_city)
        if existing is None or alert.timestamp > existing.timestamp:
            self._store[alert.oref_city] = alert
            return True
        return False

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def get_recent(self) -> list[Alert]:
        """
        Return all alerts whose timestamp falls within the retention window,
        sorted oldest → newest (ready to replay to a newly connected client).

        Side-effect: expired entries are pruned from the store on each call.
        """
        cutoff = datetime.now(tz=timezone.utc).timestamp() - self._max_age

        recent: dict[str, Alert] = {
            city: alert
            for city, alert in self._store.items()
            if alert.timestamp.timestamp() >= cutoff
        }

        # prune expired entries in-place
        self._store = recent

        return sorted(recent.values(), key=lambda a: a.timestamp)

    # ------------------------------------------------------------------ #
    # Diagnostics                                                          #
    # ------------------------------------------------------------------ #

    @property
    def size(self) -> int:
        """Current number of stored alerts (before pruning)."""
        return len(self._store)


# Module-level singleton — imported by poller and ws route
store = AlertStore()
