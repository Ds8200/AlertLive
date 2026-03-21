from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional

from pydantic import BaseModel, field_validator

ISRAEL_TZ = timezone(timedelta(hours=3))


class ThreatType(str, Enum):
    NEWS = "news"
    ROCKET = "rocket"
    ROCKETS = "rockets"
    MISSILES = "missiles"
    UPDATE = "update"
    INFILTRATION = "infiltration"
    HOSTILE_AIRCRAFT = "hostile_aircraft"
    EARTHQUAKE = "earthquake"
    TSUNAMI = "tsunami"
    HAZMAT = "hazmat"
    TERROR = "terror"


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARN = "warn"
    WARNING = "warning"
    INFO = "info"


class Alert(BaseModel):
    event_id: str
    alert_id: str
    type: str
    severity: str
    region_name: Optional[str] = "Unknown"
    oref_city: Optional[str] = "Unknown"
    lat: Optional[float] = None
    lng: Optional[float] = None
    timestamp: datetime
    oref_title: Optional[str] = None
    oref_desc: Optional[str] = None
    oref_category: Optional[int] = None
    region_id: Optional[str] = None
    created_at: Optional[datetime] = None

    @field_validator("lat", "lng", mode="before")
    @classmethod
    def parse_coord(cls, v):
        return float(v) if v is not None else None

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, datetime):
            return v
        return datetime.fromtimestamp(int(v) / 1000, tz=ISRAEL_TZ)

    @field_validator("created_at", mode="before")
    @classmethod
    def parse_created_at(cls, v):
        if v is None or isinstance(v, datetime):
            return v
        v_int = int(v)
        # If value looks like milliseconds (>= year 2000 in ms), divide by 1000
        if v_int > 1_000_000_000_000:
            return datetime.fromtimestamp(v_int / 1000, tz=ISRAEL_TZ)
        return datetime.fromtimestamp(v_int, tz=ISRAEL_TZ)

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "alert_id": self.alert_id,
            "type": self.type,
            "severity": self.severity,
            "region_name": self.region_name,
            "oref_city": self.oref_city,
            "lat": self.lat,
            "lng": self.lng,
            "timestamp": self.timestamp.isoformat(),
            "oref_title": self.oref_title,
            "oref_desc": self.oref_desc,
            "oref_category": self.oref_category,
            "region_id": self.region_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
