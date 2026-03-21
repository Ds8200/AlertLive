import json

from app.models.alert import Alert


def process_alerts(raw_alerts: list[dict]) -> list[Alert]:
    results = []
    for raw in raw_alerts:
        payload = raw.get("payload_json", {})
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}

        data = {
            "event_id": payload.get("event_id"),
            "alert_id": raw.get("id"),
            "type": payload.get("threat_type"),
            "severity": raw.get("severity"),
            "region_name": raw.get("region_name") or payload.get("oref_city"),
            "oref_city": payload.get("oref_city"),
            "lat": payload.get("lat"),
            "lng": payload.get("lng"),
            "timestamp": raw.get("timestamp"),
            "oref_title": payload.get("oref_title"),
            "oref_desc": payload.get("oref_desc"),
            "oref_category": payload.get("oref_category"),
            "region_id": raw.get("region_id"),
            "created_at": raw.get("created_at"),
        }

        try:
            results.append(Alert.model_validate(data))
        except Exception as e:
            print(f"[processor] Invalid alert skipped: {e}")

    return results
