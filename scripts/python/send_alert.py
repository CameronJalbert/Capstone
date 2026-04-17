from __future__ import annotations

"""Send optional webhook alerts for detected doorbell events."""

import json
from typing import Any

import requests


def send_alert(event: dict[str, Any], alerts_cfg: dict[str, Any]) -> bool:
    """Post one event payload to the configured webhook when alerts are enabled."""
    if not alerts_cfg.get("enabled", False):
        return False

    webhook_url = alerts_cfg.get("webhook_url", "").strip()
    if not webhook_url:
        print("Alerting enabled but webhook URL is empty; skipping alert send.")
        return False

    timeout_seconds = int(alerts_cfg.get("timeout_seconds", 6))
    text = (
        f"Doorbell event: {event.get('label')} "
        f"(conf {event.get('confidence')}) at {event.get('ts_utc')}"
    )

    payload = {
        "text": text,
        "event": event,
    }

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        print(f"Alert send failed: {exc}")
        return False
