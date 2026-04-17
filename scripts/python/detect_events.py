from __future__ import annotations

"""Run object detection on camera frames and append event records locally."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import cv2
from ultralytics import YOLO

from send_alert import send_alert


ROOT = Path(__file__).resolve().parents[2]


def resolve_path(value: str) -> Path:
    """Resolve relative project paths against repository root."""
    p = Path(value)
    return p if p.is_absolute() else ROOT / p


def load_config(config_path: Path) -> dict[str, Any]:
    """Load JSON configuration from disk."""
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def append_event(events_file: Path, event: dict[str, Any]) -> None:
    """Append one NDJSON event line to the configured events file."""
    events_file.parent.mkdir(parents=True, exist_ok=True)
    with events_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def model_label(names: Any, class_id: int) -> str:
    """Map a model class id to a human-readable label."""
    if isinstance(names, dict):
        return str(names.get(class_id, class_id))
    if isinstance(names, list) and 0 <= class_id < len(names):
        return str(names[class_id])
    return str(class_id)


def main() -> int:
    """Run detection loop, save snapshots, and optionally send alerts."""
    parser = argparse.ArgumentParser(description="YOLO-based doorbell event detection.")
    parser.add_argument(
        "--config",
        default=str(ROOT / "configs" / "app" / "settings.local.json"),
        help="Path to settings JSON (defaults to settings.local.json).",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        config_path = ROOT / "configs" / "app" / "settings.example.json"
    config = load_config(config_path)

    camera_cfg = config["camera"]
    detect_cfg = config["detection"]
    alerts_cfg = config.get("alerts", {})

    stream_url = str(camera_cfg.get("stream_url", "")).strip()
    rtsp_url = str(camera_cfg.get("rtsp_url", "")).strip()
    input_url = stream_url or rtsp_url
    if not input_url:
        print("No camera stream configured. Set camera.stream_url or camera.rtsp_url.")
        return 1

    model_path = detect_cfg.get("model_path", "yolov8n.pt")
    confidence_threshold = float(detect_cfg.get("confidence_threshold", 0.45))
    frame_skip = int(detect_cfg.get("frame_skip", 5))
    target_classes = {str(name).lower() for name in detect_cfg.get("target_classes", ["person"])}
    cooldown_seconds = float(detect_cfg.get("cooldown_seconds", 8))
    events_file = resolve_path(detect_cfg.get("events_file", "data/events/events.ndjson"))
    snapshot_dir = resolve_path(detect_cfg.get("snapshot_dir", "data/snapshots"))
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    model = YOLO(model_path)

    print("Starting detection process")
    print(f"Camera stream: {input_url}")
    print(f"Model: {model_path}")
    print(f"Targets: {sorted(target_classes)}")
    print(f"Confidence threshold: {confidence_threshold}")

    cap = cv2.VideoCapture(input_url, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        print("Failed to open configured stream.")
        return 1

    frame_index = 0
    last_event_ts = 0.0

    try:
        while True:
            ok, frame = cap.read()
            if not ok:
                print("Frame read failed, retrying in 2 seconds...")
                time.sleep(2)
                cap.release()
                cap = cv2.VideoCapture(input_url, cv2.CAP_FFMPEG)
                continue

            frame_index += 1
            if frame_index % max(frame_skip, 1) != 0:
                continue

            results = model(frame, verbose=False)[0]
            now = time.time()
            if now - last_event_ts < cooldown_seconds:
                continue

            detected = None
            for box in results.boxes:
                class_id = int(box.cls[0].item())
                confidence = float(box.conf[0].item())
                label = model_label(model.names, class_id).lower()
                if confidence < confidence_threshold:
                    continue
                if label not in target_classes:
                    continue
                detected = {"class_id": class_id, "label": label, "confidence": round(confidence, 4)}
                break

            if not detected:
                continue

            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            snapshot_name = f"event_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
            snapshot_path = snapshot_dir / snapshot_name
            cv2.imwrite(str(snapshot_path), frame)

            event = {
                "ts_utc": ts_utc,
                "label": detected["label"],
                "confidence": detected["confidence"],
                "class_id": detected["class_id"],
                "snapshot": str(snapshot_path.relative_to(ROOT)).replace("\\", "/"),
            }
            append_event(events_file, event)
            sent = send_alert(event, alerts_cfg)
            print(f"Event detected: {event} alert_sent={sent}")
            last_event_ts = now
    except KeyboardInterrupt:
        print("Detection stopped by user.")
    finally:
        cap.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
