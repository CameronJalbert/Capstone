from __future__ import annotations

"""Run object detection on ingest-managed frames and append event records locally."""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import cv2
import numpy as np
from ultralytics import YOLO

from send_alert import send_alert


ROOT = Path(__file__).resolve().parents[2]
SOURCE_ERROR_LOG_INTERVAL_SECONDS = 3.0

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backend.storage.sqlite_store import (  # noqa: E402
    initialize_sqlite_schema,
    insert_event_record,
    resolve_sqlite_path,
)


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


def normalize_api_host(host: str) -> str:
    """Map wildcard API hosts to loopback values that local clients can reach."""
    host = host.strip()
    if host in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return host


def backend_frame_url(config: dict[str, Any], detect_cfg: dict[str, Any]) -> str:
    """Resolve backend frame endpoint URL from config or safe defaults."""
    configured = str(detect_cfg.get("backend_live_frame_url", "")).strip()
    if configured:
        return configured

    api_cfg = config.get("api", {})
    host = normalize_api_host(str(api_cfg.get("host", "127.0.0.1")))
    port = int(api_cfg.get("port", 8080))
    return f"http://{host}:{port}/api/live-frame?quality=85"


def open_camera_capture(input_url: str) -> cv2.VideoCapture | None:
    """Open camera stream with FFMPEG preference and backend fallback."""
    capture = cv2.VideoCapture(input_url, cv2.CAP_FFMPEG)
    if not capture.isOpened():
        capture.release()
        capture = cv2.VideoCapture(input_url)

    if not capture.isOpened():
        capture.release()
        return None

    capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    return capture


def read_backend_frame(frame_url: str, timeout_seconds: float) -> tuple[np.ndarray | None, str | None]:
    """Fetch and decode a single JPEG frame from backend live-frame API."""
    request = Request(
        frame_url,
        headers={
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "close",
        },
    )
    try:
        with urlopen(request, timeout=max(timeout_seconds, 0.1)) as response:
            payload = response.read()
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return None, str(exc)

    if not payload:
        return None, "empty frame payload"

    encoded = np.frombuffer(payload, dtype=np.uint8)
    frame = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
    if frame is None:
        return None, "failed to decode JPEG payload"

    return frame, None


def build_logger(log_file: Path, level_name: str) -> logging.Logger:
    """Create detector logger that writes to terminal and rotating log file."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logger = logging.getLogger("detector")
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(
        str(log_file),
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def normalize_odd_kernel(value: int) -> int:
    """Normalize blur kernel to a positive odd integer."""
    kernel = max(int(value), 1)
    if kernel % 2 == 0:
        kernel += 1
    return kernel


def preprocess_for_motion(frame: np.ndarray, resize_width: int, blur_kernel: int) -> np.ndarray:
    """Convert frame into compact grayscale reference used for cheap motion checks."""
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    if resize_width > 0 and gray.shape[1] != resize_width:
        scale = resize_width / float(gray.shape[1])
        target_height = max(1, int(gray.shape[0] * scale))
        gray = cv2.resize(gray, (resize_width, target_height), interpolation=cv2.INTER_AREA)
    if blur_kernel > 1:
        gray = cv2.GaussianBlur(gray, (blur_kernel, blur_kernel), 0)
    return gray


def changed_ratio(prev_gray: np.ndarray, cur_gray: np.ndarray, pixel_threshold: int) -> float:
    """Return percent of pixels that changed meaningfully between two frames."""
    diff = cv2.absdiff(prev_gray, cur_gray)
    _, mask = cv2.threshold(diff, int(pixel_threshold), 255, cv2.THRESH_BINARY)
    changed_pixels = cv2.countNonZero(mask)
    total_pixels = int(mask.shape[0] * mask.shape[1])
    if total_pixels <= 0:
        return 0.0
    return float(changed_pixels) / float(total_pixels)


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

    camera_cfg = config.get("camera", {})
    detect_cfg = config.get("detection", {})
    alerts_cfg = config.get("alerts", {})

    stream_url = str(camera_cfg.get("stream_url", "")).strip()
    rtsp_url = str(camera_cfg.get("rtsp_url", "")).strip()
    input_url = stream_url or rtsp_url

    frame_source = str(detect_cfg.get("frame_source", "backend_live_frame")).strip().lower()
    if frame_source not in {"backend_live_frame", "camera"}:
        print(
            f"Unknown detection.frame_source '{frame_source}'. "
            "Falling back to 'backend_live_frame'."
        )
        frame_source = "backend_live_frame"

    backend_url = backend_frame_url(config, detect_cfg)
    source_retry_delay_seconds = max(float(detect_cfg.get("source_retry_delay_seconds", 1.0)), 0.05)
    frame_fetch_timeout_seconds = max(float(detect_cfg.get("frame_fetch_timeout_seconds", 3.0)), 0.1)
    source_poll_fps = max(float(detect_cfg.get("source_poll_fps", 4.0)), 0.2)
    source_poll_interval_seconds = 1.0 / source_poll_fps

    if frame_source == "camera" and not input_url:
        print("No camera stream configured. Set camera.stream_url or camera.rtsp_url.")
        return 1

    model_path = detect_cfg.get("model_path", "yolov8n.pt")
    confidence_threshold = float(detect_cfg.get("confidence_threshold", 0.45))
    frame_skip = max(int(detect_cfg.get("frame_skip", 5)), 1)
    target_classes = {str(name).lower() for name in detect_cfg.get("target_classes", ["person"])}
    cooldown_seconds = max(float(detect_cfg.get("cooldown_seconds", 8)), 0.0)
    inference_max_fps = max(float(detect_cfg.get("inference_max_fps", 2.0)), 0.2)
    inference_min_interval_seconds = 1.0 / inference_max_fps
    motion_gate_enabled = bool(detect_cfg.get("motion_gate_enabled", True))
    motion_min_changed_ratio = max(float(detect_cfg.get("motion_min_changed_ratio", 0.01)), 0.0)
    motion_pixel_threshold = max(1, min(int(detect_cfg.get("motion_pixel_threshold", 22)), 255))
    motion_resize_width = max(int(detect_cfg.get("motion_resize_width", 320)), 0)
    motion_blur_kernel = normalize_odd_kernel(int(detect_cfg.get("motion_blur_kernel", 5)))
    motion_force_inference_interval_seconds = max(
        float(detect_cfg.get("motion_force_inference_interval_seconds", 12.0)),
        0.0,
    )
    status_log_interval_seconds = max(float(detect_cfg.get("status_log_interval_seconds", 20.0)), 2.0)
    write_legacy_ndjson = bool(detect_cfg.get("write_legacy_ndjson", False))
    events_file = resolve_path(detect_cfg.get("events_file", "data/events/events.ndjson"))
    snapshot_dir = resolve_path(detect_cfg.get("snapshot_dir", "data/snapshots"))
    log_file = resolve_path(detect_cfg.get("log_file", "data/logs/detector.log"))
    log_level = str(detect_cfg.get("log_level", "INFO")).strip().upper()
    sqlite_db_path = resolve_sqlite_path(config, ROOT)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    logger = build_logger(log_file, log_level)
    initialize_sqlite_schema(sqlite_db_path)
    model = YOLO(model_path)

    logger.info("Starting detection process")
    logger.info("Frame source mode: %s", frame_source)
    if frame_source == "backend_live_frame":
        logger.info("Backend frame endpoint: %s", backend_url)
    else:
        logger.info("Camera stream: %s", input_url)
    logger.info("Model: %s", model_path)
    logger.info("Targets: %s", sorted(target_classes))
    logger.info("Confidence threshold: %.2f", confidence_threshold)
    logger.info("SQLite events DB: %s", sqlite_db_path)
    logger.info(
        "Runtime tuning: source_poll_fps=%.2f inference_max_fps=%.2f frame_skip=%d motion_gate=%s force_interval=%.1fs",
        source_poll_fps,
        inference_max_fps,
        frame_skip,
        motion_gate_enabled,
        motion_force_inference_interval_seconds,
    )
    if write_legacy_ndjson:
        logger.info("Legacy NDJSON write is enabled: %s", events_file)

    cap: cv2.VideoCapture | None = None
    if frame_source == "camera":
        cap = open_camera_capture(input_url)
        if cap is None:
            logger.error("Failed to open configured camera stream.")
            return 1

    frame_index = 0
    last_event_ts = 0.0
    last_source_error_log_ts = 0.0
    last_poll_ts = 0.0
    last_inference_ts = 0.0
    last_status_log_ts = 0.0
    prev_motion_gray: np.ndarray | None = None
    frames_fetched = 0
    inferences_ran = 0
    motion_passes = 0
    forced_motion_inferences = 0
    events_detected = 0

    try:
        while True:
            now = time.time()
            if last_poll_ts > 0:
                wait_seconds = source_poll_interval_seconds - (now - last_poll_ts)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            last_poll_ts = time.time()

            frame: np.ndarray | None = None
            source_error: str | None = None

            if frame_source == "backend_live_frame":
                frame, source_error = read_backend_frame(backend_url, frame_fetch_timeout_seconds)
            else:
                if cap is None:
                    source_error = "camera capture not initialized"
                else:
                    ok, frame = cap.read()
                    if not ok or frame is None:
                        source_error = "camera read failed"
                        cap.release()
                        cap = open_camera_capture(input_url)
                        if cap is None:
                            source_error = "camera reconnect failed"

            if frame is None:
                now = time.time()
                if now - last_source_error_log_ts >= SOURCE_ERROR_LOG_INTERVAL_SECONDS:
                    source_name = "backend live-frame API" if frame_source == "backend_live_frame" else "camera stream"
                    logger.warning("Frame source unavailable (%s): %s. Retrying...", source_name, source_error)
                    last_source_error_log_ts = now
                time.sleep(source_retry_delay_seconds)
                continue

            frames_fetched += 1
            now = time.time()
            if now - last_status_log_ts >= status_log_interval_seconds:
                logger.info(
                    "Detector status fetched=%d inferences=%d motion_passes=%d forced_inferences=%d events=%d",
                    frames_fetched,
                    inferences_ran,
                    motion_passes,
                    forced_motion_inferences,
                    events_detected,
                )
                last_status_log_ts = now

            frame_index += 1
            if frame_index % frame_skip != 0:
                continue

            if now - last_event_ts < cooldown_seconds:
                continue
            if now - last_inference_ts < inference_min_interval_seconds:
                continue

            motion_ratio_value = 1.0
            forced_by_motion_interval = False
            if motion_gate_enabled:
                cur_motion_gray = preprocess_for_motion(frame, motion_resize_width, motion_blur_kernel)
                if prev_motion_gray is None:
                    prev_motion_gray = cur_motion_gray
                    continue

                motion_ratio_value = changed_ratio(prev_motion_gray, cur_motion_gray, motion_pixel_threshold)
                prev_motion_gray = cur_motion_gray
                if motion_ratio_value < motion_min_changed_ratio:
                    if motion_force_inference_interval_seconds <= 0.0:
                        continue
                    if now - last_inference_ts < motion_force_inference_interval_seconds:
                        continue
                    forced_by_motion_interval = True
                else:
                    motion_passes += 1

            last_inference_ts = now
            results = model(frame, verbose=False)[0]
            inferences_ran += 1
            if forced_by_motion_interval:
                forced_motion_inferences += 1

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
            wrote_snapshot = cv2.imwrite(str(snapshot_path), frame)
            if not wrote_snapshot:
                logger.warning("Snapshot write failed for %s", snapshot_path)

            event = {
                "ts_utc": ts_utc,
                "label": detected["label"],
                "confidence": detected["confidence"],
                "class_id": detected["class_id"],
                "snapshot": str(snapshot_path.relative_to(ROOT)).replace("\\", "/"),
            }
            sqlite_event_id: int | None = None
            write_target = "sqlite"
            try:
                sqlite_event_id = insert_event_record(sqlite_db_path, event, event_type="detection")
            except Exception as exc:
                logger.exception("SQLite event insert failed; writing fallback NDJSON event. error=%s", exc)
                append_event(events_file, event)
                write_target = "ndjson_fallback"

            if write_legacy_ndjson:
                append_event(events_file, event)
                if write_target == "sqlite":
                    write_target = "sqlite+ndjson"

            sent = send_alert(event, alerts_cfg)
            events_detected += 1
            logger.info(
                "Event detected id=%s label=%s confidence=%.4f snapshot=%s alert_sent=%s motion_ratio=%.4f sink=%s",
                sqlite_event_id,
                detected["label"],
                detected["confidence"],
                event["snapshot"],
                sent,
                motion_ratio_value,
                write_target,
            )
            last_event_ts = now
    except KeyboardInterrupt:
        logger.info("Detection stopped by user.")
    finally:
        if cap is not None:
            cap.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
