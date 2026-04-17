from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.backend.services.ingest_service import ingest_service
from app.backend.storage.sqlite_store import (
    fetch_recent_events,
    import_ndjson_events_if_sqlite_empty,
    initialize_sqlite_schema,
    resolve_sqlite_path,
    sqlite_status,
)


ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = ROOT / "app" / "frontend"
DATA_DIR = ROOT / "data"
EVENTS_FILE = DATA_DIR / "events" / "events.ndjson"
SNAPSHOT_DIR = DATA_DIR / "snapshots"
RECORDINGS_DIR = DATA_DIR / "recordings"
LOGS_DIR = DATA_DIR / "logs"
CONFIG_LOCAL = ROOT / "configs" / "app" / "settings.local.json"
CONFIG_EXAMPLE = ROOT / "configs" / "app" / "settings.example.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)

SQLITE_DB_PATH: Path | None = None

app = FastAPI(
    title="Local-First Doorbell Camera API",
    description="Backend control plane for live stream, events, recordings, and diagnostics.",
    version="0.2.0",
)

app.mount("/snapshots", StaticFiles(directory=str(SNAPSHOT_DIR)), name="snapshots")
app.mount("/recordings", StaticFiles(directory=str(RECORDINGS_DIR)), name="recordings")


def _load_config() -> dict[str, Any]:
    """Load app settings from local runtime config or safe example fallback."""
    candidate = CONFIG_LOCAL if CONFIG_LOCAL.exists() else CONFIG_EXAMPLE
    with candidate.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_events_from_ndjson(limit: int) -> list[dict[str, Any]]:
    """Read latest NDJSON events as legacy fallback."""
    if not EVENTS_FILE.exists():
        return []

    events: list[dict[str, Any]] = []
    with EVENTS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return list(reversed(events[-limit:]))


def _load_events(limit: int) -> list[dict[str, Any]]:
    """Load recent events primarily from SQLite, with NDJSON fallback."""
    if SQLITE_DB_PATH is not None and SQLITE_DB_PATH.exists():
        return fetch_recent_events(SQLITE_DB_PATH, limit=limit)
    return _load_events_from_ndjson(limit)


def _latest_snapshot_rel() -> str | None:
    """Return URL path to the most recently modified snapshot image."""
    if not SNAPSHOT_DIR.exists():
        return None
    candidates = sorted(
        [
            p
            for p in SNAPSHOT_DIR.iterdir()
            if p.suffix.lower() in {".jpg", ".jpeg", ".png"}
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    return f"/snapshots/{candidates[0].name}"


def _list_recordings(limit: int) -> list[dict[str, Any]]:
    """List recent recording files for playback tab."""
    limit = int(limit)
    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".m4v"}
    candidates = [
        p for p in RECORDINGS_DIR.glob("*") if p.is_file() and p.suffix.lower() in video_exts
    ]
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    items: list[dict[str, Any]] = []
    for p in candidates[:limit]:
        stat = p.stat()
        items.append(
            {
                "name": p.name,
                "size_bytes": stat.st_size,
                "modified_epoch": stat.st_mtime,
                "url": f"/recordings/{p.name}",
            }
        )
    return items


def _list_logs() -> list[str]:
    """List available runtime log files."""
    return sorted(
        [
            p.name
            for p in LOGS_DIR.glob("*")
            if p.is_file() and not p.name.startswith(".") and p.name.lower() != "backend.log"
        ]
    )


def _tail_file(path: Path, lines: int) -> str:
    """Return the trailing lines from a text log file."""
    dq: deque[str] = deque(maxlen=lines)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            dq.append(line.rstrip("\n"))
    return "\n".join(dq)


def _safe_camera_payload(camera_cfg: dict[str, Any]) -> dict[str, Any]:
    """Return camera configuration fields safe for UI display."""
    return {
        "name": camera_cfg.get("name", "front-door-camera"),
        "source_type": camera_cfg.get("source_type", "rtsp"),
        "stream_configured": bool(
            str(camera_cfg.get("stream_url", "")).strip()
            or str(camera_cfg.get("rtsp_url", "")).strip()
            or str(camera_cfg.get("source_type", "")).strip().lower() == "usb"
        ),
    }


@app.on_event("startup")
def startup_event() -> None:
    """Configure and start central ingest service and initialize SQLite foundation."""
    config = _load_config()
    ingest_service.configure_from_settings(config)
    ingest_service.start()

    global SQLITE_DB_PATH
    SQLITE_DB_PATH = resolve_sqlite_path(config, ROOT)
    initialize_sqlite_schema(SQLITE_DB_PATH)
    import_ndjson_events_if_sqlite_empty(SQLITE_DB_PATH, EVENTS_FILE)


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Stop ingest service and release stream resources."""
    ingest_service.stop()


@app.get("/")
def root() -> FileResponse:
    """Serve dashboard frontend."""
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/camera/live")
def camera_live() -> StreamingResponse:
    """Stream MJPEG from the central ingest service to dashboard clients."""
    return StreamingResponse(
        ingest_service.generate_mjpeg_stream(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/api/live-frame")
def live_frame(quality: int = Query(default=85, ge=40, le=95)) -> Response:
    """Return latest frame JPEG from ingest service for compatibility with older clients."""
    jpeg = ingest_service.get_latest_jpeg(quality=quality)
    if jpeg is None:
        raise HTTPException(status_code=503, detail="Camera frame is not available yet.")
    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"},
    )


@app.get("/api/health")
def health() -> JSONResponse:
    """Return overall backend health and ingest/storage diagnostics."""
    config = _load_config()
    ingest = ingest_service.status()

    storage_payload = {"db_exists": False, "event_records": 0}
    if SQLITE_DB_PATH is not None:
        storage_payload = sqlite_status(SQLITE_DB_PATH)

    return JSONResponse(
        {
            "status": "ok",
            "camera_name": config.get("camera", {}).get("name", "unknown"),
            "events_file_exists": EVENTS_FILE.exists(),
            "recording_count": len(_list_recordings(limit=5000)),
            "snapshot_count": len(
                [
                    p
                    for p in SNAPSHOT_DIR.glob("*")
                    if p.suffix.lower() in {".jpg", ".jpeg", ".png"}
                ]
            ),
            "ingest": ingest,
            "storage": storage_payload,
        }
    )


@app.get("/api/ingest/status")
def ingest_status() -> JSONResponse:
    """Expose ingest-specific runtime status for monitoring and troubleshooting."""
    return JSONResponse(ingest_service.status())


@app.get("/api/storage/status")
def storage_status() -> JSONResponse:
    """Expose minimal SQLite diagnostics for metadata foundation visibility."""
    if SQLITE_DB_PATH is None:
        return JSONResponse({"db_exists": False, "event_records": 0})
    return JSONResponse(sqlite_status(SQLITE_DB_PATH))


@app.get("/api/events")
def get_events(limit: int = Query(default=30, ge=1, le=200)) -> JSONResponse:
    """Return recent events from SQLite metadata storage."""
    return JSONResponse({"events": _load_events(limit=limit)})


@app.get("/api/latest-snapshot")
def latest_snapshot() -> JSONResponse:
    """Return URL path for latest stored snapshot image."""
    return JSONResponse({"url": _latest_snapshot_rel()})


@app.get("/api/config-safe")
def config_safe() -> JSONResponse:
    """Return non-sensitive runtime config for dashboard diagnostics panel."""
    config = _load_config()
    camera_cfg = config.get("camera", {})
    detection_cfg = config.get("detection", {})
    api_cfg = config.get("api", {})
    alerts_cfg = config.get("alerts", {})
    recording_cfg = config.get("recording", {})
    ingest_cfg = config.get("ingest", {})
    storage_cfg = config.get("storage", {})

    payload = {
        "camera": _safe_camera_payload(camera_cfg),
        "detection": {
            "confidence_threshold": detection_cfg.get("confidence_threshold"),
            "target_classes": detection_cfg.get("target_classes", []),
            "frame_skip": detection_cfg.get("frame_skip"),
        },
        "recording": {
            "segment_seconds": recording_cfg.get("segment_seconds"),
        },
        "ingest": {
            "capture_fps": ingest_cfg.get("capture_fps"),
            "output_fps": ingest_cfg.get("output_fps"),
            "reconnect_delay_seconds": ingest_cfg.get("reconnect_delay_seconds"),
        },
        "storage": {
            "sqlite_path_configured": bool(storage_cfg.get("sqlite_path")),
        },
        "alerts": {"enabled": alerts_cfg.get("enabled", False)},
        "api": {"host": api_cfg.get("host"), "port": api_cfg.get("port")},
    }
    return JSONResponse(payload)


@app.get("/api/recordings")
def recordings(limit: int = Query(default=50, ge=1, le=1000)) -> JSONResponse:
    """List recent recording files for dashboard playback."""
    return JSONResponse({"recordings": _list_recordings(limit=limit)})


@app.get("/api/logs")
def logs() -> JSONResponse:
    """List available backend log files."""
    return JSONResponse({"logs": _list_logs()})


@app.get("/api/logs/{log_name}")
def log_content(log_name: str, lines: int = Query(default=200, ge=1, le=5000)) -> JSONResponse:
    """Return tail content for a selected log file."""
    safe_name = Path(log_name).name
    file_path = LOGS_DIR / safe_name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Log file not found.")
    text = _tail_file(file_path, lines=lines)
    return JSONResponse({"name": safe_name, "lines": lines, "content": text})
