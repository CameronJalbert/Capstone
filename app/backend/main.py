from __future__ import annotations

import json
import logging
import mimetypes
import os
import signal
import sqlite3
import socket
import smtplib
import subprocess
import threading
import time
import ctypes
import shutil
from collections import deque
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

import numpy as np

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.backend.services.ingest_service import ingest_service
from app.backend.storage.sqlite_store import (
    CURRENT_SCHEMA_VERSION,
    attach_event_media_paths,
    background_job_stats,
    backfill_event_policy_fields,
    claim_next_background_job,
    complete_background_job,
    enqueue_background_job,
    fail_or_retry_background_job,
    fetch_camera_mode_state,
    fetch_recent_events,
    fetch_background_jobs,
    fetch_event_by_id,
    import_ndjson_events_if_sqlite_empty,
    initialize_sqlite_schema,
    media_integrity_report,
    requeue_stale_running_jobs,
    repair_media_integrity,
    retention_summary,
    resolve_sqlite_path,
    run_retention_cull,
    save_camera_mode_state,
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
PLACEHOLDER_MARKERS = (
    "YOUR_",
    "PUT_",
    "_HERE",
    "CHANGE_ME",
    "REPLACE_ME",
    "EXAMPLE",
)

DATA_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)

SQLITE_DB_PATH: Path | None = None
JOB_RUNNER: "BackgroundJobRunner" | None = None
PROFILE_AUTOMATION_RUNNER: "CameraProfileAutomationRunner" | None = None
CONFIG_WRITE_LOCK = threading.Lock()
SERVER_CONTROL_STATE_LOCK = threading.Lock()
SERVER_CONTROL_SIGNAL_LOCK = threading.Lock()
LOGGER = logging.getLogger("capstone.backend")
BACKEND_START_MONOTONIC = time.monotonic()

CAMERA_MODE_BASE = "base"
CAMERA_MODE_MANUAL_SAVED = "manual_saved"
CAMERA_MODE_MANUAL_TEMPORARY = "manual_temporary"
CAMERA_MODE_ADAPTIVE = "adaptive_capture"
CAMERA_MODE_AUTO_PROFILE = "auto_day_night_profile"
CAMERA_MODES = {
    CAMERA_MODE_BASE,
    CAMERA_MODE_MANUAL_SAVED,
    CAMERA_MODE_MANUAL_TEMPORARY,
    CAMERA_MODE_ADAPTIVE,
    CAMERA_MODE_AUTO_PROFILE,
}
CAMERA_SYNC_IN_SYNC = "in_sync"
CAMERA_SYNC_DRIFT = "drift_detected"
CAMERA_SYNC_REAPPLYING = "reapplying"
CAMERA_SYNC_UNAVAILABLE = "camera_unavailable"

SERVER_ACTION_SHUTDOWN = "shutdown"
SERVER_ACTION_REBOOT = "reboot"
SERVER_CONTROL_DEFAULT_DELAY_SECONDS = 5
SERVER_CONTROL_MAX_DELAY_SECONDS = 120
SERVER_CONTROL_SIGNAL_HANDLERS: dict[int, Any] = {}
SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED = False
SERVER_CONTROL_STATE: dict[str, Any] = {
    "pending": False,
    "action": "",
    "reason": "",
    "trigger": "",
    "delay_seconds": 0,
    "scheduled_at_utc": "",
    "execute_at_utc": "",
    "remaining_seconds": 0.0,
    "restart_spawned": False,
}

app = FastAPI(
    title="Local-First Doorbell Camera API",
    description="Backend control plane for live stream, events, recordings, and diagnostics.",
    version="0.2.0",
)

app.mount("/snapshots", StaticFiles(directory=str(SNAPSHOT_DIR)), name="snapshots")
app.mount("/recordings", StaticFiles(directory=str(RECORDINGS_DIR)), name="recordings")


def _load_config() -> dict[str, Any]:
    """Load app settings from local runtime config and enforce sensitive-field safety checks."""
    if not CONFIG_LOCAL.exists():
        raise RuntimeError(
            "Missing runtime config: configs/app/settings.local.json. "
            "Copy configs/app/settings.example.json to settings.local.json and set real local values."
        )
    candidate = CONFIG_LOCAL
    with candidate.open("r", encoding="utf-8") as f:
        config = json.load(f)
    _validate_sensitive_runtime_config(config)
    return config


def _save_config(config: dict[str, Any]) -> None:
    """Persist runtime config atomically after applying validation checks."""
    _validate_sensitive_runtime_config(config)
    temp_path = CONFIG_LOCAL.with_name(f"{CONFIG_LOCAL.name}.tmp")
    with CONFIG_WRITE_LOCK:
        try:
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_LOCAL)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass


def _server_control_state_snapshot() -> dict[str, Any]:
    """Return a copy of planned server-control state with fresh remaining-seconds value."""
    with SERVER_CONTROL_STATE_LOCK:
        snapshot = dict(SERVER_CONTROL_STATE)
    execute_at = _parse_utc(snapshot.get("execute_at_utc"))
    if bool(snapshot.get("pending", False)) and execute_at is not None:
        remaining = max(0.0, (execute_at - datetime.now(timezone.utc)).total_seconds())
    else:
        remaining = 0.0
    snapshot["remaining_seconds"] = round(float(remaining), 3)
    return snapshot


def _clear_server_control_state() -> None:
    """Reset server-control state when no planned action is active."""
    with SERVER_CONTROL_STATE_LOCK:
        SERVER_CONTROL_STATE.update(
            {
                "pending": False,
                "action": "",
                "reason": "",
                "trigger": "",
                "delay_seconds": 0,
                "scheduled_at_utc": "",
                "execute_at_utc": "",
                "remaining_seconds": 0.0,
                "restart_spawned": False,
            }
        )


def _spawn_backend_restart_process(*, delay_seconds: int) -> bool:
    """Spawn detached restart helper so backend can come back after a planned reboot."""
    if os.name != "nt":
        return False
    sleep_seconds = max(1, int(delay_seconds))
    root_escaped = str(ROOT).replace("'", "''")
    command = (
        f"Start-Sleep -Seconds {sleep_seconds}; "
        f"Set-Location '{root_escaped}'; "
        ".\\scripts\\windows\\start_backend.ps1"
    )
    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", command],
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except OSError:
        return False


def _restore_server_control_signal_handlers() -> None:
    """Restore original process signal handlers after interception phase."""
    global SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED
    with SERVER_CONTROL_SIGNAL_LOCK:
        for signum, handler in list(SERVER_CONTROL_SIGNAL_HANDLERS.items()):
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError, RuntimeError):
                pass
        SERVER_CONTROL_SIGNAL_HANDLERS.clear()
        SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED = False


def _terminate_process_via_signal(signum: int) -> None:
    """Terminate current process by raising the provided signal."""
    try:
        os.kill(os.getpid(), signum)
    except OSError:
        os._exit(0)


def _launch_planned_server_action_worker(
    *,
    action: str,
    execute_at_utc: str,
    relay_signal: int | None = None,
) -> None:
    """Launch background worker that executes shutdown/reboot after countdown."""

    def _worker() -> None:
        execute_at = _parse_utc(execute_at_utc)
        if execute_at is not None:
            remaining = max(0.0, (execute_at - datetime.now(timezone.utc)).total_seconds())
            if remaining > 0:
                time.sleep(remaining)

        if action == SERVER_ACTION_REBOOT:
            restart_spawned = _spawn_backend_restart_process(delay_seconds=2)
            with SERVER_CONTROL_STATE_LOCK:
                SERVER_CONTROL_STATE["restart_spawned"] = bool(restart_spawned)

        if relay_signal is not None:
            _restore_server_control_signal_handlers()
            _terminate_process_via_signal(relay_signal)
            return

        _terminate_process_via_signal(signal.SIGTERM)

    thread = threading.Thread(
        target=_worker,
        daemon=True,
        name=f"server-action-{action}",
    )
    thread.start()


def _schedule_server_action(
    *,
    action: str,
    delay_seconds: int,
    reason: str,
    trigger: str,
    relay_signal: int | None = None,
) -> dict[str, Any]:
    """Schedule shutdown/reboot action and expose countdown metadata for UI."""
    selected = str(action or "").strip().lower()
    if selected not in {SERVER_ACTION_SHUTDOWN, SERVER_ACTION_REBOOT}:
        raise ValueError(f"Unsupported server action '{selected}'.")
    clamped_delay = max(1, min(int(delay_seconds), SERVER_CONTROL_MAX_DELAY_SECONDS))

    already_pending = False
    with SERVER_CONTROL_STATE_LOCK:
        if bool(SERVER_CONTROL_STATE.get("pending", False)):
            already_pending = True
            execute_at_utc = ""
        else:
            scheduled_at = datetime.now(timezone.utc)
            execute_at = scheduled_at + timedelta(seconds=clamped_delay)
            SERVER_CONTROL_STATE.update(
                {
                    "pending": True,
                    "action": selected,
                    "reason": str(reason or "").strip(),
                    "trigger": str(trigger or "").strip(),
                    "delay_seconds": clamped_delay,
                    "scheduled_at_utc": scheduled_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "execute_at_utc": execute_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "remaining_seconds": float(clamped_delay),
                    "restart_spawned": False,
                }
            )
            execute_at_utc = str(SERVER_CONTROL_STATE["execute_at_utc"])

    if already_pending:
        return _server_control_state_snapshot()

    _launch_planned_server_action_worker(
        action=selected,
        execute_at_utc=execute_at_utc,
        relay_signal=relay_signal,
    )
    return _server_control_state_snapshot()


def _server_control_signal_handler(signum: int, _frame: Any) -> None:
    """Intercept first shutdown signal to provide a short UI-visible countdown."""
    snapshot = _server_control_state_snapshot()
    if bool(snapshot.get("pending", False)):
        _restore_server_control_signal_handlers()
        _terminate_process_via_signal(signum)
        return
    _schedule_server_action(
        action=SERVER_ACTION_SHUTDOWN,
        delay_seconds=SERVER_CONTROL_DEFAULT_DELAY_SECONDS,
        reason=f"signal_{int(signum)}",
        trigger="terminal_ctrl_c",
        relay_signal=int(signum),
    )


def _install_server_control_signal_handlers() -> None:
    """Install signal hooks so Ctrl-C schedules an observable countdown before exit."""
    global SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED
    with SERVER_CONTROL_SIGNAL_LOCK:
        if SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED:
            return
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                SERVER_CONTROL_SIGNAL_HANDLERS[signum] = signal.getsignal(signum)
                signal.signal(signum, _server_control_signal_handler)
            except (ValueError, OSError, RuntimeError):
                continue
        SERVER_CONTROL_SIGNAL_HOOKS_INSTALLED = bool(SERVER_CONTROL_SIGNAL_HANDLERS)


def _is_placeholder_text(value: Any) -> bool:
    """Return true when value appears to be a template placeholder and not a real runtime secret."""
    text = str(value or "").strip()
    if not text:
        return False
    upper = text.upper()
    return any(marker in upper for marker in PLACEHOLDER_MARKERS)


def _is_missing_or_placeholder(value: Any) -> bool:
    """Return true when sensitive config value is empty or still set to template placeholder text."""
    text = str(value or "").strip()
    if not text:
        return True
    return _is_placeholder_text(text)


def _is_strong_secret(value: Any, *, min_length: int = 24) -> bool:
    """Return true when secret text appears non-placeholder and long enough for runtime use."""
    text = str(value or "").strip()
    if len(text) < min_length:
        return False
    return not _is_placeholder_text(text)


def _validate_sensitive_runtime_config(config: dict[str, Any]) -> None:
    """Validate security-relevant config when corresponding features are enabled."""
    errors: list[str] = []
    alerts_cfg = config.get("alerts", {})
    auth_cfg = config.get("auth", {})
    camera_control_cfg = config.get("camera_control", {})
    smtp_cfg = alerts_cfg.get("smtp", {})

    alerts_enabled = bool(alerts_cfg.get("enabled", False))
    webhook_enabled = bool(alerts_cfg.get("webhook_enabled", True))
    smtp_enabled = bool(smtp_cfg.get("enabled", False))
    if alerts_enabled and not webhook_enabled and not smtp_enabled:
        errors.append("alerts.enabled=true requires at least one dispatch channel (webhook and/or smtp).")

    if alerts_enabled and webhook_enabled and _is_missing_or_placeholder(alerts_cfg.get("webhook_url")):
        errors.append(
            "alerts.enabled=true with webhook_enabled=true requires alerts.webhook_url with a real non-placeholder value."
        )

    if alerts_enabled and smtp_enabled:
        smtp_required_fields = (
            "host",
            "username",
            "password",
            "from_email",
        )
        for field_name in smtp_required_fields:
            if _is_missing_or_placeholder(smtp_cfg.get(field_name)):
                errors.append(
                    f"alerts.smtp.enabled=true requires alerts.smtp.{field_name} with a real non-placeholder value."
                )
        to_emails = smtp_cfg.get("to_emails", [])
        if not isinstance(to_emails, list) or not any(str(x).strip() for x in to_emails):
            errors.append("alerts.smtp.enabled=true requires alerts.smtp.to_emails with at least one address.")

    auth_enabled = bool(auth_cfg.get("enabled", False))
    if auth_enabled and not _is_strong_secret(auth_cfg.get("session_secret"), min_length=24):
        errors.append("auth.enabled=true requires auth.session_secret (non-placeholder, at least 24 characters).")

    camera_control_enabled = bool(camera_control_cfg.get("enabled", False))
    if camera_control_enabled:
        if _is_missing_or_placeholder(camera_control_cfg.get("token")):
            errors.append(
                "camera_control.enabled=true requires camera_control.token with a real non-placeholder value."
            )
        if not _normalize_camera_origin(camera_control_cfg.get("base_url")):
            errors.append(
                "camera_control.enabled=true requires camera_control.base_url (camera main server host, default port 80)."
            )
    automation_cfg = dict(camera_control_cfg.get("profile_automation") or {})
    if "poll_interval_seconds" in automation_cfg and int(automation_cfg.get("poll_interval_seconds", 0)) < 5:
        errors.append("camera_control.profile_automation.poll_interval_seconds must be >= 5.")
    if "min_switch_interval_seconds" in automation_cfg and int(
        automation_cfg.get("min_switch_interval_seconds", 0)
    ) < 0:
        errors.append("camera_control.profile_automation.min_switch_interval_seconds must be >= 0.")

    if errors:
        formatted = "\n- ".join(errors)
        raise RuntimeError(f"Sensitive runtime config validation failed:\n- {formatted}")


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


def _normalize_range_key(value: Any) -> str:
    key = str(value or "all").strip().lower()
    return key if key in {"all", "24h", "7d", "30d"} else "all"


def _range_seconds_from_key(range_key: str) -> int | None:
    mapping = {"all": None, "24h": 24 * 3600, "7d": 7 * 24 * 3600, "30d": 30 * 24 * 3600}
    return mapping.get(_normalize_range_key(range_key))


def _normalize_event_severity_filter(value: Any) -> str:
    key = str(value or "all").strip().lower()
    return key if key in {"all", "unknown", "low", "medium", "high", "critical"} else "all"


def _normalize_event_media_filter(value: Any) -> str:
    key = str(value or "all").strip().lower()
    return key if key in {"all", "snapshot", "clip", "both", "none"} else "all"


def _normalize_recording_sort(value: Any) -> str:
    key = str(value or "newest").strip().lower()
    return key if key in {"newest", "oldest", "largest", "smallest", "name"} else "newest"


def _load_all_events_from_sqlite(db_path: Path) -> list[dict[str, Any]]:
    """Load all events from SQLite for API-side filtering and pagination."""
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, event_uid, ts_utc, event_type, label, confidence, class_id, snapshot_path, clip_path,
                   review_state, share_state, deletion_state, lifecycle_state, lifecycle_updated_ts_utc,
                   signal_proximity_score, signal_facing_score,
                   severity_score, severity_level, severity_policy_version, severity_updated_ts_utc,
                   retention_days, delete_after_ts_utc, retention_basis, retention_updated_ts_utc, notes
            FROM event_records
            ORDER BY id DESC;
            """
        )
        rows = cursor.fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            snapshot = row["snapshot_path"]
            clip = row["clip_path"]
            events.append(
                {
                    "id": int(row["id"]),
                    "event_uid": row["event_uid"],
                    "ts_utc": row["ts_utc"],
                    "event_type": row["event_type"],
                    "label": row["label"],
                    "confidence": float(row["confidence"]) if row["confidence"] is not None else 0.0,
                    "class_id": row["class_id"],
                    "snapshot": snapshot,
                    "clip": clip,
                    "has_snapshot": bool(str(snapshot or "").strip()),
                    "has_clip": bool(str(clip or "").strip()),
                    "review_state": row["review_state"],
                    "share_state": row["share_state"],
                    "deletion_state": row["deletion_state"],
                    "lifecycle_state": row["lifecycle_state"],
                    "lifecycle_updated_ts_utc": row["lifecycle_updated_ts_utc"],
                    "signal_proximity_score": row["signal_proximity_score"],
                    "signal_facing_score": row["signal_facing_score"],
                    "severity_score": row["severity_score"],
                    "severity_level": row["severity_level"],
                    "severity_policy_version": row["severity_policy_version"],
                    "severity_updated_ts_utc": row["severity_updated_ts_utc"],
                    "retention_days": row["retention_days"],
                    "delete_after_ts_utc": row["delete_after_ts_utc"],
                    "retention_basis": row["retention_basis"],
                    "retention_updated_ts_utc": row["retention_updated_ts_utc"],
                    "notes": row["notes"],
                }
            )
        return events
    finally:
        connection.close()


def _event_matches_filters(
    event: dict[str, Any],
    *,
    query_text: str,
    severity_filter: str,
    media_filter: str,
    range_seconds: int | None,
    now_utc: datetime,
) -> bool:
    if query_text:
        haystack = " ".join(
            [
                str(event.get("id", "")),
                str(event.get("event_uid", "")),
                str(event.get("label", "")),
                str(event.get("class_id", "")),
                str(event.get("severity_level", "")),
                str(event.get("lifecycle_state", "")),
                str(event.get("notes", "")),
            ]
        ).lower()
        if query_text not in haystack:
            return False

    severity_value = str(event.get("severity_level", "unknown") or "unknown").strip().lower()
    if severity_filter != "all" and severity_value != severity_filter:
        return False

    has_snapshot = bool(str(event.get("snapshot", "")).strip())
    has_clip = bool(str(event.get("clip", "")).strip())
    if media_filter == "snapshot" and not has_snapshot:
        return False
    if media_filter == "clip" and not has_clip:
        return False
    if media_filter == "both" and not (has_snapshot and has_clip):
        return False
    if media_filter == "none" and (has_snapshot or has_clip):
        return False

    if range_seconds is not None:
        ts = _parse_utc(event.get("ts_utc"))
        if ts is None:
            return False
        age_seconds = (now_utc - ts).total_seconds()
        if age_seconds < 0 or age_seconds > float(range_seconds):
            return False

    return True


def _events_query_payload(
    *,
    limit: int,
    offset: int,
    q: str,
    severity: str,
    media: str,
    range_key: str,
) -> dict[str, Any]:
    """Return filtered/paginated events payload with stable metadata contract."""
    limit = max(1, min(200, int(limit)))
    offset = max(0, int(offset))
    query_text = str(q or "").strip().lower()
    severity_filter = _normalize_event_severity_filter(severity)
    media_filter = _normalize_event_media_filter(media)
    normalized_range = _normalize_range_key(range_key)
    range_seconds = _range_seconds_from_key(normalized_range)

    if SQLITE_DB_PATH is not None and SQLITE_DB_PATH.exists():
        all_events = _load_all_events_from_sqlite(SQLITE_DB_PATH)
    else:
        all_events = _load_events_from_ndjson(limit=1_000_000)

    now_utc = datetime.now(timezone.utc)
    filtered = [
        event
        for event in all_events
        if _event_matches_filters(
            event,
            query_text=query_text,
            severity_filter=severity_filter,
            media_filter=media_filter,
            range_seconds=range_seconds,
            now_utc=now_utc,
        )
    ]

    total = len(filtered)
    page_items = filtered[offset : offset + limit]
    pages = max(1, (total + limit - 1) // limit)
    page = min(pages, max(1, (offset // limit) + 1))
    return {
        "events": page_items,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": total,
            "page": page,
            "pages": pages,
            "has_prev": offset > 0,
            "has_next": (offset + limit) < total,
        },
        "filters": {
            "q": query_text,
            "severity": severity_filter,
            "media": media_filter,
            "range": normalized_range,
        },
    }


def _recording_item_from_path(path: Path, *, stat: os.stat_result | None = None) -> dict[str, Any]:
    current_stat = stat if stat is not None else path.stat()
    return {
        "name": path.name,
        "size_bytes": current_stat.st_size,
        "modified_epoch": current_stat.st_mtime,
        "url": f"/recordings/{path.name}",
    }


def _all_recording_paths() -> list[Path]:
    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".m4v"}
    return [p for p in RECORDINGS_DIR.glob("*") if p.is_file() and p.suffix.lower() in video_exts]


def _recordings_query_payload(
    *,
    limit: int,
    offset: int,
    q: str,
    range_key: str,
    sort_key: str,
) -> dict[str, Any]:
    """Return filtered/paginated recordings payload with stable metadata contract."""
    limit = max(1, min(200, int(limit)))
    offset = max(0, int(offset))
    query_text = str(q or "").strip().lower()
    normalized_range = _normalize_range_key(range_key)
    range_seconds = _range_seconds_from_key(normalized_range)
    normalized_sort = _normalize_recording_sort(sort_key)

    candidates = _all_recording_paths()
    now_epoch = time.time()

    filtered: list[tuple[Path, os.stat_result]] = []
    for path in candidates:
        try:
            stat = path.stat()
        except OSError:
            continue
        if query_text and query_text not in path.name.lower():
            continue
        if range_seconds is not None:
            age_seconds = now_epoch - float(stat.st_mtime)
            if age_seconds < 0 or age_seconds > float(range_seconds):
                continue
        filtered.append((path, stat))

    if normalized_sort == "oldest":
        filtered.sort(key=lambda item: item[1].st_mtime)
    elif normalized_sort == "largest":
        filtered.sort(key=lambda item: item[1].st_size, reverse=True)
    elif normalized_sort == "smallest":
        filtered.sort(key=lambda item: item[1].st_size)
    elif normalized_sort == "name":
        filtered.sort(key=lambda item: item[0].name.lower())
    else:
        filtered.sort(key=lambda item: item[1].st_mtime, reverse=True)

    total = len(filtered)
    page_entries = filtered[offset : offset + limit]
    items = [_recording_item_from_path(path, stat=stat) for path, stat in page_entries]
    pages = max(1, (total + limit - 1) // limit)
    page = min(pages, max(1, (offset // limit) + 1))
    return {
        "recordings": items,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": total,
            "page": page,
            "pages": pages,
            "has_prev": offset > 0,
            "has_next": (offset + limit) < total,
        },
        "filters": {
            "q": query_text,
            "range": normalized_range,
            "sort": normalized_sort,
        },
    }


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
    payload = _recordings_query_payload(
        limit=max(1, int(limit)),
        offset=0,
        q="",
        range_key="all",
        sort_key="newest",
    )
    return list(payload["recordings"])


def _list_logs() -> list[str]:
    """List available runtime log files."""
    return sorted(
        [
            p.name
            for p in LOGS_DIR.glob("*")
            if p.is_file() and not p.name.startswith(".")
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


def _normalize_camera_origin(value: Any) -> str:
    """Normalize camera host/base URL to origin-only form without path/query."""
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith("http://") and not text.startswith("https://"):
        text = f"http://{text}"
    parsed = urlparse(text)
    scheme = parsed.scheme or "http"
    host = (parsed.hostname or "").strip()
    if not host:
        return ""
    default_port = 443 if scheme == "https" else 80
    port = parsed.port
    if port == 81:
        # ESP32 control endpoints live on main server port 80, not stream port 81.
        port = 80
    if port is None or port == default_port:
        netloc = host
    else:
        netloc = f"{host}:{port}"
    return urlunparse((scheme, netloc, "", "", "", "")).rstrip("/")


def _infer_camera_control_origin_from_stream(camera_cfg: dict[str, Any]) -> str:
    """Infer camera control origin from stream URL while forcing main server port."""
    stream_url = str(camera_cfg.get("stream_url", "") or camera_cfg.get("rtsp_url", "")).strip()
    if not stream_url:
        return ""
    parsed = urlparse(stream_url)
    host = (parsed.hostname or "").strip()
    if not host:
        return ""
    scheme = parsed.scheme or "http"
    default_port = 443 if scheme == "https" else 80
    if default_port == 443:
        netloc = host
    else:
        netloc = f"{host}:80"
    return urlunparse((scheme, netloc, "", "", "", "")).rstrip("/")


def _resolve_camera_control_settings(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve camera-control runtime config with safe defaults and inference."""
    camera_cfg = dict(config.get("camera") or {})
    control_cfg = dict(config.get("camera_control") or {})
    configured_origin = _normalize_camera_origin(control_cfg.get("base_url"))
    inferred_origin = _infer_camera_control_origin_from_stream(camera_cfg)
    base_url = configured_origin or inferred_origin
    return {
        "enabled": bool(control_cfg.get("enabled", False)),
        "base_url": base_url,
        "token": str(control_cfg.get("token", "")).strip(),
        "timeout_seconds": max(1, min(int(control_cfg.get("timeout_seconds", 4)), 30)),
        "configured_origin": configured_origin,
        "inferred_origin": inferred_origin,
    }


def _resolve_camera_profile_automation_settings(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve profile automation settings used for day/night profile orchestration."""
    control_cfg = dict(config.get("camera_control") or {})
    automation_cfg = dict(control_cfg.get("profile_automation") or {})
    day_start_hour = max(0, min(int(automation_cfg.get("day_start_hour", 7)), 23))
    night_start_hour = max(0, min(int(automation_cfg.get("night_start_hour", 19)), 23))
    return {
        "enabled": bool(automation_cfg.get("enabled", False)),
        "day_start_hour": day_start_hour,
        "night_start_hour": night_start_hour,
        "poll_interval_seconds": max(5, int(automation_cfg.get("poll_interval_seconds", 60))),
        "min_switch_interval_seconds": max(0, int(automation_cfg.get("min_switch_interval_seconds", 300))),
    }


def _redact_token_query(url: str) -> str:
    """Return URL text with token query values redacted for UI/debug payloads."""
    parsed = urlparse(url)
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() == "token":
            query_pairs.append((key, "***"))
        else:
            query_pairs.append((key, value))
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query_pairs, doseq=True),
            parsed.fragment,
        )
    )


def _camera_control_proxy_call(
    control: dict[str, Any],
    *,
    path: str,
    query: dict[str, Any] | None = None,
    method: str = "GET",
    require_auth: bool = True,
) -> dict[str, Any]:
    """Proxy one request to camera firmware control endpoint."""
    if not bool(control.get("enabled", False)):
        raise HTTPException(status_code=503, detail="camera_control is disabled in runtime config.")
    base_url = str(control.get("base_url", "")).strip().rstrip("/")
    if not base_url:
        raise HTTPException(status_code=503, detail="camera_control.base_url is not configured.")
    token = str(control.get("token", "")).strip()
    if require_auth and not token:
        raise HTTPException(status_code=503, detail="camera_control.token is not configured.")

    params = dict(query or {})
    headers = {"Accept": "application/json"}
    if require_auth:
        params["token"] = token
        headers["X-API-Token"] = token
        headers["Authorization"] = f"Bearer {token}"

    url = f"{base_url}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    timeout_seconds = max(1, min(int(control.get("timeout_seconds", 4)), 30))
    request = Request(url=url, method=method, headers=headers)

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200))
            content_type = str(response.headers.get("Content-Type", ""))
            raw_text = response.read().decode("utf-8", errors="replace").strip()
        payload: dict[str, Any]
        if raw_text and "json" in content_type.lower():
            try:
                payload = json.loads(raw_text)
            except json.JSONDecodeError:
                payload = {"raw": raw_text}
        elif raw_text:
            payload = {"raw": raw_text}
        else:
            payload = {}
        return {
            "ok": status < 400,
            "camera_status_code": status,
            "camera_url": _redact_token_query(url),
            "payload": payload,
        }
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace").strip()
        except Exception:
            body = ""
        return {
            "ok": False,
            "camera_status_code": int(exc.code),
            "camera_url": _redact_token_query(url),
            "error": "camera_http_error",
            "payload": {"raw": body} if body else {},
        }
    except (URLError, TimeoutError, OSError) as exc:
        raise HTTPException(status_code=502, detail=f"camera_request_failed: {exc}") from exc


CURATED_CAMERA_SETTINGS_SPEC: dict[str, dict[str, Any]] = {
    "xclk": {"kind": "select", "label": "XCLK (MHz)", "values": [15, 18, 24]},
    "framesize": {"kind": "slider", "label": "Frame Size", "min": 0, "max": 13, "step": 1},
    "quality": {"kind": "slider", "label": "JPEG Quality", "min": 4, "max": 63, "step": 1},
    "brightness": {"kind": "slider", "label": "Brightness", "min": -2, "max": 2, "step": 1},
    "contrast": {"kind": "slider", "label": "Contrast", "min": -2, "max": 2, "step": 1},
    "saturation": {"kind": "slider", "label": "Saturation", "min": -2, "max": 2, "step": 1},
    "awb": {"kind": "toggle", "label": "Auto White Balance"},
    "agc": {"kind": "toggle", "label": "Auto Gain Control"},
    "aec": {"kind": "toggle", "label": "Auto Exposure"},
    "aec2": {"kind": "toggle", "label": "AEC DSP"},
    "awb_gain": {"kind": "toggle", "label": "AWB Gain"},
    "hmirror": {"kind": "toggle", "label": "Horizontal Mirror"},
    "vflip": {"kind": "toggle", "label": "Vertical Flip"},
    "wb_mode": {"kind": "slider", "label": "WB Mode", "min": 0, "max": 4, "step": 1},
    "led_intensity": {"kind": "slider", "label": "LED Intensity", "min": 0, "max": 255, "step": 1},
}
CURATED_CAMERA_SETTING_ORDER = [
    "xclk",
    "framesize",
    "quality",
    "brightness",
    "contrast",
    "saturation",
    "awb",
    "agc",
    "aec",
    "aec2",
    "awb_gain",
    "hmirror",
    "vflip",
    "wb_mode",
    "led_intensity",
]
MANUAL_COMPATIBLE_MODES = {
    CAMERA_MODE_BASE,
    CAMERA_MODE_MANUAL_SAVED,
    CAMERA_MODE_MANUAL_TEMPORARY,
}


def _normalize_bool_to_int(value: Any) -> int:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return 1
    if text in {"0", "false", "no", "off"}:
        return 0
    return 1 if bool(value) else 0


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _sanitize_camera_setting_value(key: str, value: Any) -> int:
    if key == "xclk":
        candidate = _coerce_int(value, 15)
        return candidate if candidate in {15, 18, 24} else 15
    if key in {"awb", "agc", "aec", "aec2", "awb_gain", "hmirror", "vflip"}:
        return _normalize_bool_to_int(value)
    if key == "led_intensity":
        return max(0, min(255, _coerce_int(value, 0)))
    if key in {"brightness", "contrast", "saturation"}:
        return max(-2, min(2, _coerce_int(value, 0)))
    if key == "quality":
        return max(4, min(63, _coerce_int(value, 4)))
    if key == "framesize":
        return max(0, min(13, _coerce_int(value, 7)))
    if key == "wb_mode":
        return max(0, min(4, _coerce_int(value, 0)))
    return _coerce_int(value, 0)


def _sanitize_camera_settings_input(raw: dict[str, Any]) -> dict[str, int]:
    sanitized: dict[str, int] = {}
    for key in CURATED_CAMERA_SETTING_ORDER:
        if key in raw:
            sanitized[key] = _sanitize_camera_setting_value(key, raw[key])
    return sanitized


def _camera_status_to_curated_settings(status_payload: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key in CURATED_CAMERA_SETTING_ORDER:
        if key not in status_payload:
            continue
        out[key] = _sanitize_camera_setting_value(key, status_payload[key])
    return out


def _camera_settings_spec_payload() -> list[dict[str, Any]]:
    return [{"key": key, **dict(CURATED_CAMERA_SETTINGS_SPEC[key])} for key in CURATED_CAMERA_SETTING_ORDER]


def _camera_apply_single_setting(control: dict[str, Any], key: str, value: int) -> dict[str, Any]:
    if key == "xclk":
        return _camera_control_proxy_call(control, path="/xclk", query={"xclk": int(value)}, require_auth=False)
    return _camera_control_proxy_call(
        control,
        path="/control",
        query={"var": key, "val": int(value)},
        require_auth=False,
    )


def _apply_camera_settings(
    control: dict[str, Any],
    settings: dict[str, int],
    *,
    apply_order: list[str] | None = None,
) -> dict[str, Any]:
    order = list(apply_order or CURATED_CAMERA_SETTING_ORDER)
    per_setting: dict[str, dict[str, Any]] = {}
    succeeded = True
    for key in order:
        if key not in settings:
            continue
        result = _camera_apply_single_setting(control, key, int(settings[key]))
        per_setting[key] = result
        if not bool(result.get("ok", False)):
            succeeded = False
    return {"ok": succeeded, "settings": settings, "results": per_setting}


def _load_mode_state_or_503() -> dict[str, Any]:
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return fetch_camera_mode_state(SQLITE_DB_PATH)


def _save_mode_state_or_503(state: dict[str, Any], *, bump_revision: bool = False) -> dict[str, Any]:
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return save_camera_mode_state(SQLITE_DB_PATH, state, bump_revision=bump_revision)


def _ensure_mode_state_shape(state: dict[str, Any]) -> dict[str, Any]:
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    if CAMERA_MODE_MANUAL_SAVED not in modes:
        modes[CAMERA_MODE_MANUAL_SAVED] = {"settings": {}}
    if CAMERA_MODE_MANUAL_TEMPORARY not in modes:
        modes[CAMERA_MODE_MANUAL_TEMPORARY] = {"settings": {}}
    if CAMERA_MODE_BASE not in modes:
        modes[CAMERA_MODE_BASE] = {"profile": "base", "settings": {}}
    if CAMERA_MODE_ADAPTIVE not in modes:
        modes[CAMERA_MODE_ADAPTIVE] = {"settings": {"xclk": 15, "brightness": 0, "contrast": 0, "saturation": 0, "led_intensity": 0}}
    if CAMERA_MODE_AUTO_PROFILE not in modes:
        modes[CAMERA_MODE_AUTO_PROFILE] = {"day_profile": "day", "night_profile": "night"}
    canonical["modes"] = modes
    metadata = dict(canonical.get("metadata") or {})
    canonical["metadata"] = metadata
    state["canonical_state"] = canonical
    if str(state.get("active_mode", "")).strip() not in CAMERA_MODES:
        state["active_mode"] = CAMERA_MODE_BASE
    return state


def _desired_profile_for_local_hour(hour: int, *, day_start_hour: int, night_start_hour: int) -> str:
    if day_start_hour == night_start_hour:
        return "base"
    if day_start_hour < night_start_hour:
        return "day" if day_start_hour <= hour < night_start_hour else "night"
    return "day" if hour >= day_start_hour or hour < night_start_hour else "night"


def _extract_status_subset(status_payload: dict[str, Any]) -> dict[str, Any]:
    subset = {k: status_payload.get(k) for k in ("active_profile", "xclk", "framesize", "quality", "led_intensity")}
    subset["controls"] = _camera_status_to_curated_settings(status_payload)
    return subset


def _camera_controls_from_status_payload(status_payload: dict[str, Any]) -> dict[str, int]:
    controls = status_payload.get("controls")
    if isinstance(controls, dict):
        return _sanitize_camera_settings_input(controls)
    return _camera_status_to_curated_settings(status_payload)


def _empty_drift_detail(reason: str) -> dict[str, Any]:
    return {
        "detected": False,
        "changed_keys": [],
        "timestamp_utc": _utc_now(),
        "reason": str(reason or "not_evaluated"),
        "differences": {},
        "unreported_keys": [],
    }


def _build_manual_mode_drift_detail(state: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
    detail = _empty_drift_detail("manual_mode_not_evaluated")
    if not isinstance(status_payload, dict) or not status_payload:
        detail["reason"] = "camera_status_missing"
        return detail

    safe_state = _ensure_mode_state_shape(dict(state))
    active_mode = str(safe_state.get("active_mode", CAMERA_MODE_BASE))
    canonical = dict(safe_state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    slot = dict(modes.get(active_mode) or {})
    expected_settings = _sanitize_camera_settings_input(dict(slot.get("settings") or {}))
    current_settings = _camera_controls_from_status_payload(status_payload)

    differences: dict[str, dict[str, Any]] = {}
    unreported_keys: list[str] = []
    for key, expected_value in expected_settings.items():
        if key not in current_settings:
            unreported_keys.append(key)
            continue
        current_value = int(current_settings[key])
        if current_value != int(expected_value):
            differences[key] = {"expected": int(expected_value), "camera": int(current_value)}

    if active_mode == CAMERA_MODE_BASE:
        expected_profile = str(slot.get("profile", "base")).strip().lower() or "base"
        current_profile = str(status_payload.get("active_profile", "")).strip().lower()
        if current_profile:
            if current_profile != expected_profile:
                differences["active_profile"] = {"expected": expected_profile, "camera": current_profile}
        else:
            unreported_keys.append("active_profile")

    changed_keys = sorted(differences.keys())
    detected = bool(changed_keys)
    detail.update(
        {
            "detected": detected,
            "changed_keys": changed_keys,
            "timestamp_utc": _utc_now(),
            "reason": (
                "manual_mode_drift"
                if detected
                else ("manual_mode_partial_status" if unreported_keys else "manual_mode_in_sync")
            ),
            "differences": differences,
            "unreported_keys": sorted(set(unreported_keys)),
        }
    )
    return detail


def _effective_profile_automation_enabled(config_enabled: bool) -> bool:
    if PROFILE_AUTOMATION_RUNNER is None:
        return bool(config_enabled)
    try:
        status = PROFILE_AUTOMATION_RUNNER.status()
        if "effective_enabled" in status:
            return bool(status.get("effective_enabled"))
    except Exception:
        pass
    return bool(config_enabled)


def _build_auto_profile_drift_detail(status_payload: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    detail = _empty_drift_detail("auto_profile_not_evaluated")
    if not isinstance(status_payload, dict) or not status_payload:
        detail["reason"] = "camera_status_missing"
        return detail

    effective_auto = _effective_profile_automation_enabled(bool(automation.get("enabled", False)))
    local_now = time.localtime()
    desired_profile = _desired_profile_for_local_hour(
        int(local_now.tm_hour),
        day_start_hour=int(automation.get("day_start_hour", 7)),
        night_start_hour=int(automation.get("night_start_hour", 19)),
    )
    current_profile = str(status_payload.get("active_profile", "")).strip().lower()
    differences: dict[str, dict[str, Any]] = {}
    changed_keys: list[str] = []
    unreported_keys: list[str] = []

    if not effective_auto:
        changed_keys = ["automation"]
        differences["automation"] = {"expected": "enabled", "camera": "disabled_or_paused"}
        detail.update(
            {
                "detected": True,
                "changed_keys": changed_keys,
                "timestamp_utc": _utc_now(),
                "reason": "automation_paused",
                "differences": differences,
                "unreported_keys": [],
            }
        )
        return detail

    if not current_profile:
        changed_keys = ["active_profile"]
        differences["active_profile"] = {"expected": desired_profile, "camera": "not_reported"}
        unreported_keys.append("active_profile")
    elif current_profile != desired_profile:
        changed_keys = ["active_profile"]
        differences["active_profile"] = {"expected": desired_profile, "camera": current_profile}

    detected = bool(changed_keys)
    detail.update(
        {
            "detected": detected,
            "changed_keys": changed_keys,
            "timestamp_utc": _utc_now(),
            "reason": "auto_profile_drift" if detected else "auto_profile_in_sync",
            "differences": differences,
            "unreported_keys": unreported_keys,
        }
    )
    return detail


def _build_adaptive_mode_drift_detail(state: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
    detail = _empty_drift_detail("adaptive_not_evaluated")
    if not isinstance(status_payload, dict) or not status_payload:
        detail["reason"] = "camera_status_missing"
        return detail

    safe_state = _ensure_mode_state_shape(dict(state))
    canonical = dict(safe_state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    slot = dict(modes.get(CAMERA_MODE_ADAPTIVE) or {})
    expected_settings = _sanitize_camera_settings_input(dict(slot.get("settings") or {}))
    current_settings = _camera_controls_from_status_payload(status_payload)
    tracked_keys = [key for key in ("xclk", "brightness", "contrast", "saturation", "led_intensity") if key in expected_settings]

    differences: dict[str, dict[str, Any]] = {}
    unreported_keys: list[str] = []
    for key in tracked_keys:
        if key not in current_settings:
            unreported_keys.append(key)
            continue
        expected_value = int(expected_settings[key])
        current_value = int(current_settings[key])
        if expected_value != current_value:
            differences[key] = {"expected": expected_value, "camera": current_value}

    changed_keys = sorted(differences.keys())
    detected = bool(changed_keys)
    detail.update(
        {
            "detected": detected,
            "changed_keys": changed_keys,
            "timestamp_utc": _utc_now(),
            "reason": (
                "adaptive_policy_drift"
                if detected
                else ("adaptive_partial_status" if unreported_keys else "adaptive_policy_in_sync")
            ),
            "differences": differences,
            "unreported_keys": sorted(set(unreported_keys)),
        }
    )
    return detail


def _build_mode_drift_detail(state: dict[str, Any], status_payload: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    safe_state = _ensure_mode_state_shape(dict(state))
    active_mode = str(safe_state.get("active_mode", CAMERA_MODE_BASE))
    if active_mode in MANUAL_COMPATIBLE_MODES:
        return _build_manual_mode_drift_detail(safe_state, status_payload)
    if active_mode == CAMERA_MODE_AUTO_PROFILE:
        return _build_auto_profile_drift_detail(status_payload, automation)
    if active_mode == CAMERA_MODE_ADAPTIVE:
        return _build_adaptive_mode_drift_detail(safe_state, status_payload)
    detail = _empty_drift_detail("unknown_mode")
    detail["detected"] = True
    detail["changed_keys"] = ["active_mode"]
    detail["differences"] = {"active_mode": {"expected": "known_mode", "camera": active_mode}}
    return detail


def _evaluate_manual_mode_sync(
    state: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    detail = _build_manual_mode_drift_detail(state, status_payload)
    if bool(detail.get("detected", False)):
        state["sync_status"] = CAMERA_SYNC_DRIFT
        state["last_reconcile_action"] = f"{context}_manual_drift_detected"
    else:
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = f"{context}_manual_in_sync"
    state["last_error"] = ""
    return state


def _evaluate_auto_profile_sync(
    state: dict[str, Any],
    status_payload: dict[str, Any],
    automation: dict[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    detail = _build_auto_profile_drift_detail(status_payload, automation)
    if bool(detail.get("detected", False)):
        state["sync_status"] = CAMERA_SYNC_DRIFT
        if str(detail.get("reason", "")) == "automation_paused":
            state["last_reconcile_action"] = f"{context}_auto_profile_paused"
        else:
            state["last_reconcile_action"] = f"{context}_auto_profile_drift_detected"
    else:
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = f"{context}_auto_profile_in_sync"
    state["last_error"] = ""
    return state


def _evaluate_adaptive_mode_sync(
    state: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    detail = _build_adaptive_mode_drift_detail(state, status_payload)
    if bool(detail.get("detected", False)):
        state["sync_status"] = CAMERA_SYNC_DRIFT
        state["last_reconcile_action"] = f"{context}_adaptive_drift_detected"
    else:
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = f"{context}_adaptive_in_sync"
    state["last_error"] = ""
    return state


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _seconds_since_utc(value: Any) -> float:
    ts = _parse_utc(value)
    if ts is None:
        return float("inf")
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _compute_frame_luma() -> float | None:
    frame = ingest_service.get_latest_frame_copy()
    if frame is None:
        return None
    if frame.ndim != 3 or frame.shape[2] < 3:
        return None
    # BGR frame mean provides a stable low-cost luma proxy for adaptive tuning.
    b = frame[:, :, 0].astype(np.float32)
    g = frame[:, :, 1].astype(np.float32)
    r = frame[:, :, 2].astype(np.float32)
    luma = float(np.mean(0.114 * b + 0.587 * g + 0.299 * r))
    return luma


def _is_ingest_healthy() -> bool:
    status = ingest_service.status()
    return bool(
        status.get("running")
        and status.get("capture_open")
        and not bool(status.get("stale"))
    )


def _adaptive_score_from_luma(luma: float | None) -> float:
    if luma is None:
        return -9999.0
    target = 95.0
    return -abs(float(luma) - target)


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_uint32),
        ("dwMemoryLoad", ctypes.c_uint32),
        ("ullTotalPhys", ctypes.c_uint64),
        ("ullAvailPhys", ctypes.c_uint64),
        ("ullTotalPageFile", ctypes.c_uint64),
        ("ullAvailPageFile", ctypes.c_uint64),
        ("ullTotalVirtual", ctypes.c_uint64),
        ("ullAvailVirtual", ctypes.c_uint64),
        ("ullAvailExtendedVirtual", ctypes.c_uint64),
    ]


def _cpu_percent_snapshot() -> float:
    # Windows-first implementation using WMIC fallback without extra dependencies.
    try:
        completed = subprocess.run(
            ["wmic", "cpu", "get", "loadpercentage", "/value"],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.5,
        )
        if completed.returncode == 0:
            for line in completed.stdout.splitlines():
                if "LoadPercentage=" in line:
                    _, rhs = line.split("=", 1)
                    return float(max(0, min(100, int(rhs.strip()))))
    except (FileNotFoundError, OSError, ValueError, subprocess.SubprocessError):
        pass
    return 0.0


def _memory_snapshot() -> dict[str, int | float]:
    if hasattr(ctypes, "windll"):
        mem = _MemoryStatusEx()
        mem.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem)):
            total = int(mem.ullTotalPhys)
            available = int(mem.ullAvailPhys)
            used = max(0, total - available)
            pct = (used / total * 100.0) if total > 0 else 0.0
            return {"total_bytes": total, "used_bytes": used, "available_bytes": available, "percent": pct}
    return {"total_bytes": 0, "used_bytes": 0, "available_bytes": 0, "percent": 0.0}


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += int(p.stat().st_size)
        except OSError:
            continue
    return total


def _system_utilization_payload() -> dict[str, Any]:
    drive_path = Path(ROOT.anchor or str(ROOT))
    disk = shutil.disk_usage(drive_path)
    drive_total = int(disk.total)
    drive_used = int(disk.used)
    drive_free = int(disk.free)

    sqlite_size = 0
    if SQLITE_DB_PATH is not None and SQLITE_DB_PATH.exists():
        try:
            sqlite_size = int(SQLITE_DB_PATH.stat().st_size)
        except OSError:
            sqlite_size = 0

    logs_bytes = _dir_size_bytes(LOGS_DIR)
    captures_bytes = _dir_size_bytes(SNAPSHOT_DIR)
    videos_bytes = _dir_size_bytes(RECORDINGS_DIR)
    events_db_bytes = sqlite_size
    known_bytes = logs_bytes + captures_bytes + videos_bytes + events_db_bytes
    misc_bytes = max(0, drive_used - known_bytes)

    def pct(part: int, whole: int) -> float:
        if whole <= 0:
            return 0.0
        return max(0.0, min(100.0, (float(part) / float(whole)) * 100.0))

    cpu_percent = _cpu_percent_snapshot()
    mem = _memory_snapshot()
    breakdown = [
        {"key": "logs", "label": "Logs", "bytes": logs_bytes, "percent_of_used": pct(logs_bytes, drive_used), "color": "#60a5fa"},
        {"key": "captures", "label": "Captures", "bytes": captures_bytes, "percent_of_used": pct(captures_bytes, drive_used), "color": "#2dd4bf"},
        {"key": "videos", "label": "Videos", "bytes": videos_bytes, "percent_of_used": pct(videos_bytes, drive_used), "color": "#f59e0b"},
        {"key": "events_db", "label": "Events DB", "bytes": events_db_bytes, "percent_of_used": pct(events_db_bytes, drive_used), "color": "#f97316"},
        {"key": "misc", "label": "Misc", "bytes": misc_bytes, "percent_of_used": pct(misc_bytes, drive_used), "color": "#6b7280"},
    ]

    return {
        "cpu": {
            "percent": cpu_percent,
            "logical_cores": int(os.cpu_count() or 0),
        },
        "ram": mem,
        "drive": {
            "path": str(drive_path),
            "used_bytes": drive_used,
            "total_bytes": drive_total,
            "free_bytes": drive_free,
            "percent": pct(drive_used, drive_total),
        },
        "storage_breakdown": {
            "used_bytes": drive_used,
            "total_bytes": drive_total,
            "segments": breakdown,
        },
    }


def _send_webhook_alert(event: dict[str, Any], alerts_cfg: dict[str, Any]) -> dict[str, Any]:
    """Dispatch one webhook alert payload using backend runtime config."""
    if not bool(alerts_cfg.get("enabled", False)):
        return {"sent": False, "reason": "alerts_disabled"}
    if not bool(alerts_cfg.get("webhook_enabled", True)):
        return {"sent": False, "reason": "webhook_disabled"}

    webhook_url = str(alerts_cfg.get("webhook_url", "")).strip()
    if not webhook_url:
        return {"sent": False, "reason": "webhook_url_empty"}

    timeout_seconds = max(1, int(alerts_cfg.get("timeout_seconds", 6)))
    text = (
        f"Doorbell event: {event.get('label')} "
        f"(conf {event.get('confidence')}) at {event.get('ts_utc')}"
    )
    payload = {"text": text, "event": event}
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200))
            if status >= 400:
                return {"sent": False, "reason": "http_error", "status": status}
            return {"sent": True, "status": status}
    except HTTPError as exc:
        return {"sent": False, "reason": "http_error", "status": int(exc.code)}
    except (URLError, TimeoutError, OSError) as exc:
        return {"sent": False, "reason": "request_error", "error": str(exc)}


def _normalize_base_url(value: Any) -> str:
    """Normalize a dashboard base URL to include scheme and no trailing slash."""
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text.rstrip("/")
    return f"http://{text}".rstrip("/")


def _discover_local_ipv4() -> str | None:
    """Discover one non-loopback local IPv4 address for quick dashboard links."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            candidate = str(sock.getsockname()[0]).strip()
            if candidate and not candidate.startswith("127."):
                return candidate
    except OSError:
        pass

    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET)
    except OSError:
        infos = []
    for info in infos:
        candidate = str(info[4][0]).strip()
        if candidate and not candidate.startswith("127."):
            return candidate
    return None


def _discover_tailscale_ipv4() -> str | None:
    """Try to read one Tailscale IPv4 address from local CLI when available."""
    commands = [
        ["tailscale", "ip", "-4"],
        [r"C:\Program Files\Tailscale\tailscale.exe", "ip", "-4"],
    ]
    for cmd in commands:
        try:
            completed = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=1.5,
            )
        except (FileNotFoundError, OSError, subprocess.SubprocessError):
            continue
        if completed.returncode != 0:
            continue
        for raw in completed.stdout.splitlines():
            line = raw.strip()
            if line and "." in line and "/" not in line:
                return line
    return None


def _resolve_alert_access_links(
    alerts_cfg: dict[str, Any],
    *,
    api_cfg: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Resolve dashboard quick-access links for local LAN and Tailscale access."""
    access_cfg = dict(alerts_cfg.get("access_links") or {})
    api_port = int(access_cfg.get("dashboard_port", (api_cfg or {}).get("port", 8080)))

    local_url = _normalize_base_url(access_cfg.get("local_url"))
    tailscale_url = _normalize_base_url(access_cfg.get("tailscale_url"))

    if not local_url and bool(access_cfg.get("auto_detect_local", True)):
        local_ip = _discover_local_ipv4()
        if local_ip:
            local_url = f"http://{local_ip}:{api_port}"

    if not tailscale_url and bool(access_cfg.get("auto_detect_tailscale", True)):
        tailscale_ip = _discover_tailscale_ipv4()
        if tailscale_ip:
            tailscale_url = f"http://{tailscale_ip}:{api_port}"

    return {
        "local_url": local_url,
        "tailscale_url": tailscale_url,
    }


def _resolve_media_ref_path(root: Path, ref: Any) -> Path | None:
    """Resolve one media reference into an absolute file path."""
    text = str(ref or "").strip()
    if not text:
        return None
    p = Path(text)
    return p if p.is_absolute() else root / p


def _attach_file_if_present(
    message: EmailMessage,
    *,
    root: Path,
    ref: Any,
    label: str,
    max_attachment_bytes: int,
    attached: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
) -> None:
    """Attach one file to the SMTP message when present and within attachment size budget."""
    media_path = _resolve_media_ref_path(root, ref)
    if media_path is None:
        skipped.append({"type": label, "reason": "empty_reference"})
        return
    if not media_path.exists() or not media_path.is_file():
        skipped.append({"type": label, "reason": "missing_file", "path": str(media_path)})
        return

    file_size = int(media_path.stat().st_size)
    if max_attachment_bytes > 0 and file_size > max_attachment_bytes:
        skipped.append(
            {
                "type": label,
                "reason": "too_large",
                "path": str(media_path),
                "size_bytes": file_size,
                "max_bytes": max_attachment_bytes,
            }
        )
        return

    payload = media_path.read_bytes()
    guessed, _ = mimetypes.guess_type(media_path.name)
    if guessed and "/" in guessed:
        maintype, subtype = guessed.split("/", 1)
    else:
        maintype, subtype = "application", "octet-stream"
    message.add_attachment(payload, maintype=maintype, subtype=subtype, filename=media_path.name)
    attached.append({"type": label, "name": media_path.name, "size_bytes": file_size})


def _send_smtp_alert(
    event: dict[str, Any],
    alerts_cfg: dict[str, Any],
    *,
    root: Path,
    access_links: dict[str, str] | None = None,
    include_snapshot: bool = True,
    include_clip: bool = False,
    selected_clip_path: str | None = None,
) -> dict[str, Any]:
    """Dispatch one SMTP alert email with optional media attachments."""
    if not bool(alerts_cfg.get("enabled", False)):
        return {"sent": False, "reason": "alerts_disabled"}

    smtp_cfg = dict(alerts_cfg.get("smtp") or {})
    if not bool(smtp_cfg.get("enabled", False)):
        return {"sent": False, "reason": "smtp_disabled"}

    host = str(smtp_cfg.get("host", "")).strip()
    port = int(smtp_cfg.get("port", 587))
    use_starttls = bool(smtp_cfg.get("use_starttls", True))
    timeout_seconds = max(3, int(smtp_cfg.get("timeout_seconds", 10)))
    username = str(smtp_cfg.get("username", "")).strip()
    password = str(smtp_cfg.get("password", "")).strip()
    from_email = str(smtp_cfg.get("from_email", "")).strip()
    to_emails = [str(x).strip() for x in smtp_cfg.get("to_emails", []) if str(x).strip()]
    subject_prefix = str(smtp_cfg.get("subject_prefix", "Doorbell Alert")).strip() or "Doorbell Alert"
    max_attachment_mb = float(smtp_cfg.get("max_attachment_mb", 8))
    max_attachment_bytes = max(0, int(max_attachment_mb * 1024 * 1024))

    if not host:
        return {"sent": False, "reason": "smtp_host_empty"}
    if not from_email:
        return {"sent": False, "reason": "smtp_from_email_empty"}
    if not to_emails:
        return {"sent": False, "reason": "smtp_recipients_empty"}
    if not username or not password:
        return {"sent": False, "reason": "smtp_credentials_empty"}

    severity_level = str(event.get("severity_level", "unknown")).upper()
    label = str(event.get("label", "unknown")).strip() or "unknown"
    subject = f"{subject_prefix}: {label} [{severity_level}]"
    text_lines = [
        "Doorbell event detected.",
        "",
        f"event_id: {event.get('id')}",
        f"timestamp_utc: {event.get('ts_utc')}",
        f"label: {event.get('label')}",
        f"confidence: {event.get('confidence')}",
        f"severity_level: {event.get('severity_level')}",
        f"severity_score: {event.get('severity_score')}",
        f"lifecycle_state: {event.get('lifecycle_state')}",
        f"snapshot_ref: {event.get('snapshot')}",
        f"clip_ref: {event.get('clip')}",
    ]
    link_values = dict(access_links or {})
    local_url = str(link_values.get("local_url", "")).strip()
    tailscale_url = str(link_values.get("tailscale_url", "")).strip()
    if local_url or tailscale_url:
        text_lines.extend(["", "dashboard_access:"])
        if local_url:
            text_lines.append(f"local_dashboard_url: {local_url}")
        if tailscale_url:
            text_lines.append(f"tailscale_dashboard_url: {tailscale_url}")

    message = EmailMessage()
    message["From"] = from_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject
    message.set_content("\n".join(text_lines))

    attached: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    include_snapshot_effective = bool(include_snapshot and smtp_cfg.get("include_snapshot", True))
    include_clip_effective = bool(include_clip or smtp_cfg.get("include_clip_default", False))

    if include_snapshot_effective:
        _attach_file_if_present(
            message,
            root=root,
            ref=event.get("snapshot"),
            label="snapshot",
            max_attachment_bytes=max_attachment_bytes,
            attached=attached,
            skipped=skipped,
        )
    if include_clip_effective:
        clip_ref = selected_clip_path if str(selected_clip_path or "").strip() else event.get("clip")
        _attach_file_if_present(
            message,
            root=root,
            ref=clip_ref,
            label="clip",
            max_attachment_bytes=max_attachment_bytes,
            attached=attached,
            skipped=skipped,
        )

    try:
        with smtplib.SMTP(host=host, port=port, timeout=timeout_seconds) as client:
            client.ehlo()
            if use_starttls:
                client.starttls()
                client.ehlo()
            client.login(username, password)
            client.send_message(message)
        return {
            "sent": True,
            "reason": "sent",
            "recipients": to_emails,
            "attachments": attached,
            "skipped_attachments": skipped,
        }
    except (smtplib.SMTPException, OSError, TimeoutError) as exc:
        return {
            "sent": False,
            "reason": "smtp_exception",
            "error": str(exc),
            "attachments": attached,
            "skipped_attachments": skipped,
        }


def _dispatch_alert_channels(
    event: dict[str, Any],
    alerts_cfg: dict[str, Any],
    *,
    root: Path,
    access_links: dict[str, str] | None = None,
    include_snapshot: bool = True,
    include_clip: bool = False,
    selected_clip_path: str | None = None,
) -> dict[str, Any]:
    """Dispatch enabled alert channels and return combined delivery result."""
    if not bool(alerts_cfg.get("enabled", False)):
        return {
            "sent": False,
            "reason": "alerts_disabled",
            "retry_recommended": False,
            "channels": {},
        }

    channels: dict[str, dict[str, Any]] = {}
    webhook_enabled = bool(alerts_cfg.get("webhook_enabled", True))
    smtp_enabled = bool((alerts_cfg.get("smtp") or {}).get("enabled", False))
    if webhook_enabled:
        channels["webhook"] = _send_webhook_alert(event, alerts_cfg)
    else:
        channels["webhook"] = {"sent": False, "reason": "webhook_disabled"}
    if smtp_enabled:
        channels["smtp"] = _send_smtp_alert(
            event,
            alerts_cfg,
            root=root,
            access_links=access_links,
            include_snapshot=include_snapshot,
            include_clip=include_clip,
            selected_clip_path=selected_clip_path,
        )
    else:
        channels["smtp"] = {"sent": False, "reason": "smtp_disabled"}

    sent_any = any(bool(ch.get("sent", False)) for ch in channels.values())
    transient_reasons = {"request_error", "http_error", "smtp_exception"}
    retry_recommended = (not sent_any) and any(
        str(ch.get("reason", "")) in transient_reasons for ch in channels.values()
    )
    return {
        "sent": sent_any,
        "reason": "sent" if sent_any else "no_channel_delivered",
        "retry_recommended": retry_recommended,
        "channels": channels,
    }


class BackgroundJobRunner:
    """Run queued background jobs in one backend-managed worker thread."""

    def __init__(self, db_path: Path, root: Path, snapshot_dir: Path, recordings_dir: Path) -> None:
        self._db_path = db_path
        self._root = root
        self._snapshot_dir = snapshot_dir
        self._recordings_dir = recordings_dir
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._worker_name = "backend-main-worker"

    def _load_job_settings(self) -> dict[str, Any]:
        cfg = _load_config().get("jobs", {})
        return {
            "enabled": bool(cfg.get("enabled", True)),
            "poll_interval_seconds": max(0.2, float(cfg.get("poll_interval_seconds", 2.0))),
            "retry_delay_seconds": max(1, int(cfg.get("retry_delay_seconds", 10))),
            "stale_running_seconds": max(10, int(cfg.get("stale_running_seconds", 300))),
            "sample_limit": max(1, min(int(cfg.get("sample_limit", 100)), 1000)),
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        settings = self._load_job_settings()
        recovered = requeue_stale_running_jobs(
            self._db_path,
            stale_after_seconds=int(settings["stale_running_seconds"]),
        )
        if recovered > 0:
            LOGGER.warning("Background worker recovered %d stale running jobs.", recovered)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="background-job-runner",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            settings = self._load_job_settings()
            if not bool(settings["enabled"]):
                time.sleep(1.0)
                continue

            job = claim_next_background_job(self._db_path, worker_name=self._worker_name)
            if job is None:
                time.sleep(float(settings["poll_interval_seconds"]))
                continue

            job_id = int(job["id"])
            try:
                result = self._process_job(job, sample_limit=int(settings["sample_limit"]))
                complete_background_job(self._db_path, job_id, result=result)
            except Exception as exc:
                attempt_count = int(job.get("attempt_count", 1))
                backoff = min(
                    300,
                    int(settings["retry_delay_seconds"]) * max(1, 2 ** max(0, attempt_count - 1)),
                )
                outcome = fail_or_retry_background_job(
                    self._db_path,
                    job_id,
                    f"{type(exc).__name__}: {exc}",
                    retry_delay_seconds=backoff,
                )
                LOGGER.warning(
                    "Background job id=%s type=%s errored; status=%s retry_in=%ss",
                    job_id,
                    job.get("job_type"),
                    outcome.get("status"),
                    backoff,
                )

    def _process_job(self, job: dict[str, Any], *, sample_limit: int) -> dict[str, Any]:
        job_type = str(job.get("job_type", "")).strip()
        payload = dict(job.get("payload") or {})
        if job_type == "retention_cull":
            return run_retention_cull(
                self._db_path,
                self._root,
                apply_file_delete=bool(payload.get("apply_file_delete", False)),
                sample_limit=max(1, min(int(payload.get("sample_limit", sample_limit)), 1000)),
            )
        if job_type == "policy_backfill":
            return backfill_event_policy_fields(
                self._db_path,
                only_missing=bool(payload.get("only_missing", True)),
            )
        if job_type == "media_integrity_repair":
            return repair_media_integrity(
                self._db_path,
                self._root,
                self._snapshot_dir,
                self._recordings_dir,
                mark_missing_as_expired=bool(payload.get("mark_missing_as_expired", False)),
                delete_orphan_files=bool(payload.get("delete_orphan_files", False)),
                sample_limit=max(1, min(int(payload.get("sample_limit", sample_limit)), 1000)),
            )
        if job_type in {"dispatch_alert", "dispatch_alert_webhook"}:
            event_id = int(payload.get("event_id", 0))
            if event_id <= 0:
                raise ValueError("dispatch_alert requires event_id")
            event = fetch_event_by_id(self._db_path, event_id)
            if event is None:
                raise ValueError(f"event id {event_id} not found")
            config = _load_config()
            alerts_cfg = config.get("alerts", {})
            access_links = _resolve_alert_access_links(alerts_cfg, api_cfg=config.get("api", {}))
            alert_result = _dispatch_alert_channels(
                event,
                alerts_cfg,
                root=self._root,
                access_links=access_links,
                include_snapshot=bool(payload.get("include_snapshot", True)),
                include_clip=bool(payload.get("include_clip", False)),
                selected_clip_path=str(payload.get("selected_clip_path", "")).strip() or None,
            )
            if bool(alert_result.get("retry_recommended", False)):
                raise RuntimeError("alert dispatch failed with retryable channel errors")
            return {"event_id": event_id, "alert_result": alert_result}
        raise ValueError(f"unsupported job_type '{job_type}'")


class CameraProfileAutomationRunner:
    """Run camera mode orchestration, sync reconciliation, and adaptive controls."""

    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._runtime_enabled_override: bool | None = None
        self._last_tick_local_iso = ""
        self._last_camera_profile = ""
        self._last_desired_profile = ""
        self._last_apply_result: dict[str, Any] = {}
        self._last_error = ""
        self._last_switch_attempt_monotonic = 0.0
        self._switch_attempts = 0
        self._switch_successes = 0

    def _load_settings(self) -> dict[str, Any]:
        config = _load_config()
        return {
            "control": _resolve_camera_control_settings(config),
            "automation": _resolve_camera_profile_automation_settings(config),
        }

    def _effective_enabled(self, config_enabled: bool) -> bool:
        with self._lock:
            override = self._runtime_enabled_override
        return bool(config_enabled) if override is None else bool(override)

    def set_enabled_override(self, enabled: bool | None) -> None:
        with self._lock:
            self._runtime_enabled_override = enabled

    def status(self) -> dict[str, Any]:
        try:
            settings = self._load_settings()
            control = dict(settings["control"])
            automation = dict(settings["automation"])
            config_enabled = bool(automation.get("enabled", False))
            effective_enabled = self._effective_enabled(config_enabled)
            load_error = ""
        except Exception as exc:
            control = {}
            automation = {}
            config_enabled = False
            effective_enabled = False
            load_error = f"{type(exc).__name__}: {exc}"

        try:
            mode_state = _load_mode_state_or_503()
            active_mode = str(mode_state.get("active_mode", CAMERA_MODE_BASE))
            sync_status = str(mode_state.get("sync_status", CAMERA_SYNC_UNAVAILABLE))
            mode_revision = int(mode_state.get("mode_revision", 0))
        except HTTPException:
            active_mode = CAMERA_MODE_BASE
            sync_status = CAMERA_SYNC_UNAVAILABLE
            mode_revision = 0

        with self._lock:
            return {
                "ok": not load_error,
                "load_error": load_error,
                "config_enabled": config_enabled,
                "runtime_enabled_override": self._runtime_enabled_override,
                "effective_enabled": effective_enabled,
                "automation": automation,
                "camera_control_ready": {
                    "enabled": bool(control.get("enabled", False)),
                    "base_url": str(control.get("base_url", "")),
                    "token_configured": bool(str(control.get("token", "")).strip()),
                },
                "active_mode": active_mode,
                "sync_status": sync_status,
                "mode_revision": mode_revision,
                "last_tick_local_iso": self._last_tick_local_iso,
                "last_camera_profile": self._last_camera_profile,
                "last_desired_profile": self._last_desired_profile,
                "last_apply_result": self._last_apply_result,
                "last_error": self._last_error,
                "switch_attempts": self._switch_attempts,
                "switch_successes": self._switch_successes,
            }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="camera-mode-orchestrator",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._thread = None

    def _track_apply_result(self, result: dict[str, Any], *, error_hint: str = "") -> None:
        with self._lock:
            self._last_apply_result = result
            if bool(result.get("ok", False)):
                self._last_error = ""
            elif error_hint:
                self._last_error = error_hint

    def _adopt_camera_state(self, state: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
        state = _ensure_mode_state_shape(state)
        active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
        controls = _camera_status_to_curated_settings(status_payload)
        canonical = dict(state.get("canonical_state") or {})
        modes = dict(canonical.get("modes") or {})
        mode_slot = dict(modes.get(active_mode) or {})
        mode_slot["settings"] = controls
        if active_mode == CAMERA_MODE_BASE:
            mode_slot["profile"] = str(status_payload.get("active_profile", "base")).strip().lower() or "base"
        modes[active_mode] = mode_slot
        canonical["modes"] = modes
        metadata = dict(canonical.get("metadata") or {})
        metadata["last_adopted_mode"] = active_mode
        metadata["last_adopted_ts_utc"] = _utc_now()
        canonical["metadata"] = metadata
        state["canonical_state"] = canonical
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = "adopt_current_camera_state"
        state["last_error"] = ""
        return state

    def _reapply_manual_or_adaptive(self, control: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        state = _ensure_mode_state_shape(state)
        active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
        canonical = dict(state.get("canonical_state") or {})
        modes = dict(canonical.get("modes") or {})
        mode_slot = dict(modes.get(active_mode) or {})
        desired_settings = _sanitize_camera_settings_input(dict(mode_slot.get("settings") or {}))
        if not desired_settings:
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = "reapply_skipped_empty_settings"
            return state
        state["sync_status"] = CAMERA_SYNC_REAPPLYING
        apply_result = _apply_camera_settings(control, desired_settings)
        self._track_apply_result(apply_result, error_hint="manual_or_adaptive_reapply_failed")
        state["last_apply_result"] = apply_result
        if bool(apply_result.get("ok", False)):
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = "reapply_canonical_settings"
            state["last_error"] = ""
        else:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "reapply_failed"
            state["last_error"] = "canonical_reapply_failed"
        return state

    def _run_auto_profile(self, control: dict[str, Any], state: dict[str, Any], automation: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
        state = _ensure_mode_state_shape(state)
        local_now = time.localtime()
        desired_profile = _desired_profile_for_local_hour(
            int(local_now.tm_hour),
            day_start_hour=int(automation.get("day_start_hour", 7)),
            night_start_hour=int(automation.get("night_start_hour", 19)),
        )
        current_profile = str(status_payload.get("active_profile", "")).strip().lower()
        with self._lock:
            self._last_desired_profile = desired_profile
            self._last_camera_profile = current_profile
        effective_auto = self._effective_enabled(bool(automation.get("enabled", False)))
        if not effective_auto:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "auto_profile_paused"
            return state
        if current_profile == desired_profile:
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = "auto_profile_in_sync"
            return state

        now_mono = time.monotonic()
        min_switch = max(0, int(automation.get("min_switch_interval_seconds", 300)))
        with self._lock:
            too_soon = (now_mono - self._last_switch_attempt_monotonic) < float(min_switch)
        if too_soon:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "auto_profile_waiting_dwell"
            return state

        state["sync_status"] = CAMERA_SYNC_REAPPLYING
        result = _camera_control_proxy_call(
            control,
            path="/profile",
            query={"name": desired_profile},
            require_auth=True,
        )
        with self._lock:
            self._last_switch_attempt_monotonic = now_mono
            self._switch_attempts += 1
            if bool(result.get("ok", False)):
                self._switch_successes += 1
                self._last_camera_profile = desired_profile
        self._track_apply_result(result, error_hint="auto_profile_apply_failed")
        state["last_apply_result"] = result
        if bool(result.get("ok", False)):
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = f"auto_profile_applied_{desired_profile}"
            state["last_error"] = ""
        else:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "auto_profile_apply_failed"
            state["last_error"] = "auto_profile_apply_failed"
        return state

    def _adaptive_cycle(self, control: dict[str, Any], state: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
        state = _ensure_mode_state_shape(state)
        canonical = dict(state.get("canonical_state") or {})
        modes = dict(canonical.get("modes") or {})
        adaptive_slot = dict(modes.get(CAMERA_MODE_ADAPTIVE) or {})
        desired = _sanitize_camera_settings_input(dict(adaptive_slot.get("settings") or {}))
        runtime = dict(adaptive_slot.get("runtime") or {})
        now_utc = _utc_now()
        current = _camera_status_to_curated_settings(status_payload)

        if not _is_ingest_healthy():
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "adaptive_paused_ingest_unhealthy"
            return state

        # Reapply canonical adaptive policy on drift before making new adaptive moves.
        drift_detected = any(
            int(current.get(k, desired.get(k, 0))) != int(desired.get(k, current.get(k, 0)))
            for k in ("xclk", "brightness", "contrast", "saturation", "led_intensity")
            if k in desired
        )
        if drift_detected:
            state = self._reapply_manual_or_adaptive(control, state)
            if str(state.get("sync_status")) != CAMERA_SYNC_IN_SYNC:
                return state
            current = _camera_status_to_curated_settings(status_payload)

        luma_before = _compute_frame_luma()
        score_before = _adaptive_score_from_luma(luma_before)
        if luma_before is None:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "adaptive_paused_no_frame"
            return state

        low_on_threshold = 65.0
        low_off_threshold = 86.0
        bright_off_threshold = 110.0
        low_active = bool(runtime.get("low_light_active", False))
        if luma_before < low_on_threshold:
            low_active = True
        elif luma_before > low_off_threshold:
            low_active = False
        runtime["low_light_active"] = low_active

        # LED-first policy in low light.
        target_led = 0
        if low_active:
            deficit = max(0.0, 95.0 - float(luma_before))
            target_led = int(max(200, min(255, 200 + deficit * 1.5)))
        current_led = int(current.get("led_intensity", desired.get("led_intensity", 0)))
        led_handled = False
        if float(luma_before) >= bright_off_threshold:
            low_active = False
            runtime["low_light_active"] = False
            if current_led > 0 and _seconds_since_utc(runtime.get("last_led_change_ts_utc")) >= 8:
                led_result = _camera_apply_single_setting(control, "led_intensity", 0)
                self._track_apply_result(led_result, error_hint="adaptive_led_bright_off_failed")
                state["last_apply_result"] = led_result
                if bool(led_result.get("ok", False)):
                    desired["led_intensity"] = 0
                    runtime["last_led_change_ts_utc"] = now_utc
                    adaptive_slot["settings"] = desired
                    adaptive_slot["runtime"] = runtime
                    modes[CAMERA_MODE_ADAPTIVE] = adaptive_slot
                    canonical["modes"] = modes
                    state["canonical_state"] = canonical
                led_handled = True

        led_diff = target_led - current_led
        if (not led_handled) and abs(led_diff) >= 16 and _seconds_since_utc(runtime.get("last_led_change_ts_utc")) >= 30:
            if low_active and current_led < 64 and target_led >= 200:
                next_led = target_led
            else:
                step = 16 if led_diff > 0 else -16
                next_led = max(0, min(255, current_led + step))
            led_result = _camera_apply_single_setting(control, "led_intensity", next_led)
            self._track_apply_result(led_result, error_hint="adaptive_led_apply_failed")
            state["last_apply_result"] = led_result
            if bool(led_result.get("ok", False)):
                desired["led_intensity"] = int(next_led)
                runtime["last_led_change_ts_utc"] = now_utc
                adaptive_slot["settings"] = desired
                adaptive_slot["runtime"] = runtime
                modes[CAMERA_MODE_ADAPTIVE] = adaptive_slot
                canonical["modes"] = modes
                state["canonical_state"] = canonical

        # XCLK guarded policy: allow-list + 30m spacing.
        current_xclk = int(current.get("xclk", desired.get("xclk", 15)))
        candidate_xclk = 15 if low_active else (24 if float(luma_before) > 115 else 18)
        if candidate_xclk not in {15, 18, 24}:
            candidate_xclk = 15
        if candidate_xclk != current_xclk and _seconds_since_utc(runtime.get("last_xclk_change_ts_utc")) >= 1800:
            xclk_result = _camera_apply_single_setting(control, "xclk", candidate_xclk)
            self._track_apply_result(xclk_result, error_hint="adaptive_xclk_apply_failed")
            state["last_apply_result"] = xclk_result
            runtime["last_xclk_attempt_ts_utc"] = now_utc
            if bool(xclk_result.get("ok", False)):
                desired["xclk"] = int(candidate_xclk)
                runtime["last_xclk_change_ts_utc"] = now_utc

        # One-at-a-time brightness/contrast/saturation adjustments with rollback.
        if _seconds_since_utc(runtime.get("last_tweak_ts_utc")) >= 20:
            tweak_order = ["brightness", "contrast", "saturation"]
            target = 95.0
            direction = 1 if float(luma_before) < 85.0 else (-1 if float(luma_before) > 115.0 else 0)
            if direction != 0:
                for key in tweak_order:
                    current_value = int(desired.get(key, 0))
                    candidate_value = max(-2, min(2, current_value + direction))
                    if candidate_value == current_value:
                        continue
                    apply_result = _camera_apply_single_setting(control, key, candidate_value)
                    if not bool(apply_result.get("ok", False)):
                        continue
                    time.sleep(2.5)
                    luma_after = _compute_frame_luma()
                    score_after = _adaptive_score_from_luma(luma_after)
                    if _is_ingest_healthy() and score_after > score_before:
                        desired[key] = candidate_value
                        runtime["last_tweak_ts_utc"] = _utc_now()
                    else:
                        _camera_apply_single_setting(control, key, current_value)
                    self._track_apply_result(apply_result, error_hint="adaptive_tweak_apply_failed")
                    state["last_apply_result"] = apply_result
                    break

        adaptive_slot["settings"] = desired
        adaptive_slot["runtime"] = runtime
        modes[CAMERA_MODE_ADAPTIVE] = adaptive_slot
        canonical["modes"] = modes
        state["canonical_state"] = canonical
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = "adaptive_policy_cycle"
        state["last_error"] = ""
        return state

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            sleep_seconds = 15
            try:
                settings = self._load_settings()
                control = dict(settings["control"])
                automation = dict(settings["automation"])
                sleep_seconds = max(5, int(automation.get("poll_interval_seconds", 60)))
                with self._lock:
                    self._last_tick_local_iso = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime())

                if SQLITE_DB_PATH is None:
                    time.sleep(sleep_seconds)
                    continue
                state = _ensure_mode_state_shape(fetch_camera_mode_state(SQLITE_DB_PATH))

                if not bool(control.get("enabled", False)):
                    state["sync_status"] = CAMERA_SYNC_UNAVAILABLE
                    state["last_reconcile_action"] = "camera_control_disabled"
                    state["last_error"] = "camera_control_disabled"
                    save_camera_mode_state(SQLITE_DB_PATH, state, bump_revision=False)
                    time.sleep(sleep_seconds)
                    continue

                status_result = _camera_control_proxy_call(control, path="/status", require_auth=False)
                if not bool(status_result.get("ok", False)):
                    self._track_apply_result(status_result, error_hint="camera_status_failed")
                    state["sync_status"] = CAMERA_SYNC_UNAVAILABLE
                    state["last_reconcile_action"] = "camera_status_unavailable"
                    state["last_apply_result"] = status_result
                    state["last_error"] = "camera_status_unavailable"
                    save_camera_mode_state(SQLITE_DB_PATH, state, bump_revision=False)
                    time.sleep(sleep_seconds)
                    continue

                status_payload = dict(status_result.get("payload") or {})
                state["last_camera_status"] = _extract_status_subset(status_payload)
                active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
                if active_mode in MANUAL_COMPATIBLE_MODES:
                    state = _evaluate_manual_mode_sync(state, status_payload, context="loop")
                elif active_mode == CAMERA_MODE_AUTO_PROFILE:
                    state = self._run_auto_profile(control, state, automation, status_payload)
                elif active_mode == CAMERA_MODE_ADAPTIVE:
                    state = self._adaptive_cycle(control, state, status_payload)
                else:
                    state["sync_status"] = CAMERA_SYNC_DRIFT
                    state["last_reconcile_action"] = "unknown_mode_noop"
                save_camera_mode_state(SQLITE_DB_PATH, state, bump_revision=False)
            except Exception as exc:
                with self._lock:
                    self._last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(sleep_seconds)


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
    backfill_event_policy_fields(SQLITE_DB_PATH, only_missing=True)

    global JOB_RUNNER
    JOB_RUNNER = BackgroundJobRunner(SQLITE_DB_PATH, ROOT, SNAPSHOT_DIR, RECORDINGS_DIR)
    JOB_RUNNER.start()

    global PROFILE_AUTOMATION_RUNNER
    PROFILE_AUTOMATION_RUNNER = CameraProfileAutomationRunner()
    PROFILE_AUTOMATION_RUNNER.start()

    _clear_server_control_state()
    _install_server_control_signal_handlers()


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Stop ingest service and release stream resources."""
    global JOB_RUNNER
    if JOB_RUNNER is not None:
        JOB_RUNNER.stop()
        JOB_RUNNER = None
    global PROFILE_AUTOMATION_RUNNER
    if PROFILE_AUTOMATION_RUNNER is not None:
        PROFILE_AUTOMATION_RUNNER.stop()
        PROFILE_AUTOMATION_RUNNER = None
    ingest_service.stop()
    _restore_server_control_signal_handlers()


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


@app.get("/api/server/control")
def server_control_status() -> JSONResponse:
    """Return planned backend shutdown/reboot state for UI countdown banner."""
    return JSONResponse(_server_control_state_snapshot())


@app.post("/api/server/control")
def server_control_schedule(
    action: str = Query(..., min_length=1, max_length=16),
    delay_seconds: int = Query(default=SERVER_CONTROL_DEFAULT_DELAY_SECONDS, ge=1, le=SERVER_CONTROL_MAX_DELAY_SECONDS),
) -> JSONResponse:
    """Schedule backend shutdown/reboot with a short countdown visible to clients."""
    selected = str(action or "").strip().lower()
    if selected not in {SERVER_ACTION_SHUTDOWN, SERVER_ACTION_REBOOT}:
        raise HTTPException(status_code=400, detail="action must be one of: shutdown, reboot.")
    try:
        state = _schedule_server_action(
            action=selected,
            delay_seconds=int(delay_seconds),
            reason="api_request",
            trigger="dashboard_api",
            relay_signal=None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "state": state})


@app.get("/api/health")
def health() -> JSONResponse:
    """Return overall backend health and ingest/storage diagnostics."""
    config = _load_config()
    ingest = ingest_service.status()

    storage_payload = {
        "db_exists": False,
        "event_records": 0,
        "schema_version": 0,
        "latest_schema_version": CURRENT_SCHEMA_VERSION,
    }
    if SQLITE_DB_PATH is not None:
        storage_payload = sqlite_status(SQLITE_DB_PATH)
    jobs_payload = {"total": 0, "ready_now": 0}
    if SQLITE_DB_PATH is not None:
        jobs_payload = background_job_stats(SQLITE_DB_PATH)

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
            "uptime_seconds": max(0.0, round(time.monotonic() - BACKEND_START_MONOTONIC, 3)),
            "ingest": ingest,
            "storage": storage_payload,
            "jobs": jobs_payload,
            "server_control": _server_control_state_snapshot(),
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
        return JSONResponse(
            {
                "db_exists": False,
                "event_records": 0,
                "schema_version": 0,
                "latest_schema_version": CURRENT_SCHEMA_VERSION,
            }
        )
    return JSONResponse(sqlite_status(SQLITE_DB_PATH))


@app.get("/api/events")
def get_events(
    limit: int = Query(default=24, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    q: str = Query(default=""),
    severity: str = Query(default="all"),
    media: str = Query(default="all"),
    range: str = Query(default="all"),
) -> JSONResponse:
    """Return filterable, paginated events from metadata storage."""
    payload = _events_query_payload(
        limit=limit,
        offset=offset,
        q=q,
        severity=severity,
        media=media,
        range_key=range,
    )
    return JSONResponse(payload)


@app.post("/api/events/{event_id}/media")
def set_event_media_links(
    event_id: int,
    snapshot: str | None = Query(default=None),
    clip: str | None = Query(default=None),
) -> JSONResponse:
    """Attach canonical snapshot/clip links to one event record."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    if snapshot is None and clip is None:
        raise HTTPException(status_code=400, detail="Provide snapshot and/or clip path.")
    updated = attach_event_media_paths(
        SQLITE_DB_PATH,
        event_id,
        snapshot_path=snapshot,
        clip_path=clip,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Event record not found.")
    return JSONResponse({"updated": True, "event_id": event_id})


@app.get("/api/media/integrity")
def media_integrity(sample_limit: int = Query(default=100, ge=1, le=1000)) -> JSONResponse:
    """Return media-integrity report for missing refs, orphans, and state mismatches."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse(
        media_integrity_report(
            SQLITE_DB_PATH,
            ROOT,
            SNAPSHOT_DIR,
            RECORDINGS_DIR,
            sample_limit=sample_limit,
        )
    )


@app.post("/api/media/integrity/repair")
def media_integrity_repair(
    mark_missing_as_expired: bool = Query(default=False),
    delete_orphan_files: bool = Query(default=False),
    sample_limit: int = Query(default=100, ge=1, le=1000),
) -> JSONResponse:
    """Apply optional media-integrity remediations and return before/after summary."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    if not mark_missing_as_expired and not delete_orphan_files:
        raise HTTPException(
            status_code=400,
            detail="No repair action selected. Set mark_missing_as_expired and/or delete_orphan_files.",
        )
    return JSONResponse(
        repair_media_integrity(
            SQLITE_DB_PATH,
            ROOT,
            SNAPSHOT_DIR,
            RECORDINGS_DIR,
            mark_missing_as_expired=mark_missing_as_expired,
            delete_orphan_files=delete_orphan_files,
            sample_limit=sample_limit,
        )
    )


@app.get("/api/retention/summary")
def retention_status() -> JSONResponse:
    """Expose retention policy readiness summary for diagnostics."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse(retention_summary(SQLITE_DB_PATH))


@app.post("/api/retention/cull")
def retention_cull(
    apply_file_delete: bool = Query(default=False),
    sample_limit: int = Query(default=100, ge=1, le=1000),
) -> JSONResponse:
    """Run retention culling and optionally delete due media files."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse(
        run_retention_cull(
            SQLITE_DB_PATH,
            ROOT,
            apply_file_delete=apply_file_delete,
            sample_limit=sample_limit,
        )
    )


@app.post("/api/policy/backfill")
def policy_backfill(only_missing: bool = Query(default=True)) -> JSONResponse:
    """Recompute canonical identity, severity, and retention fields."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse(backfill_event_policy_fields(SQLITE_DB_PATH, only_missing=only_missing))


@app.get("/api/jobs/stats")
def job_stats() -> JSONResponse:
    """Expose background job queue statistics for runtime monitoring."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse(background_job_stats(SQLITE_DB_PATH))


@app.get("/api/jobs")
def list_jobs(
    limit: int = Query(default=100, ge=1, le=1000),
    status: str | None = Query(default=None),
) -> JSONResponse:
    """List recent background jobs with optional status filter."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    return JSONResponse({"jobs": fetch_background_jobs(SQLITE_DB_PATH, limit=limit, status=status)})


@app.post("/api/jobs/retention-cull")
def queue_retention_cull(
    apply_file_delete: bool = Query(default=False),
    sample_limit: int = Query(default=100, ge=1, le=1000),
    max_attempts: int = Query(default=3, ge=1, le=20),
) -> JSONResponse:
    """Queue a retention cull job for asynchronous background processing."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    job_id = enqueue_background_job(
        SQLITE_DB_PATH,
        "retention_cull",
        payload={
            "apply_file_delete": bool(apply_file_delete),
            "sample_limit": int(sample_limit),
        },
        max_attempts=max_attempts,
    )
    return JSONResponse({"queued": True, "job_id": job_id, "job_type": "retention_cull"})


@app.post("/api/jobs/policy-backfill")
def queue_policy_backfill(
    only_missing: bool = Query(default=True),
    max_attempts: int = Query(default=3, ge=1, le=20),
) -> JSONResponse:
    """Queue a policy backfill job for asynchronous background processing."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    job_id = enqueue_background_job(
        SQLITE_DB_PATH,
        "policy_backfill",
        payload={"only_missing": bool(only_missing)},
        max_attempts=max_attempts,
    )
    return JSONResponse({"queued": True, "job_id": job_id, "job_type": "policy_backfill"})


@app.post("/api/jobs/media-integrity-repair")
def queue_media_integrity_repair(
    mark_missing_as_expired: bool = Query(default=False),
    delete_orphan_files: bool = Query(default=False),
    sample_limit: int = Query(default=100, ge=1, le=1000),
    max_attempts: int = Query(default=3, ge=1, le=20),
) -> JSONResponse:
    """Queue media-integrity remediation for asynchronous processing."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    if not mark_missing_as_expired and not delete_orphan_files:
        raise HTTPException(
            status_code=400,
            detail="No repair action selected. Set mark_missing_as_expired and/or delete_orphan_files.",
        )
    job_id = enqueue_background_job(
        SQLITE_DB_PATH,
        "media_integrity_repair",
        payload={
            "mark_missing_as_expired": bool(mark_missing_as_expired),
            "delete_orphan_files": bool(delete_orphan_files),
            "sample_limit": int(sample_limit),
        },
        max_attempts=max_attempts,
    )
    return JSONResponse({"queued": True, "job_id": job_id, "job_type": "media_integrity_repair"})


@app.post("/api/jobs/dispatch-alert")
def queue_alert_dispatch(
    event_id: int = Query(..., ge=1),
    include_snapshot: bool = Query(default=True),
    include_clip: bool = Query(default=False),
    selected_clip_path: str | None = Query(default=None),
    max_attempts: int = Query(default=3, ge=1, le=20),
) -> JSONResponse:
    """Queue alert dispatch for one event id using current alert config."""
    if SQLITE_DB_PATH is None:
        raise HTTPException(status_code=503, detail="SQLite metadata store is not ready.")
    if fetch_event_by_id(SQLITE_DB_PATH, event_id) is None:
        raise HTTPException(status_code=404, detail="Event record not found.")
    job_id = enqueue_background_job(
        SQLITE_DB_PATH,
        "dispatch_alert",
        payload={
            "event_id": int(event_id),
            "include_snapshot": bool(include_snapshot),
            "include_clip": bool(include_clip),
            "selected_clip_path": str(selected_clip_path or "").strip() or None,
        },
        max_attempts=max_attempts,
    )
    return JSONResponse({"queued": True, "job_id": job_id, "job_type": "dispatch_alert"})


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
    access_cfg = alerts_cfg.get("access_links", {})
    smtp_cfg = alerts_cfg.get("smtp", {})
    auth_cfg = config.get("auth", {})
    recording_cfg = config.get("recording", {})
    ingest_cfg = config.get("ingest", {})
    storage_cfg = config.get("storage", {})
    jobs_cfg = config.get("jobs", {})
    camera_control_cfg = _resolve_camera_control_settings(config)
    profile_automation_cfg = _resolve_camera_profile_automation_settings(config)
    automation_status = PROFILE_AUTOMATION_RUNNER.status() if PROFILE_AUTOMATION_RUNNER is not None else {}
    mode_state = _ensure_mode_state_shape(_load_mode_state_or_503())

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
        "jobs": {
            "enabled": bool(jobs_cfg.get("enabled", True)),
            "poll_interval_seconds": jobs_cfg.get("poll_interval_seconds", 2.0),
            "retry_delay_seconds": jobs_cfg.get("retry_delay_seconds", 10),
            "stale_running_seconds": jobs_cfg.get("stale_running_seconds", 300),
        },
        "alerts": {
            "enabled": alerts_cfg.get("enabled", False),
            "webhook_enabled": bool(alerts_cfg.get("webhook_enabled", True)),
            "webhook_configured": not _is_missing_or_placeholder(alerts_cfg.get("webhook_url")),
            "enqueue_background": bool(alerts_cfg.get("enqueue_background", True)),
            "smtp_enabled": bool(smtp_cfg.get("enabled", False)),
            "smtp_include_snapshot": bool(smtp_cfg.get("include_snapshot", True)),
            "smtp_include_clip_default": bool(smtp_cfg.get("include_clip_default", False)),
            "smtp_configured": (
                not _is_missing_or_placeholder(smtp_cfg.get("host"))
                and not _is_missing_or_placeholder(smtp_cfg.get("username"))
                and not _is_missing_or_placeholder(smtp_cfg.get("password"))
                and not _is_missing_or_placeholder(smtp_cfg.get("from_email"))
                and isinstance(smtp_cfg.get("to_emails", []), list)
                and any(str(x).strip() for x in smtp_cfg.get("to_emails", []))
            ),
            "access_links": {
                "local_configured": bool(str(access_cfg.get("local_url", "")).strip()),
                "tailscale_configured": bool(str(access_cfg.get("tailscale_url", "")).strip()),
                "auto_detect_local": bool(access_cfg.get("auto_detect_local", True)),
                "auto_detect_tailscale": bool(access_cfg.get("auto_detect_tailscale", True)),
            },
        },
        "auth": {
            "enabled": bool(auth_cfg.get("enabled", False)),
            "session_secret_configured": _is_strong_secret(auth_cfg.get("session_secret"), min_length=24),
            "session_hours": int(auth_cfg.get("session_hours", 24)),
        },
        "api": {"host": api_cfg.get("host"), "port": api_cfg.get("port")},
        "camera_control": {
            "enabled": bool(camera_control_cfg.get("enabled", False)),
            "base_url": camera_control_cfg.get("base_url", ""),
            "configured_base_url": camera_control_cfg.get("configured_origin", ""),
            "inferred_base_url": camera_control_cfg.get("inferred_origin", ""),
            "token_configured": not _is_missing_or_placeholder(camera_control_cfg.get("token", "")),
            "timeout_seconds": camera_control_cfg.get("timeout_seconds", 4),
            "active_mode": mode_state.get("active_mode", CAMERA_MODE_BASE),
            "mode_revision": int(mode_state.get("mode_revision", 0)),
            "sync_status": mode_state.get("sync_status", CAMERA_SYNC_UNAVAILABLE),
            "profile_automation": {
                "config_enabled": bool(profile_automation_cfg.get("enabled", False)),
                "effective_enabled": bool(automation_status.get("effective_enabled", False)),
                "runtime_enabled_override": automation_status.get("runtime_enabled_override"),
                "day_start_hour": profile_automation_cfg.get("day_start_hour", 7),
                "night_start_hour": profile_automation_cfg.get("night_start_hour", 19),
                "poll_interval_seconds": profile_automation_cfg.get("poll_interval_seconds", 60),
                "min_switch_interval_seconds": profile_automation_cfg.get("min_switch_interval_seconds", 300),
                "last_camera_profile": automation_status.get("last_camera_profile", ""),
                "last_desired_profile": automation_status.get("last_desired_profile", ""),
            },
        },
    }
    return JSONResponse(payload)


def _mode_state_payload(state: dict[str, Any], *, control: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    state = _ensure_mode_state_shape(dict(state))
    last_camera_status = dict(state.get("last_camera_status") or {})
    if str(state.get("sync_status", "")) == CAMERA_SYNC_UNAVAILABLE:
        drift_detail = _empty_drift_detail("camera_unavailable")
    else:
        drift_detail = _build_mode_drift_detail(state, last_camera_status, automation)
    return {
        "ok": True,
        "active_mode": state.get("active_mode", CAMERA_MODE_BASE),
        "mode_revision": int(state.get("mode_revision", 0)),
        "sync_status": state.get("sync_status", CAMERA_SYNC_UNAVAILABLE),
        "last_reconcile_action": state.get("last_reconcile_action", ""),
        "last_camera_status": last_camera_status,
        "drift_detail": drift_detail,
        "last_apply_result": dict(state.get("last_apply_result") or {}),
        "last_error": str(state.get("last_error", "")),
        "canonical_state": dict(state.get("canonical_state") or {}),
        "updated_ts_utc": str(state.get("updated_ts_utc", "")),
        "effective_owners": {
            "manual_lockout_active": str(state.get("active_mode")) in {CAMERA_MODE_MANUAL_SAVED, CAMERA_MODE_MANUAL_TEMPORARY},
            "auto_day_night_profile_active": str(state.get("active_mode")) == CAMERA_MODE_AUTO_PROFILE,
            "adaptive_capture_active": str(state.get("active_mode")) == CAMERA_MODE_ADAPTIVE,
        },
        "camera_control": {
            "enabled": bool(control.get("enabled", False)),
            "base_url": control.get("base_url", ""),
            "token_configured": bool(str(control.get("token", "")).strip()),
        },
        "profile_automation": {
            "config_enabled": bool(automation.get("enabled", False)),
            "runtime_enabled_override": (
                PROFILE_AUTOMATION_RUNNER.status().get("runtime_enabled_override")
                if PROFILE_AUTOMATION_RUNNER is not None
                else None
            ),
        },
        "settings_spec": _camera_settings_spec_payload(),
    }


def _apply_canonical_policy_for_mode(
    state: dict[str, Any],
    *,
    control: dict[str, Any],
    automation: dict[str, Any],
) -> dict[str, Any]:
    state = _ensure_mode_state_shape(state)
    active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})

    if active_mode == CAMERA_MODE_BASE:
        result = _camera_control_proxy_call(control, path="/profile", query={"name": "base"}, require_auth=True)
        state["last_apply_result"] = result
        if bool(result.get("ok", False)):
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = "base_profile_reapplied"
            state["last_error"] = ""
        else:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "base_profile_reapply_failed"
            state["last_error"] = "base_profile_reapply_failed"
        return state

    if active_mode == CAMERA_MODE_AUTO_PROFILE:
        desired_profile = _desired_profile_for_local_hour(
            time.localtime().tm_hour,
            day_start_hour=int(automation.get("day_start_hour", 7)),
            night_start_hour=int(automation.get("night_start_hour", 19)),
        )
        result = _camera_control_proxy_call(
            control,
            path="/profile",
            query={"name": desired_profile},
            require_auth=True,
        )
        state["last_apply_result"] = result
        if bool(result.get("ok", False)):
            state["sync_status"] = CAMERA_SYNC_IN_SYNC
            state["last_reconcile_action"] = f"auto_profile_reapplied_{desired_profile}"
            state["last_error"] = ""
        else:
            state["sync_status"] = CAMERA_SYNC_DRIFT
            state["last_reconcile_action"] = "auto_profile_reapply_failed"
            state["last_error"] = "auto_profile_reapply_failed"
        return state

    mode_slot = dict(modes.get(active_mode) or {})
    settings = _sanitize_camera_settings_input(dict(mode_slot.get("settings") or {}))
    result = _apply_camera_settings(control, settings)
    state["last_apply_result"] = result
    if bool(result.get("ok", False)):
        state["sync_status"] = CAMERA_SYNC_IN_SYNC
        state["last_reconcile_action"] = "canonical_settings_reapplied"
        state["last_error"] = ""
    else:
        state["sync_status"] = CAMERA_SYNC_DRIFT
        state["last_reconcile_action"] = "canonical_settings_reapply_failed"
        state["last_error"] = "canonical_settings_reapply_failed"
    return state


@app.get("/api/camera-control/status")
def camera_control_status() -> JSONResponse:
    """Return camera-control readiness and current camera /status payload."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation_status = PROFILE_AUTOMATION_RUNNER.status() if PROFILE_AUTOMATION_RUNNER is not None else {}
    mode_state = _ensure_mode_state_shape(_load_mode_state_or_503())
    if not bool(control.get("enabled", False)):
        return JSONResponse(
            {
                "ok": False,
                "reason": "camera_control_disabled",
                "camera_control": {
                    "enabled": False,
                    "base_url": control.get("base_url", ""),
                    "token_configured": bool(str(control.get("token", "")).strip()),
                    "timeout_seconds": control.get("timeout_seconds", 4),
                },
                "profile_automation": automation_status,
                "mode_state": mode_state,
            }
        )
    camera_status = _camera_control_proxy_call(control, path="/status", require_auth=False)
    payload = {
        "ok": bool(camera_status.get("ok", False)),
        "camera_control": {
            "enabled": True,
            "base_url": control.get("base_url", ""),
            "token_configured": bool(str(control.get("token", "")).strip()),
            "timeout_seconds": control.get("timeout_seconds", 4),
        },
        "profile_automation": automation_status,
        "mode_state": mode_state,
        "status": camera_status,
    }
    return JSONResponse(payload, status_code=200 if payload["ok"] else 502)


@app.get("/api/camera-control/automation")
def camera_control_automation_status() -> JSONResponse:
    """Return profile automation runtime state and effective enablement."""
    if PROFILE_AUTOMATION_RUNNER is None:
        raise HTTPException(status_code=503, detail="Camera profile automation runner is not initialized.")
    return JSONResponse(PROFILE_AUTOMATION_RUNNER.status())


@app.post("/api/camera-control/automation")
def camera_control_automation_toggle(enabled: bool = Query(...)) -> JSONResponse:
    """Set runtime enable override for day/night profile automation."""
    if PROFILE_AUTOMATION_RUNNER is None:
        raise HTTPException(status_code=503, detail="Camera profile automation runner is not initialized.")
    PROFILE_AUTOMATION_RUNNER.set_enabled_override(bool(enabled))
    return JSONResponse(
        {
            "ok": True,
            "runtime_enabled_override": bool(enabled),
            "status": PROFILE_AUTOMATION_RUNNER.status(),
        }
    )


@app.post("/api/camera-control/automation/use-config")
def camera_control_automation_use_config() -> JSONResponse:
    """Clear runtime override and use config-file automation setting again."""
    if PROFILE_AUTOMATION_RUNNER is None:
        raise HTTPException(status_code=503, detail="Camera profile automation runner is not initialized.")
    PROFILE_AUTOMATION_RUNNER.set_enabled_override(None)
    return JSONResponse({"ok": True, "runtime_enabled_override": None, "status": PROFILE_AUTOMATION_RUNNER.status()})


@app.post("/api/camera-control/automation/schedule")
def camera_control_automation_schedule_update(
    day_start_hour: int = Query(..., ge=0, le=23),
    night_start_hour: int = Query(..., ge=0, le=23),
) -> JSONResponse:
    """Update day/night profile schedule hours in runtime config."""
    config = _load_config()
    camera_control_cfg = dict(config.get("camera_control") or {})
    profile_automation_cfg = dict(camera_control_cfg.get("profile_automation") or {})
    profile_automation_cfg["day_start_hour"] = int(day_start_hour)
    profile_automation_cfg["night_start_hour"] = int(night_start_hour)
    camera_control_cfg["profile_automation"] = profile_automation_cfg
    config["camera_control"] = camera_control_cfg
    _save_config(config)

    updated = {
        "day_start_hour": int(profile_automation_cfg.get("day_start_hour", 7)),
        "night_start_hour": int(profile_automation_cfg.get("night_start_hour", 19)),
    }
    status_payload = PROFILE_AUTOMATION_RUNNER.status() if PROFILE_AUTOMATION_RUNNER is not None else {"ok": True}
    return JSONResponse({"ok": True, "updated": updated, "status": status_payload})


@app.get("/api/camera-control/mode-state")
def camera_control_mode_state() -> JSONResponse:
    """Return canonical camera mode/revision/sync payload for Camera tab orchestration."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    state = _load_mode_state_or_503()
    return JSONResponse(_mode_state_payload(state, control=control, automation=automation))


@app.post("/api/camera-control/mode")
def camera_control_set_mode(name: str = Query(..., min_length=1, max_length=64)) -> JSONResponse:
    """Set active camera orchestration mode and apply canonical policy immediately."""
    selected = str(name).strip().lower()
    if selected not in CAMERA_MODES:
        raise HTTPException(status_code=400, detail=f"Invalid mode '{selected}'.")

    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    state = _ensure_mode_state_shape(_load_mode_state_or_503())
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})

    if selected in {CAMERA_MODE_BASE, CAMERA_MODE_MANUAL_SAVED, CAMERA_MODE_MANUAL_TEMPORARY}:
        mode_slot = dict(modes.get(selected) or {})
        slot_settings = _sanitize_camera_settings_input(dict(mode_slot.get("settings") or {}))
        if not slot_settings and bool(control.get("enabled", False)):
            status_result = _camera_control_proxy_call(control, path="/status", require_auth=False)
            if bool(status_result.get("ok", False)):
                slot_settings = _camera_status_to_curated_settings(dict(status_result.get("payload") or {}))
                mode_slot["settings"] = slot_settings
                if selected == CAMERA_MODE_BASE:
                    status_payload = dict(status_result.get("payload") or {})
                    mode_slot["profile"] = str(status_payload.get("active_profile", "base")).strip().lower() or "base"
                modes[selected] = mode_slot
                canonical["modes"] = modes
                state["canonical_state"] = canonical

    state["active_mode"] = selected
    state["last_reconcile_action"] = f"mode_selected_{selected}"
    state["last_error"] = ""

    if selected == CAMERA_MODE_AUTO_PROFILE and PROFILE_AUTOMATION_RUNNER is not None:
        PROFILE_AUTOMATION_RUNNER.set_enabled_override(True)

    if bool(control.get("enabled", False)):
        state = _apply_canonical_policy_for_mode(state, control=control, automation=automation)
    saved = _save_mode_state_or_503(state, bump_revision=True)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.post("/api/camera-control/settings/apply-temporary")
def camera_control_apply_temporary_settings(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    """Apply curated settings now and persist as manual_temporary canonical state."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    if not bool(control.get("enabled", False)):
        raise HTTPException(status_code=503, detail="camera_control is disabled in runtime config.")

    incoming = payload.get("settings") if isinstance(payload.get("settings"), dict) else payload
    if not isinstance(incoming, dict):
        raise HTTPException(status_code=400, detail="Body must include a settings object.")
    settings = _sanitize_camera_settings_input(incoming)
    if not settings:
        raise HTTPException(status_code=400, detail="No supported curated settings provided.")

    apply_result = _apply_camera_settings(control, settings)
    if not bool(apply_result.get("ok", False)):
        raise HTTPException(status_code=502, detail="One or more settings failed to apply.")

    state = _ensure_mode_state_shape(_load_mode_state_or_503())
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    slot = dict(modes.get(CAMERA_MODE_MANUAL_TEMPORARY) or {})
    slot["settings"] = settings
    modes[CAMERA_MODE_MANUAL_TEMPORARY] = slot
    canonical["modes"] = modes
    state["canonical_state"] = canonical
    state["active_mode"] = CAMERA_MODE_MANUAL_TEMPORARY
    state["sync_status"] = CAMERA_SYNC_IN_SYNC
    state["last_reconcile_action"] = "manual_temporary_applied"
    state["last_apply_result"] = apply_result
    state["last_error"] = ""
    saved = _save_mode_state_or_503(state, bump_revision=True)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.post("/api/camera-control/settings/save-manual")
def camera_control_save_manual_settings(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    """Apply curated settings now and persist as manual_saved canonical state."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    if not bool(control.get("enabled", False)):
        raise HTTPException(status_code=503, detail="camera_control is disabled in runtime config.")

    incoming = payload.get("settings") if isinstance(payload.get("settings"), dict) else payload
    if not isinstance(incoming, dict):
        raise HTTPException(status_code=400, detail="Body must include a settings object.")
    settings = _sanitize_camera_settings_input(incoming)
    if not settings:
        raise HTTPException(status_code=400, detail="No supported curated settings provided.")

    apply_result = _apply_camera_settings(control, settings)
    if not bool(apply_result.get("ok", False)):
        raise HTTPException(status_code=502, detail="One or more settings failed to apply.")

    state = _ensure_mode_state_shape(_load_mode_state_or_503())
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    slot = dict(modes.get(CAMERA_MODE_MANUAL_SAVED) or {})
    slot["settings"] = settings
    modes[CAMERA_MODE_MANUAL_SAVED] = slot
    canonical["modes"] = modes
    state["canonical_state"] = canonical
    state["active_mode"] = CAMERA_MODE_MANUAL_SAVED
    state["sync_status"] = CAMERA_SYNC_IN_SYNC
    state["last_reconcile_action"] = "manual_saved_applied"
    state["last_apply_result"] = apply_result
    state["last_error"] = ""
    saved = _save_mode_state_or_503(state, bump_revision=True)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.post("/api/camera-control/sync/adopt-current")
def camera_control_sync_adopt_current() -> JSONResponse:
    """Adopt camera's current runtime settings into canonical state for active mode."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    if not bool(control.get("enabled", False)):
        raise HTTPException(status_code=503, detail="camera_control is disabled in runtime config.")

    status_result = _camera_control_proxy_call(control, path="/status", require_auth=False)
    if not bool(status_result.get("ok", False)):
        raise HTTPException(status_code=502, detail="Camera status unavailable for adoption.")
    payload = dict(status_result.get("payload") or {})
    current_settings = _camera_status_to_curated_settings(payload)

    state = _ensure_mode_state_shape(_load_mode_state_or_503())
    active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
    canonical = dict(state.get("canonical_state") or {})
    modes = dict(canonical.get("modes") or {})
    slot = dict(modes.get(active_mode) or {})
    slot["settings"] = current_settings
    if active_mode == CAMERA_MODE_BASE:
        slot["profile"] = str(payload.get("active_profile", "base")).strip().lower() or "base"
    modes[active_mode] = slot
    canonical["modes"] = modes
    metadata = dict(canonical.get("metadata") or {})
    metadata["last_adopted_mode"] = active_mode
    metadata["last_adopted_ts_utc"] = _utc_now()
    canonical["metadata"] = metadata
    state["canonical_state"] = canonical
    state["sync_status"] = CAMERA_SYNC_IN_SYNC
    state["last_reconcile_action"] = "manual_adopt_current"
    state["last_camera_status"] = _extract_status_subset(payload)
    state["last_apply_result"] = status_result
    state["last_error"] = ""
    saved = _save_mode_state_or_503(state, bump_revision=True)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.post("/api/camera-control/sync/reapply-canonical")
def camera_control_sync_reapply_canonical() -> JSONResponse:
    """Reapply canonical policy/settings for current active mode."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    if not bool(control.get("enabled", False)):
        raise HTTPException(status_code=503, detail="camera_control is disabled in runtime config.")

    state = _ensure_mode_state_shape(_load_mode_state_or_503())
    state = _apply_canonical_policy_for_mode(state, control=control, automation=automation)
    saved = _save_mode_state_or_503(state, bump_revision=False)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.post("/api/camera-control/sync/reconcile-now")
def camera_control_sync_reconcile_now() -> JSONResponse:
    """Refresh sync state from current camera status without forcing policy writes."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    automation = _resolve_camera_profile_automation_settings(config)
    state = _ensure_mode_state_shape(_load_mode_state_or_503())

    if not bool(control.get("enabled", False)):
        state["sync_status"] = CAMERA_SYNC_UNAVAILABLE
        state["last_reconcile_action"] = "reconcile_now_camera_control_disabled"
        state["last_apply_result"] = {"ok": False, "reason": "camera_control_disabled"}
        state["last_error"] = "camera_control_disabled"
        saved = _save_mode_state_or_503(state, bump_revision=False)
        return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))

    status_result = _camera_control_proxy_call(control, path="/status", require_auth=False)
    if not bool(status_result.get("ok", False)):
        state["sync_status"] = CAMERA_SYNC_UNAVAILABLE
        state["last_reconcile_action"] = "reconcile_now_camera_status_unavailable"
        state["last_apply_result"] = status_result
        state["last_error"] = "camera_status_unavailable"
        saved = _save_mode_state_or_503(state, bump_revision=False)
        return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))

    status_payload = dict(status_result.get("payload") or {})
    state["last_camera_status"] = _extract_status_subset(status_payload)
    state["last_apply_result"] = status_result
    active_mode = str(state.get("active_mode", CAMERA_MODE_BASE))
    if active_mode in MANUAL_COMPATIBLE_MODES:
        state = _evaluate_manual_mode_sync(state, status_payload, context="reconcile_now")
    elif active_mode == CAMERA_MODE_AUTO_PROFILE:
        state = _evaluate_auto_profile_sync(
            state,
            status_payload,
            automation,
            context="reconcile_now",
        )
    elif active_mode == CAMERA_MODE_ADAPTIVE:
        state = _evaluate_adaptive_mode_sync(state, status_payload, context="reconcile_now")
    else:
        state["sync_status"] = CAMERA_SYNC_DRIFT
        state["last_reconcile_action"] = "reconcile_now_unknown_mode"
        state["last_error"] = "unknown_mode_noop"

    saved = _save_mode_state_or_503(state, bump_revision=False)
    return JSONResponse(_mode_state_payload(saved, control=control, automation=automation))


@app.get("/api/system/utilization")
def system_utilization() -> JSONResponse:
    """Return host CPU/RAM/drive usage plus storage breakdown for System tab visuals."""
    return JSONResponse(_system_utilization_payload())


@app.get("/api/camera-control/health")
def camera_control_health() -> JSONResponse:
    """Proxy protected camera /health endpoint through backend token config."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    result = _camera_control_proxy_call(control, path="/health", require_auth=True)
    return JSONResponse(result, status_code=200 if result.get("ok") else 502)


@app.post("/api/camera-control/profile")
def camera_control_profile(name: str = Query(..., min_length=1, max_length=16)) -> JSONResponse:
    """Apply one named camera profile via protected /profile endpoint."""
    selected = str(name).strip().lower()
    if selected not in {"base", "day", "night"}:
        raise HTTPException(status_code=400, detail="Profile name must be one of: base, day, night.")
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    result = _camera_control_proxy_call(
        control,
        path="/profile",
        query={"name": selected},
        require_auth=True,
    )
    return JSONResponse(result, status_code=200 if result.get("ok") else 502)


@app.post("/api/camera-control/reboot")
def camera_control_reboot(delay_ms: int = Query(default=250, ge=100, le=10000)) -> JSONResponse:
    """Request camera reboot using protected /reboot endpoint."""
    config = _load_config()
    control = _resolve_camera_control_settings(config)
    result = _camera_control_proxy_call(
        control,
        path="/reboot",
        query={"delay_ms": int(delay_ms)},
        require_auth=True,
    )
    return JSONResponse(result, status_code=200 if result.get("ok") else 502)


@app.get("/api/recordings")
def recordings(
    limit: int = Query(default=24, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    q: str = Query(default=""),
    range: str = Query(default="all"),
    sort: str = Query(default="newest"),
) -> JSONResponse:
    """Return filterable, paginated recording list for dashboard playback."""
    payload = _recordings_query_payload(
        limit=limit,
        offset=offset,
        q=q,
        range_key=range,
        sort_key=sort,
    )
    return JSONResponse(payload)


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
