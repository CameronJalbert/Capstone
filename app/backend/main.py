from __future__ import annotations

import json
import logging
import mimetypes
import socket
import smtplib
import subprocess
import threading
import time
from collections import deque
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
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
LOGGER = logging.getLogger("capstone.backend")
BACKEND_START_MONOTONIC = time.monotonic()

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


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Stop ingest service and release stream resources."""
    global JOB_RUNNER
    if JOB_RUNNER is not None:
        JOB_RUNNER.stop()
        JOB_RUNNER = None
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
def get_events(limit: int = Query(default=30, ge=1, le=200)) -> JSONResponse:
    """Return recent events from SQLite metadata storage."""
    return JSONResponse({"events": _load_events(limit=limit)})


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
