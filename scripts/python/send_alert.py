from __future__ import annotations

"""Send optional webhook/SMTP alerts for detected doorbell events."""

import json
import mimetypes
import socket
import smtplib
import subprocess
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]


def _normalize_base_url(value: Any) -> str:
    """Normalize dashboard URL text to include scheme and trim trailing slash."""
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        return text.rstrip("/")
    return f"http://{text}".rstrip("/")


def _discover_local_ipv4() -> str | None:
    """Best-effort local IPv4 discovery used when alert link config is blank."""
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
    """Try to read one Tailscale IPv4 address from local CLI."""
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


def _resolve_alert_access_links(alerts_cfg: dict[str, Any]) -> dict[str, str]:
    """Resolve local and Tailscale dashboard links for SMTP body context."""
    access_cfg = dict(alerts_cfg.get("access_links") or {})
    dashboard_port = int(access_cfg.get("dashboard_port", 8080))

    local_url = _normalize_base_url(access_cfg.get("local_url"))
    tailscale_url = _normalize_base_url(access_cfg.get("tailscale_url"))

    if not local_url and bool(access_cfg.get("auto_detect_local", True)):
        local_ip = _discover_local_ipv4()
        if local_ip:
            local_url = f"http://{local_ip}:{dashboard_port}"
    if not tailscale_url and bool(access_cfg.get("auto_detect_tailscale", True)):
        tailscale_ip = _discover_tailscale_ipv4()
        if tailscale_ip:
            tailscale_url = f"http://{tailscale_ip}:{dashboard_port}"

    return {
        "local_url": local_url,
        "tailscale_url": tailscale_url,
    }


def _resolve_media_ref_path(ref: Any) -> Path | None:
    text = str(ref or "").strip()
    if not text:
        return None
    p = Path(text)
    return p if p.is_absolute() else ROOT / p


def _attach_media_if_present(
    message: EmailMessage,
    *,
    ref: Any,
    label: str,
    max_attachment_bytes: int,
) -> None:
    media_path = _resolve_media_ref_path(ref)
    if media_path is None:
        return
    if not media_path.exists() or not media_path.is_file():
        return
    size_bytes = int(media_path.stat().st_size)
    if max_attachment_bytes > 0 and size_bytes > max_attachment_bytes:
        return
    payload = media_path.read_bytes()
    guessed, _ = mimetypes.guess_type(media_path.name)
    if guessed and "/" in guessed:
        maintype, subtype = guessed.split("/", 1)
    else:
        maintype, subtype = "application", "octet-stream"
    message.add_attachment(payload, maintype=maintype, subtype=subtype, filename=media_path.name)


def _send_webhook_alert(event: dict[str, Any], alerts_cfg: dict[str, Any]) -> bool:
    """Post one event payload to the configured webhook channel when enabled."""
    if not alerts_cfg.get("enabled", False):
        return False
    if not bool(alerts_cfg.get("webhook_enabled", True)):
        return False

    webhook_url = str(alerts_cfg.get("webhook_url", "")).strip()
    if not webhook_url:
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
        print(f"Webhook alert send failed: {exc}")
        return False


def _send_smtp_alert(event: dict[str, Any], alerts_cfg: dict[str, Any]) -> bool:
    """Send one SMTP alert email when SMTP channel is enabled."""
    if not alerts_cfg.get("enabled", False):
        return False
    smtp_cfg = dict(alerts_cfg.get("smtp") or {})
    if not bool(smtp_cfg.get("enabled", False)):
        return False

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

    if not host or not username or not password or not from_email or not to_emails:
        print("SMTP alert skipped due to missing required SMTP fields.")
        return False

    severity_level = str(event.get("severity_level", "unknown")).upper()
    label = str(event.get("label", "unknown")).strip() or "unknown"
    subject = f"{subject_prefix}: {label} [{severity_level}]"
    message = EmailMessage()
    message["From"] = from_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject
    text_lines = [
        "Doorbell event detected.",
        "",
        f"event_id: {event.get('id')}",
        f"timestamp_utc: {event.get('ts_utc')}",
        f"label: {event.get('label')}",
        f"confidence: {event.get('confidence')}",
        f"severity_level: {event.get('severity_level')}",
        f"snapshot_ref: {event.get('snapshot')}",
        f"clip_ref: {event.get('clip')}",
    ]
    links = _resolve_alert_access_links(alerts_cfg)
    local_url = str(links.get("local_url", "")).strip()
    tailscale_url = str(links.get("tailscale_url", "")).strip()
    if local_url or tailscale_url:
        text_lines.extend(["", "dashboard_access:"])
        if local_url:
            text_lines.append(f"local_dashboard_url: {local_url}")
        if tailscale_url:
            text_lines.append(f"tailscale_dashboard_url: {tailscale_url}")

    message.set_content("\n".join(text_lines))

    if bool(smtp_cfg.get("include_snapshot", True)):
        _attach_media_if_present(
            message,
            ref=event.get("snapshot"),
            label="snapshot",
            max_attachment_bytes=max_attachment_bytes,
        )
    if bool(smtp_cfg.get("include_clip_default", False)):
        _attach_media_if_present(
            message,
            ref=event.get("clip"),
            label="clip",
            max_attachment_bytes=max_attachment_bytes,
        )

    try:
        with smtplib.SMTP(host=host, port=port, timeout=timeout_seconds) as client:
            client.ehlo()
            if use_starttls:
                client.starttls()
                client.ehlo()
            client.login(username, password)
            client.send_message(message)
        return True
    except (smtplib.SMTPException, OSError, TimeoutError) as exc:
        print(f"SMTP alert send failed: {exc}")
        return False


def send_alert(event: dict[str, Any], alerts_cfg: dict[str, Any]) -> bool:
    """Dispatch enabled alert channels inline and return true when any channel succeeds."""
    webhook_sent = _send_webhook_alert(event, alerts_cfg)
    smtp_sent = _send_smtp_alert(event, alerts_cfg)
    return bool(webhook_sent or smtp_sent)
