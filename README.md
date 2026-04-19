# Remote Doorbell Camera System (Local-First Capstone)

This repository contains the implementation baseline for a local-first remote doorbell camera system.
The project is built around the following decisions:

- Windows-based workflow and documentation
- OpenWrt router as the network/security control point
- VLAN segmentation and firewall isolation
- Mini PC server for local recording, processing, and logs
- Tailscale for secure remote access
- No vendor-cloud-first redesign

## Current Implementation Stage

The current build includes:

- starter OpenWrt config templates under `configs/openwrt/`
- app and script configuration templates under `configs/app/`
- a FastAPI backend + dashboard with backend-served live stream route (`/camera/live`)
- Windows scripts for setup and service startup
- Python scripts for RTSP ingest, YOLO-based event detection, and alerts

Current phase summary:

- Live streaming through backend is operational.
- Detector uses backend-provided frames and persists events to SQLite.
- Event/log visibility is available through the dashboard.
- Background job queue baseline is active for async retention/integrity/policy/alert tasks.
- Recorder integration is still transitional and is the main remaining pipeline gap.

## What Hosts the Website

For local development, the website is hosted by:

- `uvicorn` (ASGI server)
- `FastAPI` app in `app/backend/main.py`
- static frontend file `app/frontend/index.html`

Apache/IIS is not required for the current phase.

## Repository Layout

```text
app/
  backend/
  frontend/
configs/
  app/
  openwrt/
  tailscale/
data/
  events/
  logs/
  models/
  recordings/
  snapshots/
scripts/
  python/
  windows/
```

## Quick Start (Windows, local run)

1. Run setup:

```powershell
.\scripts\windows\setup_project.ps1
```

If script execution is blocked by policy, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\setup_project.ps1
```

2. Copy and edit config:

```powershell
Copy-Item .\configs\app\settings.example.json .\configs\app\settings.local.json
notepad .\configs\app\settings.local.json
```

Local runtime safety note:

- `configs/app/settings.local.json` is intentionally ignored by Git and should contain your real camera/private runtime values.
- If you switch checkouts/worktrees or clone elsewhere, recreate/copy local runtime files manually because ignored files are not transferred by Git.
- Runtime services now require `settings.local.json` and will fail fast if it is missing (no automatic `.example` fallback at runtime).
- Keep secret values (SMTP credentials, session secret) in `settings.local.json` only; leave placeholders in `settings.example.json`.

3. Start webserver first:

```powershell
.\scripts\windows\start_backend.ps1
```

Start backend + detector together:

```powershell
.\scripts\windows\start_backend.ps1 -WithDetector
```

Quick backend-only launcher:

```powershell
.\scripts\windows\run_v1_stack.ps1
```

4. Open dashboard:

- http://localhost:8080

Note:

- `start_detection.ps1` now uses ingest-managed backend frames by default (`/api/live-frame`).
- detector uses motion-gated, rate-limited inference controls from `detection` config to reduce CPU load.
- detector writes runtime logs to `data/logs/detector.log` (visible in dashboard Logs tab).
- backend startup writes service logs to `data/logs/backend.log` (visible in dashboard Logs tab via left/right log carousel).
- backend startup also launches the in-process background job runner (`jobs` config block).
- queue visibility endpoints are available:
  - `GET /api/jobs/stats`
  - `GET /api/jobs`
- alert dispatch queue endpoint is available:
  - `POST /api/jobs/dispatch-alert`
  - supports `include_snapshot`, `include_clip`, and `selected_clip_path`
- `uvicorn.error` in startup output is logger naming from uvicorn, not automatically a backend fault.
- detector includes a periodic fallback inference (`motion_force_inference_interval_seconds`) so it still runs occasional checks when motion gating is too strict.
- detector alerts can enqueue async dispatch jobs when `alerts.enqueue_background=true`.
- dispatch channels now support webhook and/or SMTP:
  - set `alerts.enabled=true`
  - use `alerts.webhook_enabled` and `alerts.smtp.enabled` to choose channel(s)
  - SMTP media attachment controls are under `alerts.smtp` (`include_snapshot`, `include_clip_default`, `max_attachment_mb`)
  - dashboard quick links in alert body are configured under `alerts.access_links` (`local_url`, `tailscale_url`, auto-detect toggles)
- Recording script is still transitional and may still use direct camera input.

## Current Work In Progress

- Recorder path migration to fully ingest-managed flow
- Additional event lifecycle features (review/share/delete states)
- Mobile stream stability tuning under varying network conditions
- Dashboard expansion for operational visibility (events/logging/system status)

## Project Direction

The focus of this capstone is a reliable, reproducible local security-camera stack:
- local-first ingestion and processing
- practical remote access through Tailscale
- maintainable service startup and troubleshooting workflow
- progressive feature completion toward a full production-style capstone demo
