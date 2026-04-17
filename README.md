# Remote Doorbell Camera System (Local-First Capstone)

This repository is the implementation baseline for a local-first remote doorbell camera system.
It follows the fixed project decisions:

- Windows-based workflow and documentation
- OpenWrt router as the network/security control point
- VLAN segmentation and firewall isolation
- Mini PC server for local recording, processing, and logs
- Tailscale for secure remote access
- No vendor-cloud-first redesign

## Current Implementation Stage

The repository was bootstrapped from authoritative handoff context and now contains:

- an active continuity and architecture flow under `Handoff/`
- starter OpenWrt config templates under `configs/openwrt/`
- app and script configuration templates under `configs/app/`
- a FastAPI backend + dashboard with backend-served live stream route (`/camera/live`)
- Windows scripts for setup and service startup
- Python scripts for RTSP ingest, YOLO-based event detection, and alerts

Current known follow-up:

- Detector/event pipeline migration is in progress: detector now defaults to backend ingest frames, events API now reads SQLite metadata, and recorder path remains transitional.

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
Handoff/
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

Local-only safety note:

- `configs/app/settings.local.json` is intentionally ignored by Git and should contain your real camera/private runtime values.
- `Handoff/` is intentionally ignored by Git for agent continuity context and prompts.
- If you switch checkouts/worktrees or clone elsewhere, recreate/copy these local-only files manually because ignored files are not transferred by Git.

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
- backend startup now writes service logs to `data/logs/backend.log` (also visible in Logs tab).
- dashboard now has a dedicated `Server Logs` tab for `backend.log`.
- `uvicorn.error` in startup output is logger naming from uvicorn, not automatically a backend fault.
- detector includes a periodic fallback inference (`motion_force_inference_interval_seconds`) so it still runs occasional checks when motion gating is too strict.
- Recording script is still transitional and may still use direct camera input.

## Documentation Index

These continuity docs are local-only and intentionally excluded from Git via `.gitignore`:

- [State Reconstruction](Handoff/STATE_RECONSTRUCTION.md)
- [Perma Agent Rules](Handoff/Permainfo/PERMA_AGENT_RULES.md)
- [Current Architecture Assessment](Handoff/CURRENT_ARCHITECTURE_ASSESSMENT.md)
- [Roadmap for Agents](Handoff/ROADMAP_NEXT_AGENTS.md)
- [Service Startup Runbook](Handoff/Setup/SERVICE_STARTUP_RUNBOOK.md)
- [Host Profile](Handoff/Permainfo/HOST_PROFILE.md)

Historical context and prior draft docs remain available under:

- `Handoff/old/`

Continuity note:

- If runtime camera details are missing or stale in current context files, confirm them with the user before implementing assumptions.
