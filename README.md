# Remote Doorbell Camera System (Local-First Capstone)

Local-first doorbell camera platform for a capstone project.
The stack is hosted on a Windows laptop and provides live view, event detection, and local logging/metadata.

## What This Is

- FastAPI + Uvicorn backend serving the dashboard and APIs
- Live stream route (`/camera/live`) with backend-managed ingest
- YOLO-based person detection with snapshot/event creation
- SQLite-backed event metadata
- Local media and log storage
- Tailscale-ready remote access path
- OpenWrt firewall hardening scope (no VLAN buildout in this phase)

## Current Phase

- Live stream through backend is working
- Detector is active and writing events to SQLite
- Dashboard has tabs for Live Feed, Events, Logs, Server Logs, and System
- Recorder pipeline is still transitional
- RTSP is currently under evaluation (not finalized as required source path)

## What’s In This Repo

- `app/backend/`: FastAPI API, ingest service, SQLite/event routes
- `app/frontend/`: dashboard UI
- `scripts/windows/`: startup/setup scripts
- `scripts/python/`: detection/recording/support scripts
- `configs/app/`: app settings templates
- `configs/openwrt/`: firewall/config templates

## Quick Start (Windows)

```powershell
.\scripts\windows\setup_project.ps1
Copy-Item .\configs\app\settings.example.json .\configs\app\settings.local.json
.\scripts\windows\start_backend.ps1 -WithDetector
```

Open: [http://localhost:8080](http://localhost:8080)
