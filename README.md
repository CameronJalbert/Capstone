# LAN Cam

LAN Cam is a local-network camera control and monitoring platform.

## Stack

- Backend: FastAPI + Uvicorn (Python)
- Frontend: single-page HTML/CSS/JavaScript dashboard
- Storage: SQLite + local filesystem media/logs
- Vision: YOLO-based detection pipeline
- Firmware: ESP32-CAM firmware tracked in `ESPConf/`

## Recent Repository Changes

- Added dedicated `Camera` tab with mode engine actions and camera-control workflows.
- Added strict sync-drift behavior and immediate reconciliation support (`In Sync`, `D-Sync`, `Reapplying`, `Camera Unavailable`).
- Added readable camera/system panels with raw JSON toggles instead of JSON-only views.
- Added Events and Recordings pagination + filters (default page size `24`).
- Added System utilization cards/bars (CPU, RAM, storage with category breakdown visuals).
- Added backend shutdown/reboot controls in System with confirmation + UI countdown state.
- Updated interactive UI behavior (hover/press feedback and action-focused output scrolling).
- Added `ESPConf/CurrentProd/CurrentProdESP` safe firmware mirror from current production source with sanitized Wi-Fi/token values.
