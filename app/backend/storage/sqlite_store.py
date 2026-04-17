from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def resolve_sqlite_path(settings: dict[str, Any], root: Path) -> Path:
    """Resolve SQLite path from settings with a safe default."""
    storage_cfg = settings.get("storage", {})
    configured = str(storage_cfg.get("sqlite_path", "data/events/events.db"))
    path = Path(configured)
    if not path.is_absolute():
        path = root / path
    return path


def initialize_sqlite_schema(db_path: Path) -> None:
    """Create minimal SQLite schema required for future event-state workflows."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS event_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                label TEXT NOT NULL,
                confidence REAL,
                snapshot_path TEXT,
                clip_path TEXT,
                review_state TEXT NOT NULL DEFAULT 'unreviewed',
                share_state TEXT NOT NULL DEFAULT 'not_shared',
                deletion_state TEXT NOT NULL DEFAULT 'present',
                notes TEXT
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_records_ts
            ON event_records (ts_utc DESC);
            """
        )
        connection.commit()
    finally:
        connection.close()


def sqlite_status(db_path: Path) -> dict[str, Any]:
    """Return basic SQLite diagnostics without exposing sensitive runtime data."""
    if not db_path.exists():
        return {"db_exists": False, "event_records": 0}

    connection = sqlite3.connect(str(db_path))
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM event_records;")
        count = int(cursor.fetchone()[0])
        return {"db_exists": True, "event_records": count}
    finally:
        connection.close()

