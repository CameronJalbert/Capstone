from __future__ import annotations

import json
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


def _connect(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection with practical defaults for local service usage."""
    connection = sqlite3.connect(str(db_path), timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def _ensure_optional_columns(connection: sqlite3.Connection) -> None:
    """Ensure newer schema columns exist even when DB was created in older sessions."""
    cursor = connection.cursor()
    cursor.execute("PRAGMA table_info(event_records);")
    columns = {str(row["name"]) for row in cursor.fetchall()}
    if "class_id" not in columns:
        cursor.execute("ALTER TABLE event_records ADD COLUMN class_id INTEGER;")


def initialize_sqlite_schema(db_path: Path) -> None:
    """Create and evolve SQLite schema used for event-state workflows."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS event_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                label TEXT NOT NULL,
                confidence REAL,
                class_id INTEGER,
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
        _ensure_optional_columns(connection)
        connection.commit()
    finally:
        connection.close()


def insert_event_record(db_path: Path, event: dict[str, Any], event_type: str = "detection") -> int:
    """Insert one event record and return created row id."""
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO event_records (
                ts_utc,
                event_type,
                label,
                confidence,
                class_id,
                snapshot_path,
                clip_path,
                review_state,
                share_state,
                deletion_state,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                str(event.get("ts_utc", "")),
                str(event_type),
                str(event.get("label", "unknown")),
                float(event["confidence"]) if event.get("confidence") is not None else None,
                int(event["class_id"]) if event.get("class_id") is not None else None,
                str(event.get("snapshot", "")) or None,
                str(event.get("clip", "")) or None,
                str(event.get("review_state", "unreviewed")),
                str(event.get("share_state", "not_shared")),
                str(event.get("deletion_state", "present")),
                str(event.get("notes", "")) or None,
            ),
        )
        connection.commit()
        return int(cursor.lastrowid)
    finally:
        connection.close()


def fetch_recent_events(db_path: Path, limit: int) -> list[dict[str, Any]]:
    """Return recent events in frontend-compatible shape."""
    limit = max(1, int(limit))
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT
                id,
                ts_utc,
                event_type,
                label,
                confidence,
                class_id,
                snapshot_path,
                clip_path,
                review_state,
                share_state,
                deletion_state,
                notes
            FROM event_records
            ORDER BY id DESC
            LIMIT ?;
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            confidence = row["confidence"]
            events.append(
                {
                    "id": int(row["id"]),
                    "ts_utc": row["ts_utc"],
                    "event_type": row["event_type"],
                    "label": row["label"],
                    "confidence": float(confidence) if confidence is not None else 0.0,
                    "class_id": row["class_id"],
                    "snapshot": row["snapshot_path"],
                    "clip": row["clip_path"],
                    "review_state": row["review_state"],
                    "share_state": row["share_state"],
                    "deletion_state": row["deletion_state"],
                    "notes": row["notes"],
                }
            )
        return events
    finally:
        connection.close()


def import_ndjson_events_if_sqlite_empty(db_path: Path, events_file: Path) -> int:
    """Backfill NDJSON events into SQLite only when SQLite has no existing rows."""
    if not events_file.exists():
        return 0

    connection = _connect(db_path)
    imported_count = 0
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM event_records;")
        existing_count = int(cursor.fetchone()["c"])
        if existing_count > 0:
            return 0

        with events_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue

                cursor.execute(
                    """
                    INSERT INTO event_records (
                        ts_utc,
                        event_type,
                        label,
                        confidence,
                        class_id,
                        snapshot_path,
                        clip_path,
                        review_state,
                        share_state,
                        deletion_state,
                        notes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        str(payload.get("ts_utc", "")),
                        "detection",
                        str(payload.get("label", "unknown")),
                        float(payload["confidence"]) if payload.get("confidence") is not None else None,
                        int(payload["class_id"]) if payload.get("class_id") is not None else None,
                        str(payload.get("snapshot", "")) or None,
                        str(payload.get("clip", "")) or None,
                        str(payload.get("review_state", "unreviewed")),
                        str(payload.get("share_state", "not_shared")),
                        str(payload.get("deletion_state", "present")),
                        str(payload.get("notes", "")) or None,
                    ),
                )
                imported_count += 1
        connection.commit()
        return imported_count
    finally:
        connection.close()


def sqlite_status(db_path: Path) -> dict[str, Any]:
    """Return basic SQLite diagnostics without exposing sensitive runtime data."""
    if not db_path.exists():
        return {"db_exists": False, "event_records": 0}

    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM event_records;")
        count = int(cursor.fetchone()[0])
        return {"db_exists": True, "event_records": count}
    finally:
        connection.close()
