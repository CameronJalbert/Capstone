
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

MigrationFunc = Callable[[sqlite3.Connection], None]

LIFECYCLE_STATES: tuple[str, ...] = (
    "detected",
    "media_attached",
    "alerted",
    "saved",
    "expired",
    "deleted",
)
LIFECYCLE_STATE_INDEX = {state: idx for idx, state in enumerate(LIFECYCLE_STATES)}
DEFAULT_LIFECYCLE_STATE = "detected"

DEFAULT_SEVERITY_POLICY_VERSION = "v1"
DEFAULT_RETENTION_POLICY_VERSION = "v2_confidence_multiplier"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".m4v", ".webm"}
JOB_STATUSES: tuple[str, ...] = (
    "queued",
    "running",
    "retry_scheduled",
    "succeeded",
    "failed",
    "cancelled",
)
CAMERA_MODES: tuple[str, ...] = (
    "base",
    "manual_saved",
    "manual_temporary",
    "adaptive_capture",
    "auto_day_night_profile",
)
CAMERA_SYNC_STATUSES: tuple[str, ...] = (
    "in_sync",
    "drift_detected",
    "reapplying",
    "camera_unavailable",
)
DEFAULT_CAMERA_MODE = "base"
DEFAULT_CAMERA_SYNC_STATUS = "camera_unavailable"


def resolve_sqlite_path(settings: dict[str, Any], root: Path) -> Path:
    storage_cfg = settings.get("storage", {})
    configured = str(storage_cfg.get("sqlite_path", "data/events/events.db"))
    path = Path(configured)
    return path if path.is_absolute() else root / path


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path), timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalized_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _normalized_confidence(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, f))


def _clamp_optional_01(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, f))


def _parse_utc(ts_utc: Any) -> datetime | None:
    text = _normalized_text(ts_utc)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_event_uid(existing: Any) -> str:
    value = _normalized_text(existing)
    return value if value else f"evt_{uuid.uuid4().hex[:16]}"


def normalize_lifecycle_state(state: Any) -> str:
    normalized = _normalized_text(state).lower()
    return normalized if normalized in LIFECYCLE_STATE_INDEX else DEFAULT_LIFECYCLE_STATE


def can_transition_lifecycle_state(current_state: Any, next_state: Any) -> bool:
    current = normalize_lifecycle_state(current_state)
    target = normalize_lifecycle_state(next_state)
    return LIFECYCLE_STATE_INDEX[target] >= LIFECYCLE_STATE_INDEX[current]


def infer_lifecycle_state(
    lifecycle_state: Any,
    *,
    review_state: Any,
    share_state: Any,
    deletion_state: Any,
    clip_path: Any,
) -> str:
    explicit = _normalized_text(lifecycle_state).lower()
    if explicit in LIFECYCLE_STATE_INDEX:
        return explicit
    normalized_deletion = _normalized_text(deletion_state).lower()
    if normalized_deletion in {"deleted", "removed"}:
        return "deleted"
    if normalized_deletion in {"expired", "culled"}:
        return "expired"
    normalized_share = _normalized_text(share_state).lower()
    if normalized_share in {"shared", "emailed", "sent", "alerted"}:
        return "alerted"
    normalized_review = _normalized_text(review_state).lower()
    if normalized_review in {"saved", "keep", "preserved"}:
        return "saved"
    return "media_attached" if _normalized_text(clip_path) else DEFAULT_LIFECYCLE_STATE


def compute_retention_days(confidence: Any, lifecycle_state: Any) -> int:
    lifecycle = normalize_lifecycle_state(lifecycle_state)
    if lifecycle in {"deleted", "expired"}:
        return 0
    if lifecycle == "saved":
        # Saved items are retention-exempt until manually deleted.
        return 365_000
    conf = _normalized_confidence(confidence)
    days = 7.0 * conf
    return max(1, int(round(days)))


def compute_delete_after_ts(ts_utc: Any, retention_days: int, now_utc: str | None = None) -> str:
    base_dt = _parse_utc(ts_utc) or _parse_utc(now_utc) or datetime.now(timezone.utc)
    return _format_utc(base_dt + timedelta(days=max(0, int(retention_days))))


def compute_retention_basis(confidence: Any, lifecycle_state: Any, retention_days: int) -> str:
    return (
        f"{DEFAULT_RETENTION_POLICY_VERSION}:"
        f"confidence={_normalized_confidence(confidence):.4f};"
        f"lifecycle={normalize_lifecycle_state(lifecycle_state)};"
        f"retention_days={int(retention_days)}"
    )


def compute_severity_score_and_level(
    *,
    confidence: Any,
    lifecycle_state: Any,
    label: Any,
    signal_proximity_score: Any,
    signal_facing_score: Any,
) -> tuple[float, str, str]:
    conf = _normalized_confidence(confidence)
    proximity = _clamp_optional_01(signal_proximity_score)
    facing = _clamp_optional_01(signal_facing_score)
    weighted = [(conf, 0.75)]
    if proximity is not None:
        weighted.append((proximity, 0.15))
    if facing is not None:
        weighted.append((facing, 0.10))
    total = sum(weight for _, weight in weighted)
    score = sum(value * weight for value, weight in weighted) / total if total > 0 else 0.0
    if _normalized_text(label).lower() == "person":
        score += 0.05
    lifecycle = normalize_lifecycle_state(lifecycle_state)
    if lifecycle == "alerted":
        score += 0.05
    elif lifecycle == "saved":
        score += 0.08
    score = round(max(0.0, min(1.0, score)), 4)
    if score >= 0.85:
        level = "critical"
    elif score >= 0.65:
        level = "high"
    elif score >= 0.40:
        level = "medium"
    else:
        level = "low"
    return score, level, DEFAULT_SEVERITY_POLICY_VERSION

def _resolve_media_path(root: Path, media_path: Any) -> Path | None:
    text = _normalized_text(media_path)
    if not text:
        return None
    p = Path(text)
    return p if p.is_absolute() else root / p


def _normalize_media_ref(media_path: Any) -> str | None:
    text = _normalized_text(media_path)
    return text or None


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(connection, table_name):
        return set()
    cursor = connection.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return {str(row["name"]) for row in cursor.fetchall()}


def _ensure_schema_migrations_table(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL
        );
        """
    )


def _applied_migration_versions(connection: sqlite3.Connection) -> set[int]:
    if not _table_exists(connection, "schema_migrations"):
        return set()
    cursor = connection.cursor()
    cursor.execute("SELECT version FROM schema_migrations;")
    return {int(row["version"]) for row in cursor.fetchall()}


def _record_migration(connection: sqlite3.Connection, version: int, name: str) -> None:
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO schema_migrations (version, name, applied_at_utc) VALUES (?, ?, ?);",
        (int(version), str(name), _utc_now()),
    )


def _migration_001_create_event_records_base(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS event_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT 'detection',
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


def _migration_002_add_class_id_column(connection: sqlite3.Connection) -> None:
    if "class_id" in _table_columns(connection, "event_records"):
        return
    connection.cursor().execute("ALTER TABLE event_records ADD COLUMN class_id INTEGER;")


def _migration_003_add_event_indexes(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_ts ON event_records (ts_utc DESC);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_deletion_state ON event_records (deletion_state);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_type_ts ON event_records (event_type, ts_utc DESC);")


def _migration_004_add_lifecycle_columns(connection: sqlite3.Connection) -> None:
    columns = _table_columns(connection, "event_records")
    cursor = connection.cursor()
    if "lifecycle_state" not in columns:
        cursor.execute("ALTER TABLE event_records ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT 'detected';")
    if "lifecycle_updated_ts_utc" not in columns:
        cursor.execute("ALTER TABLE event_records ADD COLUMN lifecycle_updated_ts_utc TEXT;")
    now_utc = _utc_now()
    cursor.execute(
        """
        UPDATE event_records
        SET
            lifecycle_state = CASE
                WHEN LOWER(COALESCE(deletion_state, '')) IN ('deleted', 'removed') THEN 'deleted'
                WHEN LOWER(COALESCE(deletion_state, '')) IN ('expired', 'culled') THEN 'expired'
                WHEN LOWER(COALESCE(share_state, '')) IN ('shared', 'emailed', 'sent', 'alerted') THEN 'alerted'
                WHEN LOWER(COALESCE(review_state, '')) IN ('saved', 'keep', 'preserved') THEN 'saved'
                WHEN TRIM(COALESCE(clip_path, '')) <> '' THEN 'media_attached'
                ELSE 'detected'
            END,
            lifecycle_updated_ts_utc = CASE
                WHEN lifecycle_updated_ts_utc IS NULL OR TRIM(lifecycle_updated_ts_utc) = ''
                    THEN COALESCE(NULLIF(TRIM(ts_utc), ''), ?)
                ELSE lifecycle_updated_ts_utc
            END;
        """,
        (now_utc,),
    )


def _migration_005_add_lifecycle_indexes(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_lifecycle_state_ts ON event_records (lifecycle_state, ts_utc DESC);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_lifecycle_updated ON event_records (lifecycle_updated_ts_utc DESC);")


def _migration_006_add_canonical_and_policy_columns(connection: sqlite3.Connection) -> None:
    columns = _table_columns(connection, "event_records")
    cursor = connection.cursor()
    add_columns: list[tuple[str, str]] = [
        ("event_uid", "TEXT"),
        ("signal_proximity_score", "REAL"),
        ("signal_facing_score", "REAL"),
        ("severity_score", "REAL"),
        ("severity_level", "TEXT"),
        ("severity_policy_version", "TEXT"),
        ("severity_updated_ts_utc", "TEXT"),
        ("retention_days", "INTEGER"),
        ("delete_after_ts_utc", "TEXT"),
        ("retention_basis", "TEXT"),
        ("retention_updated_ts_utc", "TEXT"),
    ]
    for column_name, column_type in add_columns:
        if column_name not in columns:
            cursor.execute(f"ALTER TABLE event_records ADD COLUMN {column_name} {column_type};")
    cursor.execute(
        "UPDATE event_records SET event_uid = printf('evt_%08d', id) WHERE event_uid IS NULL OR TRIM(event_uid) = '';"
    )


def _build_policy_fields_from_row(row: sqlite3.Row, now_utc: str | None = None) -> dict[str, Any]:
    resolved_now = now_utc or _utc_now()
    lifecycle_state = infer_lifecycle_state(
        row["lifecycle_state"],
        review_state=row["review_state"],
        share_state=row["share_state"],
        deletion_state=row["deletion_state"],
        clip_path=row["clip_path"],
    )
    retention_days = compute_retention_days(row["confidence"], lifecycle_state)
    delete_after_ts_utc = compute_delete_after_ts(row["ts_utc"], retention_days, now_utc=resolved_now)
    retention_basis = compute_retention_basis(row["confidence"], lifecycle_state, retention_days)
    severity_score, severity_level, severity_policy_version = compute_severity_score_and_level(
        confidence=row["confidence"],
        lifecycle_state=lifecycle_state,
        label=row["label"],
        signal_proximity_score=row["signal_proximity_score"],
        signal_facing_score=row["signal_facing_score"],
    )
    return {
        "event_uid": _make_event_uid(row["event_uid"]),
        "lifecycle_state": lifecycle_state,
        "lifecycle_updated_ts_utc": _normalized_text(row["lifecycle_updated_ts_utc"]) or _normalized_text(row["ts_utc"]) or resolved_now,
        "retention_days": int(retention_days),
        "delete_after_ts_utc": delete_after_ts_utc,
        "retention_basis": retention_basis,
        "retention_updated_ts_utc": resolved_now,
        "severity_score": float(severity_score),
        "severity_level": severity_level,
        "severity_policy_version": severity_policy_version,
        "severity_updated_ts_utc": resolved_now,
    }


def _backfill_policy_fields(connection: sqlite3.Connection, *, only_missing: bool, now_utc: str | None = None) -> int:
    resolved_now = now_utc or _utc_now()
    cursor = connection.cursor()
    where_clause = ""
    if only_missing:
        where_clause = (
            "WHERE event_uid IS NULL OR TRIM(event_uid) = '' OR "
            "retention_days IS NULL OR delete_after_ts_utc IS NULL OR "
            "severity_score IS NULL OR severity_level IS NULL"
        )
    cursor.execute(
        f"""
        SELECT id, ts_utc, label, confidence, snapshot_path, clip_path, review_state, share_state,
               deletion_state, lifecycle_state, lifecycle_updated_ts_utc, event_uid,
               signal_proximity_score, signal_facing_score
        FROM event_records
        {where_clause};
        """
    )
    rows = cursor.fetchall()
    updated_count = 0
    for row in rows:
        fields = _build_policy_fields_from_row(row, now_utc=resolved_now)
        cursor.execute(
            """
            UPDATE event_records
            SET event_uid=?, lifecycle_state=?, lifecycle_updated_ts_utc=?, retention_days=?,
                delete_after_ts_utc=?, retention_basis=?, retention_updated_ts_utc=?,
                severity_score=?, severity_level=?, severity_policy_version=?, severity_updated_ts_utc=?
            WHERE id=?;
            """,
            (
                fields["event_uid"],
                fields["lifecycle_state"],
                fields["lifecycle_updated_ts_utc"],
                fields["retention_days"],
                fields["delete_after_ts_utc"],
                fields["retention_basis"],
                fields["retention_updated_ts_utc"],
                fields["severity_score"],
                fields["severity_level"],
                fields["severity_policy_version"],
                fields["severity_updated_ts_utc"],
                int(row["id"]),
            ),
        )
        updated_count += 1
    return updated_count


def _migration_007_backfill_policy_and_add_indexes(connection: sqlite3.Connection) -> None:
    _backfill_policy_fields(connection, only_missing=False)
    cursor = connection.cursor()
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_event_records_event_uid_unique ON event_records (event_uid);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_delete_after ON event_records (delete_after_ts_utc);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_event_records_severity_level ON event_records (severity_level);")


def _migration_008_add_background_jobs_table(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS background_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            payload_json TEXT NOT NULL DEFAULT '{}',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            available_after_ts_utc TEXT NOT NULL,
            worker_name TEXT,
            created_ts_utc TEXT NOT NULL,
            started_ts_utc TEXT,
            finished_ts_utc TEXT,
            last_error TEXT,
            last_result_json TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_background_jobs_status_available
        ON background_jobs (status, available_after_ts_utc, id);
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_background_jobs_created_ts
        ON background_jobs (created_ts_utc DESC);
        """
    )
    now_utc = _utc_now()
    cursor.execute(
        """
        UPDATE background_jobs
        SET created_ts_utc = ?
        WHERE created_ts_utc IS NULL OR TRIM(created_ts_utc) = '';
        """,
        (now_utc,),
    )


def _default_camera_mode_canonical_state() -> dict[str, Any]:
    return {
        "modes": {
            "base": {"profile": "base", "settings": {}},
            "manual_saved": {"settings": {}},
            "manual_temporary": {"settings": {}},
            "adaptive_capture": {
                "settings": {
                    "xclk": 15,
                    "brightness": 0,
                    "contrast": 0,
                    "saturation": 0,
                    "led_intensity": 0,
                },
                "runtime": {
                    "low_light_active": False,
                    "last_xclk_change_ts_utc": "",
                    "last_xclk_attempt_ts_utc": "",
                    "last_led_change_ts_utc": "",
                    "last_tweak_ts_utc": "",
                },
            },
            "auto_day_night_profile": {
                "day_profile": "day",
                "night_profile": "night",
            },
        },
        "metadata": {
            "last_adopted_mode": "",
            "last_adopted_ts_utc": "",
        },
    }


def _migration_009_add_camera_mode_state_table(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS camera_mode_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            active_mode TEXT NOT NULL DEFAULT 'base',
            mode_revision INTEGER NOT NULL DEFAULT 1,
            sync_status TEXT NOT NULL DEFAULT 'camera_unavailable',
            last_reconcile_action TEXT NOT NULL DEFAULT 'initialized',
            canonical_state_json TEXT NOT NULL DEFAULT '{}',
            last_camera_status_json TEXT NOT NULL DEFAULT '{}',
            last_apply_result_json TEXT NOT NULL DEFAULT '{}',
            last_error TEXT,
            created_ts_utc TEXT NOT NULL,
            updated_ts_utc TEXT NOT NULL
        );
        """
    )
    now_utc = _utc_now()
    cursor.execute("SELECT id FROM camera_mode_state WHERE id = 1;")
    if cursor.fetchone() is None:
        cursor.execute(
            """
            INSERT INTO camera_mode_state (
                id, active_mode, mode_revision, sync_status, last_reconcile_action,
                canonical_state_json, last_camera_status_json, last_apply_result_json,
                last_error, created_ts_utc, updated_ts_utc
            )
            VALUES (1, 'base', 1, 'camera_unavailable', 'initialized', ?, '{}', '{}', NULL, ?, ?);
            """,
            (
                json.dumps(_default_camera_mode_canonical_state(), separators=(",", ":"), sort_keys=True),
                now_utc,
                now_utc,
            ),
        )


def _migration_010_add_saved_and_restart_tables(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            event_uid TEXT,
            title TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'event',
            snapshot_path TEXT,
            clip_path TEXT NOT NULL,
            notes TEXT,
            created_ts_utc TEXT NOT NULL,
            updated_ts_utc TEXT NOT NULL,
            deleted_ts_utc TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_saved_items_created
        ON saved_items (created_ts_utc DESC);
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_saved_items_event_id
        ON saved_items (event_id);
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS server_restart_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            classification TEXT NOT NULL,
            reason TEXT,
            trigger TEXT,
            action TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_server_restart_events_ts
        ON server_restart_events (ts_utc DESC);
        """
    )


MIGRATIONS: list[tuple[int, str, MigrationFunc]] = [
    (1, "create_event_records_base", _migration_001_create_event_records_base),
    (2, "add_class_id_column", _migration_002_add_class_id_column),
    (3, "add_event_indexes", _migration_003_add_event_indexes),
    (4, "add_lifecycle_columns", _migration_004_add_lifecycle_columns),
    (5, "add_lifecycle_indexes", _migration_005_add_lifecycle_indexes),
    (6, "add_canonical_and_policy_columns", _migration_006_add_canonical_and_policy_columns),
    (7, "backfill_policy_and_add_indexes", _migration_007_backfill_policy_and_add_indexes),
    (8, "add_background_jobs_table", _migration_008_add_background_jobs_table),
    (9, "add_camera_mode_state_table", _migration_009_add_camera_mode_state_table),
    (10, "add_saved_and_restart_tables", _migration_010_add_saved_and_restart_tables),
]
CURRENT_SCHEMA_VERSION = MIGRATIONS[-1][0]


def _apply_pending_migrations(connection: sqlite3.Connection) -> None:
    _ensure_schema_migrations_table(connection)
    applied = _applied_migration_versions(connection)
    for version, name, migration_fn in MIGRATIONS:
        if version not in applied:
            migration_fn(connection)
            _record_migration(connection, version, name)


def initialize_sqlite_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        _apply_pending_migrations(connection)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

def _prepare_event_for_insert(event: dict[str, Any], event_type: str, now_utc: str | None = None) -> dict[str, Any]:
    resolved_now = now_utc or _utc_now()
    ts_utc = _normalized_text(event.get("ts_utc")) or resolved_now
    review_state = _normalized_text(event.get("review_state", "unreviewed")) or "unreviewed"
    share_state = _normalized_text(event.get("share_state", "not_shared")) or "not_shared"
    deletion_state = _normalized_text(event.get("deletion_state", "present")) or "present"
    snapshot_path = _normalize_media_ref(event.get("snapshot"))
    clip_path = _normalize_media_ref(event.get("clip"))
    lifecycle_state = infer_lifecycle_state(
        event.get("lifecycle_state"),
        review_state=review_state,
        share_state=share_state,
        deletion_state=deletion_state,
        clip_path=clip_path,
    )
    lifecycle_updated_ts_utc = _normalized_text(event.get("lifecycle_updated_ts_utc")) or ts_utc or resolved_now
    signal_proximity_score = _clamp_optional_01(event.get("signal_proximity_score"))
    signal_facing_score = _clamp_optional_01(event.get("signal_facing_score"))
    retention_days = compute_retention_days(event.get("confidence"), lifecycle_state)
    delete_after_ts_utc = compute_delete_after_ts(ts_utc, retention_days, now_utc=resolved_now)
    retention_basis = compute_retention_basis(event.get("confidence"), lifecycle_state, retention_days)
    severity_score, severity_level, severity_policy_version = compute_severity_score_and_level(
        confidence=event.get("confidence"),
        lifecycle_state=lifecycle_state,
        label=event.get("label"),
        signal_proximity_score=signal_proximity_score,
        signal_facing_score=signal_facing_score,
    )
    return {
        "ts_utc": ts_utc,
        "event_type": _normalized_text(event_type) or "detection",
        "label": _normalized_text(event.get("label", "unknown")) or "unknown",
        "confidence": float(event["confidence"]) if event.get("confidence") is not None else None,
        "class_id": int(event["class_id"]) if event.get("class_id") is not None else None,
        "snapshot_path": snapshot_path,
        "clip_path": clip_path,
        "review_state": review_state,
        "share_state": share_state,
        "deletion_state": deletion_state,
        "lifecycle_state": lifecycle_state,
        "lifecycle_updated_ts_utc": lifecycle_updated_ts_utc,
        "event_uid": _make_event_uid(event.get("event_uid")),
        "signal_proximity_score": signal_proximity_score,
        "signal_facing_score": signal_facing_score,
        "severity_score": severity_score,
        "severity_level": severity_level,
        "severity_policy_version": severity_policy_version,
        "severity_updated_ts_utc": resolved_now,
        "retention_days": int(retention_days),
        "delete_after_ts_utc": delete_after_ts_utc,
        "retention_basis": retention_basis,
        "retention_updated_ts_utc": resolved_now,
        "notes": _normalize_media_ref(event.get("notes")),
    }


def _insert_prepared_event(cursor: sqlite3.Cursor, payload: dict[str, Any]) -> int:
    cursor.execute(
        """
        INSERT INTO event_records (
            ts_utc, event_type, label, confidence, class_id, snapshot_path, clip_path,
            review_state, share_state, deletion_state, lifecycle_state, lifecycle_updated_ts_utc,
            event_uid, signal_proximity_score, signal_facing_score,
            severity_score, severity_level, severity_policy_version, severity_updated_ts_utc,
            retention_days, delete_after_ts_utc, retention_basis, retention_updated_ts_utc, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            payload["ts_utc"],
            payload["event_type"],
            payload["label"],
            payload["confidence"],
            payload["class_id"],
            payload["snapshot_path"],
            payload["clip_path"],
            payload["review_state"],
            payload["share_state"],
            payload["deletion_state"],
            payload["lifecycle_state"],
            payload["lifecycle_updated_ts_utc"],
            payload["event_uid"],
            payload["signal_proximity_score"],
            payload["signal_facing_score"],
            payload["severity_score"],
            payload["severity_level"],
            payload["severity_policy_version"],
            payload["severity_updated_ts_utc"],
            payload["retention_days"],
            payload["delete_after_ts_utc"],
            payload["retention_basis"],
            payload["retention_updated_ts_utc"],
            payload["notes"],
        ),
    )
    return int(cursor.lastrowid)


def insert_event_record(db_path: Path, event: dict[str, Any], event_type: str = "detection") -> int:
    prepared = _prepare_event_for_insert(event, event_type=event_type)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        row_id = _insert_prepared_event(cursor, prepared)
        connection.commit()
        return row_id
    finally:
        connection.close()


def fetch_recent_events(db_path: Path, limit: int) -> list[dict[str, Any]]:
    limit = max(1, int(limit))
    connection = _connect(db_path)
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
            ORDER BY id DESC
            LIMIT ?;
            """,
            (limit,),
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
                    "has_snapshot": bool(_normalized_text(snapshot)),
                    "has_clip": bool(_normalized_text(clip)),
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


def fetch_event_by_id(db_path: Path, event_id: int) -> dict[str, Any] | None:
    """Return one event record by id in API-compatible shape."""
    event_id = int(event_id)
    connection = _connect(db_path)
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
            WHERE id = ?;
            """,
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        snapshot = row["snapshot_path"]
        clip = row["clip_path"]
        return {
            "id": int(row["id"]),
            "event_uid": row["event_uid"],
            "ts_utc": row["ts_utc"],
            "event_type": row["event_type"],
            "label": row["label"],
            "confidence": float(row["confidence"]) if row["confidence"] is not None else 0.0,
            "class_id": row["class_id"],
            "snapshot": snapshot,
            "clip": clip,
            "has_snapshot": bool(_normalized_text(snapshot)),
            "has_clip": bool(_normalized_text(clip)),
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
    finally:
        connection.close()


def enqueue_background_job(
    db_path: Path,
    job_type: str,
    payload: dict[str, Any] | None = None,
    *,
    max_attempts: int = 3,
    available_after_ts_utc: str | None = None,
) -> int:
    """Insert one background job request and return created job id."""
    payload_json = json.dumps(payload or {}, separators=(",", ":"), sort_keys=True)
    created = _utc_now()
    available = _normalized_text(available_after_ts_utc) or created
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO background_jobs (
                job_type,
                status,
                payload_json,
                attempt_count,
                max_attempts,
                available_after_ts_utc,
                created_ts_utc
            )
            VALUES (?, 'queued', ?, 0, ?, ?, ?);
            """,
            (
                _normalized_text(job_type) or "unknown_job",
                payload_json,
                max(1, int(max_attempts)),
                available,
                created,
            ),
        )
        connection.commit()
        return int(cursor.lastrowid)
    finally:
        connection.close()


def claim_next_background_job(db_path: Path, worker_name: str) -> dict[str, Any] | None:
    """Atomically claim the next available queued/retry job for one worker."""
    now_utc = _utc_now()
    worker = _normalized_text(worker_name) or "worker"
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("BEGIN IMMEDIATE;")
        cursor.execute(
            """
            SELECT id, job_type, status, payload_json, attempt_count, max_attempts, available_after_ts_utc, created_ts_utc
            FROM background_jobs
            WHERE status IN ('queued', 'retry_scheduled')
              AND available_after_ts_utc <= ?
            ORDER BY id ASC
            LIMIT 1;
            """,
            (now_utc,),
        )
        row = cursor.fetchone()
        if row is None:
            connection.commit()
            return None

        next_attempt = int(row["attempt_count"]) + 1
        cursor.execute(
            """
            UPDATE background_jobs
            SET
                status = 'running',
                worker_name = ?,
                attempt_count = ?,
                started_ts_utc = ?,
                last_error = NULL
            WHERE id = ?;
            """,
            (worker, next_attempt, now_utc, int(row["id"])),
        )
        connection.commit()

        payload: dict[str, Any]
        try:
            payload = json.loads(_normalized_text(row["payload_json"]) or "{}")
        except json.JSONDecodeError:
            payload = {}
        return {
            "id": int(row["id"]),
            "job_type": row["job_type"],
            "status": "running",
            "payload": payload,
            "attempt_count": next_attempt,
            "max_attempts": int(row["max_attempts"]),
            "available_after_ts_utc": row["available_after_ts_utc"],
            "created_ts_utc": row["created_ts_utc"],
            "started_ts_utc": now_utc,
            "worker_name": worker,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def complete_background_job(db_path: Path, job_id: int, result: dict[str, Any] | None = None) -> bool:
    """Mark one running job as succeeded with optional result payload."""
    job_id = int(job_id)
    finished = _utc_now()
    result_json = json.dumps(result or {}, separators=(",", ":"), sort_keys=True)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE background_jobs
            SET
                status = 'succeeded',
                finished_ts_utc = ?,
                last_error = NULL,
                last_result_json = ?
            WHERE id = ?;
            """,
            (finished, result_json, job_id),
        )
        connection.commit()
        return cursor.rowcount == 1
    finally:
        connection.close()


def fail_or_retry_background_job(
    db_path: Path,
    job_id: int,
    error_message: str,
    *,
    retry_delay_seconds: int = 10,
) -> dict[str, Any]:
    """Move job to retry or failed state based on attempts remaining."""
    job_id = int(job_id)
    retry_delay_seconds = max(1, int(retry_delay_seconds))
    now_utc = _utc_now()
    now_dt = _parse_utc(now_utc) or datetime.now(timezone.utc)
    available_after = _format_utc(now_dt + timedelta(seconds=retry_delay_seconds))
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT attempt_count, max_attempts FROM background_jobs WHERE id = ?;",
            (job_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return {"updated": False, "status": "missing"}
        attempt_count = int(row["attempt_count"])
        max_attempts = int(row["max_attempts"])
        should_fail = attempt_count >= max_attempts
        next_status = "failed" if should_fail else "retry_scheduled"
        cursor.execute(
            """
            UPDATE background_jobs
            SET
                status = ?,
                available_after_ts_utc = ?,
                finished_ts_utc = ?,
                last_error = ?,
                last_result_json = NULL
            WHERE id = ?;
            """,
            (
                next_status,
                now_utc if should_fail else available_after,
                now_utc if should_fail else None,
                _normalized_text(error_message)[:2000],
                job_id,
            ),
        )
        connection.commit()
        return {
            "updated": cursor.rowcount == 1,
            "status": next_status,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "available_after_ts_utc": now_utc if should_fail else available_after,
        }
    finally:
        connection.close()


def requeue_stale_running_jobs(db_path: Path, *, stale_after_seconds: int = 300) -> int:
    """Recover stale running jobs back into retry queue after restart/crash scenarios."""
    stale_after_seconds = max(1, int(stale_after_seconds))
    now = datetime.now(timezone.utc)
    threshold = _format_utc(now - timedelta(seconds=stale_after_seconds))
    now_utc = _format_utc(now)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE background_jobs
            SET
                status = 'retry_scheduled',
                available_after_ts_utc = ?,
                last_error = COALESCE(last_error, 'stale running job requeued')
            WHERE status = 'running'
              AND started_ts_utc IS NOT NULL
              AND started_ts_utc <= ?;
            """,
            (now_utc, threshold),
        )
        connection.commit()
        return int(cursor.rowcount)
    finally:
        connection.close()


def fetch_background_jobs(
    db_path: Path,
    *,
    limit: int = 100,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Return recent background jobs for diagnostics/UI visibility."""
    limit = max(1, min(int(limit), 1000))
    status_text = _normalized_text(status).lower()
    use_status_filter = status_text in JOB_STATUSES

    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        if use_status_filter:
            cursor.execute(
                """
                SELECT id, job_type, status, payload_json, attempt_count, max_attempts, available_after_ts_utc,
                       worker_name, created_ts_utc, started_ts_utc, finished_ts_utc, last_error, last_result_json
                FROM background_jobs
                WHERE status = ?
                ORDER BY id DESC
                LIMIT ?;
                """,
                (status_text, limit),
            )
        else:
            cursor.execute(
                """
                SELECT id, job_type, status, payload_json, attempt_count, max_attempts, available_after_ts_utc,
                       worker_name, created_ts_utc, started_ts_utc, finished_ts_utc, last_error, last_result_json
                FROM background_jobs
                ORDER BY id DESC
                LIMIT ?;
                """,
                (limit,),
            )
        rows = cursor.fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload_text = _normalized_text(row["payload_json"]) or "{}"
            result_text = _normalized_text(row["last_result_json"]) or "{}"
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                payload = {"_raw": payload_text}
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                result = {"_raw": result_text}
            items.append(
                {
                    "id": int(row["id"]),
                    "job_type": row["job_type"],
                    "status": row["status"],
                    "payload": payload,
                    "attempt_count": int(row["attempt_count"]),
                    "max_attempts": int(row["max_attempts"]),
                    "available_after_ts_utc": row["available_after_ts_utc"],
                    "worker_name": row["worker_name"],
                    "created_ts_utc": row["created_ts_utc"],
                    "started_ts_utc": row["started_ts_utc"],
                    "finished_ts_utc": row["finished_ts_utc"],
                    "last_error": row["last_error"],
                    "last_result": result,
                }
            )
        return items
    finally:
        connection.close()


def background_job_stats(db_path: Path) -> dict[str, Any]:
    """Return queue statistics for background job monitoring endpoints."""
    now_utc = _utc_now()
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status = 'retry_scheduled' THEN 1 ELSE 0 END) AS retry_scheduled,
                SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status IN ('queued', 'retry_scheduled') AND available_after_ts_utc <= ? THEN 1 ELSE 0 END) AS ready_now
            FROM background_jobs;
            """,
            (now_utc,),
        )
        row = cursor.fetchone()
        return {
            "total": int(row["total"] or 0),
            "queued": int(row["queued"] or 0),
            "running": int(row["running"] or 0),
            "retry_scheduled": int(row["retry_scheduled"] or 0),
            "succeeded": int(row["succeeded"] or 0),
            "failed": int(row["failed"] or 0),
            "cancelled": int(row["cancelled"] or 0),
            "ready_now": int(row["ready_now"] or 0),
            "as_of_utc": now_utc,
        }
    finally:
        connection.close()


def _camera_mode_from_text(value: Any) -> str:
    text = _normalized_text(value).lower()
    return text if text in CAMERA_MODES else DEFAULT_CAMERA_MODE


def _camera_sync_status_from_text(value: Any) -> str:
    text = _normalized_text(value).lower()
    return text if text in CAMERA_SYNC_STATUSES else DEFAULT_CAMERA_SYNC_STATUS


def _json_to_object(value: Any, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    text = _normalized_text(value)
    if not text:
        return dict(default or {})
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return dict(default or {})
    return decoded if isinstance(decoded, dict) else dict(default or {})


def _normalize_camera_mode_canonical_state(raw: Any) -> dict[str, Any]:
    state = _default_camera_mode_canonical_state()
    candidate = raw if isinstance(raw, dict) else {}
    modes = candidate.get("modes")
    if isinstance(modes, dict):
        for mode_name in state["modes"]:
            incoming = modes.get(mode_name)
            if isinstance(incoming, dict):
                merged = dict(state["modes"][mode_name])
                merged.update(incoming)
                state["modes"][mode_name] = merged
    metadata = candidate.get("metadata")
    if isinstance(metadata, dict):
        merged_meta = dict(state["metadata"])
        merged_meta.update(metadata)
        state["metadata"] = merged_meta
    return state


def _camera_mode_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "active_mode": _camera_mode_from_text(row["active_mode"]),
        "mode_revision": int(row["mode_revision"] or 1),
        "sync_status": _camera_sync_status_from_text(row["sync_status"]),
        "last_reconcile_action": _normalized_text(row["last_reconcile_action"]) or "unknown",
        "canonical_state": _normalize_camera_mode_canonical_state(_json_to_object(row["canonical_state_json"])),
        "last_camera_status": _json_to_object(row["last_camera_status_json"]),
        "last_apply_result": _json_to_object(row["last_apply_result_json"]),
        "last_error": _normalized_text(row["last_error"]),
        "created_ts_utc": row["created_ts_utc"],
        "updated_ts_utc": row["updated_ts_utc"],
    }


def _ensure_camera_mode_state_row(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "camera_mode_state"):
        return
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM camera_mode_state WHERE id = 1;")
    if cursor.fetchone() is not None:
        return
    now_utc = _utc_now()
    cursor.execute(
        """
        INSERT INTO camera_mode_state (
            id, active_mode, mode_revision, sync_status, last_reconcile_action,
            canonical_state_json, last_camera_status_json, last_apply_result_json,
            last_error, created_ts_utc, updated_ts_utc
        )
        VALUES (1, 'base', 1, 'camera_unavailable', 'initialized', ?, '{}', '{}', NULL, ?, ?);
        """,
        (
            json.dumps(_default_camera_mode_canonical_state(), separators=(",", ":"), sort_keys=True),
            now_utc,
            now_utc,
        ),
    )
    connection.commit()


def fetch_camera_mode_state(db_path: Path) -> dict[str, Any]:
    """Return canonical camera mode state payload used by camera-control orchestration."""
    connection = _connect(db_path)
    try:
        _ensure_camera_mode_state_row(connection)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT active_mode, mode_revision, sync_status, last_reconcile_action,
                   canonical_state_json, last_camera_status_json, last_apply_result_json,
                   last_error, created_ts_utc, updated_ts_utc
            FROM camera_mode_state
            WHERE id = 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("camera_mode_state row is missing after initialization")
        return _camera_mode_row_to_payload(row)
    finally:
        connection.close()


def save_camera_mode_state(
    db_path: Path,
    state: dict[str, Any],
    *,
    bump_revision: bool = False,
) -> dict[str, Any]:
    """Persist canonical camera mode state and return normalized saved payload."""
    connection = _connect(db_path)
    try:
        _ensure_camera_mode_state_row(connection)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT active_mode, mode_revision, sync_status, last_reconcile_action,
                   canonical_state_json, last_camera_status_json, last_apply_result_json,
                   last_error, created_ts_utc, updated_ts_utc
            FROM camera_mode_state
            WHERE id = 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("camera_mode_state row is missing after initialization")
        current = _camera_mode_row_to_payload(row)
        mode_revision = int(current["mode_revision"])
        if bump_revision:
            mode_revision += 1
        merged = {
            "active_mode": _camera_mode_from_text(state.get("active_mode", current["active_mode"])),
            "mode_revision": mode_revision,
            "sync_status": _camera_sync_status_from_text(state.get("sync_status", current["sync_status"])),
            "last_reconcile_action": _normalized_text(
                state.get("last_reconcile_action", current["last_reconcile_action"])
            )
            or "unknown",
            "canonical_state": _normalize_camera_mode_canonical_state(
                state.get("canonical_state", current["canonical_state"])
            ),
            "last_camera_status": (
                state["last_camera_status"]
                if isinstance(state.get("last_camera_status"), dict)
                else current["last_camera_status"]
            ),
            "last_apply_result": (
                state["last_apply_result"]
                if isinstance(state.get("last_apply_result"), dict)
                else current["last_apply_result"]
            ),
            "last_error": _normalized_text(state.get("last_error", current["last_error"])) or "",
            "created_ts_utc": current["created_ts_utc"],
            "updated_ts_utc": _utc_now(),
        }
        cursor.execute(
            """
            UPDATE camera_mode_state
            SET active_mode=?, mode_revision=?, sync_status=?, last_reconcile_action=?,
                canonical_state_json=?, last_camera_status_json=?, last_apply_result_json=?,
                last_error=?, updated_ts_utc=?
            WHERE id = 1;
            """,
            (
                merged["active_mode"],
                int(merged["mode_revision"]),
                merged["sync_status"],
                merged["last_reconcile_action"],
                json.dumps(merged["canonical_state"], separators=(",", ":"), sort_keys=True),
                json.dumps(merged["last_camera_status"], separators=(",", ":"), sort_keys=True),
                json.dumps(merged["last_apply_result"], separators=(",", ":"), sort_keys=True),
                merged["last_error"] or None,
                merged["updated_ts_utc"],
            ),
        )
        connection.commit()
        return merged
    finally:
        connection.close()


def import_ndjson_events_if_sqlite_empty(db_path: Path, events_file: Path) -> int:
    if not events_file.exists():
        return 0
    connection = _connect(db_path)
    imported_count = 0
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM event_records;")
        if int(cursor.fetchone()["c"]) > 0:
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
                prepared = _prepare_event_for_insert(payload, event_type="detection")
                _insert_prepared_event(cursor, prepared)
                imported_count += 1
        connection.commit()
        return imported_count
    finally:
        connection.close()


def backfill_event_policy_fields(db_path: Path, *, only_missing: bool = True) -> dict[str, Any]:
    connection = _connect(db_path)
    try:
        updated = _backfill_policy_fields(connection, only_missing=only_missing)
        connection.commit()
        return {"updated_records": updated, "only_missing": bool(only_missing)}
    finally:
        connection.close()


def reset_event_retention_from_now(
    db_path: Path,
    *,
    include_saved: bool = False,
    now_utc: str | None = None,
) -> dict[str, Any]:
    """Reset existing event delete-after timers so countdown starts from now."""
    resolved_now = now_utc or _utc_now()
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        if include_saved:
            cursor.execute(
                """
                SELECT id, confidence, lifecycle_state
                FROM event_records
                WHERE lifecycle_state NOT IN ('deleted', 'expired');
                """
            )
        else:
            cursor.execute(
                """
                SELECT id, confidence, lifecycle_state
                FROM event_records
                WHERE lifecycle_state NOT IN ('deleted', 'expired', 'saved');
                """
            )
        rows = cursor.fetchall()
        updated = 0
        for row in rows:
            lifecycle_state = normalize_lifecycle_state(row["lifecycle_state"])
            retention_days = compute_retention_days(row["confidence"], lifecycle_state)
            delete_after_ts_utc = compute_delete_after_ts(resolved_now, retention_days, now_utc=resolved_now)
            retention_basis = (
                f"{compute_retention_basis(row['confidence'], lifecycle_state, retention_days)};"
                "reset=from_now"
            )
            cursor.execute(
                """
                UPDATE event_records
                SET retention_days=?, delete_after_ts_utc=?, retention_basis=?, retention_updated_ts_utc=?
                WHERE id=?;
                """,
                (
                    int(retention_days),
                    delete_after_ts_utc,
                    retention_basis,
                    resolved_now,
                    int(row["id"]),
                ),
            )
            updated += 1
        connection.commit()
        return {
            "updated_records": updated,
            "include_saved": bool(include_saved),
            "reset_from_utc": resolved_now,
        }
    finally:
        connection.close()


def attach_event_media_paths(
    db_path: Path,
    event_id: int,
    *,
    snapshot_path: str | None = None,
    clip_path: str | None = None,
) -> bool:
    event_id = int(event_id)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id, ts_utc, event_type, label, confidence, class_id, snapshot_path, clip_path, review_state, share_state, deletion_state, lifecycle_state, notes, event_uid, signal_proximity_score, signal_facing_score FROM event_records WHERE id = ?;",
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return False
        new_snapshot = _normalize_media_ref(snapshot_path) if snapshot_path is not None else _normalize_media_ref(row["snapshot_path"])
        new_clip = _normalize_media_ref(clip_path) if clip_path is not None else _normalize_media_ref(row["clip_path"])
        lifecycle_state = normalize_lifecycle_state(row["lifecycle_state"])
        if lifecycle_state == "detected" and (new_snapshot or new_clip):
            lifecycle_state = "media_attached"
        payload = {
            "ts_utc": row["ts_utc"],
            "label": row["label"],
            "confidence": row["confidence"],
            "class_id": row["class_id"],
            "snapshot": new_snapshot,
            "clip": new_clip,
            "review_state": row["review_state"],
            "share_state": row["share_state"],
            "deletion_state": row["deletion_state"],
            "lifecycle_state": lifecycle_state,
            "lifecycle_updated_ts_utc": _utc_now(),
            "event_uid": row["event_uid"],
            "signal_proximity_score": row["signal_proximity_score"],
            "signal_facing_score": row["signal_facing_score"],
            "notes": row["notes"],
        }
        prepared = _prepare_event_for_insert(payload, event_type=row["event_type"])
        cursor.execute(
            """
            UPDATE event_records
            SET snapshot_path=?, clip_path=?, lifecycle_state=?, lifecycle_updated_ts_utc=?,
                severity_score=?, severity_level=?, severity_policy_version=?, severity_updated_ts_utc=?,
                retention_days=?, delete_after_ts_utc=?, retention_basis=?, retention_updated_ts_utc=?, event_uid=?
            WHERE id=?;
            """,
            (
                prepared["snapshot_path"], prepared["clip_path"], prepared["lifecycle_state"], prepared["lifecycle_updated_ts_utc"],
                prepared["severity_score"], prepared["severity_level"], prepared["severity_policy_version"], prepared["severity_updated_ts_utc"],
                prepared["retention_days"], prepared["delete_after_ts_utc"], prepared["retention_basis"], prepared["retention_updated_ts_utc"], prepared["event_uid"],
                event_id,
            ),
        )
        connection.commit()
        return cursor.rowcount == 1
    finally:
        connection.close()


def update_event_lifecycle_state(db_path: Path, event_id: int, next_state: str) -> bool:
    event_id = int(event_id)
    target_state = normalize_lifecycle_state(next_state)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id, ts_utc, event_type, label, confidence, class_id, snapshot_path, clip_path, review_state, share_state, deletion_state, lifecycle_state, notes, event_uid, signal_proximity_score, signal_facing_score FROM event_records WHERE id = ?;",
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return False
        current_state = normalize_lifecycle_state(row["lifecycle_state"])
        if not can_transition_lifecycle_state(current_state, target_state):
            return False
        if current_state == target_state:
            return True
        payload = {
            "ts_utc": row["ts_utc"],
            "label": row["label"],
            "confidence": row["confidence"],
            "class_id": row["class_id"],
            "snapshot": row["snapshot_path"],
            "clip": row["clip_path"],
            "review_state": row["review_state"],
            "share_state": row["share_state"],
            "deletion_state": row["deletion_state"],
            "lifecycle_state": target_state,
            "lifecycle_updated_ts_utc": _utc_now(),
            "event_uid": row["event_uid"],
            "signal_proximity_score": row["signal_proximity_score"],
            "signal_facing_score": row["signal_facing_score"],
            "notes": row["notes"],
        }
        prepared = _prepare_event_for_insert(payload, event_type=row["event_type"])
        cursor.execute(
            """
            UPDATE event_records
            SET lifecycle_state=?, lifecycle_updated_ts_utc=?, severity_score=?, severity_level=?, severity_policy_version=?,
                severity_updated_ts_utc=?, retention_days=?, delete_after_ts_utc=?, retention_basis=?, retention_updated_ts_utc=?
            WHERE id=?;
            """,
            (
                prepared["lifecycle_state"], prepared["lifecycle_updated_ts_utc"], prepared["severity_score"],
                prepared["severity_level"], prepared["severity_policy_version"], prepared["severity_updated_ts_utc"],
                prepared["retention_days"], prepared["delete_after_ts_utc"], prepared["retention_basis"],
                prepared["retention_updated_ts_utc"], event_id,
            ),
        )
        connection.commit()
        return cursor.rowcount == 1
    finally:
        connection.close()


def create_saved_item(
    db_path: Path,
    *,
    clip_path: str,
    snapshot_path: str | None = None,
    title: str | None = None,
    source_type: str = "event",
    event_id: int | None = None,
    event_uid: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    """Create one saved-media item and return normalized payload."""
    now_utc = _utc_now()
    normalized_clip = _normalize_media_ref(clip_path)
    if not normalized_clip:
        raise ValueError("clip_path is required")
    normalized_snapshot = _normalize_media_ref(snapshot_path)
    normalized_title = _normalized_text(title) or f"Saved {Path(normalized_clip).stem}"
    normalized_source = _normalized_text(source_type).lower() or "event"
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO saved_items (
                event_id, event_uid, title, source_type, snapshot_path, clip_path, notes,
                created_ts_utc, updated_ts_utc, deleted_ts_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);
            """,
            (
                int(event_id) if event_id is not None else None,
                _normalized_text(event_uid) or None,
                normalized_title,
                normalized_source,
                normalized_snapshot,
                normalized_clip,
                _normalized_text(notes) or None,
                now_utc,
                now_utc,
            ),
        )
        saved_id = int(cursor.lastrowid)
        connection.commit()
        return {
            "id": saved_id,
            "event_id": int(event_id) if event_id is not None else None,
            "event_uid": _normalized_text(event_uid) or None,
            "title": normalized_title,
            "source_type": normalized_source,
            "snapshot": normalized_snapshot,
            "clip": normalized_clip,
            "notes": _normalized_text(notes) or "",
            "created_ts_utc": now_utc,
            "updated_ts_utc": now_utc,
        }
    finally:
        connection.close()


def fetch_saved_item_by_id(db_path: Path, saved_id: int) -> dict[str, Any] | None:
    saved_id = int(saved_id)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, event_id, event_uid, title, source_type, snapshot_path, clip_path, notes, created_ts_utc, updated_ts_utc
            FROM saved_items
            WHERE id = ? AND deleted_ts_utc IS NULL;
            """,
            (saved_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "event_id": int(row["event_id"]) if row["event_id"] is not None else None,
            "event_uid": _normalized_text(row["event_uid"]) or None,
            "title": _normalized_text(row["title"]) or f"Saved #{saved_id}",
            "source_type": _normalized_text(row["source_type"]) or "event",
            "snapshot": _normalize_media_ref(row["snapshot_path"]),
            "clip": _normalize_media_ref(row["clip_path"]),
            "notes": _normalized_text(row["notes"]),
            "created_ts_utc": _normalized_text(row["created_ts_utc"]),
            "updated_ts_utc": _normalized_text(row["updated_ts_utc"]),
        }
    finally:
        connection.close()


def list_saved_items(
    db_path: Path,
    *,
    limit: int = 24,
    offset: int = 0,
    q: str = "",
    range_key: str = "all",
    sort_key: str = "newest",
) -> dict[str, Any]:
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    query = _normalized_text(q).lower()
    normalized_range = _normalized_text(range_key).lower()
    if normalized_range not in {"all", "24h", "7d", "30d"}:
        normalized_range = "all"
    normalized_sort = _normalized_text(sort_key).lower()
    if normalized_sort not in {"newest", "oldest", "title"}:
        normalized_sort = "newest"
    range_seconds: int | None = None
    if normalized_range == "24h":
        range_seconds = 24 * 3600
    elif normalized_range == "7d":
        range_seconds = 7 * 24 * 3600
    elif normalized_range == "30d":
        range_seconds = 30 * 24 * 3600

    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, event_id, event_uid, title, source_type, snapshot_path, clip_path, notes, created_ts_utc, updated_ts_utc
            FROM saved_items
            WHERE deleted_ts_utc IS NULL;
            """
        )
        rows = cursor.fetchall()
        cursor.execute(
            """
            SELECT id, snapshot_path, clip_path
            FROM event_records
            WHERE lifecycle_state NOT IN ('deleted', 'expired');
            """
        )
        event_rows = cursor.fetchall()
        snapshot_by_event_id: dict[int, str] = {}
        snapshot_by_clip_basename: dict[str, str] = {}
        for event_row in event_rows:
            event_id = int(event_row["id"])
            snapshot_ref = _normalize_media_ref(event_row["snapshot_path"])
            clip_ref = _normalize_media_ref(event_row["clip_path"])
            if snapshot_ref:
                snapshot_by_event_id[event_id] = snapshot_ref
                if clip_ref:
                    snapshot_by_clip_basename[Path(clip_ref).name] = snapshot_ref
        now_dt = datetime.now(timezone.utc)
        items: list[dict[str, Any]] = []
        for row in rows:
            title = _normalized_text(row["title"]) or f"Saved #{int(row['id'])}"
            source = _normalized_text(row["source_type"]) or "event"
            event_uid = _normalized_text(row["event_uid"])
            notes = _normalized_text(row["notes"])
            created_ts = _normalized_text(row["created_ts_utc"])
            if query:
                haystack = " ".join([title, source, event_uid, notes]).lower()
                if query not in haystack:
                    continue
            if range_seconds is not None:
                created_dt = _parse_utc(created_ts)
                if created_dt is None:
                    continue
                age_seconds = (now_dt - created_dt).total_seconds()
                if age_seconds < 0 or age_seconds > float(range_seconds):
                    continue
            event_id = int(row["event_id"]) if row["event_id"] is not None else None
            snapshot_ref = _normalize_media_ref(row["snapshot_path"])
            if not snapshot_ref:
                if event_id is not None and event_id in snapshot_by_event_id:
                    snapshot_ref = snapshot_by_event_id[event_id]
                else:
                    clip_ref = _normalize_media_ref(row["clip_path"])
                    clip_base = Path(clip_ref).name if clip_ref else ""
                    if clip_base and clip_base in snapshot_by_clip_basename:
                        snapshot_ref = snapshot_by_clip_basename[clip_base]
            items.append(
                {
                    "id": int(row["id"]),
                    "event_id": event_id,
                    "event_uid": event_uid or None,
                    "title": title,
                    "source_type": source,
                    "snapshot": snapshot_ref,
                    "clip": _normalize_media_ref(row["clip_path"]),
                    "notes": notes,
                    "created_ts_utc": created_ts,
                    "updated_ts_utc": _normalized_text(row["updated_ts_utc"]),
                }
            )

        if normalized_sort == "oldest":
            items.sort(key=lambda item: _normalized_text(item.get("created_ts_utc")))
        elif normalized_sort == "title":
            items.sort(key=lambda item: _normalized_text(item.get("title")).lower())
        else:
            items.sort(key=lambda item: _normalized_text(item.get("created_ts_utc")), reverse=True)

        total = len(items)
        page_items = items[offset : offset + limit]
        pages = max(1, (total + limit - 1) // limit)
        page = min(pages, max(1, (offset // limit) + 1))
        return {
            "saved_items": page_items,
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
                "q": query,
                "range": normalized_range,
                "sort": normalized_sort,
            },
        }
    finally:
        connection.close()


def mark_saved_item_deleted(db_path: Path, saved_id: int) -> dict[str, Any] | None:
    saved_id = int(saved_id)
    existing = fetch_saved_item_by_id(db_path, saved_id)
    if existing is None:
        return None
    now_utc = _utc_now()
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE saved_items SET deleted_ts_utc=?, updated_ts_utc=? WHERE id=?;",
            (now_utc, now_utc, saved_id),
        )
        connection.commit()
        existing["deleted_ts_utc"] = now_utc
        return existing
    finally:
        connection.close()


def insert_server_restart_event(
    db_path: Path,
    *,
    classification: str,
    reason: str = "",
    trigger: str = "",
    action: str = "",
    metadata: dict[str, Any] | None = None,
    ts_utc: str | None = None,
) -> int:
    """Persist one restart/outage classification event."""
    normalized_class = _normalized_text(classification).lower() or "unexpected"
    if normalized_class not in {"planned", "unexpected"}:
        normalized_class = "unexpected"
    resolved_ts = _normalized_text(ts_utc) or _utc_now()
    metadata_json = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO server_restart_events (ts_utc, classification, reason, trigger, action, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                resolved_ts,
                normalized_class,
                _normalized_text(reason)[:200],
                _normalized_text(trigger)[:200],
                _normalized_text(action)[:64],
                metadata_json,
            ),
        )
        connection.commit()
        return int(cursor.lastrowid)
    finally:
        connection.close()


def fetch_recent_server_restart_events(db_path: Path, *, limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, ts_utc, classification, reason, trigger, action, metadata_json
            FROM server_restart_events
            ORDER BY id DESC
            LIMIT ?;
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            metadata_text = _normalized_text(row["metadata_json"]) or "{}"
            try:
                metadata = json.loads(metadata_text)
            except json.JSONDecodeError:
                metadata = {"_raw": metadata_text}
            items.append(
                {
                    "id": int(row["id"]),
                    "ts_utc": _normalized_text(row["ts_utc"]),
                    "classification": _normalized_text(row["classification"]) or "unexpected",
                    "reason": _normalized_text(row["reason"]),
                    "trigger": _normalized_text(row["trigger"]),
                    "action": _normalized_text(row["action"]),
                    "metadata": metadata,
                }
            )
        return items
    finally:
        connection.close()


def media_integrity_report(
    db_path: Path,
    root: Path,
    snapshot_dir: Path,
    recordings_dir: Path,
    *,
    sample_limit: int = 100,
) -> dict[str, Any]:
    sample_limit = max(1, min(int(sample_limit), 1000))
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id, event_uid, snapshot_path, clip_path, lifecycle_state, deletion_state FROM event_records;")
        rows = cursor.fetchall()

        missing_snapshot_refs: list[dict[str, Any]] = []
        missing_clip_refs: list[dict[str, Any]] = []
        state_mismatches: list[dict[str, Any]] = []
        referenced_snapshot_paths: set[str] = set()
        referenced_clip_paths: set[str] = set()

        for row in rows:
            event_id = int(row["id"])
            event_uid = _normalized_text(row["event_uid"]) or f"event_{event_id}"
            lifecycle = normalize_lifecycle_state(row["lifecycle_state"])
            deletion_state = _normalized_text(row["deletion_state"]).lower()

            snapshot_ref = _normalized_text(row["snapshot_path"])
            clip_ref = _normalized_text(row["clip_path"])
            snapshot_abs = _resolve_media_path(root, snapshot_ref)
            clip_abs = _resolve_media_path(root, clip_ref)

            snapshot_exists = bool(snapshot_abs and snapshot_abs.exists())
            clip_exists = bool(clip_abs and clip_abs.exists())

            if snapshot_ref:
                if snapshot_abs is not None:
                    referenced_snapshot_paths.add(str(snapshot_abs.resolve()))
                if not snapshot_exists:
                    missing_snapshot_refs.append({"id": event_id, "event_uid": event_uid, "path": snapshot_ref})
            if clip_ref:
                if clip_abs is not None:
                    referenced_clip_paths.add(str(clip_abs.resolve()))
                if not clip_exists:
                    missing_clip_refs.append({"id": event_id, "event_uid": event_uid, "path": clip_ref})

            if lifecycle == "media_attached" and not (snapshot_ref or clip_ref):
                state_mismatches.append({"id": event_id, "event_uid": event_uid, "reason": "lifecycle_media_attached_without_media_refs"})
            if lifecycle in {"deleted", "expired"} and (snapshot_exists or clip_exists):
                state_mismatches.append({"id": event_id, "event_uid": event_uid, "reason": "terminal_lifecycle_but_media_still_exists"})
            if deletion_state in {"deleted", "expired"} and lifecycle not in {"deleted", "expired"}:
                state_mismatches.append({"id": event_id, "event_uid": event_uid, "reason": "deletion_state_terminal_but_lifecycle_not_terminal"})

        orphan_snapshot_files: list[str] = []
        orphan_clip_files: list[str] = []

        if snapshot_dir.exists():
            for media_file in snapshot_dir.glob("*"):
                if not media_file.is_file() or media_file.suffix.lower() not in IMAGE_EXTENSIONS:
                    continue
                if str(media_file.resolve()) not in referenced_snapshot_paths:
                    orphan_snapshot_files.append(str(media_file.relative_to(root)).replace("\\", "/"))

        if recordings_dir.exists():
            for media_file in recordings_dir.glob("*"):
                if not media_file.is_file() or media_file.suffix.lower() not in VIDEO_EXTENSIONS:
                    continue
                if str(media_file.resolve()) not in referenced_clip_paths:
                    orphan_clip_files.append(str(media_file.relative_to(root)).replace("\\", "/"))

        return {
            "summary": {
                "events_scanned": len(rows),
                "missing_snapshot_refs": len(missing_snapshot_refs),
                "missing_clip_refs": len(missing_clip_refs),
                "orphan_snapshot_files": len(orphan_snapshot_files),
                "orphan_clip_files": len(orphan_clip_files),
                "state_mismatches": len(state_mismatches),
            },
            "findings": {
                "missing_snapshot_refs": missing_snapshot_refs[:sample_limit],
                "missing_clip_refs": missing_clip_refs[:sample_limit],
                "orphan_snapshot_files": orphan_snapshot_files[:sample_limit],
                "orphan_clip_files": orphan_clip_files[:sample_limit],
                "state_mismatches": state_mismatches[:sample_limit],
            },
            "recommended_actions": ["mark_missing_as_expired", "delete_orphan_files"],
        }
    finally:
        connection.close()


def repair_media_integrity(
    db_path: Path,
    root: Path,
    snapshot_dir: Path,
    recordings_dir: Path,
    *,
    mark_missing_as_expired: bool = False,
    delete_orphan_files: bool = False,
    sample_limit: int = 100,
) -> dict[str, Any]:
    before = media_integrity_report(db_path, root, snapshot_dir, recordings_dir, sample_limit=sample_limit)
    actions = {"marked_expired_events": 0, "deleted_orphan_files": 0, "deleted_paths": []}

    if mark_missing_as_expired:
        missing_event_ids = {int(i["id"]) for i in before["findings"]["missing_snapshot_refs"]} | {int(i["id"]) for i in before["findings"]["missing_clip_refs"]}
        if missing_event_ids:
            connection = _connect(db_path)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    f"UPDATE event_records SET lifecycle_state='expired', lifecycle_updated_ts_utc=?, deletion_state='expired', notes=TRIM(COALESCE(notes,'') || ' [auto-marked-expired:missing-media]') WHERE id IN ({','.join(['?'] * len(missing_event_ids))});",
                    (_utc_now(), *sorted(missing_event_ids)),
                )
                actions["marked_expired_events"] = int(cursor.rowcount)
                connection.commit()
            finally:
                connection.close()

    if delete_orphan_files:
        orphan_paths = list(before["findings"]["orphan_snapshot_files"]) + list(before["findings"]["orphan_clip_files"])
        deleted_paths: list[str] = []
        for rel_path in orphan_paths:
            target = _resolve_media_path(root, rel_path)
            if target is None:
                continue
            try:
                if target.exists() and target.is_file():
                    os.remove(target)
                    deleted_paths.append(rel_path)
            except OSError:
                continue
        actions["deleted_orphan_files"] = len(deleted_paths)
        actions["deleted_paths"] = deleted_paths[:sample_limit]

    after = media_integrity_report(db_path, root, snapshot_dir, recordings_dir, sample_limit=sample_limit)
    return {
        "applied": {
            "mark_missing_as_expired": bool(mark_missing_as_expired),
            "delete_orphan_files": bool(delete_orphan_files),
        },
        "actions": actions,
        "before": before["summary"],
        "after": after["summary"],
    }


def run_retention_cull(
    db_path: Path,
    root: Path,
    *,
    apply_file_delete: bool = False,
    now_utc: str | None = None,
    sample_limit: int = 100,
) -> dict[str, Any]:
    resolved_now = now_utc or _utc_now()
    now_dt = _parse_utc(resolved_now) or datetime.now(timezone.utc)
    sample_limit = max(1, min(int(sample_limit), 1000))

    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT id, event_uid, snapshot_path, clip_path, lifecycle_state, delete_after_ts_utc FROM event_records WHERE lifecycle_state NOT IN ('deleted', 'saved');"
        )
        rows = cursor.fetchall()

        due_rows: list[sqlite3.Row] = []
        due_samples: list[dict[str, Any]] = []
        for row in rows:
            due_ts = _parse_utc(row["delete_after_ts_utc"])
            if due_ts is None or due_ts > now_dt:
                continue
            due_rows.append(row)
            if len(due_samples) < sample_limit:
                due_samples.append(
                    {
                        "id": int(row["id"]),
                        "event_uid": row["event_uid"],
                        "delete_after_ts_utc": row["delete_after_ts_utc"],
                        "lifecycle_state": row["lifecycle_state"],
                    }
                )

        expired_events = 0
        deleted_events = 0
        deleted_files: list[str] = []

        for row in due_rows:
            event_id = int(row["id"])
            next_lifecycle = "deleted" if apply_file_delete else "expired"
            next_deletion_state = "deleted" if apply_file_delete else "expired"

            if apply_file_delete:
                for media_ref in (row["snapshot_path"], row["clip_path"]):
                    media_abs = _resolve_media_path(root, media_ref)
                    if media_abs is None:
                        continue
                    try:
                        if media_abs.exists() and media_abs.is_file():
                            os.remove(media_abs)
                            deleted_files.append(str(media_abs.relative_to(root)).replace("\\", "/"))
                    except OSError:
                        continue

            cursor.execute(
                "UPDATE event_records SET lifecycle_state=?, lifecycle_updated_ts_utc=?, deletion_state=?, retention_updated_ts_utc=? WHERE id=?;",
                (next_lifecycle, resolved_now, next_deletion_state, resolved_now, event_id),
            )
            if apply_file_delete:
                deleted_events += 1
            else:
                expired_events += 1

        connection.commit()
        return {
            "now_utc": resolved_now,
            "due_events": len(due_rows),
            "expired_events": expired_events,
            "deleted_events": deleted_events,
            "deleted_files_count": len(deleted_files),
            "deleted_files_sample": deleted_files[:sample_limit],
            "due_events_sample": due_samples,
        }
    finally:
        connection.close()


def retention_summary(db_path: Path, *, now_utc: str | None = None) -> dict[str, Any]:
    resolved_now = now_utc or _utc_now()
    now_dt = _parse_utc(resolved_now) or datetime.now(timezone.utc)
    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN lifecycle_state='deleted' THEN 1 ELSE 0 END) AS deleted_count, SUM(CASE WHEN lifecycle_state='expired' THEN 1 ELSE 0 END) AS expired_count FROM event_records;"
        )
        row = cursor.fetchone()
        total_events = int(row["total"] or 0)
        deleted_events = int(row["deleted_count"] or 0)
        expired_events = int(row["expired_count"] or 0)

        cursor.execute(
            "SELECT id, event_uid, delete_after_ts_utc, lifecycle_state FROM event_records WHERE lifecycle_state NOT IN ('deleted', 'saved') ORDER BY delete_after_ts_utc ASC LIMIT 1;"
        )
        next_due_row = cursor.fetchone()

        cursor.execute("SELECT delete_after_ts_utc FROM event_records WHERE lifecycle_state NOT IN ('deleted', 'saved') AND delete_after_ts_utc IS NOT NULL;")
        due_now = 0
        for due_row in cursor.fetchall():
            due_ts = _parse_utc(due_row["delete_after_ts_utc"])
            if due_ts is not None and due_ts <= now_dt:
                due_now += 1

        return {
            "now_utc": resolved_now,
            "total_events": total_events,
            "due_now": due_now,
            "expired_events": expired_events,
            "deleted_events": deleted_events,
            "next_due_event": (
                {
                    "id": int(next_due_row["id"]),
                    "event_uid": next_due_row["event_uid"],
                    "delete_after_ts_utc": next_due_row["delete_after_ts_utc"],
                    "lifecycle_state": next_due_row["lifecycle_state"],
                }
                if next_due_row is not None
                else None
            ),
        }
    finally:
        connection.close()


def sqlite_status(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {
            "db_exists": False,
            "event_records": 0,
            "schema_version": 0,
            "latest_schema_version": CURRENT_SCHEMA_VERSION,
        }

    connection = _connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM event_records;")
        count = int(cursor.fetchone()[0])
        schema_version = 0
        if _table_exists(connection, "schema_migrations"):
            cursor.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations;")
            schema_version = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT SUM(CASE WHEN event_uid IS NULL OR TRIM(event_uid)='' THEN 1 ELSE 0 END) AS missing_uid, SUM(CASE WHEN severity_score IS NULL THEN 1 ELSE 0 END) AS missing_severity, SUM(CASE WHEN retention_days IS NULL OR delete_after_ts_utc IS NULL THEN 1 ELSE 0 END) AS missing_retention FROM event_records;"
        )
        policy = cursor.fetchone()
        job_totals = {
            "total": 0,
            "queued": 0,
            "running": 0,
            "retry_scheduled": 0,
            "succeeded": 0,
            "failed": 0,
            "cancelled": 0,
            "ready_now": 0,
        }
        if _table_exists(connection, "background_jobs"):
            now_utc = _utc_now()
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                    SUM(CASE WHEN status = 'retry_scheduled' THEN 1 ELSE 0 END) AS retry_scheduled,
                    SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN status IN ('queued', 'retry_scheduled') AND available_after_ts_utc <= ? THEN 1 ELSE 0 END) AS ready_now
                FROM background_jobs;
                """,
                (now_utc,),
            )
            jobs = cursor.fetchone()
            job_totals = {
                "total": int(jobs["total"] or 0),
                "queued": int(jobs["queued"] or 0),
                "running": int(jobs["running"] or 0),
                "retry_scheduled": int(jobs["retry_scheduled"] or 0),
                "succeeded": int(jobs["succeeded"] or 0),
                "failed": int(jobs["failed"] or 0),
                "cancelled": int(jobs["cancelled"] or 0),
                "ready_now": int(jobs["ready_now"] or 0),
            }
        camera_mode = {
            "active_mode": DEFAULT_CAMERA_MODE,
            "mode_revision": 0,
            "sync_status": DEFAULT_CAMERA_SYNC_STATUS,
        }
        saved_items_count = 0
        if _table_exists(connection, "camera_mode_state"):
            _ensure_camera_mode_state_row(connection)
            cursor.execute(
                "SELECT active_mode, mode_revision, sync_status FROM camera_mode_state WHERE id = 1;"
            )
            mode_row = cursor.fetchone()
            if mode_row is not None:
                camera_mode = {
                    "active_mode": _camera_mode_from_text(mode_row["active_mode"]),
                    "mode_revision": int(mode_row["mode_revision"] or 0),
                    "sync_status": _camera_sync_status_from_text(mode_row["sync_status"]),
                }
        if _table_exists(connection, "saved_items"):
            cursor.execute("SELECT COUNT(*) FROM saved_items WHERE deleted_ts_utc IS NULL;")
            saved_items_count = int(cursor.fetchone()[0] or 0)
        return {
            "db_exists": True,
            "event_records": count,
            "schema_version": schema_version,
            "latest_schema_version": CURRENT_SCHEMA_VERSION,
            "missing_event_uid": int(policy["missing_uid"] or 0),
            "missing_severity": int(policy["missing_severity"] or 0),
            "missing_retention": int(policy["missing_retention"] or 0),
            "background_jobs": job_totals,
            "camera_mode_state": camera_mode,
            "saved_items": saved_items_count,
        }
    finally:
        connection.close()
