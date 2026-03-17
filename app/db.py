"""
SQLite local database layer.

Schema design: each entity table has indexed top-level fields for fast filtering/sorting
plus a full `data_json` blob that stores the complete API response. This means no
schema migration is needed when BeProduct adds custom fields to your tenant.

`is_dirty=1` marks records that have been locally edited and not yet pushed back to SaaS.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterator, Optional

from app.config import settings


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _ensure_db_dir() -> None:
    settings.DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """Context manager that yields a configured SQLite connection."""
    _ensure_db_dir()
    conn = sqlite3.connect(str(settings.DB_PATH), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS styles (
    id            TEXT PRIMARY KEY,
    folder_id     TEXT,
    folder_name   TEXT,
    header_number TEXT,
    header_name   TEXT,
    active        INTEGER,
    created_at    TEXT,
    modified_at   TEXT,
    synced_at     TEXT,
    is_dirty      INTEGER DEFAULT 0,
    data_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_styles_folder    ON styles(folder_id);
CREATE INDEX IF NOT EXISTS idx_styles_modified  ON styles(modified_at);
CREATE INDEX IF NOT EXISTS idx_styles_dirty     ON styles(is_dirty);

CREATE TABLE IF NOT EXISTS materials (
    id            TEXT PRIMARY KEY,
    folder_id     TEXT,
    folder_name   TEXT,
    header_number TEXT,
    header_name   TEXT,
    active        INTEGER,
    created_at    TEXT,
    modified_at   TEXT,
    synced_at     TEXT,
    is_dirty      INTEGER DEFAULT 0,
    data_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_materials_folder   ON materials(folder_id);
CREATE INDEX IF NOT EXISTS idx_materials_modified ON materials(modified_at);
CREATE INDEX IF NOT EXISTS idx_materials_dirty    ON materials(is_dirty);

CREATE TABLE IF NOT EXISTS colors (
    id            TEXT PRIMARY KEY,
    folder_id     TEXT,
    folder_name   TEXT,
    header_number TEXT,
    header_name   TEXT,
    active        INTEGER,
    created_at    TEXT,
    modified_at   TEXT,
    synced_at     TEXT,
    is_dirty      INTEGER DEFAULT 0,
    data_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_colors_folder   ON colors(folder_id);
CREATE INDEX IF NOT EXISTS idx_colors_modified ON colors(modified_at);
CREATE INDEX IF NOT EXISTS idx_colors_dirty    ON colors(is_dirty);

CREATE TABLE IF NOT EXISTS directory (
    id            TEXT PRIMARY KEY,
    directory_id  TEXT,
    name          TEXT,
    partner_type  TEXT,
    country       TEXT,
    active        INTEGER,
    synced_at     TEXT,
    is_dirty      INTEGER DEFAULT 0,
    data_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_directory_type  ON directory(partner_type);
CREATE INDEX IF NOT EXISTS idx_directory_dirty ON directory(is_dirty);

CREATE TABLE IF NOT EXISTS sync_meta (
    entity        TEXT PRIMARY KEY,
    last_sync_at  TEXT,
    sync_type     TEXT
);

CREATE TABLE IF NOT EXISTS rate_limit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity          TEXT,
    timestamp       TEXT,
    requests_used   INTEGER,
    requests_limit  INTEGER,
    reset_at        TEXT
);
"""


def init_schema() -> None:
    """Create tables and indexes if they don't exist yet."""
    with get_conn() as conn:
        conn.executescript(_DDL)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

def upsert_style(record: dict[str, Any]) -> None:
    """Insert or replace a style record. Does NOT overwrite is_dirty=1 records
    unless the remote modifiedAt is newer than the local data."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT is_dirty, modified_at FROM styles WHERE id=?", (record["id"],)
        ).fetchone()

        if existing and existing["is_dirty"] == 1:
            # Don't overwrite dirty (locally modified) record with remote data
            # unless remote is newer
            remote_modified = record.get("modifiedAt", "")
            local_modified = existing["modified_at"] or ""
            if remote_modified <= local_modified:
                return  # keep local pending changes

        folder = record.get("folder") or {}
        conn.execute(
            """
            INSERT INTO styles
                (id, folder_id, folder_name, header_number, header_name,
                 active, created_at, modified_at, synced_at, is_dirty, data_json)
            VALUES (?,?,?,?,?,?,?,?,?,0,?)
            ON CONFLICT(id) DO UPDATE SET
                folder_id=excluded.folder_id,
                folder_name=excluded.folder_name,
                header_number=excluded.header_number,
                header_name=excluded.header_name,
                active=excluded.active,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                synced_at=excluded.synced_at,
                is_dirty=0,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                folder.get("id"),
                folder.get("name"),
                record.get("headerNumber"),
                record.get("headerName"),
                1 if str(record.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_styles(
    folder_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    dirty_only: bool = False,
) -> list[dict[str, Any]]:
    """Return styles from local DB with optional filters."""
    sql = "SELECT * FROM styles WHERE 1=1"
    params: list[Any] = []
    if folder_id:
        sql += " AND folder_id=?"
        params.append(folder_id)
    if search:
        sql += " AND (header_number LIKE ? OR header_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    if dirty_only:
        sql += " AND is_dirty=1"
    sql += " ORDER BY header_number LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_style(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM styles WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


def update_style_local(record_id: str, updated_json: dict[str, Any]) -> None:
    """Mark a style as dirty and update its local JSON."""
    data_json = json.dumps(updated_json)
    folder = updated_json.get("folder") or {}
    with get_conn() as conn:
        conn.execute(
            """UPDATE styles SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                1 if str(updated_json.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                data_json,
                record_id,
            ),
        )


def mark_style_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE styles SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Materials
# ---------------------------------------------------------------------------

def upsert_material(record: dict[str, Any]) -> None:
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT is_dirty, modified_at FROM materials WHERE id=?", (record["id"],)
        ).fetchone()

        if existing and existing["is_dirty"] == 1:
            remote_modified = record.get("modifiedAt", "")
            local_modified = existing["modified_at"] or ""
            if remote_modified <= local_modified:
                return

        folder = record.get("folder") or {}
        conn.execute(
            """
            INSERT INTO materials
                (id, folder_id, folder_name, header_number, header_name,
                 active, created_at, modified_at, synced_at, is_dirty, data_json)
            VALUES (?,?,?,?,?,?,?,?,?,0,?)
            ON CONFLICT(id) DO UPDATE SET
                folder_id=excluded.folder_id,
                folder_name=excluded.folder_name,
                header_number=excluded.header_number,
                header_name=excluded.header_name,
                active=excluded.active,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                synced_at=excluded.synced_at,
                is_dirty=0,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                folder.get("id"),
                folder.get("name"),
                record.get("headerNumber"),
                record.get("headerName"),
                1 if str(record.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_materials(
    folder_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    dirty_only: bool = False,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM materials WHERE 1=1"
    params: list[Any] = []
    if folder_id:
        sql += " AND folder_id=?"
        params.append(folder_id)
    if search:
        sql += " AND (header_number LIKE ? OR header_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    if dirty_only:
        sql += " AND is_dirty=1"
    sql += " ORDER BY header_number LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_material(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM materials WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


def update_material_local(record_id: str, updated_json: dict[str, Any]) -> None:
    data_json = json.dumps(updated_json)
    with get_conn() as conn:
        conn.execute(
            """UPDATE materials SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                1 if str(updated_json.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                data_json,
                record_id,
            ),
        )


def mark_material_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE materials SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

def upsert_color(record: dict[str, Any]) -> None:
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT is_dirty, modified_at FROM colors WHERE id=?", (record["id"],)
        ).fetchone()

        if existing and existing["is_dirty"] == 1:
            remote_modified = record.get("modifiedAt", "")
            local_modified = existing["modified_at"] or ""
            if remote_modified <= local_modified:
                return

        folder = record.get("folder") or {}
        conn.execute(
            """
            INSERT INTO colors
                (id, folder_id, folder_name, header_number, header_name,
                 active, created_at, modified_at, synced_at, is_dirty, data_json)
            VALUES (?,?,?,?,?,?,?,?,?,0,?)
            ON CONFLICT(id) DO UPDATE SET
                folder_id=excluded.folder_id,
                folder_name=excluded.folder_name,
                header_number=excluded.header_number,
                header_name=excluded.header_name,
                active=excluded.active,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                synced_at=excluded.synced_at,
                is_dirty=0,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                folder.get("id"),
                folder.get("name"),
                record.get("headerNumber"),
                record.get("headerName"),
                1 if str(record.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_colors(
    folder_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    dirty_only: bool = False,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM colors WHERE 1=1"
    params: list[Any] = []
    if folder_id:
        sql += " AND folder_id=?"
        params.append(folder_id)
    if search:
        sql += " AND (header_number LIKE ? OR header_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    if dirty_only:
        sql += " AND is_dirty=1"
    sql += " ORDER BY header_number LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_color(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM colors WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


def update_color_local(record_id: str, updated_json: dict[str, Any]) -> None:
    data_json = json.dumps(updated_json)
    with get_conn() as conn:
        conn.execute(
            """UPDATE colors SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                1 if str(updated_json.get("active", "false")).lower() in ("true", "1", "yes") else 0,
                data_json,
                record_id,
            ),
        )


def mark_color_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE colors SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Directory
# ---------------------------------------------------------------------------

def upsert_directory_record(record: dict[str, Any]) -> None:
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO directory
                (id, directory_id, name, partner_type, country, active, synced_at, is_dirty, data_json)
            VALUES (?,?,?,?,?,?,?,0,?)
            ON CONFLICT(id) DO UPDATE SET
                directory_id=excluded.directory_id,
                name=excluded.name,
                partner_type=excluded.partner_type,
                country=excluded.country,
                active=excluded.active,
                synced_at=excluded.synced_at,
                is_dirty=0,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                record.get("directoryId"),
                record.get("name"),
                record.get("partnerType"),
                record.get("country"),
                1 if record.get("active", False) else 0,
                synced_at,
                data_json,
            ),
        )


def get_directory_records(
    partner_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM directory WHERE 1=1"
    params: list[Any] = []
    if partner_type:
        sql += " AND partner_type=?"
        params.append(partner_type)
    if search:
        sql += " AND (name LIKE ? OR directory_id LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    sql += " ORDER BY name LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_directory_record(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM directory WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


# ---------------------------------------------------------------------------
# Sync metadata
# ---------------------------------------------------------------------------

def get_sync_meta(entity: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM sync_meta WHERE entity=?", (entity,)
        ).fetchone()
    return _row_to_dict(row) if row else None


def set_sync_meta(entity: str, sync_type: str = "full") -> None:
    now = _now_iso()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO sync_meta (entity, last_sync_at, sync_type)
            VALUES (?, ?, ?)
            ON CONFLICT(entity) DO UPDATE SET last_sync_at=excluded.last_sync_at, sync_type=excluded.sync_type
            """,
            (entity, now, sync_type),
        )


def get_row_counts() -> dict[str, int]:
    """Return row counts for each entity table (for status display)."""
    with get_conn() as conn:
        counts: dict[str, int] = {}
        for table in ("styles", "materials", "colors", "directory"):
            row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
            counts[table] = row["c"] if row else 0
        dirty: dict[str, int] = {}
        for table in ("styles", "materials", "colors"):
            row = conn.execute(f"SELECT COUNT(*) as c FROM {table} WHERE is_dirty=1").fetchone()
            dirty[f"{table}_dirty"] = row["c"] if row else 0
    return {**counts, **dirty}
