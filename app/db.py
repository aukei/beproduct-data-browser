"""
SQLite local database layer.

Schema design: each entity table has indexed top-level fields for fast filtering/sorting
plus a full `data_json` blob that stores the complete API response. This means no
schema migration is needed when BeProduct adds custom fields to your tenant.

`is_dirty=1` marks records that have been locally edited and not yet pushed back to SaaS.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterator, Optional

from app.config import settings

logger = logging.getLogger(__name__)


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

CREATE TABLE IF NOT EXISTS images (
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
CREATE INDEX IF NOT EXISTS idx_images_folder    ON images(folder_id);
CREATE INDEX IF NOT EXISTS idx_images_modified  ON images(modified_at);
CREATE INDEX IF NOT EXISTS idx_images_dirty     ON images(is_dirty);

CREATE TABLE IF NOT EXISTS blocks (
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
CREATE INDEX IF NOT EXISTS idx_blocks_folder    ON blocks(folder_id);
CREATE INDEX IF NOT EXISTS idx_blocks_modified  ON blocks(modified_at);
CREATE INDEX IF NOT EXISTS idx_blocks_dirty     ON blocks(is_dirty);

CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    email         TEXT,
    username      TEXT,
    first_name    TEXT,
    last_name     TEXT,
    title         TEXT,
    account_type  TEXT,
    role          TEXT,
    registered_on TEXT,
    active        INTEGER,
    synced_at     TEXT,
    data_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);

CREATE TABLE IF NOT EXISTS directory (
    id            TEXT PRIMARY KEY,
    directory_id  TEXT,
    name          TEXT,
    partner_type  TEXT,
    country       TEXT,
    active        INTEGER,
    modified_at   TEXT,
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

CREATE TABLE IF NOT EXISTS data_tables (
    id            TEXT PRIMARY KEY,
    name          TEXT,
    description   TEXT,
    active        INTEGER DEFAULT 1,
    created_at    TEXT,
    modified_at   TEXT,
    synced_at     TEXT,
    data_json     TEXT
);

CREATE TABLE IF NOT EXISTS data_table_rows (
    id              TEXT PRIMARY KEY,
    data_table_id   TEXT NOT NULL,
    created_at      TEXT,
    modified_at     TEXT,
    synced_at       TEXT,
    is_dirty        INTEGER DEFAULT 0,
    data_json       TEXT,
    FOREIGN KEY (data_table_id) REFERENCES data_tables(id)
);
CREATE INDEX IF NOT EXISTS idx_dt_rows_table ON data_table_rows(data_table_id);
CREATE INDEX IF NOT EXISTS idx_dt_rows_dirty ON data_table_rows(is_dirty);
"""


def init_schema() -> None:
    """Create tables and indexes if they don't exist yet. Run migrations."""
    with get_conn() as conn:
        conn.executescript(_DDL)
        
        # Migration: add modified_at column to directory if it doesn't exist
        try:
            conn.execute("ALTER TABLE directory ADD COLUMN modified_at TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _extract_active_from_fields(record: dict[str, Any]) -> int:
    """Extract active status from headerData.fields array.
    
    Returns 1 if active field is truthy, 0 otherwise.
    """
    fields = record.get("headerData", {}).get("fields", [])
    for f in fields:
        if f.get("id") == "active":
            val = f.get("value", "")
            return 1 if str(val).lower() in ("yes", "true", "1") else 0
    return 0


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
        active = _extract_active_from_fields(record)
        
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
                active,
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
    active = _extract_active_from_fields(updated_json)
    
    with get_conn() as conn:
        conn.execute(
            """UPDATE styles SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                active,
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
        active = _extract_active_from_fields(record)
        
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
                active,
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
    active = _extract_active_from_fields(updated_json)
    
    with get_conn() as conn:
        conn.execute(
            """UPDATE materials SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                active,
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
        active = _extract_active_from_fields(record)
        
        # Color palette field names changed: colorPaletteNumber, colorPaletteName
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
                record.get("colorPaletteNumber") or record.get("headerNumber"),
                record.get("colorPaletteName") or record.get("headerName"),
                active,
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
    active = _extract_active_from_fields(updated_json)
    
    with get_conn() as conn:
        conn.execute(
            """UPDATE colors SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("colorPaletteName") or updated_json.get("headerName"),
                active,
                data_json,
                record_id,
            ),
        )


def mark_color_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE colors SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Images
# ---------------------------------------------------------------------------

def upsert_image(record: dict[str, Any]) -> None:
    """Insert or update an image record."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT is_dirty, modified_at FROM images WHERE id=?", (record["id"],)
        ).fetchone()

        if existing and existing["is_dirty"] == 1:
            remote_modified = record.get("modifiedAt", "")
            local_modified = existing["modified_at"] or ""
            if remote_modified <= local_modified:
                return

        folder = record.get("folder") or {}
        active = _extract_active_from_fields(record)
        
        conn.execute(
            """
            INSERT INTO images
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
                active,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_images(
    folder_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    dirty_only: bool = False,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM images WHERE 1=1"
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


def get_image(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM images WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


def update_image_local(record_id: str, updated_json: dict[str, Any]) -> None:
    data_json = json.dumps(updated_json)
    active = _extract_active_from_fields(updated_json)
    
    with get_conn() as conn:
        conn.execute(
            """UPDATE images SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                active,
                data_json,
                record_id,
            ),
        )


def mark_image_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE images SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------

def upsert_block(record: dict[str, Any]) -> None:
    """Insert or update a block record."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT is_dirty, modified_at FROM blocks WHERE id=?", (record["id"],)
        ).fetchone()

        if existing and existing["is_dirty"] == 1:
            remote_modified = record.get("modifiedAt", "")
            local_modified = existing["modified_at"] or ""
            if remote_modified <= local_modified:
                return

        folder = record.get("folder") or {}
        active = _extract_active_from_fields(record)
        
        conn.execute(
            """
            INSERT INTO blocks
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
                active,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_blocks(
    folder_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    dirty_only: bool = False,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM blocks WHERE 1=1"
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


def get_block(record_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM blocks WHERE id=?", (record_id,)).fetchone()
    return _row_to_dict(row) if row else None


def update_block_local(record_id: str, updated_json: dict[str, Any]) -> None:
    data_json = json.dumps(updated_json)
    active = _extract_active_from_fields(updated_json)
    
    with get_conn() as conn:
        conn.execute(
            """UPDATE blocks SET
                header_name=?, active=?, data_json=?, is_dirty=1
               WHERE id=?""",
            (
                updated_json.get("headerName"),
                active,
                data_json,
                record_id,
            ),
        )


def mark_block_clean(record_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE blocks SET is_dirty=0 WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Users (read-only metadata)
# ---------------------------------------------------------------------------

def upsert_user(record: dict[str, Any]) -> None:
    """Insert or update a user record (read-only, no dirty tracking)."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO users
                (id, email, username, first_name, last_name, title,
                 account_type, role, registered_on, active, synced_at, data_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                email=excluded.email,
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                title=excluded.title,
                account_type=excluded.account_type,
                role=excluded.role,
                registered_on=excluded.registered_on,
                active=excluded.active,
                synced_at=excluded.synced_at,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                record.get("email"),
                record.get("username"),
                record.get("firstName"),
                record.get("lastName"),
                record.get("title"),
                record.get("accountType"),
                record.get("role"),
                record.get("registerdOn"),  # Note: typo in API field name
                1 if record.get("active") else 0,
                synced_at,
                data_json,
            ),
        )


def get_users(
    search: Optional[str] = None,
    active_only: bool = False,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return users from local DB with optional filters."""
    sql = "SELECT * FROM users WHERE 1=1"
    params: list[Any] = []
    if search:
        sql += " AND (username LIKE ? OR email LIKE ? OR first_name LIKE ? OR last_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%"])
    if active_only:
        sql += " AND active=1"
    sql += " ORDER BY username LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_user(user_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    return _row_to_dict(row) if row else None


# ---------------------------------------------------------------------------
# Directory
# ---------------------------------------------------------------------------

def upsert_directory_record(record: dict[str, Any]) -> bool:
    """Insert or update a directory record. Returns True if the record was new or changed."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        # Check if record exists and if data has changed
        existing = conn.execute(
            "SELECT data_json FROM directory WHERE id=?", (record["id"],)
        ).fetchone()

        if existing:
            # Compare existing data with new data
            if existing["data_json"] == data_json:
                # No change - just update synced_at
                conn.execute(
                    "UPDATE directory SET synced_at=? WHERE id=?",
                    (synced_at, record.get("id"))
                )
                return False
            else:
                # Data changed - update record
                conn.execute(
                    """
                    UPDATE directory SET
                        directory_id=?,
                        name=?,
                        partner_type=?,
                        country=?,
                        active=?,
                        modified_at=?,
                        synced_at=?,
                        is_dirty=0,
                        data_json=?
                    WHERE id=?
                    """,
                    (
                        record.get("directoryId"),
                        record.get("name"),
                        record.get("partnerType"),
                        record.get("country"),
                        1 if record.get("active", False) else 0,
                        record.get("modifiedAt"),
                        synced_at,
                        data_json,
                        record.get("id"),
                    ),
                )
                return True
        else:
            # New record - insert
            conn.execute(
                """
                INSERT INTO directory
                    (id, directory_id, name, partner_type, country, active, modified_at, synced_at, is_dirty, data_json)
                VALUES (?,?,?,?,?,?,?,?,0,?)
                """,
                (
                    record.get("id"),
                    record.get("directoryId"),
                    record.get("name"),
                    record.get("partnerType"),
                    record.get("country"),
                    1 if record.get("active", False) else 0,
                    record.get("modifiedAt"),
                    synced_at,
                    data_json,
                ),
            )
            return True


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
        for table in ("styles", "materials", "colors", "images", "blocks", "users", "directory"):
            row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
            counts[table] = row["c"] if row else 0
        # Data tables count
        try:
            row = conn.execute("SELECT COUNT(*) as c FROM data_tables").fetchone()
            counts["data_tables"] = row["c"] if row else 0
        except Exception:
            counts["data_tables"] = 0
        dirty: dict[str, int] = {}
        for table in ("styles", "materials", "colors", "images", "blocks"):
            row = conn.execute(f"SELECT COUNT(*) as c FROM {table} WHERE is_dirty=1").fetchone()
            dirty[f"{table}_dirty"] = row["c"] if row else 0
    return {**counts, **dirty}


# ---------------------------------------------------------------------------
# Delete operations
# ---------------------------------------------------------------------------

def delete_style(record_id: str) -> None:
    """Delete a style from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM styles WHERE id=?", (record_id,))


def delete_material(record_id: str) -> None:
    """Delete a material from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM materials WHERE id=?", (record_id,))


def delete_color(record_id: str) -> None:
    """Delete a color palette from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM colors WHERE id=?", (record_id,))


def delete_image(record_id: str) -> None:
    """Delete an image from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM images WHERE id=?", (record_id,))


def delete_block(record_id: str) -> None:
    """Delete a block from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM blocks WHERE id=?", (record_id,))


# ---------------------------------------------------------------------------
# Cross-reference query helpers
# ---------------------------------------------------------------------------

def get_colorways_referencing_color(color_source_id: str) -> list[dict[str, Any]]:
    """
    Find all styles/materials whose colorways reference a specific color by colorSourceId.
    Returns list of dicts: {entity_type, entity_id, header_number, colorway_count}
    """
    results = []
    
    with get_conn() as conn:
        # Search styles
        style_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM styles
            WHERE data_json LIKE ?
            """,
            (f'%"colorSourceId":"{color_source_id}"%',),
        ).fetchall()
        
        for row in style_rows:
            try:
                data = json.loads(row["data_json"])
                colorway_count = sum(
                    1 for cw in data.get("colorways", [])
                    if cw.get("colorSourceId") == color_source_id
                )
                results.append({
                    "entity_type": "Style",
                    "entity_id": row["id"],
                    "header_number": row["header_number"],
                    "colorway_count": colorway_count,
                })
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Search materials
        material_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM materials
            WHERE data_json LIKE ?
            """,
            (f'%"colorSourceId":"{color_source_id}"%',),
        ).fetchall()
        
        for row in material_rows:
            try:
                data = json.loads(row["data_json"])
                colorway_count = sum(
                    1 for cw in data.get("colorways", [])
                    if cw.get("colorSourceId") == color_source_id
                )
                results.append({
                    "entity_type": "Material",
                    "entity_id": row["id"],
                    "header_number": row["header_number"],
                    "colorway_count": colorway_count,
                })
            except (json.JSONDecodeError, KeyError):
                pass
    
    return results


def get_colorways_referencing_image(image_header_id: str) -> list[dict[str, Any]]:
    """
    Find all styles/materials whose colorways reference a specific image by imageHeaderId.
    Returns list of dicts: {entity_type, entity_id, header_number, colorway_count}
    """
    results = []
    
    with get_conn() as conn:
        # Search styles
        style_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM styles
            WHERE data_json LIKE ?
            """,
            (f'%"imageHeaderId":"{image_header_id}"%',),
        ).fetchall()
        
        for row in style_rows:
            try:
                data = json.loads(row["data_json"])
                colorway_count = sum(
                    1 for cw in data.get("colorways", [])
                    if cw.get("imageHeaderId") == image_header_id
                )
                results.append({
                    "entity_type": "Style",
                    "entity_id": row["id"],
                    "header_number": row["header_number"],
                    "colorway_count": colorway_count,
                })
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Search materials
        material_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM materials
            WHERE data_json LIKE ?
            """,
            (f'%"imageHeaderId":"{image_header_id}"%',),
        ).fetchall()
        
        for row in material_rows:
            try:
                data = json.loads(row["data_json"])
                colorway_count = sum(
                    1 for cw in data.get("colorways", [])
                    if cw.get("imageHeaderId") == image_header_id
                )
                results.append({
                    "entity_type": "Material",
                    "entity_id": row["id"],
                    "header_number": row["header_number"],
                    "colorway_count": colorway_count,
                })
            except (json.JSONDecodeError, KeyError):
                pass
    
    return results


# ---------------------------------------------------------------------------
# Data Tables
# ---------------------------------------------------------------------------

def upsert_data_table(record: dict[str, Any]) -> None:
    """Insert or update a data table metadata record."""
    synced_at = _now_iso()
    data_json = json.dumps(record)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO data_tables
                (id, name, description, active, created_at, modified_at, synced_at, data_json)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                description=excluded.description,
                active=excluded.active,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                synced_at=excluded.synced_at,
                data_json=excluded.data_json
            """,
            (
                record.get("id"),
                record.get("name"),
                record.get("description"),
                1 if record.get("active", True) else 0,
                record.get("createdAt"),
                record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_data_tables(
    search: Optional[str] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return data tables from local DB."""
    sql = "SELECT * FROM data_tables WHERE 1=1"
    params: list[Any] = []
    if search:
        sql += " AND (name LIKE ? OR description LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    sql += " ORDER BY name LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_data_table(table_id: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM data_tables WHERE id=?", (table_id,)).fetchone()
    return _row_to_dict(row) if row else None


def upsert_data_table_row(table_id: str, row_record: dict[str, Any]) -> None:
    """Insert or update a data table row."""
    synced_at = _now_iso()
    data_json = json.dumps(row_record)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO data_table_rows
                (id, data_table_id, created_at, modified_at, synced_at, is_dirty, data_json)
            VALUES (?,?,?,?,?,0,?)
            ON CONFLICT(id) DO UPDATE SET
                data_table_id=excluded.data_table_id,
                created_at=excluded.created_at,
                modified_at=excluded.modified_at,
                synced_at=excluded.synced_at,
                is_dirty=0,
                data_json=excluded.data_json
            """,
            (
                row_record.get("id"),
                table_id,
                row_record.get("createdAt"),
                row_record.get("modifiedAt"),
                synced_at,
                data_json,
            ),
        )


def get_data_table_rows(
    table_id: str,
    dirty_only: bool = False,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return rows for a specific data table."""
    sql = "SELECT * FROM data_table_rows WHERE data_table_id=?"
    params: list[Any] = [table_id]
    if dirty_only:
        sql += " AND is_dirty=1"
    sql += " LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_data_table_row_local(row_id: str, updated_json: dict[str, Any]) -> None:
    """Mark a data table row as dirty and update its JSON."""
    data_json = json.dumps(updated_json)
    with get_conn() as conn:
        conn.execute(
            "UPDATE data_table_rows SET data_json=?, is_dirty=1 WHERE id=?",
            (data_json, row_id),
        )


def mark_data_table_row_clean(row_id: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE data_table_rows SET is_dirty=0 WHERE id=?", (row_id,))


def delete_data_table_row(row_id: str) -> None:
    """Delete a data table row from local DB."""
    with get_conn() as conn:
        conn.execute("DELETE FROM data_table_rows WHERE id=?", (row_id,))


def get_entities_by_partner(directory_id: str) -> list[dict[str, Any]]:
    """
    Find all styles/materials that reference a specific directory partner.
    Returns list of dicts: {entity_type, entity_id, header_number, field_name}
    """
    results = []
    
    with get_conn() as conn:
        # Search styles
        style_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM styles
            """,
        ).fetchall()
        
        for row in style_rows:
            try:
                data = json.loads(row["data_json"])
                fields_list = data.get("headerData", {}).get("fields", [])
                for field in fields_list:
                    if field.get("type") == "PartnerDropDown" and field.get("value") == directory_id:
                        results.append({
                            "entity_type": "Style",
                            "entity_id": row["id"],
                            "header_number": row["header_number"],
                            "field_name": field.get("name", field.get("id")),
                        })
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Search materials
        material_rows = conn.execute(
            """
            SELECT id, header_number, data_json FROM materials
            """,
        ).fetchall()
        
        for row in material_rows:
            try:
                data = json.loads(row["data_json"])
                fields_list = data.get("headerData", {}).get("fields", [])
                for field in fields_list:
                    if field.get("type") == "PartnerDropDown" and field.get("value") == directory_id:
                        results.append({
                            "entity_type": "Material",
                            "entity_id": row["id"],
                            "header_number": row["header_number"],
                            "field_name": field.get("name", field.get("id")),
                        })
            except (json.JSONDecodeError, KeyError):
                pass
    
    return results
