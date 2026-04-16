"""
BeProduct → Databricks Upload Helper
=====================================

Uploads a local BeProduct SQLite table to an Azure Databricks Delta table via
the Databricks SQL Connector, authenticated with a Personal Access Token.

Prerequisites
-------------
    pip install -r requirements-scripts.txt          # databricks-sql-connector

Environment variables (add to .env):
    DATABRICKS_HOST      = https://adb-XXXXXXXX.azuredatabricks.net
    DATABRICKS_PAT       = dapi...
    DATABRICKS_HTTP_PATH = /sql/1.0/warehouses/<id>

Usage
-----
    python scripts/upload_to_databricks.py

The script is fully interactive — it will prompt for the model, destination
catalog/schema/table, and how to handle an existing table (overwrite/append).
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Optional

# ── Ensure project root is importable ────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")


# ─────────────────────────────────────────────────────────────────────────────
# ANSI colour helpers (degrade gracefully if terminal doesn't support)
# ─────────────────────────────────────────────────────────────────────────────

def _c(code: str, text: str) -> str:
    """Wrap text in an ANSI escape code (no-op if NO_COLOR is set)."""
    if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


OK   = lambda t: _c("32",   t)   # green     ✅ success
WARN = lambda t: _c("33",   t)   # yellow    ⚠  warning
ERR  = lambda t: _c("31",   t)   # red       ✗  error
INFO = lambda t: _c("36",   t)   # cyan      ─  info
BOLD = lambda t: _c("1",    t)   # bold


# ─────────────────────────────────────────────────────────────────────────────
# MODEL METADATA
# ─────────────────────────────────────────────────────────────────────────────

# Maps menu index → (display_name, sqlite_table_name)
MODELS: list[tuple[str, str]] = [
    ("Styles",    "styles"),
    ("Materials", "materials"),
    ("Colors",    "colors"),
    ("Images",    "images"),
    ("Blocks",    "blocks"),
    ("Directory", "directory"),
    ("Users",     "users"),
]

# DDL for each entity.  SQLite TEXT → STRING, SQLite INTEGER → BIGINT.
# All tables are created as Delta tables (USING DELTA).
_SHARED_DDL = """
    id            STRING,
    folder_id     STRING,
    folder_name   STRING,
    header_number STRING,
    header_name   STRING,
    active        BIGINT,
    created_at    STRING,
    modified_at   STRING,
    synced_at     STRING,
    is_dirty      BIGINT,
    data_json     STRING
""".strip()

_ENTITY_COLUMNS: dict[str, str] = {
    "styles":    _SHARED_DDL,
    "materials": _SHARED_DDL,
    "colors":    _SHARED_DDL,
    "images":    _SHARED_DDL,
    "blocks":    _SHARED_DDL,
    "directory": """
    id            STRING,
    directory_id  STRING,
    name          STRING,
    partner_type  STRING,
    country       STRING,
    active        BIGINT,
    modified_at   STRING,
    synced_at     STRING,
    is_dirty      BIGINT,
    data_json     STRING
""".strip(),
    "users": """
    id            STRING,
    email         STRING,
    username      STRING,
    first_name    STRING,
    last_name     STRING,
    title         STRING,
    account_type  STRING,
    role          STRING,
    registered_on STRING,
    active        BIGINT,
    synced_at     STRING,
    data_json     STRING
""".strip(),
}


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def _load_config() -> dict[str, str]:
    """Load and validate required env vars.  Aborts on missing values."""
    required = {
        "DATABRICKS_HOST":      "Databricks workspace URL",
        "DATABRICKS_PAT":       "Personal Access Token",
        "DATABRICKS_HTTP_PATH": "SQL Warehouse HTTP path",
    }
    cfg: dict[str, str] = {}
    missing: list[str] = []

    for key, label in required.items():
        val = os.getenv(key, "").strip()
        if not val or val.startswith("your_"):
            missing.append(f"  {key:<30} # {label}")
        else:
            cfg[key] = val

    if missing:
        print(ERR("✗ Missing Databricks configuration in .env:\n"))
        for m in missing:
            print(ERR(m))
        print(f"\nAdd the above to {_ROOT / '.env'} then retry.")
        print("See .env.example for reference.")
        sys.exit(1)

    # Normalise host: strip trailing slash, ensure no trailing path
    host = cfg["DATABRICKS_HOST"].rstrip("/")
    if host.startswith("https://"):
        cfg["_hostname"] = host[len("https://"):]
    elif host.startswith("http://"):
        cfg["_hostname"] = host[len("http://"):]
    else:
        cfg["_hostname"] = host

    # Resolve local DB path from project .env / default
    db_path_str = os.getenv("DB_PATH", "data/beproduct.db")
    cfg["_db_path"] = str(_ROOT / db_path_str)

    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# DATABRICKS CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

def _connect(cfg: dict[str, str]):
    """Open a Databricks SQL connection. Returns (connection, cursor)."""
    try:
        from databricks import sql as dbsql  # type: ignore[import]
    except ImportError:
        print(ERR("✗ 'databricks-sql-connector' is not installed."))
        print("  Run:  pip install -r requirements-scripts.txt")
        sys.exit(1)

    print(INFO(f"Connecting to {cfg['DATABRICKS_HOST']} …"))
    try:
        conn = dbsql.connect(
            server_hostname=cfg["_hostname"],
            http_path=cfg["DATABRICKS_HTTP_PATH"],
            access_token=cfg["DATABRICKS_PAT"],
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as exc:
        _handle_db_error("Connection failed", exc)


def _verify_connection(cursor) -> dict[str, str]:
    """Run SELECT CURRENT_USER() and return identity info."""
    cursor.execute("SELECT CURRENT_USER(), CURRENT_CATALOG()")
    row = cursor.fetchone()
    return {"user": row[0] or "unknown", "catalog": row[1] or "unknown"}


def _handle_db_error(context: str, exc: Exception) -> None:
    """Parse Databricks exception text into a human-readable message, then exit."""
    msg = str(exc)
    if "PERMISSION_DENIED" in msg or "PermissionDenied" in msg or "403" in msg:
        print(ERR(f"✗ {context}: Permission denied."))
        print("  You may be missing one of: USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY")
    elif "SCHEMA_NOT_FOUND" in msg or "schema" in msg.lower() and "not found" in msg.lower():
        print(ERR(f"✗ {context}: Schema not found."))
    elif "TABLE_OR_VIEW_NOT_FOUND" in msg or "table" in msg.lower() and "not found" in msg.lower():
        print(ERR(f"✗ {context}: Table not found."))
    elif "Invalid access token" in msg or "401" in msg or "Unauthorized" in msg:
        print(ERR(f"✗ {context}: Authentication failed — check DATABRICKS_PAT in .env."))
    elif "ConnectTimeout" in msg or "connection" in msg.lower():
        print(ERR(f"✗ {context}: Could not reach Databricks."))
        print("  Check DATABRICKS_HOST and DATABRICKS_HTTP_PATH in .env.")
    else:
        print(ERR(f"✗ {context}: {exc}"))
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL SQLITE
# ─────────────────────────────────────────────────────────────────────────────

def _local_counts(db_path: str) -> dict[str, int]:
    """Return row count per table from the local SQLite database."""
    counts: dict[str, int] = {}
    if not Path(db_path).exists():
        return counts
    try:
        conn = sqlite3.connect(db_path)
        for _, table in MODELS:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = row[0] if row else 0
            except sqlite3.OperationalError:
                counts[table] = 0
        conn.close()
    except Exception:
        pass
    return counts


def _get_local_records(db_path: str, table: str) -> tuple[list[str], list[tuple]]:
    """Fetch all rows from a local SQLite table.

    Returns (column_names, rows).
    """
    if not Path(db_path).exists():
        print(ERR(f"✗ Local database not found: {db_path}"))
        print("  Run the Streamlit app and perform a sync first.")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(f"SELECT * FROM {table}")
        rows_raw = cursor.fetchall()
        if not rows_raw:
            conn.close()
            return [], []
        columns = list(rows_raw[0].keys())
        rows = [tuple(r) for r in rows_raw]
        conn.close()
        return columns, rows
    except sqlite3.OperationalError as exc:
        conn.close()
        print(ERR(f"✗ Could not read local table '{table}': {exc}"))
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# USER PROMPTS
# ─────────────────────────────────────────────────────────────────────────────

def _prompt(question: str, default: str = "") -> str:
    """Prompt the user for input, showing an optional default value."""
    if default:
        suffix = f" [{default}]: "
    else:
        suffix = ": "
    try:
        answer = input(f"{question}{suffix}").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    return answer if answer else default


def _prompt_choice(question: str, choices: list[str]) -> str:
    """Prompt until user enters one of the given choices (case-insensitive)."""
    choices_upper = [c.upper() for c in choices]
    display = "/".join(f"[{c}]" for c in choices)
    while True:
        try:
            answer = input(f"{question} {display}: ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if answer in choices_upper:
            return answer
        print(f"  Please enter one of: {', '.join(choices)}")


def _select_model(counts: dict[str, int]) -> tuple[str, str]:
    """Display model menu and return (display_name, sqlite_table_name)."""
    print()
    print(BOLD("Select model to upload:"))
    for i, (name, table) in enumerate(MODELS, 1):
        count = counts.get(table, 0)
        count_str = f"{count:>6,}" if count else "    --"
        marker = OK("✓") if count > 0 else WARN("·")
        print(f"  {marker}  {i}.  {name:<12}  ({count_str} records locally)")

    print()
    while True:
        raw = _prompt(f"Enter model number [1-{len(MODELS)}]")
        try:
            idx = int(raw)
            if 1 <= idx <= len(MODELS):
                return MODELS[idx - 1]
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(MODELS)}.")


_VALID_NAME = re.compile(r"^[a-z_][a-z0-9_.]*$")


def _validate_identifier(name: str, what: str) -> str:
    """Ensure a catalog/schema/table name is safe to use in SQL."""
    name = name.strip().lower()
    if not name:
        print(ERR(f"✗ {what} name cannot be empty."))
        sys.exit(1)
    if not _VALID_NAME.match(name):
        print(ERR(f"✗ {what} name '{name}' contains invalid characters."))
        print("  Use only: lowercase letters, digits, underscores.")
        sys.exit(1)
    return name


def _get_destination() -> tuple[str, str, str]:
    """Prompt for catalog, schema, table and return validated identifiers."""
    print()
    print(BOLD("Destination (Unity Catalog):"))
    catalog = _validate_identifier(_prompt("  Catalog", "main"), "Catalog")
    schema  = _validate_identifier(_prompt("  Schema"), "Schema")
    table   = _validate_identifier(_prompt("  Table"), "Table")
    return catalog, schema, table


# ─────────────────────────────────────────────────────────────────────────────
# PRIVILEGE CHECK
# ─────────────────────────────────────────────────────────────────────────────

def _check_privileges(
    cursor, catalog: str, schema: str, table: str
) -> dict[str, Any]:
    """
    Run pre-flight checks for catalog/schema/table access.

    Returns a dict with keys:
        catalog_ok      bool
        schema_ok       bool   (False = schema doesn't exist yet)
        table_exists    bool
        row_count       int    (0 if table doesn't exist)
    """
    result = {
        "catalog_ok":   False,
        "schema_ok":    False,
        "table_exists": False,
        "row_count":    0,
    }

    print()
    print(BOLD("── Privilege Check ──────────────────────────────────────"))

    # 1. Catalog visibility
    try:
        cursor.execute(f"SHOW CATALOGS LIKE '{catalog}'")
        rows = cursor.fetchall()
        if rows:
            result["catalog_ok"] = True
            print(OK(f"  ✅ Catalog '{catalog}' is accessible"))
        else:
            print(WARN(f"  ⚠  Catalog '{catalog}' not found — check the name or"
                       " request USE CATALOG privilege"))
            return result
    except Exception as exc:
        print(WARN(f"  ⚠  Could not list catalogs: {exc}"))
        print("     Continuing — catalog check will be implicit during transfer.")
        result["catalog_ok"] = True  # assume ok, real error surfaces later

    # 2. Schema visibility
    try:
        cursor.execute(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
        rows = cursor.fetchall()
        if rows:
            result["schema_ok"] = True
            print(OK(f"  ✅ Schema '{catalog}.{schema}' is accessible"))
        else:
            result["schema_ok"] = False
            print(WARN(f"  ⚠  Schema '{catalog}.{schema}' not found — will attempt"
                       " to create it during upload"))
    except Exception as exc:
        print(WARN(f"  ⚠  Could not list schemas: {exc}"))
        result["schema_ok"] = False  # treat as missing

    # 3. Table existence + read access
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        row = cursor.fetchone()
        table_found = (row and row[0] > 0) if row else False
    except Exception:
        # Fallback: SHOW TABLES
        try:
            cursor.execute(f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'")
            rows = cursor.fetchall()
            table_found = len(rows) > 0
        except Exception:
            table_found = False

    if table_found:
        result["table_exists"] = True
        # Try to count rows (confirms SELECT privilege)
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.`{table}`")
            cnt_row = cursor.fetchone()
            result["row_count"] = cnt_row[0] if cnt_row else 0
            print(WARN(
                f"  ⚠  Table '{catalog}.{schema}.{table}' already exists "
                f"({result['row_count']:,} rows)"
            ))
        except Exception as exc:
            result["row_count"] = -1
            print(WARN(
                f"  ⚠  Table '{catalog}.{schema}.{table}' exists but row count "
                f"failed (SELECT privilege may be missing): {exc}"
            ))
    else:
        print(INFO(f"  ─  Table '{catalog}.{schema}.{table}' does not exist yet — will be created"))

    # 4. Advisory note about write privileges
    print(INFO(
        "  ─  Write access will be verified at transfer start.\n"
        "     Required privileges: CREATE TABLE, MODIFY (on the schema)"
    ))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────

def _build_ddl(catalog: str, schema: str, table: str, entity: str) -> str:
    """Return the CREATE TABLE ... USING DELTA statement for the given entity."""
    columns = _ENTITY_COLUMNS.get(entity, _SHARED_DDL)
    return (
        f"CREATE TABLE `{catalog}`.`{schema}`.`{table}` (\n"
        f"    {columns.strip()}\n"
        f") USING DELTA"
    )


# ─────────────────────────────────────────────────────────────────────────────
# VALUE ESCAPING
# ─────────────────────────────────────────────────────────────────────────────

def _escape_value(v: Any) -> str:
    """Convert a Python value to a SQL literal safe for inline INSERT."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    # String: escape single quotes, wrap in '...'
    escaped = str(v).replace("'", "''").replace("\\", "\\\\")
    return f"'{escaped}'"


def _build_insert_batch(
    catalog: str,
    schema: str,
    table: str,
    columns: list[str],
    batch: list[tuple],
) -> str:
    """Build a single multi-row INSERT statement for a batch of rows."""
    col_list = ", ".join(f"`{c}`" for c in columns)
    value_rows = []
    for row in batch:
        vals = ", ".join(_escape_value(v) for v in row)
        value_rows.append(f"    ({vals})")
    values_clause = ",\n".join(value_rows)
    return (
        f"INSERT INTO `{catalog}`.`{schema}`.`{table}` ({col_list})\nVALUES\n"
        + values_clause
    )


# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────────────────────────────────────

def _upload(
    cursor,
    catalog: str,
    schema: str,
    table: str,
    columns: list[str],
    rows: list[tuple],
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    Insert rows in batches.  Returns (uploaded_count, failed_count).
    On batch failure the error is logged and upload continues.
    """
    total = len(rows)
    uploaded = 0
    failed = 0
    start = time.time()

    for batch_start in range(0, total, batch_size):
        batch = rows[batch_start: batch_start + batch_size]
        try:
            sql = _build_insert_batch(catalog, schema, table, columns, batch)
            cursor.execute(sql)
            uploaded += len(batch)
        except Exception as exc:
            failed += len(batch)
            batch_end = min(batch_start + batch_size, total)
            err_short = str(exc)[:120]
            print(WARN(f"\n  ⚠  Batch rows {batch_start+1}–{batch_end} failed: {err_short}"))

        # Progress
        done = uploaded + failed
        pct = int(done / total * 28)
        bar = "█" * pct + "░" * (28 - pct)
        elapsed = time.time() - start
        rate = done / elapsed if elapsed > 0 else 0
        print(
            f"\r  [{bar}] {done:>{len(str(total))}}/{total}  "
            f"({rate:.0f} rows/s)    ",
            end="",
            flush=True,
        )

    print()  # newline after progress bar
    return uploaded, failed


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # ── 1. Config ────────────────────────────────────────────────────────────
    cfg = _load_config()

    print()
    print(BOLD("═" * 54))
    print(BOLD("  BeProduct  →  Databricks Upload Helper"))
    print(BOLD("═" * 54))

    # ── 2. Connect ───────────────────────────────────────────────────────────
    conn, cursor = _connect(cfg)
    try:
        identity = _verify_connection(cursor)
    except Exception as exc:
        _handle_db_error("Could not verify connection", exc)

    print(OK(f"  ✅ Connected as: {identity['user']}"))
    print(INFO(f"  ─  Current catalog: {identity['catalog']}"))

    # ── 3. Select model ──────────────────────────────────────────────────────
    counts = _local_counts(cfg["_db_path"])
    display_name, entity = _select_model(counts)

    local_count = counts.get(entity, 0)
    if local_count == 0:
        print(WARN(f"\n⚠  Local table '{entity}' is empty — nothing to upload."))
        print("   Run the Streamlit app and perform a Full Sync first.")
        cursor.close()
        conn.close()
        sys.exit(0)

    # ── 4. Destination ───────────────────────────────────────────────────────
    catalog, schema, table = _get_destination()
    full_name = f"`{catalog}`.`{schema}`.`{table}`"
    print()
    print(INFO(f"  Target: {catalog}.{schema}.{table}"))

    # ── 5. Privilege check ───────────────────────────────────────────────────
    pcheck = _check_privileges(cursor, catalog, schema, table)

    if not pcheck["catalog_ok"]:
        print(ERR(f"\n✗ Catalog '{catalog}' is not accessible. Aborting."))
        cursor.close()
        conn.close()
        sys.exit(1)

    # ── 6. Existence check + overwrite/append decision ────────────────────────
    mode = "create"  # create | overwrite | append
    if pcheck["table_exists"]:
        print()
        row_info = (
            f"({pcheck['row_count']:,} rows)" if pcheck["row_count"] >= 0
            else "(row count unavailable)"
        )
        print(WARN(
            f"⚠  Table {catalog}.{schema}.{table} already exists {row_info}."
        ))
        choice = _prompt_choice(
            "\n  Action?",
            ["O", "A", "C"],
        )
        # Annotate choices
        print(f"     O = Overwrite (drop + recreate)   A = Append   C = Cancel")
        choice = choice.upper()
        if choice == "C":
            print("  Cancelled.")
            cursor.close()
            conn.close()
            sys.exit(0)
        elif choice == "O":
            mode = "overwrite"
        else:
            mode = "append"
    
    # Re-prompt after annotation for clarity when choices aren't obvious
    if pcheck["table_exists"]:
        print()
        if mode == "overwrite":
            print(WARN("  Mode: Overwrite — existing table will be dropped and recreated."))
        else:
            print(INFO("  Mode: Append — rows will be added to the existing table."))

    # ── 7. Fetch local data ──────────────────────────────────────────────────
    print()
    print(INFO(f"  Fetching {local_count:,} rows from local '{entity}' table…"))
    columns, rows = _get_local_records(cfg["_db_path"], entity)

    if not rows:
        print(WARN("  Local table is empty. Nothing to upload."))
        cursor.close()
        conn.close()
        sys.exit(0)

    actual_count = len(rows)
    print(OK(f"  ✅ Loaded {actual_count:,} rows ({len(columns)} columns)"))

    # ── 8. Create schema + table ─────────────────────────────────────────────
    print()
    print(BOLD("── Schema / Table Setup ─────────────────────────────────"))

    if not pcheck["schema_ok"]:
        print(INFO(f"  Creating schema {catalog}.{schema}…"))
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(OK(f"  ✅ Schema created: {catalog}.{schema}"))
        except Exception as exc:
            _handle_db_error(f"Could not create schema '{catalog}.{schema}'", exc)

    if mode == "overwrite":
        print(WARN(f"  Dropping existing table {catalog}.{schema}.{table}…"))
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
            print(OK(f"  ✅ Table dropped"))
        except Exception as exc:
            _handle_db_error("Could not drop table", exc)

    if mode in ("create", "overwrite"):
        ddl = _build_ddl(catalog, schema, table, entity)
        print(INFO(f"  Creating Delta table {catalog}.{schema}.{table}…"))
        try:
            cursor.execute(ddl)
            print(OK(f"  ✅ Table created"))
        except Exception as exc:
            _handle_db_error("Could not create table", exc)
    else:
        # Append: keep the table as-is, but ensure DDL is compatible by trying CREATE IF NOT EXISTS
        ddl = f"CREATE TABLE IF NOT EXISTS {full_name} (\n    {_ENTITY_COLUMNS[entity].strip()}\n) USING DELTA"
        try:
            cursor.execute(ddl)
            print(OK(f"  ✅ Table schema verified"))
        except Exception:
            # Table exists from a prior non-delta create or schema mismatch — just proceed
            print(WARN(f"  ⚠  Skipping DDL check on existing table (will append directly)"))

    # ── 9. Upload ────────────────────────────────────────────────────────────
    print()
    print(BOLD("── Data Transfer ────────────────────────────────────────"))
    print(INFO(f"  Uploading {actual_count:,} rows in batches of 500…"))

    t_start = time.time()
    uploaded, failed = _upload(cursor, catalog, schema, table, columns, rows)
    elapsed = time.time() - t_start

    # ── 10. Summary ──────────────────────────────────────────────────────────
    print()
    print(BOLD("── Summary ──────────────────────────────────────────────"))

    if failed == 0:
        print(OK(f"  ✅ Upload complete"))
    else:
        print(WARN(f"  ⚠  Upload completed with errors"))

    print(f"  Table    : {catalog}.{schema}.{table}")
    print(f"  Uploaded : {OK(str(uploaded))} / {actual_count} rows")
    if failed > 0:
        print(f"  Failed   : {ERR(str(failed))} rows  (see warnings above)")
    print(f"  Duration : {elapsed:.1f} seconds")
    print()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
