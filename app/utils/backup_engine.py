"""
Backup engine: connects to external databases and produces a backup file.

Supported engines:
  - MySQL      → .sql  (CREATE TABLE + INSERT statements via pymysql)
  - PostgreSQL → .sql  (CREATE TABLE + INSERT statements via psycopg2)
  - MongoDB    → .json (collection export via pymongo)

No external CLI tools (mysqldump, pg_dump, mongodump) are required.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Backup files are written to DB/ at project root
_BACKUP_DIR = Path(__file__).resolve().parents[2] / "DB"


@dataclass
class BackupResult:
    success: bool
    message: str
    file_name: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None   # bytes (compressed size after upload step)
    compression: str = "none"        # none | gzip
    original_file_name: Optional[str] = None
    original_file_size: Optional[int] = None


@dataclass
class RestoreResult:
    success: bool
    message: str


# ── helpers ──────────────────────────────────────────────────────────

def _ensure_backup_dir() -> Path:
    _BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return _BACKUP_DIR


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _iter_sql_statements(sql: str) -> list[str]:
    """Split SQL text into executable statements while skipping comment-only chunks."""
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                current.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            # Preserve escaped '' inside strings.
            if in_single and nxt == "'":
                current.extend([ch, nxt])
                i += 2
                continue
            in_single = not in_single
            current.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single and not in_double:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


# ── MySQL ─────────────────────────────────────────────────────────────

def _backup_mysql(
    host: str, port: int, database: str,
    username: str, password: str,
    out_path: Path,
) -> None:
    import pymysql

    conn = pymysql.connect(
        host=host, port=port,
        user=username, password=password,
        database=database,
        connect_timeout=10,
        charset="utf8mb4",
    )
    cur = conn.cursor()

    lines: list[str] = [
        f"-- MySQL backup of `{database}`",
        f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
        "-- -----------------------------------------------------\n",
        "SET FOREIGN_KEY_CHECKS=0;\n",
    ]

    cur.execute("SHOW TABLES;")
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        # CREATE TABLE statement
        cur.execute(f"SHOW CREATE TABLE `{table}`;")
        create_sql = cur.fetchone()[1]
        lines.append(f"DROP TABLE IF EXISTS `{table}`;")
        lines.append(create_sql + ";\n")

        # Data rows
        cur.execute(f"SELECT * FROM `{table}`;")
        rows = cur.fetchall()
        if rows:
            col_names = [desc[0] for desc in cur.description]
            cols = ", ".join(f"`{c}`" for c in col_names)
            for row in rows:
                vals = []
                for v in row:
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        vals.append(f"'{escaped}'")
                lines.append(
                    f"INSERT INTO `{table}` ({cols}) VALUES ({', '.join(vals)});"
                )
        lines.append("")

    lines.append("SET FOREIGN_KEY_CHECKS=1;\n")

    cur.close()
    conn.close()

    out_path.write_text("\n".join(lines), encoding="utf-8")


async def backup_mysql(
    host: str, port: int, database: str,
    username: str, password: str,
) -> BackupResult:
    backup_dir = _ensure_backup_dir()
    file_name = f"mysql_{database}_{_timestamp()}.sql"
    file_path = backup_dir / file_name
    try:
        await asyncio.to_thread(
            _backup_mysql, host, port, database, username, password, file_path
        )
        return BackupResult(
            success=True,
            message=f"MySQL backup of '{database}' completed successfully.",
            file_name=file_name,
            file_path=str(file_path),
            file_size=file_path.stat().st_size,
            compression="none",
            original_file_name=file_name,
            original_file_size=file_path.stat().st_size,
        )
    except Exception as e:
        return BackupResult(success=False, message=str(e))


# ── PostgreSQL ────────────────────────────────────────────────────────

def _backup_postgresql(
    host: str, port: int, database: str,
    username: str, password: str,
    out_path: Path,
) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=host, port=port, dbname=database,
        user=username, password=password,
        connect_timeout=10,
    )
    cur = conn.cursor()

    lines: list[str] = [
        f"-- PostgreSQL backup of \"{database}\"",
        f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
        "-- -----------------------------------------------------\n",
    ]

    # List all user tables in public schema
    cur.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
    )
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        # Column definitions
        cur.execute(
            """
            SELECT column_name, data_type,
                   character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table,),
        )
        columns = cur.fetchall()

        col_defs = []
        for col_name, data_type, char_max, nullable, default in columns:
            serial_type = None
            if default and "nextval(" in str(default):
                if data_type == "integer":
                    serial_type = "serial"
                elif data_type == "bigint":
                    serial_type = "bigserial"
                elif data_type == "smallint":
                    serial_type = "smallserial"

            col_def = f"  {col_name} {serial_type or data_type}"
            if char_max:
                col_def += f"({char_max})"
            if default is not None and not serial_type:
                col_def += f" DEFAULT {default}"
            if nullable == "NO":
                col_def += " NOT NULL"
            col_defs.append(col_def)

        lines.append(f'DROP TABLE IF EXISTS "{table}" CASCADE;')
        lines.append(f'CREATE TABLE "{table}" (')
        lines.append(",\n".join(col_defs))
        lines.append(");\n")

        # Data rows
        cur.execute(f'SELECT * FROM "{table}";')
        rows = cur.fetchall()
        if rows:
            col_names = [desc[0] for desc in cur.description]
            cols = ", ".join(f'"{c}"' for c in col_names)
            for row in rows:
                vals = []
                for v in row:
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    else:
                        escaped = str(v).replace("'", "''")
                        vals.append(f"'{escaped}'")
                lines.append(
                    f'INSERT INTO "{table}" ({cols}) VALUES ({", ".join(vals)});'
                )
        lines.append("")

    cur.close()
    conn.close()

    out_path.write_text("\n".join(lines), encoding="utf-8")


async def backup_postgresql(
    host: str, port: int, database: str,
    username: str, password: str,
) -> BackupResult:
    backup_dir = _ensure_backup_dir()
    file_name = f"postgresql_{database}_{_timestamp()}.sql"
    file_path = backup_dir / file_name
    try:
        await asyncio.to_thread(
            _backup_postgresql, host, port, database, username, password, file_path
        )
        return BackupResult(
            success=True,
            message=f"PostgreSQL backup of '{database}' completed successfully.",
            file_name=file_name,
            file_path=str(file_path),
            file_size=file_path.stat().st_size,
            compression="none",
            original_file_name=file_name,
            original_file_size=file_path.stat().st_size,
        )
    except Exception as e:
        return BackupResult(success=False, message=str(e))


# ── MongoDB ───────────────────────────────────────────────────────────

def _backup_mongodb(
    host: str, port: int, database: str,
    username: str, password: str,
    out_path: Path,
) -> None:
    from pymongo import MongoClient
    from bson import json_util as bson_json_util

    uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db = client[database]

    backup_data: dict = {}
    for col_name in db.list_collection_names():
        docs = list(db[col_name].find())
        # Convert ObjectId / bson types to JSON-safe types
        serialisable = json.loads(bson_json_util.dumps(docs))
        backup_data[col_name] = serialisable

    client.close()

    out_path.write_text(
        json.dumps(backup_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


async def backup_mongodb(
    host: str, port: int, database: str,
    username: str, password: str,
) -> BackupResult:
    backup_dir = _ensure_backup_dir()
    file_name = f"mongodb_{database}_{_timestamp()}.json"
    file_path = backup_dir / file_name
    try:
        await asyncio.to_thread(
            _backup_mongodb, host, port, database, username, password, file_path
        )
        return BackupResult(
            success=True,
            message=f"MongoDB backup of '{database}' completed successfully.",
            file_name=file_name,
            file_path=str(file_path),
            file_size=file_path.stat().st_size,
            compression="none",
            original_file_name=file_name,
            original_file_size=file_path.stat().st_size,
        )
    except Exception as e:
        return BackupResult(success=False, message=str(e))


# ── Dispatcher ────────────────────────────────────────────────────────

_ENGINES = {
    "mysql": backup_mysql,
    "postgresql": backup_postgresql,
    "mongodb": backup_mongodb,
}


async def run_backup(
    database_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
) -> BackupResult:
    """
    Route to the correct backup engine based on database_type.
    Returns a BackupResult with file info on success.
    """
    engine = _ENGINES.get(database_type.lower())
    if engine is None:
        return BackupResult(
            success=False,
            message=f"Unsupported database type: {database_type}",
        )
    return await engine(host, port, database_name, username, password)


# ── Restore helpers ───────────────────────────────────────────────────


def _restore_mysql(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    file_path: Path,
) -> None:
    import pymysql

    sql = file_path.read_text(encoding="utf-8")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database,
        autocommit=False,
        charset="utf8mb4",
    )
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    statements = _iter_sql_statements(sql)
    for stmt in statements:
        cur.execute(stmt)
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    conn.commit()
    cur.close()
    conn.close()


def _restore_postgresql(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    file_path: Path,
) -> None:
    import psycopg2

    sql = file_path.read_text(encoding="utf-8")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
    )
    cur = conn.cursor()
    statements = _iter_sql_statements(sql)
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()


def _restore_mongodb(
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    file_path: Path,
) -> None:
    from pymongo import MongoClient
    from bson import json_util as bson_json_util

    data = json.loads(file_path.read_text(encoding="utf-8"))
    uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    db = client[database]

    for col_name, docs in data.items():
        db[col_name].drop()
        if docs:
            # docs may contain ObjectId wrappers; bson_json_util helps round-trip
            normalised_docs = bson_json_util.loads(json.dumps(docs))
            db[col_name].insert_many(normalised_docs)

    client.close()


_RESTORE_ENGINES = {
    "mysql": _restore_mysql,
    "postgresql": _restore_postgresql,
    "mongodb": _restore_mongodb,
}


def _guess_database_type(file_name: str, fallback: str | None = None) -> str | None:
    name = file_name.lower()
    if name.startswith("mysql_"):
        return "mysql"
    if name.startswith("postgresql_") or name.startswith("postgres_"):
        return "postgresql"
    if name.startswith("mongodb_"):
        return "mongodb"
    return fallback


async def run_restore(
    database_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
    file_path: str,
    file_name: str | None = None,
) -> RestoreResult:
    """
    Apply a backup file to the target database. Determines the engine from
    `database_type` or the file name prefix (mysql_/postgresql_/mongodb_).
    """
    dtype = (database_type or "").lower()
    if not dtype:
        dtype = _guess_database_type(file_name or "", None) or ""

    engine = _RESTORE_ENGINES.get(dtype)
    if engine is None:
        return RestoreResult(False, f"Unsupported database type for restore: {database_type}")

    try:
        await asyncio.to_thread(
            engine,
            host,
            port,
            database_name,
            username,
            password,
            Path(file_path),
        )
        return RestoreResult(True, "Restore completed successfully.")
    except Exception as e:
        return RestoreResult(False, str(e))


