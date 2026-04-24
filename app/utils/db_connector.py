"""
Utility that connects to external databases (MySQL, PostgreSQL, MongoDB)
and returns connection-test results.

Drivers used:
  - MySQL      → pymysql
  - PostgreSQL → psycopg2
  - MongoDB    → pymongo
"""

import asyncio
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionTestResult:
    success: bool
    message: str
    server_version: Optional[str] = None


# ── MySQL (pymysql) ──────────────────────────────────────────────────

async def test_mysql(
    host: str, port: int, database: str,
    username: str, password: str,
) -> ConnectionTestResult:
    """Test a MySQL connection using pymysql."""
    def _connect():
        import pymysql
        conn = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
            connect_timeout=10,
        )
        version = conn.server_version
        # server_version is a tuple like (8, 0, 35) → convert to string
        if isinstance(version, tuple):
            version = ".".join(str(v) for v in version)
        conn.close()
        return str(version)

    try:
        version = await asyncio.to_thread(_connect)
        return ConnectionTestResult(
            success=True,
            message="Successfully connected to MySQL",
            server_version=version,
        )
    except Exception as e:
        return ConnectionTestResult(success=False, message=str(e))


# ── PostgreSQL (psycopg2) ────────────────────────────────────────────

async def test_postgresql(
    host: str, port: int, database: str,
    username: str, password: str,
) -> ConnectionTestResult:
    """Test a PostgreSQL connection using psycopg2."""
    def _connect():
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return version

    try:
        version = await asyncio.to_thread(_connect)
        return ConnectionTestResult(
            success=True,
            message="Successfully connected to PostgreSQL",
            server_version=version,
        )
    except Exception as e:
        return ConnectionTestResult(success=False, message=str(e))


# ── MongoDB (pymongo) ────────────────────────────────────────────────

async def test_mongodb(
    host: str, port: int, database: str,
    username: str, password: str,
) -> ConnectionTestResult:
    """Test a MongoDB connection using pymongo."""
    def _connect():
        from pymongo import MongoClient
        uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        info = client.server_info()          # forces a real connection
        version = info.get("version", "unknown")
        client.close()
        return version

    try:
        version = await asyncio.to_thread(_connect)
        return ConnectionTestResult(
            success=True,
            message="Successfully connected to MongoDB",
            server_version=version,
        )
    except Exception as e:
        return ConnectionTestResult(success=False, message=str(e))


# ── Dispatcher ────────────────────────────────────────────────────────

_TESTERS = {
    "mysql": test_mysql,
    "postgresql": test_postgresql,
    "mongodb": test_mongodb,
}


async def test_connection(
    database_type: str,
    host: str,
    port: int,
    database_name: str,
    username: str,
    password: str,
) -> ConnectionTestResult:
    """
    Route to the correct driver based on database_type and return the result.
    """
    tester = _TESTERS.get(database_type)
    if tester is None:
        return ConnectionTestResult(
            success=False,
            message=f"Unsupported database type: {database_type}",
        )
    return await tester(host, port, database_name, username, password)
