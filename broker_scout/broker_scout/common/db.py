"""Shared Postgres connection pool.

Lazily creates a single `psycopg_pool.ConnectionPool` from environment
variables. Reused by the DLD fetch tool and the Phase 3 spider pipeline.
"""

from __future__ import annotations

import os
from threading import Lock

from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None
_lock = Lock()


def _build_dsn() -> str:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "broker_scout")
    user = os.getenv("POSTGRES_USER", "root")
    password = os.getenv("POSTGRES_PASSWORD", "root")
    return f"host={host} port={port} dbname={db} user={user} password={password}"


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:
                _pool = ConnectionPool(
                    conninfo=_build_dsn(),
                    min_size=1,
                    max_size=5,
                    open=True,
                )
    return _pool


def close_pool() -> None:
    global _pool
    with _lock:
        if _pool is not None:
            _pool.close()
            _pool = None
