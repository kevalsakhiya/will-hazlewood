"""Minimal forward-only SQL migration runner.

Reads `*.sql` files from `sql/migrations/` (sorted), tracks applied ones
in `_migrations`, runs each pending file in a transaction.

Run with: `python -m broker_scout.tools.migrate`.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from broker_scout.common.db import get_pool
from broker_scout.utils.logging_setup import configure_logging

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "sql" / "migrations"

BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS _migrations (
    filename    TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _applied(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM _migrations")
        return {row[0] for row in cur.fetchall()}


def _apply_one(conn, path: Path) -> None:
    sql = path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "INSERT INTO _migrations (filename) VALUES (%s)",
            (path.name,),
        )


def run() -> int:
    logger = logging.getLogger("migrate")
    if not MIGRATIONS_DIR.is_dir():
        logger.error("migrations dir not found: %s", MIGRATIONS_DIR)
        return 1

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        logger.info("no migration files found at %s", MIGRATIONS_DIR)
        return 0

    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(BOOTSTRAP_SQL)
        conn.commit()

        already = _applied(conn)
        pending = [p for p in files if p.name not in already]

        if not pending:
            logger.info("no pending migrations (%s already applied)", len(already))
            return 0

        for path in pending:
            logger.info("applying %s", path.name)
            try:
                _apply_one(conn, path)
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception("failed to apply %s", path.name)
                return 1
            logger.info("applied %s", path.name)

    logger.info("done — %s migration(s) applied", len(pending))
    return 0


def main() -> None:
    configure_logging("INFO")
    sys.exit(run())


if __name__ == "__main__":
    main()
