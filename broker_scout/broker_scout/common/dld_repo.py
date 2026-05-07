"""Postgres persistence for DLDBroker records."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator

from psycopg.rows import dict_row, tuple_row

from broker_scout.common.db import get_pool
from broker_scout.common.dld_models import DLDBroker

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000

_COLUMNS = (
    "brn",
    "office_license_number",
    "broker_name_en",
    "broker_name_ar",
    "phone",
    "mobile",
    "email",
    "real_estate_number",
    "office_name_en",
    "office_name_ar",
    "card_issue_date",
    "card_expiry_date",
    "office_issue_date",
    "office_expiry_date",
    "photo_url",
    "office_logo_url",
    "card_rank_id",
    "card_rank",
    "office_rank_id",
    "office_rank",
    "awards_count",
)

_PLACEHOLDERS = ", ".join(f"%({c})s" for c in _COLUMNS)
_INSERT_COLUMNS = ", ".join(_COLUMNS) + ", first_seen_at, last_seen_at, last_seen_run_id"
_VALUES = _PLACEHOLDERS + ", now(), now(), %(run_id)s"

_UPDATE_ASSIGNMENTS = ",\n    ".join(
    f"{c} = EXCLUDED.{c}" for c in _COLUMNS if c != "brn"
) + ",\n    last_seen_at = now(),\n    last_seen_run_id = EXCLUDED.last_seen_run_id"

UPSERT_SQL = f"""
INSERT INTO dld_brokers ({_INSERT_COLUMNS})
VALUES ({_VALUES})
ON CONFLICT (brn) DO UPDATE SET
    {_UPDATE_ASSIGNMENTS}
RETURNING (xmax = 0) AS inserted
"""


def _row(broker: DLDBroker, run_id: str) -> dict:
    d = broker.to_dict()
    d["run_id"] = run_id
    return d


def upsert_brokers(brokers: Iterable[DLDBroker], run_id: str) -> tuple[int, int]:
    """Insert new brokers, update existing. Returns (inserted_count, updated_count)."""
    inserted = 0
    updated = 0
    pool = get_pool()
    batch: list[dict] = []

    with pool.connection() as conn:
        conn.row_factory = tuple_row
        with conn.cursor() as cur:
            for broker in brokers:
                batch.append(_row(broker, run_id))
                if len(batch) >= BATCH_SIZE:
                    ins, upd = _flush(cur, batch)
                    inserted += ins
                    updated += upd
                    batch.clear()
            if batch:
                ins, upd = _flush(cur, batch)
                inserted += ins
                updated += upd
        conn.commit()

    logger.info(
        "upsert complete — inserted=%s updated=%s total=%s",
        inserted,
        updated,
        inserted + updated,
    )
    return inserted, updated


_SELECT_COLS = ", ".join(_COLUMNS)
_SELECT_ALL_SQL = f"SELECT {_SELECT_COLS} FROM dld_brokers"
_SELECT_BY_RUN_SQL = (
    f"SELECT {_SELECT_COLS} FROM dld_brokers WHERE last_seen_run_id = %s"
)


def iter_active_brokers(run_id: str | None = None) -> Iterator[DLDBroker]:
    """Yield DLDBroker rows from Postgres for the spider to seed from.

    Args:
        run_id: when set, only yield brokers whose `last_seen_run_id`
                equals this run (i.e. those present in the most recent
                fetch). Default yields the entire `dld_brokers` table —
                appropriate when we want the full registry, not just
                this-week's snapshot, since brokers occasionally drop
                out of one DLD response and reappear in the next.

    Streams via the cursor iterator so a 30k-row table stays modest in
    memory (~a few MB).
    """
    pool = get_pool()
    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        if run_id is None:
            cur.execute(_SELECT_ALL_SQL)
        else:
            cur.execute(_SELECT_BY_RUN_SQL, (run_id,))
        for row in cur:
            yield DLDBroker(**row)


def _flush(cur, rows: list[dict]) -> tuple[int, int]:
    """Run UPSERT for one batch and tally insert/update counts from the RETURNING."""
    ins = 0
    upd = 0
    for row in rows:
        cur.execute(UPSERT_SQL, row)
        result = cur.fetchone()
        was_insert = bool(result and result[0])
        if was_insert:
            ins += 1
        else:
            upd += 1
    return ins, upd
