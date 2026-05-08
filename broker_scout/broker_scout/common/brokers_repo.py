"""Postgres persistence for `brokers`, `bad_items`, and `scrape_runs`.

Mirrors the pattern in `dld_repo.py`: a single column tuple drives the
INSERT template so the schema lives in one place. The pipeline
(`pipelines/postgres.py`) calls these four functions; the SQL details
stay here so the pipeline stays testable without touching psycopg.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from psycopg.types.json import Jsonb

from broker_scout.common.db import get_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

# Column order matches PropertyFinderBrokerItem field order, prefixed by the
# provenance + match columns. Drives both the INSERT template and the
# parameter dict produced in `_to_row`.
_PROVENANCE_COLUMNS = (
    "run_id",
    "scrape_date",
    "platform",
    "brn",
    "match_status",
    "match_confidence",
)

_ITEM_COLUMNS = (
    "agent_url",
    "broker_name",
    "nationality",
    "agent_specialization",
    "experience_since",
    "whatsapp_response_time",
    "is_superagent",
    "agency_url",
    "agency_registration_number",
    "dld_brn",                   # Phase 6.1: DLD ground truth
    "dld_broker_name",
    "agency_name",
    "listings_for_sale",
    "listings_for_rent",
    "listings_total",
    "listings_with_marketing_spend",
    "average_listing_price_sale",
    "average_listing_price_rent",
    "average_listing_age_days_sale",
    "average_listing_age_days_rent",
    "most_recent_listing_date_sale",
    "most_recent_listing_date_rent",
    "closed_transaction_sale",
    "closed_transaction_rent",
    "closed_deals_total",
    "closed_transaction_deal_value",
    "closed_transaction_sale_total_amount",
    "closed_transaction_rent_total_amount",
    "closed_transaction_sale_avg_amount",
    "closed_transaction_rent_avg_amount",
    "most_recent_deal_date_sale",
    "most_recent_deal_date_rent",
    "average_monthly_deal_volume_sale",
    "average_monthly_deal_volume_rent",
)

_BROKER_COLUMNS = _PROVENANCE_COLUMNS + _ITEM_COLUMNS + ("raw",)
_BROKER_PLACEHOLDERS = ", ".join(f"%({c})s" for c in _BROKER_COLUMNS)
_BROKER_COLUMN_LIST = ", ".join(_BROKER_COLUMNS)

_BROKER_INSERT_SQL = f"""
INSERT INTO brokers ({_BROKER_COLUMN_LIST})
VALUES ({_BROKER_PLACEHOLDERS})
ON CONFLICT (run_id, platform, brn) DO NOTHING
"""

_BAD_ITEM_INSERT_SQL = """
INSERT INTO bad_items (run_id, platform, reason, payload)
VALUES (%(run_id)s, %(platform)s, %(reason)s, %(payload)s)
"""

_OPEN_RUN_SQL = """
INSERT INTO scrape_runs (run_id, spider) VALUES (%s, %s)
ON CONFLICT (run_id) DO NOTHING
"""

_CLOSE_RUN_SQL = """
UPDATE scrape_runs
   SET finished_at   = now(),
       status        = %(status)s,
       items_scraped = %(items_scraped)s,
       items_dropped = %(items_dropped)s,
       stats         = %(stats)s
 WHERE run_id        = %(run_id)s
"""

_UPDATE_STATS_SQL = """
UPDATE scrape_runs
   SET stats   = %(stats)s
 WHERE run_id  = %(run_id)s
"""


def open_run(run_id: str, spider: str) -> None:
    """Insert the run header so subsequent broker FKs can reference it."""
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(_OPEN_RUN_SQL, (run_id, spider))
        conn.commit()
    logger.info("opened scrape_run", extra={"run_id": run_id, "spider": spider})


def close_run(
    run_id: str,
    status: str,
    items_scraped: int,
    items_dropped: int,
    stats: dict,
) -> None:
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            _CLOSE_RUN_SQL,
            {
                "run_id": run_id,
                "status": status,
                "items_scraped": items_scraped,
                "items_dropped": items_dropped,
                "stats": Jsonb(stats),
            },
        )
        conn.commit()
    logger.info(
        "closed scrape_run",
        extra={
            "run_id": run_id,
            "status": status,
            "items_scraped": items_scraped,
            "items_dropped": items_dropped,
        },
    )


def update_run_stats(run_id: str, stats: dict) -> None:
    """Replace the `stats` JSONB on a scrape_runs row.

    Called from `PostgresPipeline.engine_stopped` (Phase 9.5) so the
    blob captures the FINAL stats — including gsheets/* and
    gdrive_csv/* counters that get incremented by their respective
    pipelines' spider_closed handlers, which fire AFTER PostgresPipeline's.
    `close_run` writes an early snapshot for defense-in-depth; this
    overwrites it with the post-flush state.
    """
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            _UPDATE_STATS_SQL,
            {"run_id": run_id, "stats": Jsonb(stats)},
        )
        conn.commit()
    logger.info("updated scrape_run stats", extra={"run_id": run_id})


def insert_brokers(
    rows: Iterable[dict], run_id: str, scrape_date: str
) -> int:
    """Bulk-insert validated broker dicts. Idempotent on (run_id, platform, brn).

    Returns the number of input rows submitted (not necessarily inserted —
    ON CONFLICT DO NOTHING may skip duplicates within the batch).
    """
    rows = list(rows)
    if not rows:
        return 0
    pool = get_pool()
    submitted = 0
    with pool.connection() as conn, conn.cursor() as cur:
        batch: list[dict] = []
        for item in rows:
            batch.append(_to_broker_row(item, run_id, scrape_date))
            if len(batch) >= BATCH_SIZE:
                cur.executemany(_BROKER_INSERT_SQL, batch)
                submitted += len(batch)
                batch = []  # rebind so the prior list isn't mutated post-handoff
        if batch:
            cur.executemany(_BROKER_INSERT_SQL, batch)
            submitted += len(batch)
        conn.commit()
    logger.info("inserted broker rows", extra={"submitted": submitted, "run_id": run_id})
    return submitted


def insert_bad_items(rows: Iterable[dict]) -> int:
    """Drain `spider.bad_items` into the `bad_items` table."""
    rows = list(rows)
    if not rows:
        return 0
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        params = [
            {
                "run_id": r.get("run_id"),
                "platform": r.get("platform"),
                "reason": r.get("reason"),
                "payload": Jsonb(r.get("payload")),
            }
            for r in rows
        ]
        cur.executemany(_BAD_ITEM_INSERT_SQL, params)
        conn.commit()
    logger.info("inserted bad_items rows", extra={"count": len(rows)})
    return len(rows)


def _to_broker_row(item: dict, run_id: str, scrape_date: str) -> dict:
    """Build a parameter dict matching `_BROKER_COLUMNS`. Pulls values from
    the validated item; injects provenance; defaults match_status to
    'unknown' (Phase 6 will populate it for real)."""
    row = {col: item.get(col) for col in _ITEM_COLUMNS}
    row.update(
        run_id=run_id,
        scrape_date=item.get("scrape_date") or scrape_date,
        platform=item.get("platform"),
        brn=item.get("brn"),
        match_status=item.get("match_status") or "unknown",
        match_confidence=item.get("match_confidence"),
        raw=Jsonb(item),
    )
    return row
