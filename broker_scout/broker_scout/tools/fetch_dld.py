"""CLI entrypoint: fetch DLD broker list, snapshot to JSONL, upsert to Postgres."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from broker_scout.common import dld_client, dld_repo
from broker_scout.common.dld_models import DLDBroker
from broker_scout.common.run_context import RunContext, set_run_context
from broker_scout.utils.logging_setup import configure_logging

logger = logging.getLogger("fetch_dld")

REPO_ROOT = Path(__file__).resolve().parents[3]
SNAPSHOTS_DIR = REPO_ROOT / "dld_snapshots"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch DLD brokers + persist.")
    parser.add_argument(
        "--run-id",
        default=None,
        help="Override run_id (default: new uuid4 hex).",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip Postgres upsert; write JSONL snapshot only.",
    )
    return parser.parse_args(argv)


def _records_to_models(records: list[dict]) -> list[DLDBroker]:
    """Parse + dedupe by brn. DLD returns each broker many times (one row per
    license-period); we keep the last occurrence so the most recent fields win.

    Records missing a CardNumber get a synthesized `NOBRN:...` surrogate inside
    `DLDBroker.from_api` so they're still persisted (searchable by name)."""
    by_brn: dict[str, DLDBroker] = {}
    synthesized = 0
    for rec in records:
        broker = DLDBroker.from_api(rec)
        if broker.brn.startswith("NOBRN:"):
            synthesized += 1
        by_brn[broker.brn] = broker
    if synthesized:
        logger.info(
            "synthesized %s NOBRN surrogate keys for records missing CardNumber",
            synthesized,
        )
    duplicates = len(records) - len(by_brn)
    if duplicates:
        logger.info("collapsed %s duplicate-brn rows in DLD response", duplicates)
    return list(by_brn.values())


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_id = args.run_id or uuid4().hex
    scrape_date = datetime.now(UTC).date().isoformat()
    set_run_context(
        RunContext(run_id=run_id, scrape_date=scrape_date, spider_label="fetch_dld")
    )
    logger.info("starting fetch_dld", extra={"run_id": run_id, "no_db": args.no_db})

    t0 = time.monotonic()
    try:
        records = dld_client.fetch_all()
    except Exception:
        logger.exception("DLD fetch failed")
        return 1
    fetch_elapsed = time.monotonic() - t0

    snapshot_path = dld_client.write_snapshot(records, run_id, SNAPSHOTS_DIR)

    inserted = updated = 0
    if not args.no_db:
        models = _records_to_models(records)
        try:
            inserted, updated = dld_repo.upsert_brokers(models, run_id)
        except Exception:
            logger.exception("DB upsert failed")
            return 1

    total_elapsed = time.monotonic() - t0
    logger.info(
        "fetch_dld done",
        extra={
            "run_id": run_id,
            "fetched": len(records),
            "inserted": inserted,
            "updated": updated,
            "snapshot": str(snapshot_path),
            "fetch_elapsed_s": round(fetch_elapsed, 2),
            "total_elapsed_s": round(total_elapsed, 2),
        },
    )
    return 0


def main() -> None:
    configure_logging("INFO")
    sys.exit(run())


if __name__ == "__main__":
    main()
