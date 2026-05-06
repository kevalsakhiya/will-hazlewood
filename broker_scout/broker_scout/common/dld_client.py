"""DLD API client — single fetch + JSONL snapshot writer."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

DLD_URL = "https://gateway.dubailand.gov.ae/brokers/"
DLD_PARAMS = {
    "pageSize": "50000",
    "consumer-id": "gkb3WvEG0rY9eilwXC0P2pTz8UzvLj9F",
}
DLD_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip",
    "accept-language": "en-GB,en;q=0.9",
    "origin": "https://dubailand.gov.ae",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
}

REQUEST_TIMEOUT_S = 120.0


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, max=30),
    retry=retry_if_exception_type(
        (httpx.TimeoutException, httpx.HTTPStatusError, httpx.TransportError)
    ),
)
def _do_fetch() -> list[dict]:
    logger.info("requesting DLD broker list")
    with httpx.Client(timeout=REQUEST_TIMEOUT_S) as client:
        resp = client.get(DLD_URL, params=DLD_PARAMS, headers=DLD_HEADERS)
        if resp.status_code >= 500:
            resp.raise_for_status()
        resp.raise_for_status()
        payload = resp.json()
    return _extract_records(payload)


def _extract_records(payload) -> list[dict]:
    """DLD wraps the broker list under the top-level `Response` key."""
    if isinstance(payload, dict):
        inner = payload.get("Response")
        if isinstance(inner, list):
            return inner
    raise RuntimeError(
        f"unexpected DLD response shape: type={type(payload).__name__}, "
        f"keys={list(payload)[:10] if isinstance(payload, dict) else 'n/a'}"
    )


def fetch_all() -> list[dict]:
    """Fetch the full DLD broker list. Returns the raw item dicts."""
    records = _do_fetch()
    logger.info("fetched %s DLD records", len(records))
    return records


def write_snapshot(records: list[dict], run_id: str, snapshots_dir: Path) -> Path:
    """Write each record as one JSON line to `dld_snapshots/{run_id}.jsonl`."""
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    path = snapshots_dir / f"{run_id}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")
    logger.info("wrote snapshot %s (%s records)", path, len(records))
    return path
