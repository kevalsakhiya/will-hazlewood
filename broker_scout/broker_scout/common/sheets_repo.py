"""Google Sheets repository for the monthly-rotation pipeline.

Three responsibilities:

  1. Resolve which spreadsheet is "active" for a platform right now,
     creating a new one (via Drive copy of a template) if none exists
     for the current month. Backed by the `sheet_registry` table.
  2. Append batches of rows with retry on transient errors.
  3. Pre-flight capacity guard so we never silently overflow the
     10M-cells-per-spreadsheet hard limit.

Column layout mirrors `brokers_repo._BROKER_COLUMNS` minus the `raw`
JSONB blob — Sheets is a human view, the blob lives in Postgres only.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

from googleapiclient.errors import HttpError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from broker_scout.common import brokers_repo
from broker_scout.common.db import get_pool
from broker_scout.utils import gauth

logger = logging.getLogger(__name__)

SHEETS_CELL_LIMIT = 10_000_000
CAPACITY_SAFETY_MARGIN = 0.9            # raise if incoming > remaining * 0.9
PERIOD_FORMAT = "%Y-%m"

# Sheet column order is decoupled from `brokers_repo._BROKER_COLUMNS`
# (the Postgres table order) because the two have different audiences:
# the DB is queried with explicit SELECTs, the Sheet is browsed by
# humans. Layout below groups identity → links → listings → deals →
# provenance, with sale/rent always paired side-by-side.
#
# `dict` (insertion-ordered in Py3.7+) so the column→label mapping is
# explicit and a missing or extra column fails loudly via the
# integrity check below. The mapping doubles as both the data column
# order (`_SHEET_COLUMNS`) and the template's row-1 labels.
_SHEET_HEADERS: dict[str, str] = {
    # --- Identity ---
    "broker_name": "Broker Name",
    "brn": "BRN",
    "nationality": "Nationality",
    "agent_specialization": "Specialization",
    "is_superagent": "Superagent",
    "experience_since": "Experience Since",
    "whatsapp_response_time": "WhatsApp Response Time (s)",
    # --- Links ---
    "agent_url": "Agent URL",
    "agency_url": "Agency URL",
    "agency_registration_number": "Agency Registration Number",
    # --- Listing counts ---
    "listings_for_sale": "Listings for Sale",
    "listings_for_rent": "Listings for Rent",
    "listings_total": "Total Listings",
    "listings_with_marketing_spend": "Listings with Marketing Spend",
    # --- Listing calcs (sale/rent paired) ---
    "average_listing_price_sale": "Avg Listing Price (Sale AED)",
    "average_listing_price_rent": "Avg Listing Price (Rent AED)",
    "average_listing_age_days_sale": "Avg Listing Age (Sale days)",
    "average_listing_age_days_rent": "Avg Listing Age (Rent days)",
    # --- Listing dates ---
    "most_recent_listing_date_sale": "Latest Sale Listing Date",
    "most_recent_listing_date_rent": "Latest Rent Listing Date",
    # --- Closed deal counts ---
    "closed_transaction_sale": "Closed Sales",
    "closed_transaction_rent": "Closed Rentals",
    "closed_deals_total": "Total Closed Deals",
    # --- Closed deal financials ---
    "closed_transaction_deal_value": "Closed Deal Value (AED)",
    "closed_transaction_sale_total_amount": "Total Sale Amount (AED)",
    "closed_transaction_rent_total_amount": "Total Rent Amount (AED)",
    "closed_transaction_sale_avg_amount": "Avg Sale Amount (AED)",
    "closed_transaction_rent_avg_amount": "Avg Rent Amount (AED)",
    # --- Deal activity (dates + monthly volume) ---
    "most_recent_deal_date_sale": "Latest Sale Deal Date",
    "most_recent_deal_date_rent": "Latest Rent Deal Date",
    "average_monthly_deal_volume_sale": "Monthly Sale Deals (Avg)",
    "average_monthly_deal_volume_rent": "Monthly Rent Deals (Avg)",
    # --- Provenance / metadata (right edge) ---
    "platform": "Platform",
    "match_status": "Match Status",
    "match_confidence": "Match Confidence",
    "scrape_date": "Scrape Date",
    "run_id": "Run ID",
    # --- DLD ground truth (appended end-of-row in Phase 6.1) ---
    # New columns go HERE, not interleaved with existing fields, so old
    # data rows in already-deployed spreadsheets stay column-aligned.
    "dld_broker_name": "DLD Broker Name",
    "dld_brn": "DLD BRN",
    "agency_name": "Agency Name",
}

_SHEET_COLUMNS: tuple[str, ...] = tuple(_SHEET_HEADERS.keys())

# Integrity guard: every Sheet column must exist in the brokers table
# (minus the `raw` blob, which is Postgres-only). Catches typos and
# drift if Phase 6 / 8 add new fields to items.py without updating
# both ends.
_BROKER_COLUMN_SET = set(brokers_repo._BROKER_COLUMNS) - {"raw"}
_missing_in_brokers = set(_SHEET_COLUMNS) - _BROKER_COLUMN_SET
_missing_in_sheet = _BROKER_COLUMN_SET - set(_SHEET_COLUMNS)
assert not _missing_in_brokers, (
    f"_SHEET_COLUMNS references columns not in brokers table: {_missing_in_brokers}"
)
assert not _missing_in_sheet, (
    f"brokers columns missing from _SHEET_COLUMNS: {_missing_in_sheet}"
)


def template_header_row() -> list[str]:
    """The display labels for row 1 of a fresh template spreadsheet,
    in the same order the pipeline writes data. Operators paste this
    into A1 of the template once."""
    return list(_SHEET_HEADERS.values())

# Per-platform display label + env-var names. Two-platform world; a
# dict beats parameterizing both at every call site.
_PLATFORM_CONFIG: dict[str, dict[str, str]] = {
    "propertyfinder": {
        "label": "PropertyFinder Brokers",
        "template_env": "GSHEET_TEMPLATE_PF_ID",
        "folder_env": "GSHEET_PF_FOLDER_ID",
    },
    "bayut": {
        "label": "Bayut Brokers",
        "template_env": "GSHEET_TEMPLATE_BAYUT_ID",
        "folder_env": "GSHEET_BAYUT_FOLDER_ID",
    },
}

APPEND_RANGE = "brokers!A:A"  # values.append uses the table that intersects this range


class SheetsCapacityError(RuntimeError):
    """Raised when projected run cells would exceed the safe headroom on
    the active spreadsheet. Operator action: rotate manually or
    investigate column drift."""


# ----------------------------------------------------------- public helpers


def current_period() -> str:
    """Period key for monthly rotation: 'YYYY-MM' in UTC."""
    return datetime.now(UTC).strftime(PERIOD_FORMAT)


def to_row(item: dict) -> list:
    """Project a validated item dict onto the spreadsheet column order.

    Missing keys → empty string (Sheets renders None as 'None' which is
    ugly; empty cell is the human-friendly default).
    """
    return [item.get(col, "") if item.get(col) is not None else "" for col in _SHEET_COLUMNS]


# ----------------------------------------------------------- registry ops


def get_or_create_active_sheet(platform: str) -> str:
    """Return the spreadsheet ID for `platform` in the current period.

    Reads from `sheet_registry`. If no active row exists for the period,
    copies the template via Drive API, shares with viewers, registers
    the new file, and deactivates prior periods.
    """
    cfg = _platform_config(platform)
    period = current_period()

    existing = _select_active_sheet(platform, period)
    if existing is not None:
        return existing

    template_id = _required_env(cfg["template_env"])
    folder_id = _required_env(cfg["folder_env"])
    name = f"{cfg['label']} — {period}"

    sheet_id = _drive_copy_template(template_id, name, folder_id)
    _share_with_viewers(sheet_id)

    inserted = _insert_registry_row(platform, period, sheet_id)
    if not inserted:
        # Concurrent winner registered first — orphan our just-created
        # copy and use the winner's sheet. Rare; logged as a warning so
        # ops can clean up the stray Drive file.
        winner = _select_active_sheet(platform, period)
        logger.warning(
            "registry race; orphaned Drive copy",
            extra={
                "platform": platform,
                "period": period,
                "orphaned_sheet_id": sheet_id,
                "winner_sheet_id": winner,
            },
        )
        return winner

    _deactivate_prior_periods(platform, period)
    logger.info(
        "registered new monthly spreadsheet",
        extra={"platform": platform, "period": period, "sheet_id": sheet_id},
    )
    return sheet_id


def _select_active_sheet(platform: str, period: str) -> str | None:
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT sheet_id FROM sheet_registry "
            "WHERE platform=%s AND period=%s AND is_active=TRUE LIMIT 1",
            (platform, period),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _insert_registry_row(platform: str, period: str, sheet_id: str) -> bool:
    """Returns True if this call inserted; False on conflict (race)."""
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sheet_registry (platform, period, sheet_id, is_active) "
            "VALUES (%s, %s, %s, TRUE) "
            "ON CONFLICT (platform, period) DO NOTHING",
            (platform, period, sheet_id),
        )
        affected = cur.rowcount
        conn.commit()
    return affected == 1


def _deactivate_prior_periods(platform: str, period: str) -> None:
    pool = get_pool()
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE sheet_registry SET is_active=FALSE "
            "WHERE platform=%s AND period<>%s AND is_active=TRUE",
            (platform, period),
        )
        conn.commit()


# ----------------------------------------------------------- Drive ops


def _drive_copy_template(template_id: str, name: str, folder_id: str) -> str:
    drive = gauth.get_drive_client()
    new_file = drive.files().copy(
        fileId=template_id,
        body={"name": name, "parents": [folder_id]},
        fields="id",
    ).execute()
    return new_file["id"]


def _share_with_viewers(sheet_id: str) -> None:
    raw = os.getenv("GSHEET_VIEWER_EMAILS", "")
    emails = [e.strip() for e in raw.split(",") if e.strip()]
    if not emails:
        return
    drive = gauth.get_drive_client()
    for email in emails:
        try:
            drive.permissions().create(
                fileId=sheet_id,
                body={"role": "reader", "type": "user", "emailAddress": email},
                sendNotificationEmail=False,
                fields="id",
            ).execute()
        except HttpError as exc:
            # don't fail the run on a share error — sheet exists, just
            # log so ops can fix the address.
            logger.warning(
                "failed to share sheet",
                extra={
                    "sheet_id": sheet_id,
                    "email": email,
                    "status": getattr(exc.resp, "status", "?"),
                },
            )


# ----------------------------------------------------------- Sheets ops


def _is_transient_sheets_error(exc: BaseException) -> bool:
    if isinstance(exc, HttpError):
        status = getattr(exc.resp, "status", 0)
        return status >= 500 or status == 429
    return isinstance(exc, (ConnectionError, TimeoutError))


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, max=60),
    retry=retry_if_exception(_is_transient_sheets_error),
)
def append_rows(sheet_id: str, rows: list[list]) -> int:
    """Append rows to the `brokers` tab. Retries 5xx/429 transient errors
    with exponential backoff up to 60s. Returns rows submitted on
    success; raises after retry budget exhausted."""
    if not rows:
        return 0
    sheets = gauth.get_sheets_client()
    sheets.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=APPEND_RANGE,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        includeValuesInResponse=False,
        body={"values": rows},
    ).execute()
    logger.info("appended rows to sheet", extra={"sheet_id": sheet_id, "rows": len(rows)})
    return len(rows)


def pre_flight_capacity_check(sheet_id: str, expected_run_cells: int) -> None:
    """Fail loudly if the active sheet doesn't have safe headroom for
    the projected run.

    `expected_run_cells` is the caller's best estimate (Phase 4.3 will
    pass `~30k * len(_SHEET_COLUMNS)` for PF). The 10% safety margin
    absorbs estimate error and re-runs.
    """
    used = _used_cells(sheet_id)
    remaining = SHEETS_CELL_LIMIT - used
    headroom = remaining * CAPACITY_SAFETY_MARGIN
    if expected_run_cells > headroom:
        raise SheetsCapacityError(
            f"sheet {sheet_id!r} has insufficient capacity: "
            f"used={used:,} remaining={remaining:,} "
            f"headroom={int(headroom):,} expected={expected_run_cells:,}"
        )


def _used_cells(sheet_id: str) -> int:
    sheets = gauth.get_sheets_client()
    meta = sheets.spreadsheets().get(
        spreadsheetId=sheet_id,
        fields="sheets/properties/gridProperties",
    ).execute()
    used = 0
    for sheet in meta.get("sheets", []):
        gp = sheet.get("properties", {}).get("gridProperties", {})
        used += int(gp.get("rowCount", 0)) * int(gp.get("columnCount", 0))
    return used


# ----------------------------------------------------------- internals


def _platform_config(platform: str) -> dict[str, str]:
    cfg = _PLATFORM_CONFIG.get(platform)
    if cfg is None:
        raise ValueError(
            f"unknown platform {platform!r}; expected one of {sorted(_PLATFORM_CONFIG)}"
        )
    return cfg


def _required_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"required env var {name} is unset — see .env.example for setup"
        )
    return val
