"""Google Sheets pipeline (priority 500) — buffered append per run.

Lifecycle mirrors `pipelines/postgres.py`:

  * spider_opened (signal) → resolve / create the active monthly sheet
    via `sheets_repo.get_or_create_active_sheet`.
  * process_item            → buffer in memory, flush at BATCH_SIZE.
  * spider_closed (signal)  → drain the buffer; on failure, log and
                              swallow so other pipelines' close
                              handlers still run.
"""

from __future__ import annotations

import logging

from scrapy import signals

from broker_scout.common import sheets_repo

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000

# Conservative upper bound for capacity pre-flight: PF run averages ~30k
# brokers, Phase 6 may add ~15% as DLD-driven not_found stubs land.
EXPECTED_RUN_ROWS = 35_000


class GSheetsBatchPipeline:
    """Buffered Sheets append. One flush per BATCH_SIZE items + a final
    flush on spider close. Sheet ID is resolved once at spider_opened
    and cached for the run."""

    def __init__(self, batch_size: int = BATCH_SIZE):
        self._buffer: list[list] = []
        self._batch_size = batch_size
        self._sheet_id: str | None = None
        self._platform: str | None = None
        self._capacity_checked = False

    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls()
        # Use signals so we run *after* RunIdExtension's spider_opened,
        # same reasoning as PostgresPipeline.
        crawler.signals.connect(pipe.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(pipe.spider_closed, signal=signals.spider_closed)
        return pipe

    # ------------------------------------------------------------ open

    def spider_opened(self, spider) -> None:
        self._ensure_sheet_resolved(spider)

    def _ensure_sheet_resolved(self, spider) -> None:
        if self._sheet_id is not None:
            return
        # Phase 6 will set spider.platform on the base spider class.
        # Until then, agent_spider hardcodes 'propertyfinder'.
        platform = getattr(spider, "platform", "propertyfinder")
        self._platform = platform
        self._sheet_id = sheets_repo.get_or_create_active_sheet(platform)
        spider.crawler.stats.set_value("gsheets/sheet_id", self._sheet_id)
        logger.info(
            "gsheets pipeline ready",
            extra={"sheet_id": self._sheet_id, "platform": platform},
        )

    # ------------------------------------------------------------ items

    def process_item(self, item: dict, spider) -> dict:
        # Defensive lazy resolution in case the open signal fired in an
        # unexpected order.
        self._ensure_sheet_resolved(spider)
        self._buffer.append(sheets_repo.to_row(item))
        if len(self._buffer) >= self._batch_size:
            self._flush(spider)
        return item

    # ------------------------------------------------------------ close

    def spider_closed(self, spider, reason: str) -> None:
        try:
            self._flush(spider)
        except Exception:
            # Log and swallow — Postgres + Drive CSV have their own close
            # handlers that must run. Phase 12 tools/replay_run.py is the
            # recovery path for missed Sheets writes.
            logger.exception(
                "final gsheets flush failed",
                extra={"sheet_id": self._sheet_id},
            )
            spider.crawler.stats.set_value("gsheets/flush_failed", 1)

    # ------------------------------------------------------------ flush

    def _flush(self, spider) -> None:
        if not self._buffer:
            return
        self._ensure_sheet_resolved(spider)

        # First flush of the run: pre-flight capacity guard. Loud
        # failure here protects the operator from a silent overflow.
        if not self._capacity_checked:
            expected_cells = EXPECTED_RUN_ROWS * len(sheets_repo._SHEET_COLUMNS)
            sheets_repo.pre_flight_capacity_check(self._sheet_id, expected_cells)
            self._capacity_checked = True

        # Rebind before handoff so a still-arriving item lands in a
        # fresh buffer; on failure we re-attach so retry covers it.
        rows = self._buffer
        self._buffer = []
        try:
            sheets_repo.append_rows(self._sheet_id, rows)
        except Exception:
            self._buffer = rows + self._buffer
            raise
        spider.crawler.stats.inc_value("gsheets/rows_appended", len(rows))
