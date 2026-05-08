"""Postgres pipeline (priority 400) — authoritative store for scrape outputs.

Receives validated dicts from the upstream `ValidationPipeline`, buffers
in memory, flushes every `BATCH_SIZE` items + on `spider_closed`. On
close, also drains `spider.bad_items` and updates the `scrape_runs` row
with final counts + the full Scrapy stats blob.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from scrapy import signals

from broker_scout.common import brokers_repo

logger = logging.getLogger(__name__)

# Reasons we treat as successful: the spider met a deliberate stop
# condition. `closespider_errorcount` is excluded because it indicates
# the error threshold was hit (a real signal). `cancelled` / `shutdown`
# are SIGINT/process-kill — also failures.
SUCCESSFUL_REASONS = frozenset(
    {
        "finished",
        "closespider_itemcount",
        "closespider_pagecount",
        "closespider_timeout",
    }
)


def _jsonable_stats(stats: dict) -> dict:
    """Coerce Scrapy stats values into JSON-safe shapes.

    Scrapy adds `datetime` objects (`start_time`, `finish_time`) and
    occasional exotic values; `Jsonb` would reject them. Walk one level
    of dicts/lists; ISO-format datetimes/dates; `repr` everything else
    that isn't trivially JSON-safe.
    """

    def _coerce(v):
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, dict):
            return {str(k): _coerce(val) for k, val in v.items()}
        if isinstance(v, (list, tuple, set)):
            return [_coerce(x) for x in v]
        return repr(v)

    return _coerce(stats)


class PostgresPipeline:
    """Wires the validated stream to Postgres.

    Lifecycle:
        open_spider     → brokers_repo.open_run
        process_item    → buffer; flush at BATCH_SIZE
        spider_closed   → final flush + drain bad_items + close_run
    """

    def __init__(self, batch_size: int = brokers_repo.BATCH_SIZE):
        self._broker_buffer: list[dict] = []
        self._batch_size = batch_size
        self._run_id: str | None = None
        self._scrape_date: str | None = None
        self._crawler = None  # set in from_crawler (needed by engine_stopped)

    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls()
        # Use signals rather than the auto-wired open_spider/close_spider
        # methods. Reasons:
        #   * spider_closed carries `reason`, the method does not.
        #   * The auto-wired open_spider runs *before* the spider_opened
        #     signal fires, so RunIdExtension.spider_opened hasn't yet
        #     set spider.run_id. Subscribing via the signal puts us in
        #     line *after* RunIdExtension (extensions register first).
        crawler.signals.connect(pipe.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(pipe.spider_closed, signal=signals.spider_closed)
        # engine_stopped fires AFTER every spider_closed handler completes
        # — by then gsheets/gdrive_csv pipelines have flushed their final
        # batches and incremented their stats. Re-snapshot the stats blob
        # so scrape_runs.stats captures the full post-flush state (Phase
        # 10's cross-run drift monitors read from this).
        crawler.signals.connect(pipe.engine_stopped, signal=signals.engine_stopped)
        pipe._crawler = crawler
        return pipe

    def spider_opened(self, spider) -> None:
        self._ensure_run_opened(spider)

    def process_item(self, item: dict, spider) -> dict:
        # Defensive: if the open signal somehow fired in a different order,
        # this still creates the run before we try to insert FK-bound rows.
        self._ensure_run_opened(spider)
        self._broker_buffer.append(item)
        if len(self._broker_buffer) >= self._batch_size:
            self._flush_brokers(spider)
        return item

    def spider_closed(self, spider, reason: str) -> None:
        # If the spider opened normally, this is a no-op. If it died before
        # spider_opened fired, we still want a row in scrape_runs so the
        # failure is recorded.
        self._ensure_run_opened(spider)
        flush_failed = False
        try:
            self._flush_brokers(spider)
            self._flush_bad_items(spider)
        except Exception:
            flush_failed = True
            raise
        finally:
            stats_dict = dict(spider.crawler.stats.get_stats())
            status = (
                "failed"
                if flush_failed or reason not in SUCCESSFUL_REASONS
                else "ok"
            )
            brokers_repo.close_run(
                run_id=self._run_id,
                status=status,
                items_scraped=int(stats_dict.get("item_scraped_count", 0)),
                items_dropped=int(stats_dict.get("item_dropped_count", 0)),
                stats=_jsonable_stats(stats_dict),
            )

    def engine_stopped(self) -> None:
        """Final stats-blob refresh, AFTER every spider_closed handler
        has completed. Captures gsheets/* and gdrive_csv/* counters that
        get incremented by those pipelines' spider_closed handlers
        (which fire later in registration order than ours).
        """
        if self._run_id is None or self._crawler is None:
            return
        try:
            stats_dict = dict(self._crawler.stats.get_stats())
        except Exception:
            logger.exception("engine_stopped: failed to read crawler.stats")
            return
        try:
            brokers_repo.update_run_stats(
                run_id=self._run_id,
                stats=_jsonable_stats(stats_dict),
            )
        except Exception:
            # Don't propagate — Scrapy's reactor is already shutting
            # down. Log loudly so ops can spot it.
            logger.exception(
                "engine_stopped: failed to refresh scrape_runs.stats",
                extra={"run_id": self._run_id},
            )

    def _ensure_run_opened(self, spider) -> None:
        if self._run_id is not None:
            return
        run_id = getattr(spider, "run_id", None)
        if run_id is None:
            return  # extension hasn't run yet; try again next call
        self._run_id = run_id
        self._scrape_date = getattr(spider, "scrape_date", None)
        brokers_repo.open_run(self._run_id, spider.name)

    def _flush_brokers(self, spider) -> None:
        if not self._broker_buffer:
            return
        items = self._broker_buffer
        self._broker_buffer = []
        n = brokers_repo.insert_brokers(items, self._run_id, self._scrape_date)
        spider.crawler.stats.inc_value("postgres/brokers_inserted", n)

    def _flush_bad_items(self, spider) -> None:
        bad = getattr(spider, "bad_items", None) or []
        if not bad:
            return
        n = brokers_repo.insert_bad_items(bad)
        spider.crawler.stats.inc_value("postgres/bad_items_inserted", n)
        # Rebind, don't clear() — decouples ownership of the list we
        # just handed to the repo from any reference still in flight.
        spider.bad_items = []
