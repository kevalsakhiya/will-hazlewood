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

    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls()
        # spider_closed signal carries `reason`; the auto-wired close_spider
        # method does not. Connect explicitly so we can mark status correctly.
        crawler.signals.connect(pipe.spider_closed, signal=signals.spider_closed)
        return pipe

    def open_spider(self, spider) -> None:
        self._run_id = spider.run_id
        self._scrape_date = spider.scrape_date
        brokers_repo.open_run(self._run_id, spider.name)

    def process_item(self, item: dict, spider) -> dict:
        self._broker_buffer.append(item)
        if len(self._broker_buffer) >= self._batch_size:
            self._flush_brokers(spider)
        return item

    def spider_closed(self, spider, reason: str) -> None:
        flush_failed = False
        try:
            self._flush_brokers(spider)
            self._flush_bad_items(spider)
        except Exception:
            flush_failed = True
            raise
        finally:
            stats_dict = dict(spider.crawler.stats.get_stats())
            status = "failed" if flush_failed or reason != "finished" else "ok"
            brokers_repo.close_run(
                run_id=self._run_id,
                status=status,
                items_scraped=int(stats_dict.get("item_scraped_count", 0)),
                items_dropped=int(stats_dict.get("item_dropped_count", 0)),
                stats=_jsonable_stats(stats_dict),
            )

    def _flush_brokers(self, spider) -> None:
        if not self._broker_buffer:
            return
        # Rebind before handoff so the buffer we passed to the repo isn't
        # later mutated (matters for caller-side reference tracking).
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
        spider.bad_items.clear()
