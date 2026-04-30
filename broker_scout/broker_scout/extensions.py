"""Scrapy extensions for the broker_scout project."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from scrapy import signals

from broker_scout.common.run_context import (
    RunContext,
    clear_run_context,
    set_run_context,
)


class RunIdExtension:
    """Generate a per-run UUID and propagate it to logs, stats, and the spider.

    The run_id is exposed in three places so any consumer can reach it:
      * `spider.run_id` — for pipelines that already hold the spider.
      * `crawler.stats.get_value("run_id")` — for monitors and Spidermon.
      * `RunContext` contextvar — for the JSON log formatter.
    """

    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        run_id = uuid4().hex
        scrape_date = datetime.now(UTC).date().isoformat()
        spider.run_id = run_id
        spider.scrape_date = scrape_date
        self.crawler.stats.set_value("run_id", run_id)
        self.crawler.stats.set_value("scrape_date", scrape_date)
        set_run_context(
            RunContext(run_id=run_id, scrape_date=scrape_date, spider_label=spider.name)
        )
        spider.logger.info("run started", extra={"run_id": run_id})

    def spider_closed(self, spider, reason):
        spider.logger.info(
            "run finished", extra={"run_id": getattr(spider, "run_id", None), "reason": reason}
        )
        clear_run_context()
