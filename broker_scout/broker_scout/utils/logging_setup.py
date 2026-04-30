"""Structured JSON logging with per-run context injection."""

from __future__ import annotations

import logging

from pythonjsonlogger import jsonlogger

from broker_scout.common.run_context import get_run_context


class RunContextJsonFormatter(jsonlogger.JsonFormatter):
    """JsonFormatter that adds run_id / scrape_date / spider from the contextvar."""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        ctx = get_run_context()
        if ctx is not None:
            log_record.setdefault("run_id", ctx.run_id)
            log_record.setdefault("scrape_date", ctx.scrape_date)
            log_record.setdefault("spider", ctx.spider_label)


def configure_logging(level: str = "INFO") -> None:
    """Replace the root logger handlers with a single JSON-formatted stream handler."""

    root = logging.getLogger()
    root.setLevel(level)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(
        RunContextJsonFormatter(
            "%(asctime)s %(message)s",
            rename_fields={"asctime": "ts"},
        )
    )
    root.addHandler(handler)
