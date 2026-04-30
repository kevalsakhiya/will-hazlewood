"""Per-run identity (run_id, scrape_date, spider name).

Stored in a contextvar so the JSON log formatter and pipelines can both
read it without coupling to the spider instance.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    scrape_date: str  # ISO date (UTC)
    spider_label: str


_run_context: ContextVar[RunContext | None] = ContextVar("run_context", default=None)


def set_run_context(ctx: RunContext) -> None:
    _run_context.set(ctx)


def get_run_context() -> RunContext | None:
    return _run_context.get()


def clear_run_context() -> None:
    _run_context.set(None)
