"""Tests for the JSON formatter — focuses on the spider-object coercion
that was tripping us up in production logs."""

from __future__ import annotations

import json
import logging
from io import StringIO

import pytest

from broker_scout.common.run_context import (
    RunContext,
    clear_run_context,
    set_run_context,
)
from broker_scout.utils.logging_setup import RunContextJsonFormatter


@pytest.fixture
def captured_logger():
    """A logger whose JSON output we can read back."""
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(
        RunContextJsonFormatter(
            "%(asctime)s %(message)s",
            rename_fields={"asctime": "ts"},
        )
    )
    logger = logging.getLogger("test_logging_setup")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield logger, buf
    clear_run_context()


def _last_line_json(buf: StringIO) -> dict:
    line = buf.getvalue().strip().splitlines()[-1]
    return json.loads(line)


class FakeSpider:
    name = "agent_spider"

    def __repr__(self) -> str:
        return f"<AgentSpider 'agent_spider' at 0x{id(self):x}>"


def test_spider_object_in_extra_is_coerced_to_name(captured_logger):
    logger, buf = captured_logger
    logger.info("Spider closed", extra={"spider": FakeSpider()})
    record = _last_line_json(buf)
    assert record["spider"] == "agent_spider"
    assert "<AgentSpider" not in record["spider"]


def test_string_spider_is_left_alone(captured_logger):
    logger, buf = captured_logger
    logger.info("hi", extra={"spider": "agent_spider"})
    record = _last_line_json(buf)
    assert record["spider"] == "agent_spider"


def test_run_context_fills_missing_fields(captured_logger):
    logger, buf = captured_logger
    set_run_context(
        RunContext(run_id="abc-123", scrape_date="2026-05-07", spider_label="agent_spider")
    )
    logger.info("hi")
    record = _last_line_json(buf)
    assert record["run_id"] == "abc-123"
    assert record["scrape_date"] == "2026-05-07"
    assert record["spider"] == "agent_spider"


def test_explicit_extra_takes_precedence_over_context(captured_logger):
    """Caller-supplied `extra={"run_id": ...}` should win over the
    contextvar so per-message overrides work."""
    logger, buf = captured_logger
    set_run_context(
        RunContext(run_id="ctx-run", scrape_date="2026-05-07", spider_label="agent_spider")
    )
    logger.info("hi", extra={"run_id": "override-run"})
    record = _last_line_json(buf)
    assert record["run_id"] == "override-run"


def test_object_without_name_is_not_coerced(captured_logger):
    """A non-Spider object accidentally landing in the spider field
    shouldn't blow up — it gets serialized to its repr by json's
    default fallback."""
    logger, buf = captured_logger

    class NoName:
        def __repr__(self) -> str:
            return "<NoName>"

    logger.info("hi", extra={"spider": NoName()})
    record = _last_line_json(buf)
    # Not a string, not coerced; falls through to json default → repr
    assert "NoName" in record["spider"]
