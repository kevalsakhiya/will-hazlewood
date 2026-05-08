"""Tests for the JSON formatter — focuses on the spider-object coercion
that was tripping us up in production logs."""

from __future__ import annotations

import json
import logging
import os
import time
from io import StringIO

import pytest

from broker_scout.common.run_context import (
    RunContext,
    clear_run_context,
    set_run_context,
)
from broker_scout.utils.logging_setup import (
    PrettyConsoleFormatter,
    RunContextJsonFormatter,
    attach_run_file_handler,
    configure_logging,
    detach_run_file_handler,
    prune_old_log_files,
)


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


@pytest.fixture
def pretty_logger():
    """A logger using the human-readable formatter (colour off so the
    captured string is comparable)."""
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(PrettyConsoleFormatter(colour=False))
    logger = logging.getLogger("test_pretty_logging")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield logger, buf
    clear_run_context()


def test_pretty_renders_single_line_with_kv(pretty_logger):
    logger, buf = pretty_logger
    logger.info("flushed batch", extra={"rows": 500, "sheet_id": "abc"})
    line = buf.getvalue().strip()
    assert "\n" not in line
    assert "INFO" in line
    assert "test_pretty_logging" in line
    assert "flushed batch" in line
    assert "rows=500" in line
    assert "sheet_id=abc" in line


def test_pretty_includes_run_context(pretty_logger):
    logger, buf = pretty_logger
    set_run_context(
        RunContext(run_id="abc-123", scrape_date="2026-05-08", spider_label="agent_spider")
    )
    logger.info("hi")
    line = buf.getvalue().strip()
    assert "run_id=abc-123" in line
    assert "spider=agent_spider" in line


def test_pretty_coerces_spider_object_to_name(pretty_logger):
    logger, buf = pretty_logger
    logger.info("hi", extra={"spider": FakeSpider()})
    line = buf.getvalue().strip()
    assert "spider=agent_spider" in line
    assert "<AgentSpider" not in line


def test_configure_logging_pretty_swaps_formatter():
    """Calling configure_logging twice swaps the active handler/format
    in place rather than stacking handlers."""
    configure_logging("INFO", "pretty")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, PrettyConsoleFormatter)

    configure_logging("INFO", "json")
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, RunContextJsonFormatter)


def test_configure_logging_unknown_format_falls_back_to_json():
    configure_logging("INFO", "banana")
    assert isinstance(
        logging.getLogger().handlers[0].formatter, RunContextJsonFormatter
    )


# =============================================================== file handler


@pytest.fixture(autouse=True)
def _detach_between_tests():
    """Make sure no run file handler leaks across tests."""
    yield
    detach_run_file_handler()


def test_attach_creates_file_with_run_id_name(tmp_path):
    log_dir = tmp_path / "logs"
    path = attach_run_file_handler("abc-123", "agent_spider", log_dir=str(log_dir))
    assert path is not None
    assert path.name == "agent_spider_abc-123.log"
    assert path.exists()
    # Emitted line should land in the file as JSON.
    logging.getLogger("test_attach").warning("hi", extra={"k": "v"})
    detach_run_file_handler()
    text = path.read_text(encoding="utf-8")
    assert text.strip()
    record = json.loads(text.strip().splitlines()[-1])
    assert record["message"] == "hi"
    assert record["k"] == "v"


def test_attach_returns_none_when_log_dir_empty(tmp_path):
    assert attach_run_file_handler("abc", "spider", log_dir="") is None


def test_attach_swaps_existing_handler(tmp_path):
    """Two consecutive attaches in the same process should leave only
    one file handler attached and close the previous file."""
    p1 = attach_run_file_handler("run1", "spider", log_dir=str(tmp_path))
    p2 = attach_run_file_handler("run2", "spider", log_dir=str(tmp_path))
    assert p1 != p2
    file_handlers = [
        h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)
    ]
    assert len(file_handlers) == 1
    assert file_handlers[0].baseFilename == str(p2)


def test_detach_is_idempotent():
    detach_run_file_handler()
    detach_run_file_handler()  # must not raise


def test_prune_deletes_only_old_files(tmp_path):
    new = tmp_path / "fresh.log"
    old = tmp_path / "stale.log"
    new.write_text("x")
    old.write_text("x")
    forty_days_ago = time.time() - 40 * 86400
    os.utime(old, (forty_days_ago, forty_days_ago))

    deleted = prune_old_log_files(str(tmp_path), retention_days=30)

    assert deleted == 1
    assert new.exists()
    assert not old.exists()


def test_prune_skips_non_log_files(tmp_path):
    """Notes / readmes that ops drop in logs/ shouldn't be touched."""
    note = tmp_path / "NOTES.md"
    note.write_text("don't delete me")
    forty_days_ago = time.time() - 40 * 86400
    os.utime(note, (forty_days_ago, forty_days_ago))

    deleted = prune_old_log_files(str(tmp_path), retention_days=30)

    assert deleted == 0
    assert note.exists()


def test_prune_zero_days_is_disabled(tmp_path):
    f = tmp_path / "ancient.log"
    f.write_text("x")
    os.utime(f, (0, 0))  # 1970
    assert prune_old_log_files(str(tmp_path), retention_days=0) == 0
    assert f.exists()


def test_prune_missing_directory_is_no_op(tmp_path):
    assert prune_old_log_files(str(tmp_path / "nope"), retention_days=30) == 0


def test_prune_empty_log_dir_is_no_op():
    assert prune_old_log_files("", retention_days=30) == 0


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
