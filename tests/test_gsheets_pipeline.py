"""Lifecycle / buffering coverage for `GSheetsBatchPipeline`.

`sheets_repo` is mocked at the pipeline's import path, so no Google
API calls or Postgres reads happen in tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from broker_scout.pipelines.gsheets import GSheetsBatchPipeline


@pytest.fixture
def fake_spider():
    spider = MagicMock()
    spider.name = "agent_spider"
    spider.platform = "propertyfinder"
    return spider


@pytest.fixture
def repo_mock():
    """Patch the `sheets_repo` symbol that the pipeline imports."""
    with patch("broker_scout.pipelines.gsheets.sheets_repo") as m:
        m.get_or_create_active_sheet.return_value = "sheet-abc"
        m._SHEET_COLUMNS = tuple(f"c{i}" for i in range(37))
        m.to_row.side_effect = lambda item: [item.get(c, "") for c in m._SHEET_COLUMNS]
        m.append_rows.return_value = 0
        m.pre_flight_capacity_check.return_value = None
        yield m


# ------------------------------------------------------------- spider_opened


def test_spider_opened_resolves_sheet(repo_mock, fake_spider):
    p = GSheetsBatchPipeline()
    p.spider_opened(fake_spider)
    repo_mock.get_or_create_active_sheet.assert_called_once_with("propertyfinder")
    fake_spider.crawler.stats.set_value.assert_any_call("gsheets/sheet_id", "sheet-abc")


def test_spider_opened_idempotent(repo_mock, fake_spider):
    p = GSheetsBatchPipeline()
    p.spider_opened(fake_spider)
    p.spider_opened(fake_spider)
    repo_mock.get_or_create_active_sheet.assert_called_once()


def test_spider_opened_uses_default_platform_when_attr_missing(repo_mock):
    """Until Phase 6 sets spider.platform, fall back to 'propertyfinder'."""

    class StubSpider:
        name = "agent_spider"
        crawler = MagicMock()

    spider = StubSpider()
    p = GSheetsBatchPipeline()
    p.spider_opened(spider)
    repo_mock.get_or_create_active_sheet.assert_called_once_with("propertyfinder")


# ------------------------------------------------------------- process_item


def test_process_item_buffers_below_batch_size(repo_mock, fake_spider):
    p = GSheetsBatchPipeline(batch_size=3)
    p.spider_opened(fake_spider)
    p.process_item({"a": 1}, fake_spider)
    p.process_item({"a": 2}, fake_spider)
    repo_mock.append_rows.assert_not_called()


def test_process_item_flushes_at_batch_size(repo_mock, fake_spider):
    p = GSheetsBatchPipeline(batch_size=3)
    p.spider_opened(fake_spider)
    repo_mock.append_rows.return_value = 3
    p.process_item({"a": 1}, fake_spider)
    p.process_item({"a": 2}, fake_spider)
    p.process_item({"a": 3}, fake_spider)
    repo_mock.append_rows.assert_called_once()
    args = repo_mock.append_rows.call_args.args
    assert args[0] == "sheet-abc"
    assert len(args[1]) == 3
    fake_spider.crawler.stats.inc_value.assert_any_call("gsheets/rows_appended", 3)


def test_process_item_returns_item_unchanged(repo_mock, fake_spider):
    p = GSheetsBatchPipeline()
    p.spider_opened(fake_spider)
    item = {"brn": "1"}
    out = p.process_item(item, fake_spider)
    assert out is item


def test_process_item_lazy_resolves_sheet_if_open_skipped(
    repo_mock, fake_spider
):
    """If spider_opened never fired (signal ordering oddity), the first
    process_item must still resolve the sheet."""
    p = GSheetsBatchPipeline()
    p.process_item({"x": 1}, fake_spider)
    repo_mock.get_or_create_active_sheet.assert_called_once()


# ------------------------------------------------------------- capacity check


def test_pre_flight_runs_only_on_first_flush(repo_mock, fake_spider):
    p = GSheetsBatchPipeline(batch_size=2)
    p.spider_opened(fake_spider)
    repo_mock.append_rows.return_value = 2
    # Two flushes
    p.process_item({}, fake_spider)
    p.process_item({}, fake_spider)
    p.process_item({}, fake_spider)
    p.process_item({}, fake_spider)
    assert repo_mock.pre_flight_capacity_check.call_count == 1


def test_pre_flight_capacity_failure_propagates(repo_mock, fake_spider):
    """If the capacity guard raises, the run fails loudly — buffer is
    preserved so the operator can investigate."""
    from broker_scout.common.sheets_repo import SheetsCapacityError

    repo_mock.pre_flight_capacity_check.side_effect = SheetsCapacityError("full")
    p = GSheetsBatchPipeline(batch_size=2)
    p.spider_opened(fake_spider)
    p.process_item({}, fake_spider)
    with pytest.raises(SheetsCapacityError):
        p.process_item({}, fake_spider)
    repo_mock.append_rows.assert_not_called()


# ------------------------------------------------------------- spider_closed


def test_spider_closed_drains_remaining(repo_mock, fake_spider):
    p = GSheetsBatchPipeline(batch_size=10)
    p.spider_opened(fake_spider)
    p.process_item({}, fake_spider)
    p.process_item({}, fake_spider)
    repo_mock.append_rows.return_value = 2
    p.spider_closed(fake_spider, reason="finished")
    repo_mock.append_rows.assert_called_once()
    assert len(repo_mock.append_rows.call_args.args[1]) == 2


def test_spider_closed_skips_when_buffer_empty(repo_mock, fake_spider):
    p = GSheetsBatchPipeline()
    p.spider_opened(fake_spider)
    p.spider_closed(fake_spider, reason="finished")
    repo_mock.append_rows.assert_not_called()


def test_spider_closed_swallows_flush_error(repo_mock, fake_spider):
    """On final flush failure, do not re-raise — Postgres + Drive CSV
    close handlers must still run."""
    p = GSheetsBatchPipeline(batch_size=10)
    p.spider_opened(fake_spider)
    p.process_item({}, fake_spider)
    repo_mock.append_rows.side_effect = RuntimeError("API down")

    p.spider_closed(fake_spider, reason="finished")  # must not raise

    fake_spider.crawler.stats.set_value.assert_any_call("gsheets/flush_failed", 1)


# ------------------------------------------------------------- buffer retention


def test_flush_failure_retains_buffer_for_retry(repo_mock, fake_spider):
    """A non-final flush failure should preserve the buffer so the next
    flush attempt re-tries with the same data."""
    p = GSheetsBatchPipeline(batch_size=2)
    p.spider_opened(fake_spider)
    repo_mock.append_rows.side_effect = RuntimeError("transient")

    p.process_item({"x": 1}, fake_spider)
    with pytest.raises(RuntimeError):
        p.process_item({"x": 2}, fake_spider)

    # buffer should still hold both rows
    assert len(p._buffer) == 2
