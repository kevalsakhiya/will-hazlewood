"""Lifecycle / buffering coverage for `PostgresPipeline`. Repo is mocked
at its import path inside `pipelines.postgres`, so no DB access."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.pipelines.postgres import PostgresPipeline, _jsonable_stats


@pytest.fixture
def fake_spider():
    spider = MagicMock()
    spider.name = "agent_spider"
    spider.run_id = "run-abc"
    spider.scrape_date = "2026-05-06"
    spider.bad_items = []
    spider.crawler.stats.get_stats.return_value = {
        "item_scraped_count": 0,
        "item_dropped_count": 0,
    }
    return spider


@pytest.fixture
def repo_mock():
    """Patch the `brokers_repo` symbol that the pipeline imports."""
    with patch("broker_scout.pipelines.postgres.brokers_repo") as m:
        m.BATCH_SIZE = 500
        m.insert_brokers.return_value = 0
        m.insert_bad_items.return_value = 0
        yield m


# ----------------------------------------------------------- spider_opened


def test_spider_opened_opens_run(repo_mock, fake_spider):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    repo_mock.open_run.assert_called_once_with("run-abc", "agent_spider")


def test_spider_opened_idempotent(repo_mock, fake_spider):
    """Calling spider_opened twice must not double-create the scrape_runs row."""
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    p.spider_opened(fake_spider)
    repo_mock.open_run.assert_called_once()


def test_open_run_deferred_when_run_id_missing(repo_mock):
    """If RunIdExtension hasn't fired yet, _ensure_run_opened should be a no-op."""

    class StubSpider:
        name = "agent_spider"
        # no run_id attr

    p = PostgresPipeline()
    p.spider_opened(StubSpider())
    repo_mock.open_run.assert_not_called()


# ------------------------------------------------------------- process_item


def test_process_item_buffers_below_batch_size(repo_mock, fake_spider):
    p = PostgresPipeline(batch_size=3)
    p.spider_opened(fake_spider)
    p.process_item({"a": 1}, fake_spider)
    p.process_item({"a": 2}, fake_spider)
    repo_mock.insert_brokers.assert_not_called()


def test_process_item_flushes_at_batch_size(repo_mock, fake_spider):
    p = PostgresPipeline(batch_size=3)
    p.spider_opened(fake_spider)
    repo_mock.insert_brokers.return_value = 3
    p.process_item({"a": 1}, fake_spider)
    p.process_item({"a": 2}, fake_spider)
    p.process_item({"a": 3}, fake_spider)
    repo_mock.insert_brokers.assert_called_once()
    args = repo_mock.insert_brokers.call_args.args
    assert len(args[0]) == 3
    assert args[1] == "run-abc"
    assert args[2] == "2026-05-06"
    fake_spider.crawler.stats.inc_value.assert_any_call("postgres/brokers_inserted", 3)


def test_process_item_returns_item_unchanged(repo_mock, fake_spider):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    item = {"brn": "1"}
    out = p.process_item(item, fake_spider)
    assert out is item


# ------------------------------------------------------------- spider_closed


def test_spider_closed_flushes_and_closes_run_ok(repo_mock, fake_spider):
    p = PostgresPipeline(batch_size=10)
    p.spider_opened(fake_spider)
    p.process_item({"brn": "1"}, fake_spider)
    p.process_item({"brn": "2"}, fake_spider)
    fake_spider.crawler.stats.get_stats.return_value = {
        "item_scraped_count": 2,
        "item_dropped_count": 0,
        "start_time": datetime(2026, 5, 6, 12, 0, 0),
    }
    repo_mock.insert_brokers.return_value = 2

    p.spider_closed(fake_spider, reason="finished")

    repo_mock.insert_brokers.assert_called_once()
    repo_mock.close_run.assert_called_once()
    kwargs = repo_mock.close_run.call_args.kwargs
    assert kwargs["run_id"] == "run-abc"
    assert kwargs["status"] == "ok"
    assert kwargs["items_scraped"] == 2
    assert kwargs["items_dropped"] == 0
    # stats should be JSON-coerced (datetime → ISO string)
    assert kwargs["stats"]["start_time"] == "2026-05-06T12:00:00"


@pytest.mark.parametrize(
    "reason",
    ["closespider_errorcount", "shutdown", "cancelled", "unexpected"],
)
def test_spider_closed_marks_failed_for_bad_reasons(
    repo_mock, fake_spider, reason
):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    p.spider_closed(fake_spider, reason=reason)
    assert repo_mock.close_run.call_args.kwargs["status"] == "failed"


@pytest.mark.parametrize(
    "reason",
    ["finished", "closespider_itemcount", "closespider_pagecount", "closespider_timeout"],
)
def test_spider_closed_marks_ok_for_deliberate_close(
    repo_mock, fake_spider, reason
):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    p.spider_closed(fake_spider, reason=reason)
    assert repo_mock.close_run.call_args.kwargs["status"] == "ok"


def test_spider_closed_drains_bad_items(repo_mock, fake_spider):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    fake_spider.bad_items = [
        {"run_id": "run-abc", "platform": "propertyfinder", "reason": "x", "payload": {}}
    ]
    repo_mock.insert_bad_items.return_value = 1

    p.spider_closed(fake_spider, reason="finished")

    repo_mock.insert_bad_items.assert_called_once_with(fake_spider.bad_items)
    fake_spider.crawler.stats.inc_value.assert_any_call("postgres/bad_items_inserted", 1)
    assert fake_spider.bad_items == []


def test_spider_closed_skips_bad_items_when_empty(repo_mock, fake_spider):
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    p.spider_closed(fake_spider, reason="finished")
    repo_mock.insert_bad_items.assert_not_called()


def test_spider_closed_close_run_runs_even_if_flush_raises(
    repo_mock, fake_spider
):
    """A failed broker flush must propagate but `close_run` still fires
    so the run isn't left as 'running' forever."""
    p = PostgresPipeline()
    p.spider_opened(fake_spider)
    p._broker_buffer.append({"brn": "1"})
    repo_mock.insert_brokers.side_effect = RuntimeError("DB exploded")

    with pytest.raises(RuntimeError):
        p.spider_closed(fake_spider, reason="finished")

    repo_mock.close_run.assert_called_once()
    assert repo_mock.close_run.call_args.kwargs["status"] == "failed"


# ------------------------------------------------------------- _jsonable_stats


def test_jsonable_stats_coerces_datetimes_recursively():
    src = {
        "scalar": 42,
        "start_time": datetime(2026, 5, 6, 12, 0, 0),
        "nested": {"finish_time": datetime(2026, 5, 6, 13, 0, 0)},
        "items": [datetime(2026, 5, 6, 14, 0, 0), "ok"],
    }
    out = _jsonable_stats(src)
    assert out["scalar"] == 42
    assert out["start_time"] == "2026-05-06T12:00:00"
    assert out["nested"]["finish_time"] == "2026-05-06T13:00:00"
    assert out["items"] == ["2026-05-06T14:00:00", "ok"]


def test_jsonable_stats_falls_back_to_repr_for_exotic_types():
    class Weird:
        def __repr__(self):
            return "Weird()"

    out = _jsonable_stats({"thing": Weird()})
    assert out["thing"] == "Weird()"
