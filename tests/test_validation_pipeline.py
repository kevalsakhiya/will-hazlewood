"""Side-effect coverage for `ValidationPipeline`. Schema rule coverage
lives in test_schemas.py — these tests focus on counters, buffer
appends, DropItem, and input-shape handling."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from scrapy.exceptions import DropItem

from broker_scout.items import PropertyFinderBrokerItem
from broker_scout.pipelines.validation import ValidationPipeline


def _today_iso() -> str:
    return datetime.now(UTC).date().isoformat()


# ------------------------------------------------------------ happy paths


def test_dict_input_returns_dict(pipeline_harness, make_item):
    pipeline, spider, _ = pipeline_harness
    out = pipeline.process_item(make_item(scrape_date=_today_iso()), spider)
    assert isinstance(out, dict)
    assert out["platform"] == "propertyfinder"


def test_dict_input_dates_round_trip_as_iso_strings(pipeline_harness, make_item):
    pipeline, spider, _ = pipeline_harness
    out = pipeline.process_item(
        make_item(
            scrape_date=_today_iso(),
            most_recent_listing_date_sale="2025-01-15",
        ),
        spider,
    )
    assert out["scrape_date"] == _today_iso()
    assert out["most_recent_listing_date_sale"] == "2025-01-15"


def test_dataclass_input_accepted(pipeline_harness):
    pipeline, spider, _ = pipeline_harness
    item = PropertyFinderBrokerItem(scrape_date=_today_iso())
    out = pipeline.process_item(item, spider)
    assert isinstance(out, dict)
    assert out["platform"] == "propertyfinder"


def test_passed_total_incremented(pipeline_harness, make_item):
    pipeline, spider, stats = pipeline_harness
    pipeline.process_item(make_item(scrape_date=_today_iso()), spider)
    stats.inc_value.assert_any_call("validation/passed_total")


# ------------------------------------------------------------- failure paths


def test_invalid_raises_dropitem(pipeline_harness, make_item):
    pipeline, spider, _ = pipeline_harness
    with pytest.raises(DropItem):
        pipeline.process_item(make_item(whatsapp_response_time=-1), spider)


def test_invalid_increments_failed_total(pipeline_harness, make_item):
    pipeline, spider, stats = pipeline_harness
    with pytest.raises(DropItem):
        pipeline.process_item(make_item(whatsapp_response_time=-1), spider)
    stats.inc_value.assert_any_call("validation/failed_total")


def test_invalid_increments_per_field_counter(pipeline_harness, make_item):
    pipeline, spider, stats = pipeline_harness
    with pytest.raises(DropItem):
        pipeline.process_item(make_item(whatsapp_response_time=-1), spider)
    stats.inc_value.assert_any_call("validation/failed_field/whatsapp_response_time")


def test_cross_field_failure_bucketed_under_model(pipeline_harness, make_item):
    pipeline, spider, stats = pipeline_harness
    payload = make_item(listings_for_sale=2, listings_for_rent=3, listings_total=99)
    with pytest.raises(DropItem):
        pipeline.process_item(payload, spider)
    stats.inc_value.assert_any_call("validation/failed_field/__model__")


def test_multiple_failing_fields_each_bucketed_once(pipeline_harness, make_item):
    pipeline, spider, stats = pipeline_harness
    payload = make_item(
        whatsapp_response_time=-1,
        broker_name="x" * 201,
        experience_since=1850,
    )
    with pytest.raises(DropItem):
        pipeline.process_item(payload, spider)
    bucket_calls = [
        c.args[0]
        for c in stats.inc_value.call_args_list
        if c.args[0].startswith("validation/failed_field/")
    ]
    assert "validation/failed_field/whatsapp_response_time" in bucket_calls
    assert "validation/failed_field/broker_name" in bucket_calls
    assert "validation/failed_field/experience_since" in bucket_calls
    # dedupe: each unique field should fire exactly once for this item
    assert len(bucket_calls) == len(set(bucket_calls))


# --------------------------------------------------------------- buffer


def test_buffer_entry_shape(pipeline_harness, make_item):
    pipeline, spider, _ = pipeline_harness
    payload = make_item(whatsapp_response_time=-1)
    with pytest.raises(DropItem):
        pipeline.process_item(payload, spider)
    assert len(spider.bad_items) == 1
    entry = spider.bad_items[0]
    assert set(entry.keys()) == {"run_id", "platform", "reason", "payload"}
    assert entry["run_id"] == "test-run"
    assert entry["platform"] == "propertyfinder"
    assert isinstance(entry["reason"], str) and entry["reason"]
    assert "whatsapp_response_time" in entry["reason"]
    assert set(entry["payload"].keys()) == {"item", "errors"}
    assert entry["payload"]["item"] == payload
    assert isinstance(entry["payload"]["errors"], list)
    assert len(entry["payload"]["errors"]) >= 1


def test_buffer_run_id_uses_spider_attr(pipeline_harness, make_item):
    pipeline, spider, _ = pipeline_harness
    spider.run_id = "custom-run-abc"
    with pytest.raises(DropItem):
        pipeline.process_item(make_item(whatsapp_response_time=-1), spider)
    assert spider.bad_items[0]["run_id"] == "custom-run-abc"


def test_buffer_initialized_when_missing(make_item):
    """Defensive path: if `RunIdExtension` didn't run, the pipeline must
    create the buffer rather than blowing up."""

    class StubSpider:
        run_id = "test"

    spider = StubSpider()
    pipeline = ValidationPipeline(stats=MagicMock())
    with pytest.raises(DropItem):
        pipeline.process_item(make_item(whatsapp_response_time=-1), spider)
    assert hasattr(spider, "bad_items")
    assert len(spider.bad_items) == 1


# --------------------------------------------------------------- exceptions


def test_unhandled_exception_propagates(pipeline_harness):
    """A non-ValidationError (e.g. malformed input that breaks
    `_to_payload`) must propagate, not be silently dropped."""
    pipeline, spider, _ = pipeline_harness
    with pytest.raises(TypeError):
        pipeline.process_item(42, spider)
    # Also: not converted to DropItem, not appended to buffer
    assert spider.bad_items == []
