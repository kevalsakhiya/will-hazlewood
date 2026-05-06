"""Shared fixtures for the validation test suite."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from broker_scout.items import PropertyFinderBrokerItem
from broker_scout.pipelines.validation import ValidationPipeline


@pytest.fixture
def make_item():
    """Factory returning a `PropertyFinderBrokerItem.to_dict()` with valid
    defaults plus any field overrides. Tests pass only the field under test."""

    def _make(**overrides) -> dict:
        item = PropertyFinderBrokerItem()
        for k, v in overrides.items():
            setattr(item, k, v)
        return item.to_dict()

    return _make


@pytest.fixture
def pipeline_harness():
    """Returns (pipeline, spider, stats_mock).

    `spider.run_id = 'test-run'`, `spider.bad_items = []`, `stats` is a
    `MagicMock` so call args are inspectable.
    """
    stats = MagicMock()
    spider = MagicMock()
    spider.run_id = "test-run"
    spider.bad_items = []
    pipeline = ValidationPipeline(stats=stats)
    return pipeline, spider, stats
