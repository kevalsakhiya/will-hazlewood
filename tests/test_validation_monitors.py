"""Phase 9.1 — `ValidationFailureRateMonitor` + `ValidationFailureByFieldMonitor`.

Drives each monitor's test method against a synthetic stats dict +
fake settings. Spidermon's runtime would normally instantiate via
`unittest`'s loader and pass `self.data` from the SpidermonExtension
— for unit-testing logic in isolation we set those attributes
manually. Real-spider verification uses the live spider run.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from broker_scout.monitors.monitors import (
    DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD,
    DEFAULT_VALIDATION_FIELD_FAILURE_RATE_THRESHOLD,
    ValidationFailureByFieldMonitor,
    ValidationFailureRateMonitor,
)


class FakeSettings:
    """Mimics scrapy.settings.Settings.getfloat enough for our monitors."""

    def __init__(self, **values):
        self._values = values

    def getfloat(self, key, default=None):
        if key in self._values:
            return float(self._values[key])
        return float(default) if default is not None else None


def _fake_data(stats: dict, settings: FakeSettings | None = None) -> SimpleNamespace:
    crawler = SimpleNamespace(
        settings=settings or FakeSettings(),
        stats=SimpleNamespace(get_stats=lambda: stats),
    )
    return SimpleNamespace(stats=stats, crawler=crawler, spider=None)


def _make(monitor_cls, method, stats, settings=None):
    instance = monitor_cls(method)
    instance.data = _fake_data(stats, settings)
    return instance


# ============================================================ ValidationFailureRateMonitor


def test_failure_rate_below_threshold_passes():
    m = _make(
        ValidationFailureRateMonitor,
        "test_failure_rate_below_threshold",
        stats={"validation/passed_total": 95, "validation/failed_total": 5},
    )
    m.test_failure_rate_below_threshold()  # 5% rate, default threshold 5% → equal, passes (assertLessEqual)


def test_failure_rate_above_threshold_fails():
    m = _make(
        ValidationFailureRateMonitor,
        "test_failure_rate_below_threshold",
        stats={"validation/passed_total": 90, "validation/failed_total": 10},
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_failure_rate_below_threshold()
    assert "10.00%" in str(exc_info.value)
    assert "10/100" in str(exc_info.value)


def test_failure_rate_zero_failed_passes():
    m = _make(
        ValidationFailureRateMonitor,
        "test_failure_rate_below_threshold",
        stats={"validation/passed_total": 100, "validation/failed_total": 0},
    )
    m.test_failure_rate_below_threshold()  # 0% rate → passes


def test_failure_rate_zero_total_skips():
    """No validation activity → skip rather than fail. ZeroItemsMonitor
    (Phase 9.3.1) handles the genuinely-broken-spider case. We pass a
    non-empty stats dict (with unrelated counters) because Spidermon's
    StatsMonitorMixin treats an empty dict as 'stats not available'."""
    m = _make(
        ValidationFailureRateMonitor,
        "test_failure_rate_below_threshold",
        stats={"item_scraped_count": 0},
    )
    import unittest

    with pytest.raises(unittest.SkipTest):
        m.test_failure_rate_below_threshold()


def test_failure_rate_threshold_configurable():
    """Loosen threshold to 20% — same data that fails at 5% now passes."""
    m = _make(
        ValidationFailureRateMonitor,
        "test_failure_rate_below_threshold",
        stats={"validation/passed_total": 90, "validation/failed_total": 10},
        settings=FakeSettings(VALIDATION_FAILURE_RATE_THRESHOLD=0.20),
    )
    m.test_failure_rate_below_threshold()


# ============================================================ ValidationFailureByFieldMonitor


def test_no_failed_fields_passes():
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={"item_scraped_count": 100},
    )
    m.test_per_field_failure_rates()


def test_field_below_threshold_passes():
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={
            "item_scraped_count": 100,
            "validation/failed_field/whatsapp_response_time": 5,  # 5%, below 10%
        },
    )
    m.test_per_field_failure_rates()


def test_single_field_above_threshold_fails():
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={
            "item_scraped_count": 100,
            "validation/failed_field/listings_total": 15,  # 15%
        },
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_per_field_failure_rates()
    msg = str(exc_info.value)
    assert "listings_total" in msg
    assert "15.00%" in msg


def test_multiple_fields_above_threshold_listed():
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={
            "item_scraped_count": 100,
            "validation/failed_field/listings_total": 15,
            "validation/failed_field/experience_since": 12,
            "validation/failed_field/broker_name": 5,  # below — not listed
        },
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_per_field_failure_rates()
    msg = str(exc_info.value)
    assert "listings_total" in msg
    assert "experience_since" in msg
    assert "broker_name" not in msg


def test_zero_items_skips():
    import unittest

    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={"item_scraped_count": 0, "validation/failed_field/x": 1},
    )
    with pytest.raises(unittest.SkipTest):
        m.test_per_field_failure_rates()


def test_per_field_threshold_configurable():
    """Tighten to 1%; a previously-passing 5% rate now fails."""
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={
            "item_scraped_count": 100,
            "validation/failed_field/whatsapp_response_time": 5,
        },
        settings=FakeSettings(VALIDATION_FIELD_FAILURE_RATE_THRESHOLD=0.01),
    )
    with pytest.raises(AssertionError):
        m.test_per_field_failure_rates()


def test_unrelated_stats_not_treated_as_field_failures():
    """Only `validation/failed_field/*` counters count — `match/exact_brn`
    and friends shouldn't trigger this monitor."""
    m = _make(
        ValidationFailureByFieldMonitor,
        "test_per_field_failure_rates",
        stats={
            "item_scraped_count": 100,
            "match/exact_brn": 100,  # 100% — but not a validation failure
            "extract/brn/fallback_used": 50,
        },
    )
    m.test_per_field_failure_rates()


# ============================================================ defaults sanity


def test_defaults_match_settings():
    """Default constants in monitors.py must match the values in
    settings.py (otherwise running without VALIDATION_*_THRESHOLD
    env override would behave differently from the configured value)."""
    from broker_scout import settings

    assert (
        DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD
        == settings.VALIDATION_FAILURE_RATE_THRESHOLD
    )
    assert (
        DEFAULT_VALIDATION_FIELD_FAILURE_RATE_THRESHOLD
        == settings.VALIDATION_FIELD_FAILURE_RATE_THRESHOLD
    )
