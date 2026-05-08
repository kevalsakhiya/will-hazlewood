"""Phase 9.2 — periodic circuit-breaker monitors + CloseSpiderAction.

Verifies the rate-limit threshold logic and the once-only firing
guarantee on the close action.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from broker_scout.monitors.actions import CloseSpiderAction, LogOnlyAction
from broker_scout.monitors.monitors import (
    DEFAULT_PERIODIC_429_THRESHOLD,
    PeriodicMonitorSuite,
    PeriodicRateLimitMonitor,
)


class FakeSettings:
    def __init__(self, **values):
        self._values = values

    def getint(self, key, default=None):
        if key in self._values:
            return int(self._values[key])
        return int(default) if default is not None else None

    def getfloat(self, key, default=None):
        if key in self._values:
            return float(self._values[key])
        return float(default) if default is not None else None


def _make_monitor(monitor_cls, method, stats, settings=None):
    instance = monitor_cls(method)
    crawler = SimpleNamespace(settings=settings or FakeSettings())
    instance.data = SimpleNamespace(stats=stats, crawler=crawler, spider=None)
    return instance


# ============================================================ PeriodicRateLimitMonitor


def test_429_below_threshold_passes():
    m = _make_monitor(
        PeriodicRateLimitMonitor,
        "test_429_below_threshold",
        stats={"downloader/response_status_count/429": 30},
    )
    m.test_429_below_threshold()


def test_429_at_threshold_passes():
    """Boundary: equal to threshold passes (`assertLessEqual`)."""
    m = _make_monitor(
        PeriodicRateLimitMonitor,
        "test_429_below_threshold",
        stats={"downloader/response_status_count/429": DEFAULT_PERIODIC_429_THRESHOLD},
    )
    m.test_429_below_threshold()


def test_429_above_threshold_fails():
    m = _make_monitor(
        PeriodicRateLimitMonitor,
        "test_429_below_threshold",
        stats={
            "downloader/response_status_count/429": DEFAULT_PERIODIC_429_THRESHOLD + 1
        },
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_429_below_threshold()
    assert "rate-limited" in str(exc_info.value)
    assert "429 responses" in str(exc_info.value)


def test_429_zero_passes():
    m = _make_monitor(
        PeriodicRateLimitMonitor,
        "test_429_below_threshold",
        stats={"downloader/response_status_count/200": 1000},
    )
    m.test_429_below_threshold()


def test_429_threshold_configurable():
    """A run with proxies tuned for high 429s sets a higher threshold."""
    m = _make_monitor(
        PeriodicRateLimitMonitor,
        "test_429_below_threshold",
        stats={"downloader/response_status_count/429": 200},
        settings=FakeSettings(PERIODIC_429_THRESHOLD=500),
    )
    m.test_429_below_threshold()


# ============================================================ CloseSpiderAction


@pytest.fixture
def reset_close_action():
    """The action's _fired flag is class-level — reset before & after
    each test so they don't bleed into one another."""
    CloseSpiderAction.reset()
    yield
    CloseSpiderAction.reset()


def test_close_spider_action_fires_engine_close(reset_close_action):
    crawler = MagicMock()
    crawler.spider = MagicMock(name="spider")
    action = CloseSpiderAction(crawler=crawler)
    action.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    action.run_action()
    crawler.engine.close_spider.assert_called_once_with(
        crawler.spider, "circuit_breaker"
    )


def test_close_spider_action_idempotent_across_ticks(reset_close_action):
    """Subsequent ticks don't repeatedly call engine.close_spider —
    sustained failures shouldn't spam the engine."""
    crawler = MagicMock()
    crawler.spider = MagicMock()

    a1 = CloseSpiderAction(crawler=crawler)
    a1.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    a1.run_action()

    a2 = CloseSpiderAction(crawler=crawler)
    a2.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    a2.run_action()

    assert crawler.engine.close_spider.call_count == 1


def test_close_spider_action_handles_missing_engine(reset_close_action, caplog):
    """Defensive: if crawler.engine is None (early init / late
    teardown), don't crash — log a warning."""
    import logging

    crawler = MagicMock()
    crawler.engine = None
    crawler.spider = MagicMock()
    action = CloseSpiderAction(crawler=crawler)
    action.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    with caplog.at_level(logging.WARNING, logger="broker_scout.monitors.actions"):
        action.run_action()
    warned = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("engine/spider unavailable" in r.message for r in warned)


def test_close_spider_action_from_crawler(reset_close_action):
    """Spidermon constructs actions via the from_crawler classmethod —
    it must accept a crawler and return a working instance."""
    crawler = MagicMock()
    action = CloseSpiderAction.from_crawler(crawler=crawler)
    assert action._crawler is crawler


# ============================================================ suite wiring


def test_periodic_suite_fires_actions_only_on_failure():
    """We deliberately DON'T put LogOnlyAction in monitors_finished_actions
    of the periodic suite — passing ticks shouldn't log every 60s."""
    assert LogOnlyAction not in PeriodicMonitorSuite.monitors_finished_actions
    assert LogOnlyAction in PeriodicMonitorSuite.monitors_failed_actions


def test_periodic_suite_includes_close_action():
    assert CloseSpiderAction in PeriodicMonitorSuite.monitors_failed_actions


def test_periodic_suite_has_both_circuit_breakers():
    from spidermon.contrib.scrapy.monitors import ErrorCountMonitor

    assert ErrorCountMonitor in PeriodicMonitorSuite.monitors
    assert PeriodicRateLimitMonitor in PeriodicMonitorSuite.monitors


def test_default_threshold_matches_settings():
    from broker_scout import settings

    assert DEFAULT_PERIODIC_429_THRESHOLD == settings.PERIODIC_429_THRESHOLD
