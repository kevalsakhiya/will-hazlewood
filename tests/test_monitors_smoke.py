"""Smoke tests for the Spidermon plumbing landed in Phase 9.0.

Verifies the package imports, suite shapes are well-formed, and the
coverage tiers cover every dataclass field exactly once. Per-monitor
behavior tests come in Phase 9.6.
"""

from __future__ import annotations

import pytest


def test_monitors_package_importable():
    from broker_scout.monitors import actions, coverage_tiers, monitors

    assert actions is not None
    assert coverage_tiers is not None
    assert monitors is not None


def test_close_suite_has_at_least_one_monitor():
    from broker_scout.monitors.monitors import SpiderCloseMonitorSuite

    assert len(SpiderCloseMonitorSuite.monitors) >= 1
    # 9.0 ships exactly one — FinishReasonMonitor.
    from spidermon.contrib.scrapy.monitors import FinishReasonMonitor

    assert FinishReasonMonitor in SpiderCloseMonitorSuite.monitors


def test_periodic_suite_has_at_least_one_monitor():
    from broker_scout.monitors.monitors import PeriodicMonitorSuite

    assert len(PeriodicMonitorSuite.monitors) >= 1
    from spidermon.contrib.scrapy.monitors import ErrorCountMonitor

    assert ErrorCountMonitor in PeriodicMonitorSuite.monitors


def test_both_suites_wire_log_only_action():
    from broker_scout.monitors.actions import LogOnlyAction
    from broker_scout.monitors.monitors import (
        PeriodicMonitorSuite,
        SpiderCloseMonitorSuite,
    )

    assert LogOnlyAction in SpiderCloseMonitorSuite.monitors_finished_actions
    assert LogOnlyAction in PeriodicMonitorSuite.monitors_finished_actions


def test_log_only_action_inherits_spidermon_action():
    from spidermon.core.actions import Action

    from broker_scout.monitors.actions import LogOnlyAction

    assert issubclass(LogOnlyAction, Action)


def test_log_only_action_can_be_constructed_from_crawler_kwargs():
    """Spidermon calls Action.from_crawler(crawler) → from_crawler_kwargs;
    ours returns {} so the call must succeed without arguments."""
    from broker_scout.monitors.actions import LogOnlyAction

    kwargs = LogOnlyAction.from_crawler_kwargs(crawler=None)
    assert kwargs == {}
    instance = LogOnlyAction(**kwargs)
    assert instance is not None


def test_coverage_tiers_no_field_appears_in_two_tiers():
    """Module-load asserts in coverage_tiers.py would have failed at
    import time, but a duplicate-tier scan here gives a clearer error
    message if someone reorganizes the constants."""
    from broker_scout.monitors import coverage_tiers as t

    all_fields = (
        t.PROVENANCE_FIELDS
        + t.PF_CRITICAL_FIELDS
        + t.PF_HIGH_FIELDS
        + t.PF_MEDIUM_FIELDS
        + t.INFORMATIONAL_FIELDS
        + t.OMITTED_FIELDS
    )
    seen = set()
    duplicates = []
    for f in all_fields:
        if f in seen:
            duplicates.append(f)
        seen.add(f)
    assert not duplicates, f"fields in multiple tiers: {duplicates}"


def test_coverage_tiers_cover_every_dataclass_field():
    """Phase 8 (Bayut) will likely add fields to PropertyFinderBrokerItem
    or a sibling class. This catches forgotten tiering."""
    from broker_scout.items import PropertyFinderBrokerItem
    from broker_scout.monitors import coverage_tiers as t

    all_tiered = set(
        t.PROVENANCE_FIELDS
        + t.PF_CRITICAL_FIELDS
        + t.PF_HIGH_FIELDS
        + t.PF_MEDIUM_FIELDS
        + t.INFORMATIONAL_FIELDS
        + t.OMITTED_FIELDS
    )
    dataclass_fields = set(PropertyFinderBrokerItem.__dataclass_fields__)
    missing = dataclass_fields - all_tiered
    extra = all_tiered - dataclass_fields
    assert not missing, f"dataclass fields not tiered: {missing}"
    assert not extra, f"tiered fields not on the dataclass: {extra}"


@pytest.fixture
def stub_action_result():
    """Spidermon's Action.run_action reads `self.result` and `self.data`.
    Build minimal stand-ins so we can drive the action without spinning
    up a full Spidermon test harness."""
    from types import SimpleNamespace

    return SimpleNamespace(
        monitors_passed_results=[1, 2, 3],  # length is what we read
        monitors_failed_results=[],
    )


def test_log_only_action_emits_info_on_success(stub_action_result, caplog):
    import logging

    from broker_scout.monitors.actions import LogOnlyAction

    action = LogOnlyAction()
    action.result = stub_action_result
    action.data = None

    with caplog.at_level(logging.INFO, logger="broker_scout.monitors.actions"):
        action.run_action()

    assert any(
        "monitors passed" in rec.message and rec.levelno == logging.INFO
        for rec in caplog.records
    )


def test_log_only_action_emits_error_per_failure(caplog):
    import logging
    from types import SimpleNamespace

    from broker_scout.monitors.actions import LogOnlyAction

    failures = [
        SimpleNamespace(
            monitor=SimpleNamespace(name="MonitorA"),
            error_message="something broke at line 42",
        ),
        SimpleNamespace(
            monitor=SimpleNamespace(name="MonitorB"),
            error_message="x" * 1000,  # truncation case
        ),
    ]
    action = LogOnlyAction()
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=failures,
    )
    action.data = None

    with caplog.at_level(logging.ERROR, logger="broker_scout.monitors.actions"):
        action.run_action()

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 2
    monitors_logged = {r.__dict__.get("monitor") for r in error_records}
    assert monitors_logged == {"MonitorA", "MonitorB"}
    # Truncation: long error message was cut off
    long_record = next(
        r for r in error_records if r.__dict__.get("monitor") == "MonitorB"
    )
    assert len(long_record.__dict__.get("reason", "")) <= 500
