"""Phase 9.3 — close-suite custom monitors.

Drives each monitor's test method against synthetic stats + settings.
The full close suite runs on engine_stopped at runtime; here we
exercise individual checks in isolation.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

import pytest

from broker_scout.monitors.monitors import (
    DEFAULT_AMBIGUOUS_RATE_THRESHOLD,
    DEFAULT_BRN_DRIFT_THRESHOLD,
    DEFAULT_MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD,
    DEFAULT_NOT_FOUND_RATE_THRESHOLD,
    DEFAULT_RETRY_RATE_THRESHOLD,
    AmbiguousRateMonitor,
    BRNDriftMonitor,
    ExtractionFailureMonitor,
    MatchStatusDistributionMonitor,
    NotFoundRateMonitor,
    PipelineFailureMonitor,
    RetryRateMonitor,
    ZeroItemsMonitor,
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


def _make(monitor_cls, method, stats, settings=None):
    instance = monitor_cls(method)
    crawler = SimpleNamespace(settings=settings or FakeSettings())
    instance.data = SimpleNamespace(stats=stats, crawler=crawler, spider=None)
    return instance


# ============================================================ ZeroItemsMonitor


def test_zero_items_fails():
    m = _make(
        ZeroItemsMonitor,
        "test_at_least_one_item_scraped",
        stats={"item_scraped_count": 0, "_marker": 1},
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_at_least_one_item_scraped()
    assert "zero items" in str(exc_info.value)


def test_one_item_passes():
    m = _make(
        ZeroItemsMonitor,
        "test_at_least_one_item_scraped",
        stats={"item_scraped_count": 1},
    )
    m.test_at_least_one_item_scraped()


# ============================================================ RetryRateMonitor


def test_retry_rate_below_threshold_passes():
    m = _make(
        RetryRateMonitor,
        "test_retry_rate_below_threshold",
        stats={"downloader/request_count": 100, "retry/count": 10},
    )
    m.test_retry_rate_below_threshold()  # 10% < 15%


def test_retry_rate_above_threshold_fails():
    m = _make(
        RetryRateMonitor,
        "test_retry_rate_below_threshold",
        stats={"downloader/request_count": 100, "retry/count": 25},
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_retry_rate_below_threshold()
    assert "25.00%" in str(exc_info.value)


def test_retry_rate_zero_requests_skips():
    m = _make(
        RetryRateMonitor,
        "test_retry_rate_below_threshold",
        stats={"_marker": 1, "downloader/request_count": 0},
    )
    with pytest.raises(unittest.SkipTest):
        m.test_retry_rate_below_threshold()


def test_retry_rate_default_matches_settings():
    from broker_scout import settings

    assert DEFAULT_RETRY_RATE_THRESHOLD == settings.RETRY_RATE_THRESHOLD


# ============================================================ PipelineFailureMonitor


def _pipeline_stats_ok(scraped: int = 5):
    return {
        "item_scraped_count": scraped,
        "postgres/brokers_inserted": scraped,
        "gsheets/rows_appended": scraped,
        "gsheets/flush_failed": 0,
        "gdrive_csv/upload_status": "ok",
        "gdrive_csv/rows_uploaded": scraped,
    }


def test_pipeline_all_sinks_match():
    s = _pipeline_stats_ok()
    for method in (
        "test_postgres_inserted_all_items",
        "test_gsheets_appended_all_items",
        "test_gsheets_no_flush_failure",
        "test_gdrive_csv_upload_status_ok",
        "test_gdrive_csv_rows_uploaded_match",
    ):
        m = _make(PipelineFailureMonitor, method, s)
        getattr(m, method)()


def test_pipeline_postgres_mismatch_fails():
    s = _pipeline_stats_ok()
    s["postgres/brokers_inserted"] = 3  # but scraped 5
    m = _make(
        PipelineFailureMonitor, "test_postgres_inserted_all_items", s
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_postgres_inserted_all_items()
    assert "3" in str(exc_info.value) and "5" in str(exc_info.value)


def test_pipeline_gsheets_mismatch_fails():
    s = _pipeline_stats_ok()
    s["gsheets/rows_appended"] = 4
    m = _make(PipelineFailureMonitor, "test_gsheets_appended_all_items", s)
    with pytest.raises(AssertionError):
        m.test_gsheets_appended_all_items()


def test_pipeline_gsheets_flush_failed_fails():
    s = _pipeline_stats_ok()
    s["gsheets/flush_failed"] = 1
    m = _make(PipelineFailureMonitor, "test_gsheets_no_flush_failure", s)
    with pytest.raises(AssertionError):
        m.test_gsheets_no_flush_failure()


def test_pipeline_gdrive_status_failed_fails():
    s = _pipeline_stats_ok()
    s["gdrive_csv/upload_status"] = "failed"
    m = _make(PipelineFailureMonitor, "test_gdrive_csv_upload_status_ok", s)
    with pytest.raises(AssertionError) as exc_info:
        m.test_gdrive_csv_upload_status_ok()
    assert "failed" in str(exc_info.value)


def test_pipeline_zero_items_skips_all_checks():
    """Zero items → ZeroItemsMonitor catches it; pipeline checks would
    spuriously assert 0==0 and pass, hiding the real signal. Skip
    instead."""
    s = {"_marker": 1, "item_scraped_count": 0}
    for method in (
        "test_postgres_inserted_all_items",
        "test_gsheets_appended_all_items",
        "test_gdrive_csv_upload_status_ok",
        "test_gdrive_csv_rows_uploaded_match",
    ):
        m = _make(PipelineFailureMonitor, method, s)
        with pytest.raises(unittest.SkipTest):
            getattr(m, method)()


# ============================================================ ExtractionFailureMonitor


def _extract_stats(scraped: int = 100, **counters):
    base = {"item_scraped_count": scraped}
    base.update(counters)
    return base


def test_extraction_clean_run_passes():
    """All counters at zero — every test method passes."""
    s = _extract_stats(scraped=100)
    methods = [
        "test_next_data_missing_rate",
        "test_next_data_bad_json_count",
        "test_agent_data_missing_rate",
        "test_search_json_fallback_rate",
        "test_brn_fallback_rate",
        "test_listings_api_non_json_rate",
        "test_listings_api_empty_rate",
        "test_agency_license_missing_rate",
    ]
    for method in methods:
        m = _make(ExtractionFailureMonitor, method, s)
        getattr(m, method)()


def test_extraction_next_data_bad_json_any_fails():
    """Absolute threshold of 0 — even one fails."""
    s = _extract_stats(scraped=100, **{"extract/next_data/bad_json": 1})
    m = _make(ExtractionFailureMonitor, "test_next_data_bad_json_count", s)
    with pytest.raises(AssertionError):
        m.test_next_data_bad_json_count()


def test_extraction_brn_fallback_above_20pct_fails():
    s = _extract_stats(scraped=100, **{"extract/brn/fallback_used": 25})
    m = _make(ExtractionFailureMonitor, "test_brn_fallback_rate", s)
    with pytest.raises(AssertionError):
        m.test_brn_fallback_rate()


def test_extraction_search_json_fallback_above_5pct_fails():
    s = _extract_stats(scraped=100, **{"extract/search_json/fallback_used": 10})
    m = _make(ExtractionFailureMonitor, "test_search_json_fallback_rate", s)
    with pytest.raises(AssertionError):
        m.test_search_json_fallback_rate()


def test_extraction_zero_items_skips():
    s = _extract_stats(scraped=0, **{"extract/brn/fallback_used": 10})
    m = _make(ExtractionFailureMonitor, "test_brn_fallback_rate", s)
    with pytest.raises(unittest.SkipTest):
        m.test_brn_fallback_rate()


# ============================================================ Match monitors


def test_match_high_confidence_above_threshold_passes():
    m = _make(
        MatchStatusDistributionMonitor,
        "test_high_confidence_match_rate",
        stats={
            "item_scraped_count": 100,
            "match/exact_brn": 50,
            "match/name_unique": 20,
        },
    )
    m.test_high_confidence_match_rate()  # 70% > 60%


def test_match_high_confidence_below_threshold_fails():
    m = _make(
        MatchStatusDistributionMonitor,
        "test_high_confidence_match_rate",
        stats={
            "item_scraped_count": 100,
            "match/exact_brn": 30,
            "match/name_unique": 20,
        },
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_high_confidence_match_rate()
    assert "50.00%" in str(exc_info.value)


def test_not_found_below_threshold_passes():
    m = _make(
        NotFoundRateMonitor,
        "test_not_found_rate_below_threshold",
        stats={"item_scraped_count": 100, "match/not_found": 30},
    )
    m.test_not_found_rate_below_threshold()


def test_not_found_above_threshold_fails():
    m = _make(
        NotFoundRateMonitor,
        "test_not_found_rate_below_threshold",
        stats={"item_scraped_count": 100, "match/not_found": 70},
    )
    with pytest.raises(AssertionError):
        m.test_not_found_rate_below_threshold()


def test_ambiguous_below_threshold_passes():
    m = _make(
        AmbiguousRateMonitor,
        "test_ambiguous_rate_below_threshold",
        stats={"item_scraped_count": 100, "match/ambiguous": 3},
    )
    m.test_ambiguous_rate_below_threshold()


def test_ambiguous_above_threshold_fails():
    m = _make(
        AmbiguousRateMonitor,
        "test_ambiguous_rate_below_threshold",
        stats={"item_scraped_count": 100, "match/ambiguous": 10},
    )
    with pytest.raises(AssertionError):
        m.test_ambiguous_rate_below_threshold()


# ============================================================ BRNDriftMonitor


def test_brn_drift_zero_passes():
    m = _make(
        BRNDriftMonitor, "test_no_brn_drift", stats={"_marker": 1}
    )
    m.test_no_brn_drift()


def test_brn_drift_one_fails():
    m = _make(
        BRNDriftMonitor,
        "test_no_brn_drift",
        stats={"match/brn_drift": 1},
    )
    with pytest.raises(AssertionError) as exc_info:
        m.test_no_brn_drift()
    assert "PF BRN ≠ DLD BRN" in str(exc_info.value)


def test_brn_drift_threshold_configurable():
    """Operations may temporarily raise the threshold while
    investigating known drift cases."""
    m = _make(
        BRNDriftMonitor,
        "test_no_brn_drift",
        stats={"match/brn_drift": 5},
        settings=FakeSettings(BRN_DRIFT_THRESHOLD=10),
    )
    m.test_no_brn_drift()


# ============================================================ default↔settings parity


# ============================================================ MatchedRowFieldCoverageMonitor


@pytest.fixture
def patch_field_coverage(monkeypatch):
    """Patch brokers_repo.matched_field_coverage so the monitor doesn't
    actually hit Postgres in unit tests."""
    calls = []

    def _fake(run_id, fields, *args, **kwargs):
        calls.append((run_id, tuple(fields)))
        return _fake.return_value or {}

    _fake.return_value = {}

    monkeypatch.setattr(
        "broker_scout.monitors.monitors.brokers_repo.matched_field_coverage",
        _fake,
    )
    return _fake, calls


def _coverage_monitor(method, run_id="r1", settings=None):
    from broker_scout.monitors.monitors import MatchedRowFieldCoverageMonitor

    stats = {"run_id": run_id, "_marker": 1}
    return _make(MatchedRowFieldCoverageMonitor, method, stats, settings)


def test_field_coverage_critical_passes_when_above_threshold(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {"broker_name": 0.99, "agent_url": 0.97, "brn": 0.96}
    m = _coverage_monitor("test_critical_field_coverage")
    m.test_critical_field_coverage()


def test_field_coverage_critical_fails_below_threshold(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {"broker_name": 0.99, "agent_url": 0.50, "brn": 0.96}
    m = _coverage_monitor("test_critical_field_coverage")
    with pytest.raises(AssertionError) as exc_info:
        m.test_critical_field_coverage()
    msg = str(exc_info.value)
    assert "agent_url=50%" in msg
    # Above-threshold fields shouldn't appear in the failure message
    assert "broker_name" not in msg


def test_field_coverage_high_uses_lower_threshold(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {f: 0.85 for f in
                         ("listings_total", "experience_since", "nationality",
                          "agency_url", "agency_name")}
    m = _coverage_monitor("test_high_field_coverage")
    m.test_high_field_coverage()  # 85% > 80%


def test_field_coverage_medium_passes_at_50pct(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {f: 0.51 for f in
                         ("whatsapp_response_time", "is_superagent",
                          "agent_specialization", "agency_registration_number")}
    m = _coverage_monitor("test_medium_field_coverage")
    m.test_medium_field_coverage()


def test_field_coverage_no_matched_rows_skips(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {}  # zero matched rows
    m = _coverage_monitor("test_critical_field_coverage")
    with pytest.raises(unittest.SkipTest):
        m.test_critical_field_coverage()


def test_field_coverage_no_run_id_skips(patch_field_coverage):
    """Spider didn't open properly → skip gracefully."""
    from broker_scout.monitors.monitors import MatchedRowFieldCoverageMonitor

    m = _make(
        MatchedRowFieldCoverageMonitor,
        "test_critical_field_coverage",
        stats={"_marker": 1},  # no run_id
    )
    with pytest.raises(unittest.SkipTest):
        m.test_critical_field_coverage()


def test_field_coverage_db_error_skips(patch_field_coverage):
    """A flaky DB shouldn't fail the suite — skip and let other monitors
    run on in-memory stats."""
    fake, _ = patch_field_coverage

    def _boom(*args, **kwargs):
        raise RuntimeError("connection refused")

    fake.side_effect = _boom
    fake.return_value = None  # ignored due to side_effect
    m = _coverage_monitor("test_critical_field_coverage")
    with pytest.raises(unittest.SkipTest):
        m.test_critical_field_coverage()


def test_field_coverage_threshold_configurable(patch_field_coverage):
    fake, _ = patch_field_coverage
    fake.return_value = {"broker_name": 0.5, "agent_url": 0.5, "brn": 0.5}
    # Override Critical threshold to 0.4 — failing data now passes.
    m = _coverage_monitor(
        "test_critical_field_coverage",
        settings=FakeSettings(FIELD_COVERAGE_CRITICAL_THRESHOLD=0.4),
    )
    m.test_critical_field_coverage()


def test_match_defaults_match_settings():
    from broker_scout import settings

    assert (
        DEFAULT_MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD
        == settings.MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD
    )
    assert DEFAULT_NOT_FOUND_RATE_THRESHOLD == settings.NOT_FOUND_RATE_THRESHOLD
    assert DEFAULT_AMBIGUOUS_RATE_THRESHOLD == settings.AMBIGUOUS_RATE_THRESHOLD
    assert DEFAULT_BRN_DRIFT_THRESHOLD == settings.BRN_DRIFT_THRESHOLD
