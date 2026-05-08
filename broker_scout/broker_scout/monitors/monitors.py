"""Spidermon monitor suites for broker_scout.

Two suites, both wired in [settings.py](../settings.py):

  * `SpiderCloseMonitorSuite` — runs once when the spider closes.
    Phase 9.0 ships only `FinishReasonMonitor`. Phase 9.1+ adds the
    custom rate / coverage / pipeline monitors.
  * `PeriodicMonitorSuite` — runs every 60s. Phase 9.0 ships only
    `ErrorCountMonitor` as a circuit breaker. Phase 9.2 adds tuned
    HTTP-code circuit breakers.

Every suite wires `LogOnlyAction` into `monitors_finished_actions` so
both pass and fail surface in the JSON log. Phase 11 will append
`GoogleChatNotifier` to the same hook.
"""

from __future__ import annotations

from spidermon import MonitorSuite
from spidermon.contrib.scrapy.monitors import (
    ErrorCountMonitor,
    FinishReasonMonitor,
    UnwantedHTTPCodesMonitor,
)
from spidermon.contrib.scrapy.monitors.base import BaseScrapyMonitor

from broker_scout.common import brokers_repo
from broker_scout.monitors import coverage_tiers
from broker_scout.monitors.actions import (
    CloseSpiderAction,
    LogOnlyAction,
    SendChatSummaryAction,
    SendCriticalChatAlertAction,
)

# ---------------------------------------------------------------- defaults

DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD = 0.05
DEFAULT_VALIDATION_FIELD_FAILURE_RATE_THRESHOLD = 0.10
DEFAULT_PERIODIC_429_THRESHOLD = 50
DEFAULT_RETRY_RATE_THRESHOLD = 0.15
DEFAULT_MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD = 0.60  # exact_brn + name_unique
DEFAULT_NOT_FOUND_RATE_THRESHOLD = 0.50
DEFAULT_AMBIGUOUS_RATE_THRESHOLD = 0.05
DEFAULT_BRN_DRIFT_THRESHOLD = 0  # any drift fires
# Per-tier matched-row field-coverage minimums.
DEFAULT_FIELD_COVERAGE_CRITICAL = 0.95
DEFAULT_FIELD_COVERAGE_HIGH = 0.80
DEFAULT_FIELD_COVERAGE_MEDIUM = 0.50
# Per-counter rate ceilings for the extraction-health monitor.
DEFAULT_EXTRACTION_THRESHOLDS: dict[str, float | int] = {
    "extract/next_data/missing": 0.01,         # rate
    "extract/next_data/bad_json": 0,           # absolute
    "extract/agent_data/missing": 0.01,        # rate
    "extract/search_json/fallback_used": 0.05, # rate
    "extract/brn/fallback_used": 0.20,         # rate
    "extract/listings_api/non_json": 0.05,     # rate
    "extract/listings_api/empty": 0.10,        # rate
    "extract/agency_license/missing": 0.30,    # rate
}


# ---------------------------------------------------------------- 9.1 monitors


class _BrokerScoutMonitor(BaseScrapyMonitor):
    """Common base for our custom monitors.

    `__test__ = False` opts these classes out of pytest's auto-
    discovery of `unittest.TestCase` subclasses. Spidermon uses the
    stdlib `TestLoader` which ignores the attribute, so suite
    execution at runtime is unaffected — only test collection in our
    own pytest runs is.

    `severity` is read by Phase 11 actions (SendChatSummaryAction)
    to colour-code alerts: critical → red, warning → yellow.
    Default 'critical' — safer to over-alert and demote individual
    monitors as we calibrate.
    """

    __test__ = False
    severity: str = "critical"


class ValidationFailureRateMonitor(_BrokerScoutMonitor):
    """Validation pipeline failure rate must stay below the threshold.

    Reads the `validation/passed_total` and `validation/failed_total`
    counters set by `pipelines/validation.py`. Skips the test entirely
    if no items reached the validator (e.g. spider crashed before the
    first item) — no signal vs. legitimate spider failure, which is
    `ZeroItemsMonitor`'s job (Phase 9.3.1).
    """

    def test_failure_rate_below_threshold(self):
        passed = self.stats.get("validation/passed_total", 0)
        failed = self.stats.get("validation/failed_total", 0)
        total = passed + failed
        if total == 0:
            self.skipTest("no validation activity")
            return
        rate = failed / total
        threshold = self.crawler.settings.getfloat(
            "VALIDATION_FAILURE_RATE_THRESHOLD",
            DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD,
        )
        self.assertLessEqual(
            rate,
            threshold,
            f"validation failure rate {rate:.2%} ({failed}/{total}) "
            f"exceeds threshold {threshold:.2%}",
        )


class ValidationFailureByFieldMonitor(_BrokerScoutMonitor):
    """No single schema field may account for >N% of items dropped.

    Reads every `validation/failed_field/{field}` counter (set by
    `pipelines/validation.py` per dropped item) and divides by
    `item_scraped_count`. A spike in one field's failure rate is a
    classic PF-schema-drift signal: the field changed shape and our
    schema rejects every item with that field present.
    """

    def test_per_field_failure_rates(self):
        item_count = self.stats.get("item_scraped_count", 0)
        if item_count == 0:
            self.skipTest("no items scraped")
            return

        threshold = self.crawler.settings.getfloat(
            "VALIDATION_FIELD_FAILURE_RATE_THRESHOLD",
            DEFAULT_VALIDATION_FIELD_FAILURE_RATE_THRESHOLD,
        )
        problem_fields: list[str] = []
        prefix = "validation/failed_field/"
        for stat_name, count in self.stats.items():
            if not stat_name.startswith(prefix):
                continue
            field = stat_name[len(prefix):]
            rate = count / item_count
            if rate > threshold:
                problem_fields.append(f"{field}={count}/{item_count}={rate:.2%}")

        self.assertFalse(
            problem_fields,
            f"fields above {threshold:.2%} failure rate: "
            f"{', '.join(problem_fields)}",
        )


# ---------------------------------------------------------------- 9.2 monitors


class PeriodicRateLimitMonitor(_BrokerScoutMonitor):
    """Circuit breaker: too many 429s mid-run → stop now.

    Decoupled from the close-suite `UnwantedHTTPCodesMonitor` (Phase
    9.3.3) so we can set a tight periodic threshold (50 by default)
    without it affecting post-run analysis. Hitting 50 rate-limited
    responses mid-run is a strong signal we're being throttled — abort
    before we burn through proxies or escalate to an account-level
    block.
    """

    def test_429_below_threshold(self):
        count = self.stats.get("downloader/response_status_count/429", 0)
        threshold = self.crawler.settings.getint(
            "PERIODIC_429_THRESHOLD", DEFAULT_PERIODIC_429_THRESHOLD
        )
        self.assertLessEqual(
            count,
            threshold,
            f"rate-limited: {count} 429 responses (threshold {threshold})",
        )


# ---------------------------------------------------------------- 9.3 monitors


class ZeroItemsMonitor(_BrokerScoutMonitor):
    """Run produced zero items — usually means the spider crashed
    before any DLD broker was processed. Loud-fail so a silent
    wasted-week is impossible."""

    def test_at_least_one_item_scraped(self):
        scraped = self.stats.get("item_scraped_count", 0)
        self.assertGreater(
            scraped,
            0,
            "spider produced zero items — start_requests likely failed "
            "before yielding anything (DLD load? spider class import?)",
        )


class RetryRateMonitor(_BrokerScoutMonitor):
    """Retries should be rare. A high rate (e.g. >15%) signals
    flakey upstream / network issues / bad proxies."""

    def test_retry_rate_below_threshold(self):
        requests = self.stats.get("downloader/request_count", 0)
        retries = self.stats.get("retry/count", 0)
        if requests == 0:
            self.skipTest("no requests issued")
            return
        threshold = self.crawler.settings.getfloat(
            "RETRY_RATE_THRESHOLD", DEFAULT_RETRY_RATE_THRESHOLD
        )
        rate = retries / requests
        self.assertLessEqual(
            rate,
            threshold,
            f"retry rate {rate:.2%} ({retries}/{requests}) exceeds {threshold:.2%}",
        )


class PipelineFailureMonitor(_BrokerScoutMonitor):
    """All four sinks must process every scraped item.

    Runs on `engine_stopped` (not `spider_closed`) — see settings.py
    for the rationale: pipelines' close handlers fire as part of
    spider_closed, so a monitor on the same signal would race them.
    `engine_stopped` fires once every spider_closed handler has
    completed, so all pipeline counters are final.
    """

    def test_postgres_inserted_all_items(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        inserted = self.stats.get("postgres/brokers_inserted", 0)
        self.assertEqual(
            inserted,
            scraped,
            f"postgres/brokers_inserted={inserted} but item_scraped_count={scraped}",
        )

    def test_gsheets_appended_all_items(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        appended = self.stats.get("gsheets/rows_appended", 0)
        self.assertEqual(
            appended,
            scraped,
            f"gsheets/rows_appended={appended} but item_scraped_count={scraped}",
        )

    def test_gsheets_no_flush_failure(self):
        failed = self.stats.get("gsheets/flush_failed", 0)
        self.assertEqual(
            failed,
            0,
            "gsheets pipeline reported a flush failure — Sheets data may be incomplete",
        )

    def test_gdrive_csv_upload_status_ok(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        status = self.stats.get("gdrive_csv/upload_status", "missing")
        # "skipped" is a legitimate outcome (zero rows), but we already
        # short-circuited above. Anything other than "ok" here is bad.
        self.assertEqual(
            status,
            "ok",
            f"gdrive_csv/upload_status={status!r} (expected 'ok')",
        )

    def test_gdrive_csv_rows_uploaded_match(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        uploaded = self.stats.get("gdrive_csv/rows_uploaded", 0)
        self.assertEqual(
            uploaded,
            scraped,
            f"gdrive_csv/rows_uploaded={uploaded} but item_scraped_count={scraped}",
        )


class ExtractionFailureMonitor(_BrokerScoutMonitor):
    """Extraction-health counters from Phase 7 must stay below thresholds.

    A spike in any single counter signals PF schema change or upstream
    breakage. Fixed thresholds defined in `DEFAULT_EXTRACTION_THRESHOLDS`
    (numeric values are rates against `item_scraped_count`; `0` means
    absolute count).
    """

    def test_next_data_missing_rate(self):
        self._assert_rate_threshold("extract/next_data/missing")

    def test_next_data_bad_json_count(self):
        self._assert_absolute_threshold("extract/next_data/bad_json")

    def test_agent_data_missing_rate(self):
        self._assert_rate_threshold("extract/agent_data/missing")

    def test_search_json_fallback_rate(self):
        self._assert_rate_threshold("extract/search_json/fallback_used")

    def test_brn_fallback_rate(self):
        self._assert_rate_threshold("extract/brn/fallback_used")

    def test_listings_api_non_json_rate(self):
        self._assert_rate_threshold("extract/listings_api/non_json")

    def test_listings_api_empty_rate(self):
        self._assert_rate_threshold("extract/listings_api/empty")

    def test_agency_license_missing_rate(self):
        self._assert_rate_threshold("extract/agency_license/missing")

    # ------------------------------------------------------------ helpers

    def _assert_rate_threshold(self, stat_name: str) -> None:
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        threshold = float(DEFAULT_EXTRACTION_THRESHOLDS[stat_name])
        count = self.stats.get(stat_name, 0)
        rate = count / scraped
        self.assertLessEqual(
            rate,
            threshold,
            f"{stat_name}: {count}/{scraped} = {rate:.2%} exceeds {threshold:.2%}",
        )

    def _assert_absolute_threshold(self, stat_name: str) -> None:
        threshold = int(DEFAULT_EXTRACTION_THRESHOLDS[stat_name])
        count = self.stats.get(stat_name, 0)
        self.assertLessEqual(
            count,
            threshold,
            f"{stat_name}: {count} (max allowed {threshold})",
        )


class MatchStatusDistributionMonitor(_BrokerScoutMonitor):
    """High-confidence matches (exact_brn + name_unique) must be
    ≥ HIGH_CONFIDENCE_RATE_THRESHOLD of total items. A drop usually
    means PF stopped exposing BRN in search-page __NEXT_DATA__ and
    we're falling back to fuzzy."""

    def test_high_confidence_match_rate(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        exact = self.stats.get("match/exact_brn", 0)
        unique = self.stats.get("match/name_unique", 0)
        rate = (exact + unique) / scraped
        threshold = self.crawler.settings.getfloat(
            "MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD",
            DEFAULT_MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD,
        )
        self.assertGreaterEqual(
            rate,
            threshold,
            f"high-confidence match rate {rate:.2%} "
            f"(exact_brn={exact} + name_unique={unique} / {scraped}) "
            f"below threshold {threshold:.2%}",
        )


class NotFoundRateMonitor(_BrokerScoutMonitor):
    """`not_found` rate above threshold suggests a broken search step
    (PF rejecting our query format) — even our DLD-driven name search
    shouldn't miss most brokers."""

    def test_not_found_rate_below_threshold(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        not_found = self.stats.get("match/not_found", 0)
        rate = not_found / scraped
        threshold = self.crawler.settings.getfloat(
            "NOT_FOUND_RATE_THRESHOLD", DEFAULT_NOT_FOUND_RATE_THRESHOLD
        )
        self.assertLess(
            rate,
            threshold,
            f"not_found rate {rate:.2%} ({not_found}/{scraped}) "
            f"exceeds threshold {threshold:.2%}",
        )


class AmbiguousRateMonitor(_BrokerScoutMonitor):
    """`ambiguous` rate above threshold suggests PF stopped exposing
    BRN in search-page JSON (BRN-first match in `match_candidates`
    falls through to name fuzzy, which produces ambiguous when ≥2
    candidates exceed the threshold)."""

    def test_ambiguous_rate_below_threshold(self):
        scraped = self.stats.get("item_scraped_count", 0)
        if scraped == 0:
            self.skipTest("no items")
            return
        ambiguous = self.stats.get("match/ambiguous", 0)
        rate = ambiguous / scraped
        threshold = self.crawler.settings.getfloat(
            "AMBIGUOUS_RATE_THRESHOLD", DEFAULT_AMBIGUOUS_RATE_THRESHOLD
        )
        self.assertLess(
            rate,
            threshold,
            f"ambiguous rate {rate:.2%} ({ambiguous}/{scraped}) "
            f"exceeds threshold {threshold:.2%}",
        )


class BRNDriftMonitor(_BrokerScoutMonitor):
    """Counts cases where PF's profile-page BRN disagrees with DLD's BRN.

    Set by `agent_spider.parse_agent` via `match/brn_drift` counter
    when both BRNs are present and unequal. The matching layer
    deliberately doesn't promote disagreeing pairs to `exact_brn` —
    they're real drift signals, not bugs to silence.
    """

    def test_no_brn_drift(self):
        drift = self.stats.get("match/brn_drift", 0)
        threshold = self.crawler.settings.getint(
            "BRN_DRIFT_THRESHOLD", DEFAULT_BRN_DRIFT_THRESHOLD
        )
        self.assertLessEqual(
            drift,
            threshold,
            f"{drift} item(s) had PF BRN ≠ DLD BRN (threshold {threshold}) — "
            f"see brokers table where brn IS NOT NULL "
            f"AND dld_brn IS NOT NULL AND brn != dld_brn",
        )


class MatchedRowFieldCoverageMonitor(_BrokerScoutMonitor):
    """Per-tier field coverage measured ONLY over matched rows.

    The native Spidermon `FieldCoverageMonitor` measures over every
    item, so PF-side fields like `broker_name` get diluted by
    not_found / ambiguous stubs (where they're always NULL). This
    monitor queries Postgres directly at engine_stopped — by then
    every item has been flushed by `PostgresPipeline` — and computes
    coverage over rows where `match_status` ∈ `MATCHED_STATUSES`.

    Three test methods, one per tier (Critical / High / Medium).
    Each gathers the per-field rates and fails if any field falls
    below the tier threshold, listing every offender for clarity.
    Runs are skipped (not failed) when zero matched rows exist —
    `ZeroItemsMonitor` and `NotFoundRateMonitor` cover those signals.
    """

    def test_critical_field_coverage(self):
        self._assert_tier(
            coverage_tiers.PF_CRITICAL_FIELDS,
            "FIELD_COVERAGE_CRITICAL_THRESHOLD",
            DEFAULT_FIELD_COVERAGE_CRITICAL,
        )

    def test_high_field_coverage(self):
        self._assert_tier(
            coverage_tiers.PF_HIGH_FIELDS,
            "FIELD_COVERAGE_HIGH_THRESHOLD",
            DEFAULT_FIELD_COVERAGE_HIGH,
        )

    def test_medium_field_coverage(self):
        self._assert_tier(
            coverage_tiers.PF_MEDIUM_FIELDS,
            "FIELD_COVERAGE_MEDIUM_THRESHOLD",
            DEFAULT_FIELD_COVERAGE_MEDIUM,
        )

    # ------------------------------------------------------------ helper

    def _assert_tier(
        self,
        fields: tuple[str, ...],
        setting_name: str,
        default_threshold: float,
    ) -> None:
        if not fields:
            self.skipTest("empty tier")
            return
        run_id = self.stats.get("run_id")
        if not run_id:
            self.skipTest("no run_id in stats")
            return
        threshold = self.crawler.settings.getfloat(setting_name, default_threshold)
        try:
            coverage = brokers_repo.matched_field_coverage(run_id, fields)
        except Exception as exc:
            # Don't fail the suite on a DB hiccup at engine_stopped —
            # other monitors run on in-memory stats and stay valid.
            self.skipTest(f"coverage query failed: {exc}")
            return
        if not coverage:
            self.skipTest("no matched rows in this run")
            return
        below = [(f, r) for f, r in coverage.items() if r < threshold]
        self.assertFalse(
            below,
            f"matched-row field coverage below {threshold:.0%}: "
            + ", ".join(f"{f}={r:.0%}" for f, r in below),
        )


# ---------------------------------------------------------------- suites


class SpiderCloseMonitorSuite(MonitorSuite):
    """Runs on `engine_stopped` (NOT `spider_closed`) — see
    `SPIDERMON_ENGINE_STOPPED_MONITORS` in [settings.py](../settings.py).

    Pipelines hook `spider_closed` to flush their final batches; if
    monitors ran on `spider_closed` too, they'd race and read
    pre-flush stats. `engine_stopped` fires once every spider_closed
    handler completes, so all `postgres/`, `gsheets/`, `gdrive_csv/`
    counters are final.
    """

    monitors = [
        # Finish reason + validation (Phase 9.0/9.1)
        FinishReasonMonitor,
        ValidationFailureRateMonitor,
        ValidationFailureByFieldMonitor,
        # Phase 9.3.1 volume
        ZeroItemsMonitor,
        # Phase 9.3.3 HTTP / network
        UnwantedHTTPCodesMonitor,
        RetryRateMonitor,
        # Phase 9.3.5 pipeline health
        PipelineFailureMonitor,
        # Phase 9.3.6 extraction health
        ExtractionFailureMonitor,
        # Phase 9.3.7 match coverage + BRN drift
        MatchStatusDistributionMonitor,
        NotFoundRateMonitor,
        AmbiguousRateMonitor,
        BRNDriftMonitor,
        # Phase 9.3.2 field coverage (matched rows only)
        MatchedRowFieldCoverageMonitor,
    ]
    # LogOnlyAction stays — useful when the webhook is misconfigured.
    # SendChatSummaryAction adds the operator-facing alert (Phase 11).
    monitors_finished_actions = [LogOnlyAction, SendChatSummaryAction]


class PeriodicMonitorSuite(MonitorSuite):
    """Circuit-breaker suite, fires every 60s during a run.

    Actions are wired to `monitors_failed_actions` only — we don't
    want a "monitors passed" INFO log every 60 seconds. CloseSpiderAction
    is idempotent (once-only class flag), so a sustained failure across
    multiple ticks doesn't repeatedly try to close the spider.
    """

    monitors = [
        ErrorCountMonitor,
        PeriodicRateLimitMonitor,
    ]
    # SendCriticalChatAlertAction fires the mid-run alert (Phase 11);
    # CloseSpiderAction closes the spider; LogOnlyAction logs the
    # failure regardless of webhook state.
    monitors_failed_actions = [
        LogOnlyAction,
        SendCriticalChatAlertAction,
        CloseSpiderAction,
    ]
