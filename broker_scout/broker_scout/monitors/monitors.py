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
)
from spidermon.contrib.scrapy.monitors.base import BaseScrapyMonitor

from broker_scout.monitors.actions import LogOnlyAction

# ---------------------------------------------------------------- defaults

DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD = 0.05
DEFAULT_VALIDATION_FIELD_FAILURE_RATE_THRESHOLD = 0.10


# ---------------------------------------------------------------- 9.1 monitors


class _BrokerScoutMonitor(BaseScrapyMonitor):
    """Common base for our custom monitors.

    `__test__ = False` opts these classes out of pytest's auto-
    discovery of `unittest.TestCase` subclasses. Spidermon uses the
    stdlib `TestLoader` which ignores the attribute, so suite
    execution at runtime is unaffected — only test collection in our
    own pytest runs is.
    """

    __test__ = False


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


# ---------------------------------------------------------------- suites


class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [
        FinishReasonMonitor,
        ValidationFailureRateMonitor,
        ValidationFailureByFieldMonitor,
    ]
    monitors_finished_actions = [LogOnlyAction]


class PeriodicMonitorSuite(MonitorSuite):
    monitors = [ErrorCountMonitor]
    monitors_finished_actions = [LogOnlyAction]
