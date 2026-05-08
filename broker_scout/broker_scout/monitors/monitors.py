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

from broker_scout.monitors.actions import LogOnlyAction


class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [FinishReasonMonitor]
    monitors_finished_actions = [LogOnlyAction]


class PeriodicMonitorSuite(MonitorSuite):
    monitors = [ErrorCountMonitor]
    monitors_finished_actions = [LogOnlyAction]
