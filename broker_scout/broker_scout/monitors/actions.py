"""Spidermon Action implementations.

Phase 9.0 ships only `LogOnlyAction` — surfaces monitor outcomes to
the JSON log so operators can see what passed / failed during a run.
Phase 11 adds `GoogleChatNotifier` alongside this; both are wired
to suites' `monitors_finished_actions`.

We extend `spidermon.core.actions.Action` directly rather than
`spidermon.contrib.actions.*` because the contrib subtree imports
`jinja2` (used by Spidermon's notification templates) which we don't
need and haven't installed.
"""

from __future__ import annotations

import logging

from spidermon.core.actions import Action

logger = logging.getLogger(__name__)

_REASON_TRUNCATE_CHARS = 500


class LogOnlyAction(Action):
    """Emits one INFO line on suite success, one ERROR line per failed
    monitor. Phase 11 swaps this for the Google Chat notifier without
    changing suite wiring — both are just `Action` implementations."""

    @classmethod
    def from_crawler_kwargs(cls, crawler):
        # No DI needed; suite + monitor state is in `self.result`
        # and `self.data` provided by Spidermon at run() time.
        return {}

    def run_action(self) -> None:
        passed = len(self.result.monitors_passed_results)
        failed = self.result.monitors_failed_results

        if not failed:
            logger.info(
                "monitors passed",
                extra={"passed_count": passed},
            )
            return

        for failure in failed:
            monitor_name = getattr(failure.monitor, "name", repr(failure.monitor))
            reason = str(getattr(failure, "error_message", "") or "")
            logger.error(
                "monitor failed",
                extra={
                    "monitor": monitor_name,
                    "reason": reason[:_REASON_TRUNCATE_CHARS],
                    "passed_count": passed,
                    "failed_count": len(failed),
                },
            )


class CloseSpiderAction(Action):
    """Circuit-breaker action — closes the spider when a periodic
    monitor fires.

    Once-only: subsequent ticks see ``_fired=True`` and short-circuit,
    so we don't spam logs or repeatedly call ``engine.close_spider``
    while the deferred shutdown completes. Class-level flag is fine
    because one spider runs per Python process in our deployment;
    a fresh process for the next run resets the flag naturally.

    Wired into the periodic suite's ``monitors_failed_actions`` only —
    we do NOT want to close the spider on a passing tick.
    """

    _fired: bool = False

    @classmethod
    def from_crawler_kwargs(cls, crawler):
        return {"crawler": crawler}

    def __init__(self, crawler):
        super().__init__()
        self._crawler = crawler

    def run_action(self) -> None:
        if CloseSpiderAction._fired:
            return
        CloseSpiderAction._fired = True
        logger.error(
            "circuit breaker tripped — closing spider",
            extra={"reason": "circuit_breaker"},
        )
        engine = getattr(self._crawler, "engine", None)
        spider = getattr(self._crawler, "spider", None)
        if engine is None or spider is None:
            logger.warning(
                "circuit breaker fired but engine/spider unavailable",
                extra={"engine": engine, "spider": spider},
            )
            return
        engine.close_spider(spider, "circuit_breaker")

    @classmethod
    def reset(cls) -> None:
        """Test-only: reset the once-fired flag between unit tests."""
        cls._fired = False
