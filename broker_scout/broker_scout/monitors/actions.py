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
        # No DI needed in 9.0; suite + monitor state is in `self.result`
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
