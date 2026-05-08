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
from datetime import datetime, timezone

from spidermon.core.actions import Action

from broker_scout.common import brokers_repo
from broker_scout.monitors.notifiers import Notifier, get_notifier

logger = logging.getLogger(__name__)

_REASON_TRUNCATE_CHARS = 500
_FAILURE_LIST_LIMIT = 10
_CIRCUIT_BREAKER_TITLE = "Circuit breaker tripped"
_DEDUPE_WINDOW_MINUTES = 30
_LEVEL_RANK = {"warning": 1, "critical": 2}


def _is_level_at_or_above(level: str, floor: str) -> bool:
    """`floor='warning'` accepts warning + critical; `floor='critical'`
    accepts only critical."""
    return _LEVEL_RANK.get(level, 0) >= _LEVEL_RANK.get(floor, 0)


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


# ============================================================ Phase 11 actions


def _format_runtime(stats: dict) -> str:
    elapsed = stats.get("elapsed_time_seconds")
    if elapsed is None:
        return "—"
    if elapsed < 60:
        return f"{elapsed:.1f}s"
    minutes, seconds = divmod(int(elapsed), 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _format_match_breakdown(stats: dict, scraped: int) -> str:
    parts: list[str] = []
    for status in ("exact_brn", "name_unique", "name_fuzzy", "ambiguous", "not_found"):
        count = stats.get(f"match/{status}", 0)
        if count:
            parts.append(f"{count} {status}")
    if not parts:
        return "—"
    return " / ".join(parts)


def _format_pipeline_status(stats: dict, scraped: int) -> str:
    if scraped == 0:
        return "—"
    pg_ok = stats.get("postgres/brokers_inserted", 0) == scraped
    sheets_ok = (
        stats.get("gsheets/rows_appended", 0) == scraped
        and stats.get("gsheets/flush_failed", 0) == 0
    )
    drive_ok = stats.get("gdrive_csv/upload_status") == "ok"
    return (
        f"postgres {'✓' if pg_ok else '✗'}  "
        f"sheets {'✓' if sheets_ok else '✗'}  "
        f"drive {'✓' if drive_ok else '✗'}"
    )


def _sheet_link(sheet_id: str | None) -> str:
    if not sheet_id:
        return ""
    return f"Sheet: https://docs.google.com/spreadsheets/d/{sheet_id}/edit"


def _drive_link(file_id: str | None) -> str:
    if not file_id:
        return ""
    return f"Drive CSV: https://drive.google.com/file/d/{file_id}/view"


def _summarize_failures(failed_results, alert_min_level: str) -> tuple[str, list[str]]:
    """Return (highest severity present, list of formatted failure lines).

    Filters failures whose monitor severity is below the floor.

    Spidermon's MonitorResult exposes:
      * `failure.monitor` — the Monitor instance (has `.name`,
        `.severity` if it's one of ours).
      * `failure.reason` — the assertion message (e.g. '1 != 2').
      * `failure.error` — full traceback string (fallback when reason
        is empty, e.g. for ERROR-status uncaught exceptions).
      * `error_message` — legacy / our unit-test mock attribute name;
        kept in the chain for backwards compat with the test fixtures.
    """
    severities: list[str] = []
    lines: list[str] = []
    for failure in failed_results:
        monitor = getattr(failure, "monitor", None)
        sev = getattr(monitor, "severity", "critical")
        if not _is_level_at_or_above(sev, alert_min_level):
            continue
        severities.append(sev)
        name = getattr(monitor, "name", None) or repr(monitor)
        # Prefer `reason` (clean message), fall back to `error`
        # (full traceback), then `error_message` (test mock shape).
        raw = (
            getattr(failure, "reason", None)
            or getattr(failure, "error", None)
            or getattr(failure, "error_message", None)
            or ""
        )
        first_line = next(iter(str(raw).splitlines()), "") or "(no detail)"
        lines.append(f"  • [{sev}] {name}: {first_line[:160]}")
    highest = "critical" if "critical" in severities else (
        "warning" if "warning" in severities else "ok"
    )
    return highest, lines[:_FAILURE_LIST_LIMIT]


class SendChatSummaryAction(Action):
    """End-of-run summary card. Fires on `monitors_finished_actions`
    of the close suite — once per spider run, regardless of pass/fail.

    Card colour reflects the worst monitor outcome at or above
    `ALERT_MIN_LEVEL` (default `warning`). Body packs the most
    actionable info: item count, validation rate, match breakdown,
    pipeline sink status, runtime, and the top failures (with
    monitor name + first line of the error).
    """

    @classmethod
    def from_crawler_kwargs(cls, crawler):
        return {"crawler": crawler, "notifier": get_notifier()}

    def __init__(self, crawler, notifier: Notifier):
        super().__init__()
        self._crawler = crawler
        self._notifier = notifier

    def run_action(self) -> None:
        stats = dict(self._crawler.stats.get_stats())
        run_id = stats.get("run_id")
        spider_name = getattr(self._crawler.spider, "name", "spider")

        scraped = int(stats.get("item_scraped_count", 0))
        passed = int(stats.get("validation/passed_total", 0))
        failed = int(stats.get("validation/failed_total", 0))
        validation_rate = (
            f"{passed / (passed + failed):.0%}" if (passed + failed) else "—"
        )

        alert_min_level = self._crawler.settings.get(
            "ALERT_MIN_LEVEL", "warning"
        ) or "warning"
        level, failure_lines = _summarize_failures(
            self.result.monitors_failed_results, alert_min_level
        )

        finish_reason = stats.get("finish_reason", "—")
        title_status = "OK" if level == "ok" else level.upper()
        title = f"{spider_name} — {title_status}"
        body_lines = [
            f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            f"Items: {scraped} scraped (validation {validation_rate} pass)",
            f"Match: {_format_match_breakdown(stats, scraped)}",
            f"Pipelines: {_format_pipeline_status(stats, scraped)}",
            f"Finish: {finish_reason}",
            f"Runtime: {_format_runtime(stats)}",
        ]
        sheet = _sheet_link(stats.get("gsheets/sheet_id"))
        drive = _drive_link(stats.get("gdrive_csv/file_id"))
        if sheet:
            body_lines.append(sheet)
        if drive:
            body_lines.append(drive)
        if failure_lines:
            body_lines.append("Failures:")
            body_lines.extend(failure_lines)

        body = "\n".join(body_lines)

        sent = self._notifier.send(level, title, body, run_id)
        if sent:
            try:
                brokers_repo.log_alert(run_id, level, title, body)
            except Exception:
                logger.exception(
                    "alert_log insert failed (chat send already succeeded)",
                    extra={"run_id": run_id, "title": title},
                )


class SendCriticalChatAlertAction(Action):
    """Mid-run critical card for circuit-breaker trips. Fires on
    `monitors_failed_actions` of the periodic suite alongside
    `CloseSpiderAction`. Idempotent (class-level `_fired`) and
    deduped against `alert_log` within `_DEDUPE_WINDOW_MINUTES`."""

    _fired: bool = False

    @classmethod
    def from_crawler_kwargs(cls, crawler):
        return {"crawler": crawler, "notifier": get_notifier()}

    def __init__(self, crawler, notifier: Notifier):
        super().__init__()
        self._crawler = crawler
        self._notifier = notifier

    def run_action(self) -> None:
        if SendCriticalChatAlertAction._fired:
            return
        try:
            already = brokers_repo.recent_alert_exists(
                "critical", _CIRCUIT_BREAKER_TITLE, _DEDUPE_WINDOW_MINUTES
            )
        except Exception:
            logger.exception(
                "alert dedupe check failed; sending anyway",
            )
            already = False
        if already:
            logger.info(
                "circuit breaker alert deduped — recent identical alert exists",
            )
            SendCriticalChatAlertAction._fired = True
            return

        stats = dict(self._crawler.stats.get_stats())
        run_id = stats.get("run_id")
        spider_name = getattr(self._crawler.spider, "name", "spider")
        body_lines = [
            f"Spider: {spider_name}",
            f"429 count: {stats.get('downloader/response_status_count/429', 0)}",
            f"Errors:    {stats.get('log_count/ERROR', 0)}",
            f"Items so far: {stats.get('item_scraped_count', 0)}",
            "Spider is closing.",
        ]
        body = "\n".join(body_lines)

        sent = self._notifier.send(
            "critical", _CIRCUIT_BREAKER_TITLE, body, run_id
        )
        if sent:
            try:
                brokers_repo.log_alert(
                    run_id, "critical", _CIRCUIT_BREAKER_TITLE, body
                )
            except Exception:
                logger.exception("alert_log insert failed")
        SendCriticalChatAlertAction._fired = True

    @classmethod
    def reset(cls) -> None:
        cls._fired = False
