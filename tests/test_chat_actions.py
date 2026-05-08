"""Phase 11 — `SendChatSummaryAction` and `SendCriticalChatAlertAction`.

Drives both actions through their `run_action()` against synthetic
stats + monitor results. Notifier is replaced with a recording stub
so we assert exactly what the operator would see.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.monitors.actions import (
    SendChatSummaryAction,
    SendCriticalChatAlertAction,
)


class RecordingNotifier:
    """Captures every send() call. Returns True by default; tests can
    flip `result_to_return` to simulate webhook failure."""

    def __init__(self):
        self.calls = []
        self.result_to_return = True

    def send(self, level, title, body, run_id):
        self.calls.append(
            {"level": level, "title": title, "body": body, "run_id": run_id}
        )
        return self.result_to_return


def _crawler(stats: dict, settings: dict | None = None, spider_name: str = "agent_spider"):
    crawler = MagicMock()
    crawler.spider.name = spider_name
    crawler.stats.get_stats.return_value = stats
    settings = settings or {}
    crawler.settings.get.side_effect = lambda key, default=None: settings.get(key, default)
    return crawler


def _failure(monitor_name: str, severity: str, error_message: str = "boom"):
    monitor = MagicMock()
    monitor.name = monitor_name
    monitor.severity = severity
    return SimpleNamespace(monitor=monitor, error_message=error_message)


# ============================================================ SendChatSummaryAction


@pytest.fixture
def patch_log_alert(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(
        "broker_scout.monitors.actions.brokers_repo.log_alert", fake
    )
    return fake


@pytest.fixture
def patch_dedupe(monkeypatch):
    fake = MagicMock(return_value=False)
    monkeypatch.setattr(
        "broker_scout.monitors.actions.brokers_repo.recent_alert_exists",
        fake,
    )
    return fake


def test_summary_clean_run_sends_ok(patch_log_alert):
    notifier = RecordingNotifier()
    crawler = _crawler(
        {
            "run_id": "abc-123",
            "item_scraped_count": 1,
            "validation/passed_total": 1,
            "validation/failed_total": 0,
            "match/exact_brn": 1,
            "postgres/brokers_inserted": 1,
            "gsheets/rows_appended": 1,
            "gsheets/sheet_id": "SHEET-1",
            "gdrive_csv/upload_status": "ok",
            "gdrive_csv/file_id": "DRIVE-1",
            "elapsed_time_seconds": 9.1,
            "finish_reason": "finished",
        }
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[1, 2, 3],
        monitors_failed_results=[],
    )
    action.run_action()

    assert len(notifier.calls) == 1
    call = notifier.calls[0]
    assert call["level"] == "ok"
    assert "agent_spider — OK" in call["title"]
    assert "Items: 1 scraped" in call["body"]
    assert "validation 100% pass" in call["body"]
    assert "Match: 1 exact_brn" in call["body"]
    assert "postgres ✓" in call["body"]
    assert "sheets ✓" in call["body"]
    assert "drive ✓" in call["body"]
    assert "SHEET-1" in call["body"]
    assert "DRIVE-1" in call["body"]
    assert "Runtime: 9.1s" in call["body"]
    patch_log_alert.assert_called_once()


def test_summary_critical_failure_sends_red(patch_log_alert):
    notifier = RecordingNotifier()
    crawler = _crawler(
        {
            "run_id": "abc",
            "item_scraped_count": 100,
            "validation/passed_total": 100,
            "validation/failed_total": 0,
        }
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=[
            _failure("PipelineFailureMonitor", "critical", "postgres lag")
        ],
    )
    action.run_action()
    call = notifier.calls[0]
    assert call["level"] == "critical"
    assert "agent_spider — CRITICAL" in call["title"]
    assert "PipelineFailureMonitor" in call["body"]
    assert "[critical]" in call["body"]


def test_summary_works_with_real_spidermon_failure_shape(patch_log_alert):
    """Regression: Spidermon failures expose `reason`/`error`, NOT
    `error_message` (which only existed in our test mocks). A
    previous version called `'.splitlines()[0]'` on the missing
    `error_message` attribute and crashed with IndexError, so the
    Discord card never sent during failed runs."""
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 17,
         "validation/passed_total": 17, "validation/failed_total": 0,
         "match/not_found": 17}
    )
    # Real Spidermon failure shape: reason + error, NO error_message.
    real_failure = SimpleNamespace(
        monitor=SimpleNamespace(
            name="MatchStatusDistributionMonitor/test_high_confidence_match_rate",
            severity="critical",
        ),
        reason="high-confidence match rate 0.00% (exact_brn=0 + name_unique=0 / 17) below threshold 60.00%",
        error="Traceback ...\nAssertionError: high-confidence match rate 0.00% ...",
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=[real_failure],
    )
    action.run_action()
    assert len(notifier.calls) == 1
    call = notifier.calls[0]
    assert call["level"] == "critical"
    assert "MatchStatusDistributionMonitor" in call["body"]
    assert "high-confidence match rate 0.00%" in call["body"]


def test_summary_handles_empty_reason_gracefully(patch_log_alert):
    """Regression: an empty error_message used to crash with
    IndexError on `''.splitlines()[0]`."""
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 1,
         "validation/passed_total": 1, "validation/failed_total": 0}
    )
    empty_failure = SimpleNamespace(
        monitor=SimpleNamespace(name="X", severity="critical"),
        reason="",  # falsy
        error="",
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=[empty_failure],
    )
    action.run_action()  # must NOT raise
    assert "(no detail)" in notifier.calls[0]["body"]


def test_summary_warning_only_sends_yellow(patch_log_alert):
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 100,
         "validation/passed_total": 100, "validation/failed_total": 0}
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=[
            _failure("NotFoundRateMonitor", "warning", "60% not_found")
        ],
    )
    action.run_action()
    assert notifier.calls[0]["level"] == "warning"


def test_summary_respects_alert_min_level(patch_log_alert):
    """ALERT_MIN_LEVEL=critical filters out warning failures from the
    body (and from the level computation)."""
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 1,
         "validation/passed_total": 1, "validation/failed_total": 0},
        settings={"ALERT_MIN_LEVEL": "critical"},
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[],
        monitors_failed_results=[
            _failure("NotFoundRateMonitor", "warning", "55%"),
        ],
    )
    action.run_action()
    call = notifier.calls[0]
    # Filtered → level reverts to ok, body has no failure detail.
    assert call["level"] == "ok"
    assert "NotFoundRateMonitor" not in call["body"]


def test_summary_no_links_when_stats_missing(patch_log_alert):
    """Dev runs without configured Sheets/Drive don't crash."""
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 1,
         "validation/passed_total": 1, "validation/failed_total": 0}
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[1], monitors_failed_results=[]
    )
    action.run_action()
    body = notifier.calls[0]["body"]
    assert "Sheet:" not in body
    assert "Drive CSV:" not in body


def test_summary_skips_log_alert_when_send_failed(patch_log_alert):
    notifier = RecordingNotifier()
    notifier.result_to_return = False
    crawler = _crawler(
        {"run_id": "abc", "item_scraped_count": 1,
         "validation/passed_total": 1, "validation/failed_total": 0}
    )
    action = SendChatSummaryAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[1], monitors_failed_results=[]
    )
    action.run_action()
    patch_log_alert.assert_not_called()


# ============================================================ SendCriticalChatAlertAction


@pytest.fixture
def reset_critical_action():
    SendCriticalChatAlertAction.reset()
    yield
    SendCriticalChatAlertAction.reset()


def test_critical_action_sends_first_call(
    reset_critical_action, patch_log_alert, patch_dedupe
):
    notifier = RecordingNotifier()
    crawler = _crawler(
        {"run_id": "abc", "downloader/response_status_count/429": 75,
         "log_count/ERROR": 12, "item_scraped_count": 200}
    )
    action = SendCriticalChatAlertAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    action.run_action()

    assert len(notifier.calls) == 1
    call = notifier.calls[0]
    assert call["level"] == "critical"
    assert call["title"] == "Circuit breaker tripped"
    assert "429 count: 75" in call["body"]
    assert "Errors:    12" in call["body"]
    patch_log_alert.assert_called_once()


def test_critical_action_idempotent_across_ticks(
    reset_critical_action, patch_log_alert, patch_dedupe
):
    notifier = RecordingNotifier()
    crawler = _crawler({"run_id": "abc"})
    a1 = SendCriticalChatAlertAction(crawler=crawler, notifier=notifier)
    a1.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    a2 = SendCriticalChatAlertAction(crawler=crawler, notifier=notifier)
    a2.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    a1.run_action()
    a2.run_action()
    assert len(notifier.calls) == 1


def test_critical_action_dedupes_via_alert_log(
    reset_critical_action, patch_log_alert, patch_dedupe
):
    """A recent identical alert means we skip the send (and don't
    insert a new row in alert_log)."""
    patch_dedupe.return_value = True
    notifier = RecordingNotifier()
    crawler = _crawler({"run_id": "abc"})
    action = SendCriticalChatAlertAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    action.run_action()
    assert notifier.calls == []
    patch_log_alert.assert_not_called()


def test_critical_action_swallows_dedupe_db_errors(
    reset_critical_action, patch_log_alert, patch_dedupe
):
    """A flaky dedupe query shouldn't drop an alert — better to
    over-send than miss a circuit-breaker."""
    patch_dedupe.side_effect = RuntimeError("connection refused")
    notifier = RecordingNotifier()
    crawler = _crawler({"run_id": "abc"})
    action = SendCriticalChatAlertAction(crawler=crawler, notifier=notifier)
    action.result = SimpleNamespace(
        monitors_passed_results=[], monitors_failed_results=[]
    )
    action.run_action()
    assert len(notifier.calls) == 1


def test_critical_action_from_crawler_constructs(reset_critical_action):
    """Spidermon constructs actions via from_crawler — our DI must
    work without arguments."""
    crawler = MagicMock()
    with patch(
        "broker_scout.monitors.actions.get_notifier",
        return_value=RecordingNotifier(),
    ):
        action = SendCriticalChatAlertAction.from_crawler(crawler=crawler)
    assert action._crawler is crawler
