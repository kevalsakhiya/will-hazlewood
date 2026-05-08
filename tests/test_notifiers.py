"""Phase 11 — notifier layer (GoogleChatNotifier + LogOnlyNotifier).

Tests cover payload shape, retry policy on transient errors, fail-
fast on 4xx, and the empty-webhook-URL graceful path.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import httpx
import pytest

from broker_scout.monitors import notifiers


@pytest.fixture(autouse=True)
def _no_dotenv(monkeypatch):
    """Don't let .env leak into tests."""
    monkeypatch.setattr(notifiers, "load_dotenv", lambda: None)


# ============================================================ _build_card


def test_card_has_severity_colour():
    payload = notifiers._build_card("critical", "Boom", "details", "run-1")
    section_header = payload["cardsV2"][0]["card"]["sections"][0]["header"]
    assert "#D93025" in section_header  # red
    assert "CRITICAL" in section_header


def test_card_uses_warning_colour_for_warning_level():
    payload = notifiers._build_card("warning", "warn", "body", "run-1")
    assert "#F9AB00" in payload["cardsV2"][0]["card"]["sections"][0]["header"]


def test_card_includes_run_id_in_subtitle():
    payload = notifiers._build_card("ok", "title", "body", "abc-123")
    assert payload["cardsV2"][0]["card"]["header"]["subtitle"] == "run_id: abc-123"


def test_card_truncates_oversized_body():
    huge = "x" * (notifiers.MAX_BODY_CHARS + 1000)
    payload = notifiers._build_card("ok", "t", huge, None)
    text = payload["cardsV2"][0]["card"]["sections"][0]["widgets"][0][
        "textParagraph"
    ]["text"]
    assert len(text) <= notifiers.MAX_BODY_CHARS
    assert "(truncated)" in text


def test_card_converts_newlines_to_br():
    payload = notifiers._build_card("ok", "t", "line1\nline2", None)
    text = payload["cardsV2"][0]["card"]["sections"][0]["widgets"][0][
        "textParagraph"
    ]["text"]
    assert "<br>" in text
    assert "\n" not in text


# ============================================================ LogOnlyNotifier


def test_log_only_notifier_emits_log(caplog):
    n = notifiers.LogOnlyNotifier()
    with caplog.at_level(logging.WARNING, logger="broker_scout.monitors.notifiers"):
        result = n.send("critical", "Boom", "body", "run-1")
    assert result is True
    assert any("alert (log-only)" in r.message for r in caplog.records)


# ============================================================ GoogleChatNotifier


def test_chat_notifier_no_url_returns_false(caplog):
    n = notifiers.GoogleChatNotifier(webhook_url="")
    with caplog.at_level(logging.WARNING, logger="broker_scout.monitors.notifiers"):
        ok = n.send("critical", "t", "b", "r")
    assert ok is False
    assert any("WEBHOOK_URL unset" in r.message for r in caplog.records)


def test_chat_notifier_happy_path_returns_true():
    """A successful POST → True. Patch _post_chat_card to no-op."""
    n = notifiers.GoogleChatNotifier(webhook_url="https://chat.example/webhook")
    with patch.object(notifiers, "_post_chat_card", return_value=None):
        assert n.send("ok", "title", "body", "r1") is True


def test_chat_notifier_4xx_no_retry():
    """4xx is a config bug; don't retry, return False."""
    response = httpx.Response(400, request=httpx.Request("POST", "https://x"))
    err = httpx.HTTPStatusError("bad", request=response.request, response=response)
    n = notifiers.GoogleChatNotifier(webhook_url="https://chat.example/webhook")
    with patch.object(notifiers, "_post_chat_card", side_effect=err):
        result = n.send("critical", "t", "b", "r")
    assert result is False


def test_chat_notifier_transient_error_returns_false():
    n = notifiers.GoogleChatNotifier(webhook_url="https://chat.example/webhook")
    with patch.object(
        notifiers, "_post_chat_card", side_effect=httpx.ConnectError("boom")
    ):
        result = n.send("critical", "t", "b", "r")
    assert result is False


def test_post_chat_card_retries_5xx():
    """Tenacity retries on 5xx (real check via the wrapped function)."""
    response_500 = httpx.Response(503, request=httpx.Request("POST", "https://x"))
    err = httpx.HTTPStatusError(
        "transient", request=response_500.request, response=response_500
    )
    call_count = {"n": 0}

    def fake_post(url, json):
        call_count["n"] += 1
        raise err

    with patch.object(httpx, "Client") as mock_client_cls:
        client = mock_client_cls.return_value.__enter__.return_value
        client.post.side_effect = fake_post
        with patch("tenacity.nap.time.sleep", return_value=None), pytest.raises(
            httpx.HTTPStatusError
        ):
            notifiers._post_chat_card("https://x", {"foo": "bar"})

    # 5 attempts before tenacity gives up.
    assert call_count["n"] == 5


def test_post_chat_card_no_retry_on_4xx():
    response_400 = httpx.Response(401, request=httpx.Request("POST", "https://x"))
    err = httpx.HTTPStatusError("bad", request=response_400.request, response=response_400)
    call_count = {"n": 0}

    def fake_post(url, json):
        call_count["n"] += 1
        raise err

    with patch.object(httpx, "Client") as mock_client_cls:
        client = mock_client_cls.return_value.__enter__.return_value
        client.post.side_effect = fake_post
        with patch("tenacity.nap.time.sleep", return_value=None), pytest.raises(
            httpx.HTTPStatusError
        ):
            notifiers._post_chat_card("https://x", {"foo": "bar"})

    assert call_count["n"] == 1  # No retry


# ============================================================ get_notifier factory


def test_get_notifier_returns_chat_when_url_set(monkeypatch):
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://chat.example/webhook")
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.GoogleChatNotifier)


def test_get_notifier_falls_back_to_log_only_when_no_url(monkeypatch):
    monkeypatch.delenv("GOOGLE_CHAT_WEBHOOK_URL", raising=False)
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.LogOnlyNotifier)
