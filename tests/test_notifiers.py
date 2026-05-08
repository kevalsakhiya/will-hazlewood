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
    with patch.object(notifiers, "_post_webhook_json", return_value=None):
        assert n.send("ok", "title", "body", "r1") is True


def test_chat_notifier_4xx_no_retry():
    """4xx is a config bug; don't retry, return False."""
    response = httpx.Response(400, request=httpx.Request("POST", "https://x"))
    err = httpx.HTTPStatusError("bad", request=response.request, response=response)
    n = notifiers.GoogleChatNotifier(webhook_url="https://chat.example/webhook")
    with patch.object(notifiers, "_post_webhook_json", side_effect=err):
        result = n.send("critical", "t", "b", "r")
    assert result is False


def test_chat_notifier_transient_error_returns_false():
    n = notifiers.GoogleChatNotifier(webhook_url="https://chat.example/webhook")
    with patch.object(
        notifiers, "_post_webhook_json", side_effect=httpx.ConnectError("boom")
    ):
        result = n.send("critical", "t", "b", "r")
    assert result is False


def test_post_webhook_json_retries_5xx():
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
            notifiers._post_webhook_json("https://x", {"foo": "bar"})

    # 5 attempts before tenacity gives up.
    assert call_count["n"] == 5


def test_post_webhook_json_no_retry_on_4xx():
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
            notifiers._post_webhook_json("https://x", {"foo": "bar"})

    assert call_count["n"] == 1  # No retry


# ============================================================ get_notifier factory


def test_get_notifier_returns_chat_when_url_set(monkeypatch):
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://chat.example/webhook")
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("ALERT_BACKEND", raising=False)
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.GoogleChatNotifier)


def test_get_notifier_falls_back_to_log_only_when_no_url(monkeypatch):
    monkeypatch.delenv("GOOGLE_CHAT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("ALERT_BACKEND", raising=False)
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.LogOnlyNotifier)


# ============================================================ Discord


def test_discord_embed_has_correct_colour():
    payload = notifiers._build_discord_embed("critical", "Boom", "details", "run-1")
    assert payload["embeds"][0]["color"] == notifiers.DISCORD_COLOURS["critical"]


def test_discord_embed_includes_run_id_in_footer():
    payload = notifiers._build_discord_embed("ok", "title", "body", "abc-123")
    assert payload["embeds"][0]["footer"]["text"] == "run_id: abc-123"


def test_discord_embed_skips_footer_when_no_run_id():
    payload = notifiers._build_discord_embed("ok", "t", "b", None)
    assert "footer" not in payload["embeds"][0]


def test_discord_embed_preserves_newlines():
    """Discord renders \\n natively as line breaks — no <br> substitution."""
    payload = notifiers._build_discord_embed("ok", "t", "line1\nline2", None)
    assert "\n" in payload["embeds"][0]["description"]
    assert "<br>" not in payload["embeds"][0]["description"]


def test_discord_embed_truncates_oversized_body():
    huge = "x" * 5000  # over Discord's 4096 description cap
    payload = notifiers._build_discord_embed("ok", "t", huge, None)
    desc = payload["embeds"][0]["description"]
    assert len(desc) <= 3900
    assert "(truncated)" in desc


def test_discord_embed_truncates_oversized_title():
    payload = notifiers._build_discord_embed("ok", "x" * 500, "body", None)
    # Discord's title cap is 256
    assert len(payload["embeds"][0]["title"]) <= 256


def test_discord_notifier_no_url_returns_false(caplog):
    n = notifiers.DiscordNotifier(webhook_url="")
    with caplog.at_level(logging.WARNING, logger="broker_scout.monitors.notifiers"):
        ok = n.send("critical", "t", "b", "r")
    assert ok is False
    assert any("DISCORD_WEBHOOK_URL unset" in r.message for r in caplog.records)


def test_discord_notifier_happy_path_returns_true():
    n = notifiers.DiscordNotifier(webhook_url="https://discord.example/webhook")
    with patch.object(notifiers, "_post_webhook_json", return_value=None):
        assert n.send("ok", "title", "body", "r1") is True


# ============================================================ factory selection


def test_get_notifier_prefers_discord_when_both_urls_set(monkeypatch):
    """Auto-detect: Discord wins because it works on personal accounts."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://chat.example/webhook")
    monkeypatch.delenv("ALERT_BACKEND", raising=False)
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.DiscordNotifier)


def test_get_notifier_explicit_alert_backend_wins(monkeypatch):
    """ALERT_BACKEND=google_chat picks Chat even if Discord URL also set."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://chat.example/webhook")
    monkeypatch.setenv("ALERT_BACKEND", "google_chat")
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.GoogleChatNotifier)


def test_get_notifier_explicit_backend_without_url_falls_back(monkeypatch):
    """ALERT_BACKEND=discord but no DISCORD_WEBHOOK_URL → LogOnly,
    not silent failure with a wrong-URL post."""
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("GOOGLE_CHAT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ALERT_BACKEND", "discord")
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.LogOnlyNotifier)


def test_get_notifier_alert_backend_typo_falls_through_to_autodetect(monkeypatch):
    """ALERT_BACKEND=banana (typo) → ignored, auto-detect by URL presence."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    monkeypatch.setenv("ALERT_BACKEND", "banana")
    n = notifiers.get_notifier()
    assert isinstance(n, notifiers.DiscordNotifier)
