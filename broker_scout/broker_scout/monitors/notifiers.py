"""Low-level notifier abstractions for Phase 11 alerting.

Two implementations:

  * `GoogleChatNotifier` — POSTs a Google Chat cardV2 payload to a
    webhook URL. Wrapped in `tenacity` retry for transient (5xx /
    network) failures; refuses to retry 4xx since those are config
    bugs that won't self-heal.
  * `LogOnlyNotifier` — emits a structured INFO log instead of
    sending. For unit tests and dev runs without a webhook URL.

Both expose the same `.send(level, title, body, run_id)` shape so
actions can swap them without changing call sites. The factory
function `get_notifier()` picks one based on env config.
"""

from __future__ import annotations

import logging
import os
from typing import Protocol

import httpx
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Google Chat caps a card text section at 4096 chars.
MAX_BODY_CHARS = 3500

CARD_COLOURS = {
    "critical": "#D93025",  # red
    "warning":  "#F9AB00",  # amber
    "ok":       "#1E8E3E",  # green
}

# Discord wants colours as decimal RGB ints, not hex strings.
DISCORD_COLOURS = {
    "critical": 0xD93025,
    "warning":  0xF9AB00,
    "ok":       0x1E8E3E,
}


class Notifier(Protocol):
    """Phase 11 alert delivery interface — implementers POST to a real
    channel (Chat, Slack later) or emit a log line."""

    def send(self, level: str, title: str, body: str, run_id: str | None) -> bool:
        ...


# ---------------------------------------------------------------- helpers


def _is_transient_http_error(exc: BaseException) -> bool:
    """Retry on 5xx / network errors; bail on 4xx (config bugs)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError)


_TRUNCATION_MARKER = "\n…(truncated)"


def _truncate(text: str, limit: int = MAX_BODY_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER


def _build_card(level: str, title: str, body: str, run_id: str | None) -> dict:
    """Build a Google Chat cardV2 payload.

    Spec: https://developers.google.com/chat/api/reference/rest/v1/cards
    """
    colour = CARD_COLOURS.get(level, CARD_COLOURS["critical"])
    # Substitute newlines BEFORE truncating so the cap reflects the
    # final character count Chat's 4096-char limit cares about.
    rendered = body.replace("\n", "<br>")
    safe_body = _truncate(rendered)
    subtitle = f"run_id: {run_id}" if run_id else ""
    return {
        "cardsV2": [
            {
                "cardId": f"broker-scout-{run_id or 'unknown'}",
                "card": {
                    "header": {
                        "title": title,
                        "subtitle": subtitle,
                        "imageUrl": "",
                        "imageType": "CIRCLE",
                    },
                    "sections": [
                        {
                            "header": f"<font color=\"{colour}\">●</font> {level.upper()}",
                            "widgets": [
                                {"textParagraph": {"text": safe_body}}
                            ],
                        }
                    ],
                },
            }
        ]
    }


# ---------------------------------------------------------------- LogOnly


_LEVEL_TO_LOG_LEVEL = {
    "critical": logging.ERROR,
    "warning":  logging.WARNING,
    "ok":       logging.INFO,
}


class LogOnlyNotifier:
    """Test- and dev-friendly notifier that just emits a JSON log line.

    Maps alert severity to Python log level: critical → ERROR,
    warning → WARNING, ok → INFO. So a healthy run's summary doesn't
    flood logs with WARNINGs.
    """

    def send(self, level: str, title: str, body: str, run_id: str | None) -> bool:
        logger.log(
            _LEVEL_TO_LOG_LEVEL.get(level, logging.WARNING),
            "alert (log-only)",
            extra={
                "alert_level": level,
                "alert_title": title,
                "alert_body": body[:500],
                "run_id": run_id,
            },
        )
        return True


# ---------------------------------------------------------------- GoogleChat


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, max=30),
    retry=retry_if_exception(_is_transient_http_error),
)
def _post_webhook_json(url: str, payload: dict) -> None:
    """Generic HTTP POST + raise_for_status, with tenacity retry on
    5xx/transport errors. Used by both `GoogleChatNotifier` and
    `DiscordNotifier` — webhook semantics are the same; only the
    payload shape differs."""
    with httpx.Client(timeout=15.0) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()


# Backwards-compat alias for any caller still importing the old name.
_post_chat_card = _post_webhook_json


def _build_discord_embed(level: str, title: str, body: str, run_id: str | None) -> dict:
    """Discord embed payload. Colour is a decimal RGB int (not hex).
    Discord renders newlines and basic Markdown natively in the
    description field; no `<br>` substitution needed."""
    safe_body = _truncate(body, limit=3900)  # Discord caps description at 4096
    embed = {
        "title": title[:256],
        "description": safe_body,
        "color": DISCORD_COLOURS.get(level, DISCORD_COLOURS["critical"]),
    }
    if run_id:
        embed["footer"] = {"text": f"run_id: {run_id}"}
    return {"embeds": [embed]}


class GoogleChatNotifier:
    """POSTs a Google Chat cardV2 payload. Returns True on 2xx."""

    def __init__(self, webhook_url: str | None = None):
        if webhook_url is None:
            load_dotenv()
            webhook_url = os.getenv("GOOGLE_CHAT_WEBHOOK_URL", "") or None
        self._webhook_url = webhook_url

    def send(self, level: str, title: str, body: str, run_id: str | None) -> bool:
        if not self._webhook_url:
            logger.warning(
                "GOOGLE_CHAT_WEBHOOK_URL unset — skipping Chat send",
                extra={"alert_level": level, "alert_title": title},
            )
            return False
        payload = _build_card(level, title, body, run_id)
        return _send_via_webhook(
            self._webhook_url, payload, level, title, channel="chat"
        )


class DiscordNotifier:
    """POSTs a Discord embed payload to an incoming webhook URL.

    Reads `DISCORD_WEBHOOK_URL` at first use. Same retry policy as
    `GoogleChatNotifier`: tenacity retries 5xx + transport errors,
    bails on 4xx. Returns False without raising when the URL is
    unset (dev-friendly).
    """

    def __init__(self, webhook_url: str | None = None):
        if webhook_url is None:
            load_dotenv()
            webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "") or None
        self._webhook_url = webhook_url

    def send(self, level: str, title: str, body: str, run_id: str | None) -> bool:
        if not self._webhook_url:
            logger.warning(
                "DISCORD_WEBHOOK_URL unset — skipping Discord send",
                extra={"alert_level": level, "alert_title": title},
            )
            return False
        payload = _build_discord_embed(level, title, body, run_id)
        return _send_via_webhook(
            self._webhook_url, payload, level, title, channel="discord"
        )


def _send_via_webhook(
    url: str, payload: dict, level: str, title: str, channel: str
) -> bool:
    """Shared send-with-graceful-error path for chat/discord notifiers."""
    try:
        _post_webhook_json(url, payload)
    except httpx.HTTPStatusError as exc:
        logger.error(
            f"{channel} webhook rejected payload",
            extra={
                "status": exc.response.status_code,
                "alert_title": title,
                "body_preview": exc.response.text[:200],
            },
        )
        return False
    except (httpx.TransportError, Exception) as exc:
        logger.error(
            f"{channel} webhook send failed",
            extra={"error": repr(exc), "alert_title": title},
        )
        return False
    logger.info(
        f"{channel} alert sent",
        extra={"alert_level": level, "alert_title": title},
    )
    return True


# ---------------------------------------------------------------- factory


def get_notifier() -> Notifier:
    """Pick a notifier based on env config.

    Selection order:
      1. ALERT_BACKEND env var (`discord`, `google_chat`) — explicit wins.
      2. Auto-detect: prefer Discord (works on personal accounts) over
         Chat (requires Workspace).
      3. Fall back to LogOnly.

    Falls through to LogOnly even when the chosen backend's URL is
    blank — defensive so a typo'd ALERT_BACKEND doesn't silently
    drop alerts.
    """
    load_dotenv()
    backend = (os.getenv("ALERT_BACKEND") or "").lower().strip()

    if backend == "discord":
        return DiscordNotifier() if os.getenv("DISCORD_WEBHOOK_URL") else LogOnlyNotifier()
    if backend == "google_chat":
        return GoogleChatNotifier() if os.getenv("GOOGLE_CHAT_WEBHOOK_URL") else LogOnlyNotifier()

    # Auto-detect: Discord first (personal-account-friendly).
    if os.getenv("DISCORD_WEBHOOK_URL"):
        return DiscordNotifier()
    if os.getenv("GOOGLE_CHAT_WEBHOOK_URL"):
        return GoogleChatNotifier()
    return LogOnlyNotifier()
