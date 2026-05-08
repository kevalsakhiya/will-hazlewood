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
def _post_chat_card(url: str, payload: dict) -> None:
    with httpx.Client(timeout=15.0) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()


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
        try:
            _post_chat_card(self._webhook_url, payload)
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Chat webhook rejected payload",
                extra={
                    "status": exc.response.status_code,
                    "alert_title": title,
                    "body_preview": exc.response.text[:200],
                },
            )
            return False
        except (httpx.TransportError, Exception) as exc:
            logger.error(
                "Chat webhook send failed",
                extra={"error": repr(exc), "alert_title": title},
            )
            return False
        logger.info(
            "Chat alert sent", extra={"alert_level": level, "alert_title": title}
        )
        return True


# ---------------------------------------------------------------- factory


def get_notifier() -> Notifier:
    """Pick a notifier based on env config. Today only Google Chat is
    implemented; ALERT_BACKEND is read but not branched on (placeholder
    for future Slack/WhatsApp). Falls back to LogOnly when the webhook
    URL isn't configured."""
    load_dotenv()
    if os.getenv("GOOGLE_CHAT_WEBHOOK_URL"):
        return GoogleChatNotifier()
    return LogOnlyNotifier()
