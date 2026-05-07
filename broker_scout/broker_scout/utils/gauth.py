"""Shared Google OAuth user credentials + Sheets/Drive clients.

Phase 4 (Sheets) and Phase 5 (Drive CSV) both authenticate as a real
Google user (not a service account), so file ownership lands in the
user's Drive instead of in the SA's zero-quota My Drive.

The flow:

  1. One-time, on a machine with a browser:
       poetry run python -m broker_scout.tools.oauth_setup
     This opens a consent screen, exchanges the auth code for a
     refresh token, and writes `secrets/oauth_token.json`.
  2. Every subsequent run (laptop or headless server) loads that
     token file. The refresh token is long-lived; the google-auth
     library transparently swaps it for a fresh access token via a
     POST to oauth2.googleapis.com when the current one expires
     (every ~1 hour). No browser needed at runtime.

Pattern mirrors `common/db.py`'s lazy connection pool: idempotent,
no per-spider state, callers just import and use.
"""

from __future__ import annotations

import logging
import os
from threading import RLock

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build

logger = logging.getLogger(__name__)

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    # Full drive scope is required: drive.file only sees files the SA
    # itself created, and our user-OAuth flow needs to read/copy
    # user-owned templates anyway.
    "https://www.googleapis.com/auth/drive",
)

DEFAULT_TOKEN_PATH = "secrets/oauth_token.json"

_creds: Credentials | None = None
_sheets: Resource | None = None
_drive: Resource | None = None
_lock = RLock()  # reentrant: get_sheets_client → _get_credentials nests this


def _token_path() -> str:
    load_dotenv()
    return os.getenv("OAUTH_TOKEN_JSON_PATH", DEFAULT_TOKEN_PATH)


def _get_credentials() -> Credentials:
    global _creds
    if _creds is None:
        with _lock:
            if _creds is None:
                path = _token_path()
                if not os.path.exists(path):
                    raise FileNotFoundError(
                        f"OAuth token file not found at {path!r}. Run "
                        "`poetry run python -m broker_scout.tools.oauth_setup` "
                        "on a machine with a browser to generate it."
                    )
                _creds = Credentials.from_authorized_user_file(path, list(SCOPES))
                _refresh_if_needed(_creds)
                logger.info("loaded google oauth user credentials")
    return _creds


def _refresh_if_needed(creds: Credentials) -> None:
    """Trigger an access-token refresh if the current one is expired or
    about to expire. The library does this lazily on first API call
    too, but doing it eagerly catches token problems at startup
    instead of mid-run."""
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())


def get_sheets_client() -> Resource:
    """Lazy-build the Sheets v4 client. Reuses cached credentials."""
    global _sheets
    if _sheets is None:
        with _lock:
            if _sheets is None:
                _sheets = build(
                    "sheets",
                    "v4",
                    credentials=_get_credentials(),
                    cache_discovery=False,
                )
    return _sheets


def get_drive_client() -> Resource:
    """Lazy-build the Drive v3 client. Reuses cached credentials."""
    global _drive
    if _drive is None:
        with _lock:
            if _drive is None:
                _drive = build(
                    "drive",
                    "v3",
                    credentials=_get_credentials(),
                    cache_discovery=False,
                )
    return _drive


def reset_clients() -> None:
    """Clear cached creds + clients. Test-only — production has no use case."""
    global _creds, _sheets, _drive
    with _lock:
        _creds = None
        _sheets = None
        _drive = None
