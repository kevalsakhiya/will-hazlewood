"""Shared Google service-account credentials + Sheets/Drive clients.

Phase 4 (Sheets) and Phase 5 (Drive CSV) both need authenticated
Google clients. This module centralizes:

  * the scopes (Sheets + Drive.file)
  * the path to the service-account JSON
  * a lazy-built, thread-safe singleton creds object
  * lazy-built `sheets` (v4) and `drive` (v3) clients

Pattern mirrors `common/db.py`'s lazy connection pool: idempotent,
no per-spider state, callers just import and use.
"""

from __future__ import annotations

import logging
import os
from threading import RLock

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import Resource, build

logger = logging.getLogger(__name__)

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",   # Sheets read/write
    "https://www.googleapis.com/auth/drive.file",     # only files this SA created/has access to
)

DEFAULT_CREDS_PATH = "secrets/service_account.json"

_creds: Credentials | None = None
_sheets: Resource | None = None
_drive: Resource | None = None
_lock = RLock()  # reentrant: get_sheets_client → _get_credentials nests this


def _get_credentials() -> Credentials:
    global _creds
    if _creds is None:
        with _lock:
            if _creds is None:
                load_dotenv()
                path = os.getenv("SERVICE_ACCOUNT_JSON_PATH", DEFAULT_CREDS_PATH)
                _creds = Credentials.from_service_account_file(
                    path, scopes=list(SCOPES)
                )
                logger.info("loaded google service account credentials")
    return _creds


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
