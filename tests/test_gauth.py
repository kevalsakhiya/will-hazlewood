"""Plumbing tests for `utils.gauth` — no live Google API calls.

Patches the credential loader and discovery builder *at the gauth
module's namespace* (since gauth imports them at module level), so
real network calls never happen.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from broker_scout.utils import gauth


@pytest.fixture(autouse=True)
def reset_gauth_cache():
    """Each test starts with no cached creds/clients."""
    gauth.reset_clients()
    yield
    gauth.reset_clients()


@pytest.fixture
def mock_google():
    """Patch the heavy Google SDK calls + load_dotenv at the gauth
    namespace so .env doesn't leak into tests."""
    fake_creds = MagicMock(name="creds")
    fake_resource = MagicMock(name="resource")
    with patch.object(
        gauth.Credentials,
        "from_service_account_file",
        return_value=fake_creds,
    ) as mock_creds, patch.object(
        gauth, "build", return_value=fake_resource
    ) as mock_build, patch.object(
        gauth, "load_dotenv", return_value=None
    ):
        yield mock_creds, mock_build, fake_creds, fake_resource


def test_scopes_include_sheets_and_drive():
    assert (
        "https://www.googleapis.com/auth/spreadsheets" in gauth.SCOPES
    ), "Sheets scope missing"
    assert (
        "https://www.googleapis.com/auth/drive.file" in gauth.SCOPES
    ), "Drive scope missing"


def test_lazy_no_load_until_called():
    """Importing gauth must not touch creds. The module-level globals
    stay None until a public function is invoked."""
    assert gauth._creds is None
    assert gauth._sheets is None
    assert gauth._drive is None


def test_get_sheets_client_uses_correct_scopes(mock_google):
    mock_creds, _, _, _ = mock_google
    gauth.get_sheets_client()

    mock_creds.assert_called_once()
    # scopes argument: either positional [1] or kwarg
    args, kwargs = mock_creds.call_args
    scopes = kwargs.get("scopes") or (args[1] if len(args) > 1 else None)
    assert scopes is not None
    assert "https://www.googleapis.com/auth/spreadsheets" in scopes
    assert "https://www.googleapis.com/auth/drive.file" in scopes


def test_clients_share_one_credentials_object(mock_google):
    mock_creds, _, _, _ = mock_google
    gauth.get_sheets_client()
    gauth.get_drive_client()
    assert mock_creds.call_count == 1


def test_build_called_with_cache_discovery_false(mock_google):
    _, mock_build, _, _ = mock_google
    gauth.get_sheets_client()
    gauth.get_drive_client()
    for call in mock_build.call_args_list:
        assert call.kwargs.get("cache_discovery") is False, (
            f"build() called without cache_discovery=False: {call}"
        )


def test_sheets_client_cached_across_calls(mock_google):
    _, mock_build, _, _ = mock_google
    first = gauth.get_sheets_client()
    second = gauth.get_sheets_client()
    assert first is second
    sheets_calls = [c for c in mock_build.call_args_list if c.args[0] == "sheets"]
    assert len(sheets_calls) == 1


def test_drive_client_cached_across_calls(mock_google):
    _, mock_build, _, _ = mock_google
    first = gauth.get_drive_client()
    second = gauth.get_drive_client()
    assert first is second
    drive_calls = [c for c in mock_build.call_args_list if c.args[0] == "drive"]
    assert len(drive_calls) == 1


def test_uses_env_var_for_creds_path(monkeypatch, mock_google):
    mock_creds, _, _, _ = mock_google
    monkeypatch.setenv("SERVICE_ACCOUNT_JSON_PATH", "/tmp/custom.json")
    gauth.get_sheets_client()
    assert mock_creds.call_args.args[0] == "/tmp/custom.json"


def test_default_creds_path_when_env_unset(monkeypatch, mock_google):
    mock_creds, _, _, _ = mock_google
    monkeypatch.delenv("SERVICE_ACCOUNT_JSON_PATH", raising=False)
    gauth.get_sheets_client()
    assert mock_creds.call_args.args[0] == gauth.DEFAULT_CREDS_PATH
