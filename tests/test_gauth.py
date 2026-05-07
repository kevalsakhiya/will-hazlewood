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
    gauth.reset_clients()
    yield
    gauth.reset_clients()


@pytest.fixture
def mock_google(tmp_path, monkeypatch):
    """Patch the heavy Google SDK calls + load_dotenv at the gauth
    namespace so .env doesn't leak into tests. A dummy token file
    exists at the path so the existence check passes."""
    token_file = tmp_path / "oauth_token.json"
    token_file.write_text('{"refresh_token": "fake"}')
    monkeypatch.setenv("OAUTH_TOKEN_JSON_PATH", str(token_file))

    fake_creds = MagicMock(name="creds")
    fake_creds.expired = False
    fake_resource = MagicMock(name="resource")

    with patch.object(
        gauth.Credentials,
        "from_authorized_user_file",
        return_value=fake_creds,
    ) as mock_creds, patch.object(
        gauth, "build", return_value=fake_resource
    ) as mock_build, patch.object(
        gauth, "load_dotenv", return_value=None
    ):
        yield mock_creds, mock_build, fake_creds, fake_resource, token_file


def test_scopes_include_sheets_and_drive():
    assert "https://www.googleapis.com/auth/spreadsheets" in gauth.SCOPES
    assert "https://www.googleapis.com/auth/drive" in gauth.SCOPES


def test_lazy_no_load_until_called():
    """Importing gauth must not touch creds."""
    assert gauth._creds is None
    assert gauth._sheets is None
    assert gauth._drive is None


def test_get_sheets_client_loads_token_with_correct_scopes(mock_google):
    mock_creds, _, _, _, token_file = mock_google
    gauth.get_sheets_client()
    mock_creds.assert_called_once()
    args, kwargs = mock_creds.call_args
    # signature is (filename, scopes)
    assert args[0] == str(token_file)
    scopes = args[1] if len(args) > 1 else kwargs.get("scopes")
    assert "https://www.googleapis.com/auth/spreadsheets" in scopes
    assert "https://www.googleapis.com/auth/drive" in scopes


def test_clients_share_one_credentials_object(mock_google):
    mock_creds, _, _, _, _ = mock_google
    gauth.get_sheets_client()
    gauth.get_drive_client()
    assert mock_creds.call_count == 1


def test_build_called_with_cache_discovery_false(mock_google):
    _, mock_build, _, _, _ = mock_google
    gauth.get_sheets_client()
    gauth.get_drive_client()
    for call in mock_build.call_args_list:
        assert call.kwargs.get("cache_discovery") is False


def test_sheets_client_cached_across_calls(mock_google):
    _, mock_build, _, _, _ = mock_google
    first = gauth.get_sheets_client()
    second = gauth.get_sheets_client()
    assert first is second
    sheets_calls = [c for c in mock_build.call_args_list if c.args[0] == "sheets"]
    assert len(sheets_calls) == 1


def test_drive_client_cached_across_calls(mock_google):
    _, mock_build, _, _, _ = mock_google
    first = gauth.get_drive_client()
    second = gauth.get_drive_client()
    assert first is second
    drive_calls = [c for c in mock_build.call_args_list if c.args[0] == "drive"]
    assert len(drive_calls) == 1


def test_missing_token_file_raises_helpful_error(monkeypatch):
    monkeypatch.setenv("OAUTH_TOKEN_JSON_PATH", "/tmp/does-not-exist-12345.json")
    with patch.object(gauth, "load_dotenv", return_value=None):
        with pytest.raises(FileNotFoundError, match="oauth_setup"):
            gauth.get_sheets_client()


def test_expired_token_is_refreshed_eagerly(mock_google):
    """If the saved access token has expired, gauth refreshes it at
    load time so we don't burn the first API call's latency on a 401
    + retry."""
    _, _, fake_creds, _, _ = mock_google
    fake_creds.expired = True
    fake_creds.refresh_token = "refresh-abc"
    gauth.get_sheets_client()
    fake_creds.refresh.assert_called_once()


def test_uses_env_var_for_token_path(monkeypatch, tmp_path):
    custom = tmp_path / "custom_token.json"
    custom.write_text("{}")
    monkeypatch.setenv("OAUTH_TOKEN_JSON_PATH", str(custom))
    fake_creds = MagicMock()
    fake_creds.expired = False
    with patch.object(
        gauth.Credentials, "from_authorized_user_file", return_value=fake_creds
    ) as mock_creds, patch.object(gauth, "build", return_value=MagicMock()), patch.object(
        gauth, "load_dotenv", return_value=None
    ):
        gauth.get_sheets_client()
    assert mock_creds.call_args.args[0] == str(custom)
