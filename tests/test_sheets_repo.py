"""Coverage for `common.sheets_repo` — registry resolution, Drive copy,
sharing, append-with-retry, and capacity guard. No live Google or DB."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError

from broker_scout.common import sheets_repo


# ============================================================ fixtures


@pytest.fixture
def mock_pool():
    """Patch `get_pool()` so cursor calls are inspectable."""
    cur = MagicMock()
    cur.fetchone.return_value = None
    cur.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    pool = MagicMock()

    @contextmanager
    def _conn_ctx():
        yield conn

    pool.connection.side_effect = lambda: _conn_ctx()

    with patch.object(sheets_repo, "get_pool", return_value=pool):
        yield cur, conn


@pytest.fixture
def mock_drive():
    """Patch `gauth.get_drive_client` and provide canned responses.

    Setup uses .return_value chains (not calls) so call_count starts at 0.
    """
    drive = MagicMock(name="drive_client")
    drive.files.return_value.copy.return_value.execute.return_value = {"id": "new-sheet-123"}
    drive.permissions.return_value.create.return_value.execute.return_value = {"id": "perm-1"}
    with patch.object(sheets_repo.gauth, "get_drive_client", return_value=drive):
        yield drive


@pytest.fixture
def mock_sheets():
    """Patch `gauth.get_sheets_client`. Default: empty grid (10M remaining)."""
    sheets = MagicMock(name="sheets_client")
    sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"gridProperties": {"rowCount": 0, "columnCount": 0}}}]
    }
    sheets.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {
        "updates": {"updatedRows": 1}
    }
    with patch.object(sheets_repo.gauth, "get_sheets_client", return_value=sheets):
        yield sheets


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("GSHEET_TEMPLATE_PF_ID", "tpl-pf")
    monkeypatch.setenv("GSHEET_TEMPLATE_BAYUT_ID", "tpl-bayut")
    monkeypatch.setenv("GSHEET_PF_FOLDER_ID", "folder-pf")
    monkeypatch.setenv("GSHEET_BAYUT_FOLDER_ID", "folder-bayut")
    monkeypatch.delenv("GSHEET_VIEWER_EMAILS", raising=False)


def _http_error(status: int) -> HttpError:
    resp = MagicMock()
    resp.status = status
    resp.reason = "fake"
    return HttpError(resp, b'{"error": "fake"}')


# ============================================================ current_period


def test_current_period_format():
    p = sheets_repo.current_period()
    # 'YYYY-MM' — 7 chars, dash at index 4
    assert len(p) == 7
    assert p[4] == "-"
    int(p[:4])
    int(p[5:])


# ============================================================ to_row


def test_to_row_preserves_column_order():
    item = {col: f"v_{col}" for col in sheets_repo._SHEET_COLUMNS}
    row = sheets_repo.to_row(item)
    assert len(row) == len(sheets_repo._SHEET_COLUMNS)
    for col, val in zip(sheets_repo._SHEET_COLUMNS, row):
        assert val == f"v_{col}"


def test_to_row_missing_keys_become_empty_string():
    row = sheets_repo.to_row({"platform": "propertyfinder"})
    assert row[sheets_repo._SHEET_COLUMNS.index("platform")] == "propertyfinder"
    # other slots should be empty string, not 'None'
    for col, val in zip(sheets_repo._SHEET_COLUMNS, row):
        if col != "platform":
            assert val == ""


def test_to_row_excludes_raw_blob():
    """`raw` lives in Postgres only; Sheets should never see it."""
    assert "raw" not in sheets_repo._SHEET_COLUMNS


def test_template_header_row_matches_columns_in_order():
    """The display headers paste-list must align 1:1 with the data
    column order — otherwise data lands under the wrong header."""
    headers = sheets_repo.template_header_row()
    assert len(headers) == len(sheets_repo._SHEET_COLUMNS)
    # every label is non-empty
    assert all(h and isinstance(h, str) for h in headers)


def test_sheet_headers_dict_keys_match_columns_exactly():
    """Integrity check: a column without a label, or a label without a
    column, would silently desync data from headers."""
    assert tuple(sheets_repo._SHEET_HEADERS.keys()) == sheets_repo._SHEET_COLUMNS


# ============================================================ get_or_create


def test_get_or_create_returns_existing(mock_pool, env):
    cur, _ = mock_pool
    cur.fetchone.return_value = ("existing-sheet-456",)
    sid = sheets_repo.get_or_create_active_sheet("propertyfinder")
    assert sid == "existing-sheet-456"


def test_get_or_create_calls_drive_copy_when_missing(mock_pool, env, mock_drive):
    cur, _ = mock_pool
    # First select returns None; insert succeeds (rowcount=1)
    cur.fetchone.return_value = None
    cur.rowcount = 1

    sid = sheets_repo.get_or_create_active_sheet("propertyfinder")

    assert sid == "new-sheet-123"
    # Drive.files.copy invoked with template + folder + name including period
    copy_calls = [c for c in mock_drive.files.return_value.copy.call_args_list if c.kwargs.get("body")]
    assert len(copy_calls) >= 1
    body = copy_calls[-1].kwargs["body"]
    assert body["parents"] == ["folder-pf"]
    assert "PropertyFinder Brokers" in body["name"]
    period = sheets_repo.current_period()
    assert period in body["name"]


def test_get_or_create_shares_with_viewer_emails(mock_pool, env, mock_drive, monkeypatch):
    cur, _ = mock_pool
    cur.fetchone.return_value = None
    cur.rowcount = 1
    monkeypatch.setenv("GSHEET_VIEWER_EMAILS", "alice@example.com, bob@example.com")

    sheets_repo.get_or_create_active_sheet("propertyfinder")

    perm_calls = [
        c for c in mock_drive.permissions.return_value.create.call_args_list
        if c.kwargs.get("body")
    ]
    addresses = [c.kwargs["body"]["emailAddress"] for c in perm_calls]
    assert "alice@example.com" in addresses
    assert "bob@example.com" in addresses


def test_get_or_create_skips_share_when_emails_unset(
    mock_pool, env, mock_drive, monkeypatch
):
    cur, _ = mock_pool
    cur.fetchone.return_value = None
    cur.rowcount = 1
    monkeypatch.delenv("GSHEET_VIEWER_EMAILS", raising=False)

    sheets_repo.get_or_create_active_sheet("propertyfinder")

    perm_calls = [
        c for c in mock_drive.permissions.return_value.create.call_args_list
        if c.kwargs.get("body")
    ]
    assert perm_calls == []


def test_get_or_create_inserts_then_deactivates_prior(mock_pool, env, mock_drive):
    cur, _ = mock_pool
    cur.fetchone.return_value = None
    cur.rowcount = 1

    sheets_repo.get_or_create_active_sheet("propertyfinder")

    sql_calls = [c.args[0] for c in cur.execute.call_args_list]
    assert any("INSERT INTO sheet_registry" in sql for sql in sql_calls)
    assert any(
        "UPDATE sheet_registry SET is_active=FALSE" in sql for sql in sql_calls
    )


def test_get_or_create_handles_race_orphans_drive_copy(
    mock_pool, env, mock_drive
):
    """If a concurrent winner inserted first, our INSERT ON CONFLICT
    DO NOTHING affects 0 rows; we re-SELECT and use the winner's id."""
    cur, _ = mock_pool
    # First fetch (initial select): empty.
    # Second fetch (after losing INSERT race): returns winner's id.
    cur.fetchone.side_effect = [None, ("winner-sheet-999",)]
    cur.rowcount = 0  # INSERT was a no-op

    sid = sheets_repo.get_or_create_active_sheet("propertyfinder")

    assert sid == "winner-sheet-999"
    # we still called drive.copy (we had to before knowing about the race)
    assert mock_drive.files.return_value.copy.called


def test_get_or_create_unknown_platform_raises(mock_pool, env):
    with pytest.raises(ValueError, match="unknown platform"):
        sheets_repo.get_or_create_active_sheet("rightmove")


def test_get_or_create_missing_env_raises(mock_pool, monkeypatch):
    monkeypatch.delenv("GSHEET_TEMPLATE_PF_ID", raising=False)
    cur, _ = mock_pool
    cur.fetchone.return_value = None
    with pytest.raises(RuntimeError, match="GSHEET_TEMPLATE_PF_ID"):
        sheets_repo.get_or_create_active_sheet("propertyfinder")


# ============================================================ append_rows


def test_append_rows_empty_input_no_api_call(mock_sheets):
    n = sheets_repo.append_rows("sid", [])
    assert n == 0
    mock_sheets.spreadsheets.return_value.values.return_value.append.assert_not_called()


def test_append_rows_uses_raw_and_insert_rows(mock_sheets):
    rows = [["a", "b"], ["c", "d"]]
    n = sheets_repo.append_rows("sid-xyz", rows)
    assert n == 2
    append_call = mock_sheets.spreadsheets.return_value.values.return_value.append
    append_call.assert_called_once()
    kwargs = append_call.call_args.kwargs
    assert kwargs["spreadsheetId"] == "sid-xyz"
    assert kwargs["valueInputOption"] == "RAW"
    assert kwargs["insertDataOption"] == "INSERT_ROWS"
    assert kwargs["includeValuesInResponse"] is False
    assert kwargs["body"] == {"values": rows}


def test_append_rows_retries_5xx(mock_sheets):
    """Transient 503 is retried via tenacity; eventually succeeds."""
    append = mock_sheets.spreadsheets.return_value.values.return_value.append
    # Patch the retry's sleep to keep test fast.
    success_resp = MagicMock()
    success_resp.execute.return_value = {"updates": {}}
    transient_resp = MagicMock()
    transient_resp.execute.side_effect = _http_error(503)
    # First two attempts raise, third succeeds.
    append.side_effect = [transient_resp, transient_resp, success_resp]

    with patch("tenacity.nap.time.sleep", return_value=None):
        n = sheets_repo.append_rows("sid", [["a"]])
    assert n == 1
    assert append.call_count == 3


def test_append_rows_does_not_retry_4xx(mock_sheets):
    """A 400/404 is not transient — should raise immediately, no retry."""
    append = mock_sheets.spreadsheets.return_value.values.return_value.append
    failing_resp = MagicMock()
    failing_resp.execute.side_effect = _http_error(404)
    append.return_value = failing_resp

    with pytest.raises(HttpError):
        sheets_repo.append_rows("sid", [["a"]])
    assert append.call_count == 1


def test_append_rows_gives_up_after_5_attempts(mock_sheets):
    append = mock_sheets.spreadsheets.return_value.values.return_value.append
    failing_resp = MagicMock()
    failing_resp.execute.side_effect = _http_error(503)
    append.return_value = failing_resp

    with patch("tenacity.nap.time.sleep", return_value=None), pytest.raises(HttpError):
        sheets_repo.append_rows("sid", [["a"]])
    assert append.call_count == 5


def test_append_rows_retries_on_429(mock_sheets):
    append = mock_sheets.spreadsheets.return_value.values.return_value.append
    transient = MagicMock()
    transient.execute.side_effect = _http_error(429)
    success = MagicMock()
    success.execute.return_value = {}
    append.side_effect = [transient, success]

    with patch("tenacity.nap.time.sleep", return_value=None):
        sheets_repo.append_rows("sid", [["a"]])
    assert append.call_count == 2


# ============================================================ capacity check


def test_capacity_check_passes_when_room(mock_sheets):
    # mock fixture defaults to empty grid (10M remaining)
    sheets_repo.pre_flight_capacity_check("sid", expected_run_cells=1_000_000)


def test_capacity_check_raises_when_full(mock_sheets):
    mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"gridProperties": {"rowCount": 1000, "columnCount": 9000}}},
            {"properties": {"gridProperties": {"rowCount": 1000, "columnCount": 1000}}},
        ]
    }
    # 9M + 1M = 10M used → 0 remaining
    with pytest.raises(sheets_repo.SheetsCapacityError):
        sheets_repo.pre_flight_capacity_check("sid", expected_run_cells=1)


def test_capacity_check_respects_safety_margin(mock_sheets):
    # 9M used → 1M remaining → 0.9M headroom; 0.95M should fail.
    mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"gridProperties": {"rowCount": 9000, "columnCount": 1000}}}]
    }
    with pytest.raises(sheets_repo.SheetsCapacityError):
        sheets_repo.pre_flight_capacity_check("sid", expected_run_cells=950_000)


def test_capacity_check_passes_within_safety_margin(mock_sheets):
    # 9M used → 1M remaining → 0.9M headroom; 0.5M should pass.
    mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"gridProperties": {"rowCount": 9000, "columnCount": 1000}}}]
    }
    sheets_repo.pre_flight_capacity_check("sid", expected_run_cells=500_000)


def test_capacity_check_aggregates_across_tabs(mock_sheets):
    """Cell limit is per-spreadsheet, so all tabs sum into used."""
    mock_sheets.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"gridProperties": {"rowCount": 100, "columnCount": 100}}},
            {"properties": {"gridProperties": {"rowCount": 200, "columnCount": 100}}},
        ]
    }
    used = sheets_repo._used_cells("sid")
    assert used == 100 * 100 + 200 * 100
