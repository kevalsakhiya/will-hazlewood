"""Coverage for `common.dld_repo.iter_active_brokers` — the spider's
DLD seed source. Mocks the connection pool / cursor; no live DB."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.common import dld_repo


def _row(brn: str = "12345", name: str = "Test Broker") -> dict:
    """A row in the shape `dict_row` would yield from the cursor — every
    `DLDBroker` field as a key with a sensible value."""
    return {
        "brn": brn,
        "office_license_number": "LIC-1",
        "broker_name_en": name,
        "broker_name_ar": None,
        "phone": None,
        "mobile": None,
        "email": None,
        "real_estate_number": None,
        "office_name_en": "Test Office",
        "office_name_ar": None,
        "card_issue_date": date(2020, 1, 1),
        "card_expiry_date": None,
        "office_issue_date": None,
        "office_expiry_date": None,
        "photo_url": None,
        "office_logo_url": None,
        "card_rank_id": None,
        "card_rank": None,
        "office_rank_id": None,
        "office_rank": None,
        "awards_count": None,
    }


@pytest.fixture
def mock_pool():
    """Patch `get_pool()` and expose the cursor for inspection."""
    cur = MagicMock()
    cur.__iter__.return_value = iter([])  # default: empty result
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    pool = MagicMock()

    @contextmanager
    def _conn_ctx():
        yield conn

    pool.connection.side_effect = lambda: _conn_ctx()

    with patch.object(dld_repo, "get_pool", return_value=pool):
        yield cur, conn


def test_iter_active_brokers_yields_dld_dataclass(mock_pool):
    cur, _ = mock_pool
    cur.__iter__.return_value = iter([_row("100"), _row("200")])

    result = list(dld_repo.iter_active_brokers())

    assert len(result) == 2
    assert all(isinstance(b, dld_repo.DLDBroker) for b in result)
    brns = [b.brn for b in result]
    assert brns == ["100", "200"]


def test_iter_active_brokers_empty_table(mock_pool):
    cur, _ = mock_pool
    cur.__iter__.return_value = iter([])
    assert list(dld_repo.iter_active_brokers()) == []


def test_iter_active_brokers_default_selects_all(mock_pool):
    cur, _ = mock_pool
    cur.__iter__.return_value = iter([])
    list(dld_repo.iter_active_brokers())
    sql = cur.execute.call_args.args[0]
    assert "SELECT" in sql and "FROM dld_brokers" in sql
    # No WHERE clause filtering by run
    assert "WHERE" not in sql


def test_iter_active_brokers_run_id_filter_uses_where_clause(mock_pool):
    cur, _ = mock_pool
    cur.__iter__.return_value = iter([])
    list(dld_repo.iter_active_brokers(run_id="abc-123"))
    sql, params = cur.execute.call_args.args
    assert "WHERE last_seen_run_id" in sql
    assert params == ("abc-123",)


def test_iter_active_brokers_streams_lazily(mock_pool):
    """Generator should not pull rows until iterated — lets callers
    process 30k rows without materializing the full list."""
    cur, _ = mock_pool

    pulled = [0]

    def lazy():
        for r in [_row("1"), _row("2")]:
            pulled[0] += 1
            yield r

    cur.__iter__.return_value = lazy()

    gen = dld_repo.iter_active_brokers()
    # Just constructing the generator does not pull rows.
    assert pulled[0] == 0

    next(gen)
    assert pulled[0] == 1


def test_iter_active_brokers_uses_dict_row_factory(mock_pool):
    """Pass-through that asserts the SELECT cursor uses dict_row so we
    get column-name keys that map onto the DLDBroker fields by name."""
    cur, conn = mock_pool
    cur.__iter__.return_value = iter([])
    list(dld_repo.iter_active_brokers())
    # row_factory is passed via kwargs to conn.cursor(...)
    cursor_kwargs = conn.cursor.call_args.kwargs
    assert "row_factory" in cursor_kwargs
    # dict_row is the function reference; identity check
    from psycopg.rows import dict_row

    assert cursor_kwargs["row_factory"] is dict_row
