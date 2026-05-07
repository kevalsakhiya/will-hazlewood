"""SQL-contract coverage for `brokers_repo`. No live Postgres — every
test patches the connection pool and asserts the exact cursor calls."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.common import brokers_repo


@pytest.fixture
def mock_pool():
    """Patch `get_pool()` so cursor calls are inspectable.

    Returns the cursor mock so tests can assert against it.
    """
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    pool = MagicMock()

    @contextmanager
    def _conn_ctx():
        yield conn

    pool.connection.side_effect = lambda: _conn_ctx()

    with patch.object(brokers_repo, "get_pool", return_value=pool):
        yield cur, conn


# ------------------------------------------------------------- open_run


def test_open_run_inserts_with_run_id_and_spider(mock_pool):
    cur, conn = mock_pool
    brokers_repo.open_run("abc-123", "agent_spider")
    cur.execute.assert_called_once()
    sql, params = cur.execute.call_args.args
    assert "INSERT INTO scrape_runs" in sql
    assert params == ("abc-123", "agent_spider")
    conn.commit.assert_called_once()


# ------------------------------------------------------------- close_run


def test_close_run_updates_all_fields(mock_pool):
    cur, conn = mock_pool
    brokers_repo.close_run(
        run_id="abc-123",
        status="ok",
        items_scraped=42,
        items_dropped=3,
        stats={"foo": "bar"},
    )
    cur.execute.assert_called_once()
    sql, params = cur.execute.call_args.args
    assert "UPDATE scrape_runs" in sql
    assert params["run_id"] == "abc-123"
    assert params["status"] == "ok"
    assert params["items_scraped"] == 42
    assert params["items_dropped"] == 3
    # stats is wrapped in Jsonb — adapter type rather than the raw dict
    assert params["stats"].__class__.__name__ == "Jsonb"
    conn.commit.assert_called_once()


# ------------------------------------------------------------- insert_brokers


def _valid_item(**overrides) -> dict:
    base = {
        "platform": "propertyfinder",
        "scrape_date": "2026-05-06",
        "agent_url": "https://www.propertyfinder.ae/en/agent/foo",
        "broker_name": "Foo Bar",
        "brn": "12345",
        "nationality": None,
        "agent_specialization": None,
        "experience_since": 2010,
        "whatsapp_response_time": None,
        "is_superagent": True,
        "agency_url": None,
        "agency_registration_number": None,
        "listings_for_sale": None,
        "listings_for_rent": None,
        "listings_total": None,
        "listings_with_marketing_spend": None,
        "average_listing_price_sale": None,
        "average_listing_price_rent": None,
        "average_listing_age_days_sale": None,
        "average_listing_age_days_rent": None,
        "most_recent_listing_date_sale": None,
        "most_recent_listing_date_rent": None,
        "closed_transaction_sale": None,
        "closed_transaction_rent": None,
        "closed_deals_total": None,
        "closed_transaction_deal_value": None,
        "closed_transaction_sale_total_amount": None,
        "closed_transaction_rent_total_amount": None,
        "closed_transaction_sale_avg_amount": None,
        "closed_transaction_rent_avg_amount": None,
        "most_recent_deal_date_sale": None,
        "most_recent_deal_date_rent": None,
        "average_monthly_deal_volume_sale": None,
        "average_monthly_deal_volume_rent": None,
    }
    base.update(overrides)
    return base


def test_insert_brokers_empty_input_no_db_call(mock_pool):
    cur, _ = mock_pool
    n = brokers_repo.insert_brokers([], "abc", "2026-05-06")
    assert n == 0
    cur.executemany.assert_not_called()


def test_insert_brokers_executemany_with_correct_template(mock_pool):
    cur, _ = mock_pool
    items = [_valid_item(brn=str(i)) for i in range(3)]
    n = brokers_repo.insert_brokers(items, "run-xyz", "2026-05-06")
    assert n == 3
    cur.executemany.assert_called_once()
    sql, rows = cur.executemany.call_args.args
    assert "INSERT INTO brokers" in sql
    assert "ON CONFLICT (run_id, platform, brn) DO NOTHING" in sql
    assert len(rows) == 3
    assert all(r["run_id"] == "run-xyz" for r in rows)
    assert all(r["scrape_date"] == "2026-05-06" for r in rows)
    assert all(r["match_status"] == "unknown" for r in rows)
    assert all(r["match_confidence"] is None for r in rows)
    # raw is wrapped as JSONB
    assert all(r["raw"].__class__.__name__ == "Jsonb" for r in rows)


def test_insert_brokers_respects_explicit_match_status(mock_pool):
    """If Phase 6 starts populating match_status on the item, the repo should
    pass it through rather than overriding to 'unknown'."""
    cur, _ = mock_pool
    item = _valid_item(brn="X")
    item["match_status"] = "exact_brn"
    item["match_confidence"] = 1.0
    brokers_repo.insert_brokers([item], "r", "2026-05-06")
    rows = cur.executemany.call_args.args[1]
    assert rows[0]["match_status"] == "exact_brn"
    assert rows[0]["match_confidence"] == 1.0


def test_insert_brokers_batches_at_BATCH_SIZE(mock_pool):
    """Two batches when input exceeds BATCH_SIZE."""
    cur, _ = mock_pool
    items = [_valid_item(brn=str(i)) for i in range(brokers_repo.BATCH_SIZE + 5)]
    n = brokers_repo.insert_brokers(items, "r", "2026-05-06")
    assert n == brokers_repo.BATCH_SIZE + 5
    assert cur.executemany.call_count == 2
    first_batch_size = len(cur.executemany.call_args_list[0].args[1])
    second_batch_size = len(cur.executemany.call_args_list[1].args[1])
    assert first_batch_size == brokers_repo.BATCH_SIZE
    assert second_batch_size == 5


def test_insert_brokers_columns_match_template(mock_pool):
    """Every key in the row dict must appear in the INSERT placeholder list,
    and vice versa, so psycopg's named-parameter substitution doesn't blow up.
    """
    cur, _ = mock_pool
    brokers_repo.insert_brokers([_valid_item()], "r", "2026-05-06")
    rows = cur.executemany.call_args.args[1]
    assert set(rows[0].keys()) == set(brokers_repo._BROKER_COLUMNS)


# ------------------------------------------------------------- insert_bad_items


def test_insert_bad_items_empty_input(mock_pool):
    cur, _ = mock_pool
    n = brokers_repo.insert_bad_items([])
    assert n == 0
    cur.executemany.assert_not_called()


def test_insert_bad_items_wraps_payload_as_jsonb(mock_pool):
    cur, _ = mock_pool
    bad = [
        {
            "run_id": "r1",
            "platform": "propertyfinder",
            "reason": "whatsapp_response_time: must be ≥ 0",
            "payload": {"item": {"brn": "1"}, "errors": []},
        }
    ]
    n = brokers_repo.insert_bad_items(bad)
    assert n == 1
    cur.executemany.assert_called_once()
    sql, params = cur.executemany.call_args.args
    assert "INSERT INTO bad_items" in sql
    assert params[0]["run_id"] == "r1"
    assert params[0]["platform"] == "propertyfinder"
    assert params[0]["payload"].__class__.__name__ == "Jsonb"
