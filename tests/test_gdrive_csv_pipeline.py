"""Lifecycle + upload coverage for `GDriveCsvPipeline`. Drive client
is mocked; CSV writes go to a tmp directory."""

from __future__ import annotations

import csv
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.pipelines.gdrive_csv import (
    RESUMABLE_THRESHOLD_BYTES,
    GDriveCsvPipeline,
)


@pytest.fixture
def fake_spider():
    spider = MagicMock()
    spider.name = "agent_spider"
    spider.run_id = "run-abc"
    return spider


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("GDRIVE_CSV_FOLDER_ID", "csv-folder-id")


@pytest.fixture
def mock_drive():
    """Patch the Drive client at the pipeline's import path."""
    drive = MagicMock(name="drive_client")
    drive.files.return_value.create.return_value.execute.return_value = {
        "id": "uploaded-file-123"
    }
    with patch(
        "broker_scout.pipelines.gdrive_csv.gauth.get_drive_client",
        return_value=drive,
    ):
        yield drive


@pytest.fixture
def pipeline(tmp_path, fake_spider):
    p = GDriveCsvPipeline(out_dir=str(tmp_path / "out"))
    p.spider_opened(fake_spider)
    return p


# ------------------------------------------------------------ open


def test_spider_opened_creates_file_with_header(tmp_path, fake_spider):
    p = GDriveCsvPipeline(out_dir=str(tmp_path / "out"))
    p.spider_opened(fake_spider)
    assert p._csv_path.exists()
    assert p._csv_path.name == "agent_spider_run-abc.csv"
    with p._csv_path.open() as f:
        rows = list(csv.reader(f))
    assert len(rows) == 1
    # First header should match _SHEET_COLUMNS' first label
    from broker_scout.common.sheets_repo import template_header_row

    assert rows[0] == template_header_row()
    p._fh.close()


def test_spider_opened_creates_out_dir(tmp_path, fake_spider):
    out = tmp_path / "deep" / "out"
    p = GDriveCsvPipeline(out_dir=str(out))
    p.spider_opened(fake_spider)
    assert out.is_dir()
    p._fh.close()


def test_spider_opened_falls_back_when_run_id_missing(tmp_path):
    spider = MagicMock()
    spider.name = "agent_spider"
    spider.run_id = None
    p = GDriveCsvPipeline(out_dir=str(tmp_path / "out"))
    p.spider_opened(spider)
    # filename uses a timestamp instead of run_id
    assert p._csv_path.name.startswith("agent_spider_")
    assert p._csv_path.name.endswith(".csv")
    p._fh.close()


# ------------------------------------------------------------ items


def test_process_item_writes_row_per_item(pipeline, fake_spider):
    pipeline.process_item({"broker_name": "A"}, fake_spider)
    pipeline.process_item({"broker_name": "B"}, fake_spider)
    pipeline._fh.flush()
    with pipeline._csv_path.open() as f:
        rows = list(csv.reader(f))
    # header + 2 data rows
    assert len(rows) == 3
    assert rows[1][0] == "A"
    assert rows[2][0] == "B"


def test_process_item_returns_item_unchanged(pipeline, fake_spider):
    item = {"broker_name": "X"}
    out = pipeline.process_item(item, fake_spider)
    assert out is item


def test_process_item_lazy_init_if_open_skipped(tmp_path, fake_spider):
    p = GDriveCsvPipeline(out_dir=str(tmp_path / "out"))
    p.process_item({"broker_name": "A"}, fake_spider)
    assert p._csv_path.exists()
    p._fh.close()


# ------------------------------------------------------------ close


def test_spider_closed_uploads_csv(pipeline, fake_spider, env, mock_drive):
    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    pipeline.spider_closed(fake_spider, reason="finished")

    create = mock_drive.files.return_value.create
    create.assert_called_once()
    body = create.call_args.kwargs["body"]
    assert body["parents"] == ["csv-folder-id"]
    assert body["name"].startswith("agent_spider_")
    assert body["name"].endswith(".csv")
    fake_spider.crawler.stats.set_value.assert_any_call(
        "gdrive_csv/upload_status", "ok"
    )
    fake_spider.crawler.stats.set_value.assert_any_call(
        "gdrive_csv/file_id", "uploaded-file-123"
    )
    fake_spider.crawler.stats.inc_value.assert_any_call(
        "gdrive_csv/rows_uploaded", 1
    )


def test_success_log_does_not_collide_with_logrecord_attrs(
    pipeline, fake_spider, env, mock_drive, caplog
):
    """Regression: a previous version used `extra={"name": ...}` which
    collides with LogRecord.name and raises KeyError. The actual upload
    succeeds but the success log explodes, mismarking status as failed.
    """
    import logging as _logging

    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    with caplog.at_level(_logging.INFO, logger="broker_scout.pipelines.gdrive_csv"):
        pipeline.spider_closed(fake_spider, reason="finished")

    # Status must be 'ok' — if the success log threw, the except branch
    # would have set it to 'failed'.
    set_calls = fake_spider.crawler.stats.set_value.call_args_list
    statuses = [
        c.args[1] for c in set_calls if c.args[0] == "gdrive_csv/upload_status"
    ]
    assert "ok" in statuses, f"upload_status never set to ok: {statuses}"
    assert "failed" not in statuses, "success log raised, status was set to failed"


def test_spider_closed_skips_upload_when_no_rows(pipeline, fake_spider, env, mock_drive):
    pipeline.spider_closed(fake_spider, reason="finished")
    mock_drive.files.return_value.create.assert_not_called()
    fake_spider.crawler.stats.set_value.assert_any_call(
        "gdrive_csv/upload_status", "skipped"
    )


def test_spider_closed_swallows_upload_failure(
    pipeline, fake_spider, env, mock_drive
):
    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    mock_drive.files.return_value.create.return_value.execute.side_effect = (
        RuntimeError("network down")
    )
    # must not raise — Postgres + Sheets close handlers must still run
    pipeline.spider_closed(fake_spider, reason="finished")
    fake_spider.crawler.stats.set_value.assert_any_call(
        "gdrive_csv/upload_status", "failed"
    )


def test_spider_closed_missing_folder_id_marks_failed(
    pipeline, fake_spider, monkeypatch, mock_drive
):
    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    monkeypatch.delenv("GDRIVE_CSV_FOLDER_ID", raising=False)
    pipeline.spider_closed(fake_spider, reason="finished")
    fake_spider.crawler.stats.set_value.assert_any_call(
        "gdrive_csv/upload_status", "failed"
    )


# ------------------------------------------------------------ resumable


def test_small_file_uses_simple_upload(pipeline, fake_spider, env, mock_drive):
    """Files below the threshold use resumable=False (one round-trip)."""
    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    with patch("broker_scout.pipelines.gdrive_csv.MediaFileUpload") as mock_mfu:
        pipeline.spider_closed(fake_spider, reason="finished")
    assert mock_mfu.call_args.kwargs["resumable"] is False


def test_large_file_uses_resumable_upload(pipeline, fake_spider, env, mock_drive):
    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    pipeline._fh.flush() if pipeline._fh else None
    with patch("broker_scout.pipelines.gdrive_csv.MediaFileUpload") as mock_mfu, patch(
        "pathlib.Path.stat"
    ) as mock_stat:
        mock_stat.return_value.st_size = RESUMABLE_THRESHOLD_BYTES + 1
        pipeline.spider_closed(fake_spider, reason="finished")
    assert mock_mfu.call_args.kwargs["resumable"] is True


# ------------------------------------------------------------ filenames


def test_uploaded_filename_contains_timestamp(pipeline, fake_spider, env, mock_drive):
    """Filename format: {spider}_{YYYYMMDD-HHMMSS}.csv — sortable."""
    import re

    pipeline.process_item({"broker_name": "Foo"}, fake_spider)
    pipeline.spider_closed(fake_spider, reason="finished")
    body = mock_drive.files.return_value.create.call_args.kwargs["body"]
    assert re.match(r"^agent_spider_\d{8}-\d{6}\.csv$", body["name"]), body["name"]
