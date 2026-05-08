"""Google Drive CSV pipeline (priority 600) — per-run archive sink.

Lifecycle:

  * spider_opened : open `out/{spider}_{run_id}.csv` for append, write
                    the header row.
  * process_item  : write one row per item, immediately flushed to disk.
  * spider_closed : close the file, upload to GDRIVE_CSV_FOLDER_ID via
                    Drive API. Resumable upload for files > 5 MB.

The on-disk CSV is kept for 7 days as a recovery / replay source for
any failed Sheets writes (Phase 12 `tools/replay_run.py`). Operators
purge `out/` via a separate cron — not the pipeline's job.
"""

from __future__ import annotations

import csv
import logging
from datetime import UTC, datetime
from pathlib import Path

from googleapiclient.http import MediaFileUpload
from scrapy import signals

from broker_scout.common import sheets_repo
from broker_scout.utils import gauth

logger = logging.getLogger(__name__)

# Resumable upload kicks in above this size. Sheets default is 5 MB
# per-chunk for resumable; below that, simple upload is faster (one
# round-trip vs. multiple).
RESUMABLE_THRESHOLD_BYTES = 5 * 1024 * 1024

DEFAULT_OUT_DIR = "out"
TIMESTAMP_FMT = "%Y%m%d-%H%M%S"


class GDriveCsvPipeline:
    """Per-run CSV writer that uploads to Drive on spider close."""

    def __init__(self, out_dir: str = DEFAULT_OUT_DIR, folder_id: str = ""):
        self._out_dir = Path(out_dir)
        self._folder_id = folder_id
        self._csv_path: Path | None = None
        self._writer: csv.writer | None = None
        self._fh = None
        self._row_count = 0
        self._spider_name: str | None = None

    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls(folder_id=crawler.settings.get("GDRIVE_CSV_FOLDER_ID", ""))
        # Use signals so we run *after* RunIdExtension has set
        # spider.run_id (same reasoning as Postgres + Sheets pipelines).
        crawler.signals.connect(pipe.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(pipe.spider_closed, signal=signals.spider_closed)
        return pipe

    # ------------------------------------------------------------ open

    def spider_opened(self, spider) -> None:
        self._spider_name = spider.name
        run_id = getattr(spider, "run_id", None)
        if run_id is None:
            # Defensive — should never happen if RunIdExtension is registered.
            logger.warning("spider has no run_id; falling back to timestamp")
            run_id = datetime.now(UTC).strftime(TIMESTAMP_FMT)

        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._csv_path = self._out_dir / f"{spider.name}_{run_id}.csv"

        self._fh = self._csv_path.open("w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(sheets_repo.template_header_row())
        self._fh.flush()
        logger.info(
            "gdrive_csv pipeline ready",
            extra={"path": str(self._csv_path), "spider": spider.name},
        )

    # ------------------------------------------------------------ items

    def process_item(self, item: dict, spider) -> dict:
        if self._writer is None:
            # Defensive: open signal didn't fire — initialize lazily.
            self.spider_opened(spider)
        row = sheets_repo.to_row(item)
        self._writer.writerow(row)
        self._row_count += 1
        # Flush per item is cheap (CSV is buffered by the OS) and means
        # a crash mid-run still leaves a partial file we can recover.
        self._fh.flush()
        return item

    # ------------------------------------------------------------ close

    def spider_closed(self, spider, reason: str) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

        if self._csv_path is None or not self._csv_path.exists():
            logger.warning("gdrive_csv: no CSV to upload (open never fired)")
            return

        if self._row_count == 0:
            logger.info(
                "gdrive_csv: skipping upload (zero rows)",
                extra={"path": str(self._csv_path)},
            )
            spider.crawler.stats.set_value("gdrive_csv/upload_status", "skipped")
            return

        try:
            file_id = self._upload(spider)
            spider.crawler.stats.set_value("gdrive_csv/upload_status", "ok")
            spider.crawler.stats.set_value("gdrive_csv/file_id", file_id)
            spider.crawler.stats.inc_value("gdrive_csv/rows_uploaded", self._row_count)
        except Exception:
            # Like the Sheets pipeline: log + swallow + flag. Postgres
            # has the data, the local CSV is preserved for replay.
            logger.exception(
                "gdrive_csv upload failed",
                extra={"path": str(self._csv_path)},
            )
            spider.crawler.stats.set_value("gdrive_csv/upload_status", "failed")

    def _upload(self, spider) -> str:
        if not self._folder_id:
            raise RuntimeError(
                "GDRIVE_CSV_FOLDER_ID is unset — see .env.example for setup"
            )
        folder_id = self._folder_id

        timestamp = datetime.now(UTC).strftime(TIMESTAMP_FMT)
        upload_name = f"{self._spider_name}_{timestamp}.csv"
        size = self._csv_path.stat().st_size
        resumable = size > RESUMABLE_THRESHOLD_BYTES

        drive = gauth.get_drive_client()
        media = MediaFileUpload(
            str(self._csv_path),
            mimetype="text/csv",
            resumable=resumable,
        )
        result = drive.files().create(
            body={"name": upload_name, "parents": [folder_id]},
            media_body=media,
            fields="id",
        ).execute()
        file_id = result["id"]
        logger.info(
            "gdrive_csv uploaded",
            extra={
                "file_id": file_id,
                # 'name' would collide with LogRecord.name (the logger
                # name) → KeyError. Renamed to 'file_name'.
                "file_name": upload_name,
                "rows": self._row_count,
                "bytes": size,
                "resumable": resumable,
            },
        )
        return file_id
