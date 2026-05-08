"""Structured logging with a dev-friendly pretty mode and per-run files.

Two output sinks share the same root logger:

  * Stream (stderr) — terminal output. Honours `LOG_FORMAT=json|pretty`.
  * File (optional) — `logs/{spider}_{run_id}.log`. Always JSON. Attached
    by `RunIdExtension.spider_opened` once the run_id exists; detached
    on `spider_closed`. Mirrors the per-run archive pattern of
    `out/*.csv` (RULES.md §14.5).

Old log files are pruned automatically at the start of each run via
`prune_old_log_files(...)`; retention defaults to 30 days. Set
`LOG_RETENTION_DAYS=0` to disable, or `LOG_FILE_DIR=` to disable file
logging entirely.

Both formatters share the contextvar-based run_id / scrape_date /
spider injection, so switching formats never changes which fields
appear — only how they're rendered.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from pythonjsonlogger import jsonlogger

from broker_scout.common.run_context import get_run_context

logger = logging.getLogger(__name__)

# Module-level handle on the active per-run FileHandler. We need a ref
# so detach can find and close the same handler we attached. One run
# per process in deployment, so module-level is fine; tests reset via
# detach() in autouse fixtures.
_run_file_handler: logging.FileHandler | None = None

# Fields the pretty formatter promotes to the prefix or hides from the
# trailing kv blob. `message` is rendered as the main text; `level`
# / `logger` go in the prefix; `ts` is the leading timestamp.
_PRETTY_RESERVED = {"ts", "asctime", "level", "levelname", "logger", "name", "message"}

_LEVEL_COLOURS = {
    "DEBUG": "\033[37m",     # grey
    "INFO": "\033[36m",      # cyan
    "WARNING": "\033[33m",   # yellow
    "ERROR": "\033[31m",     # red
    "CRITICAL": "\033[1;31m",  # bold red
}
_RESET = "\033[0m"


class RunContextJsonFormatter(jsonlogger.JsonFormatter):
    """JsonFormatter that adds run_id / scrape_date / spider from the contextvar."""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["level"] = record.levelname
        log_record["logger"] = record.name

        # Scrapy core passes the spider OBJECT in extras for some log
        # lines ("Spider opened", "Spider closed", ...). Coerce to the
        # name so logs stay JSON-friendly. Cheap duck-type check: a
        # Spider has a `.name` string attribute.
        spider_val = log_record.get("spider")
        if (
            spider_val is not None
            and not isinstance(spider_val, str)
            and isinstance(getattr(spider_val, "name", None), str)
        ):
            log_record["spider"] = spider_val.name

        ctx = get_run_context()
        if ctx is not None:
            log_record.setdefault("run_id", ctx.run_id)
            log_record.setdefault("scrape_date", ctx.scrape_date)
            log_record.setdefault("spider", ctx.spider_label)


class PrettyConsoleFormatter(logging.Formatter):
    """Human-readable single-line formatter for dev tailing.

    Format:
        HH:MM:SS LEVEL logger.name  message  k=v k=v ...

    Colours are ANSI; opt out by passing `colour=False`. Structured
    `extra=` fields are rendered as trailing `k=v` pairs; the run
    contextvar contributes run_id / spider / scrape_date the same way
    the JSON formatter does.
    """

    def __init__(self, colour: bool = True) -> None:
        super().__init__(fmt="%(asctime)s %(message)s", datefmt="%H:%M:%S")
        self._colour = colour

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        level = record.levelname
        if self._colour:
            level_str = f"{_LEVEL_COLOURS.get(level, '')}{level:<8}{_RESET}"
        else:
            level_str = f"{level:<8}"

        # Pull structured fields off the record. LogRecord has a fixed
        # set of stdlib attributes; anything else is from `extra=` or
        # injected below.
        extras: dict[str, object] = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _STDLIB_LOGRECORD_ATTRS and k not in _PRETTY_RESERVED
        }

        # Coerce a Spider object → name (mirrors the JSON formatter so
        # behaviour is identical regardless of format).
        spider_val = extras.get("spider")
        if (
            spider_val is not None
            and not isinstance(spider_val, str)
            and isinstance(getattr(spider_val, "name", None), str)
        ):
            extras["spider"] = spider_val.name

        ctx = get_run_context()
        if ctx is not None:
            extras.setdefault("run_id", ctx.run_id)
            extras.setdefault("scrape_date", ctx.scrape_date)
            extras.setdefault("spider", ctx.spider_label)

        kv = "  ".join(f"{k}={_render(v)}" for k, v in extras.items() if v is not None)
        line = f"{ts} {level_str} {record.name}  {record.getMessage()}"
        if kv:
            line = f"{line}  {kv}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


# Stdlib `LogRecord` attribute names — anything else on `record.__dict__`
# came from `extra=` or our own injection. Hardcoded because the public
# API doesn't expose this list and `vars(LogRecord)` includes a lot of
# class-level junk.
_STDLIB_LOGRECORD_ATTRS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message", "module",
        "msecs", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


def _render(value: object) -> str:
    """Compact repr for kv pairs — short strings unquoted, everything
    else through repr() to keep the line single-line."""
    if isinstance(value, str) and " " not in value and "\n" not in value:
        return value
    return repr(value)


def _build_handler(log_format: str) -> logging.Handler:
    handler = logging.StreamHandler()
    if log_format == "pretty":
        # Auto-disable colour when not attached to a TTY (file redirects,
        # CI, log aggregators) — colour codes leak as garbage otherwise.
        colour = sys.stderr.isatty()
        handler.setFormatter(PrettyConsoleFormatter(colour=colour))
    else:
        handler.setFormatter(
            RunContextJsonFormatter(
                "%(asctime)s %(message)s",
                rename_fields={"asctime": "ts"},
            )
        )
    return handler


def configure_logging(level: str = "INFO", log_format: str = "json") -> None:
    """Replace the root logger handlers with a single formatted stream
    handler. Idempotent — re-running swaps the active handler in place
    so test setups can reconfigure without leaking handlers.

    File output is NOT attached here — `run_id` doesn't exist yet at
    settings.py-import time. `RunIdExtension.spider_opened` calls
    `attach_run_file_handler(...)` once it's available.
    """

    fmt = (log_format or "json").lower()
    if fmt not in {"json", "pretty"}:
        fmt = "json"

    root = logging.getLogger()
    root.setLevel(level)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    root.addHandler(_build_handler(fmt))


# ---------------------------------------------------------- per-run file handler


def attach_run_file_handler(
    run_id: str, spider_name: str, log_dir: str = "logs"
) -> Path | None:
    """Attach a JSON FileHandler at `{log_dir}/{spider_name}_{run_id}.log`.

    Returns the path written to, or `None` if file logging is disabled
    (empty `log_dir`). If a previous run handler is still attached
    (rare — multiple spiders in one process) it's detached first so the
    older file gets a clean close.

    File output is always JSON regardless of the terminal `LOG_FORMAT`,
    so files stay grep- / aggregator-friendly even when the operator
    is tailing pretty output.
    """
    global _run_file_handler

    if not log_dir:
        return None

    if _run_file_handler is not None:
        # Stale ref from an earlier run in the same process — close
        # cleanly before swapping.
        detach_run_file_handler()

    log_path = Path(log_dir) / f"{spider_name}_{run_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(
        RunContextJsonFormatter(
            "%(asctime)s %(message)s",
            rename_fields={"asctime": "ts"},
        )
    )
    logging.getLogger().addHandler(handler)
    _run_file_handler = handler
    logger.info(
        "run log file attached", extra={"path": str(log_path), "run_id": run_id}
    )
    return log_path


def detach_run_file_handler() -> None:
    """Remove the active per-run FileHandler from the root logger and
    close it. Safe to call when nothing is attached. Called from
    `RunIdExtension.spider_closed` AFTER its final log line so the
    'run finished' record still lands in the file."""
    global _run_file_handler
    if _run_file_handler is None:
        return
    handler = _run_file_handler
    _run_file_handler = None
    logging.getLogger().removeHandler(handler)
    handler.close()


# ------------------------------------------------------------------- pruning


def prune_old_log_files(log_dir: str, retention_days: int) -> int:
    """Delete `*.log` files in `log_dir` whose mtime is older than
    `retention_days`. Returns the count deleted.

    No-op when `log_dir` is empty, the directory doesn't exist, or
    `retention_days <= 0` (operator opt-out). Per-file errors are
    logged at WARNING and don't abort the prune — a single permission
    issue shouldn't keep the rest of the directory full forever.
    """
    if not log_dir or retention_days <= 0:
        return 0
    path = Path(log_dir)
    if not path.is_dir():
        return 0

    cutoff = time.time() - retention_days * 86400
    deleted = 0
    for entry in path.glob("*.log"):
        try:
            if entry.stat().st_mtime >= cutoff:
                continue
            entry.unlink()
            deleted += 1
        except FileNotFoundError:
            # Concurrent delete (another process) — fine, treat as done.
            continue
        except OSError as exc:
            logger.warning(
                "prune_old_log_files: failed to delete file",
                extra={"path": str(entry), "error": str(exc)},
            )

    if deleted:
        logger.info(
            "pruned old log files",
            extra={"log_dir": log_dir, "deleted": deleted, "retention_days": retention_days},
        )
    return deleted
