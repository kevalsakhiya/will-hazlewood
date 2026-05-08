"""Structured logging with a dev-friendly pretty mode.

`LOG_FORMAT=json` (default) → one JSON object per line, machine-grep
friendly. Used in production / log aggregators / CI.

`LOG_FORMAT=pretty` → one human-readable line per record with ANSI
colour-coded levels and trailing `key=value` pairs for any structured
fields. Used in dev when you're tailing the terminal.

Both modes go through `RunContextJsonFormatter` for the JSON path and
`PrettyConsoleFormatter` for the pretty path. They share the same
contextvar-based run_id / scrape_date / spider injection, so switching
formats never changes which fields are surfaced.
"""

from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger

from broker_scout.common.run_context import get_run_context

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
    so test setups can reconfigure without leaking handlers."""

    fmt = (log_format or "json").lower()
    if fmt not in {"json", "pretty"}:
        fmt = "json"

    root = logging.getLogger()
    root.setLevel(level)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    root.addHandler(_build_handler(fmt))
