"""Pydantic-backed item validation pipeline (priority 200).

Drops items that don't satisfy `PropertyFinderBrokerSchema`, buffers the
failures into `spider.bad_items` for the Phase 3 Postgres pipeline to
drain, and emits stats counters consumed by the Phase 9 Spidermon
monitors.
"""

from __future__ import annotations

import dataclasses
import logging

from pydantic import ValidationError
from scrapy.exceptions import DropItem

from broker_scout.schemas import PropertyFinderBrokerSchema

logger = logging.getLogger(__name__)


def _to_payload(item) -> dict:
    """Coerce spider output (dataclass or dict-like) to a plain dict."""
    if dataclasses.is_dataclass(item) and not isinstance(item, type):
        return dataclasses.asdict(item)
    return dict(item)


def _format_error(err: dict) -> str:
    loc = ".".join(str(p) for p in err.get("loc", ())) or "__model__"
    return f"{loc}: {err.get('msg', '')}"


class ValidationPipeline:
    """Run each item through `PropertyFinderBrokerSchema.model_validate`.

    Success: pass a normalized dict (dates as ISO strings) downstream.
    Failure: log, increment counters, append to `spider.bad_items`,
    raise `DropItem`.
    """

    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(stats=crawler.stats)

    def process_item(self, item, spider) -> dict:
        payload = _to_payload(item)
        try:
            model = PropertyFinderBrokerSchema.model_validate(payload)
        except ValidationError as exc:
            self._record_failure(payload, exc, spider)
            raise DropItem("validation_failed") from exc

        self.stats.inc_value("validation/passed_total")
        return model.model_dump(mode="json")

    def _record_failure(
        self, payload: dict, exc: ValidationError, spider
    ) -> None:
        errors = exc.errors()
        self.stats.inc_value("validation/failed_total")

        # bucket per field (dedupe within one item so multi-error fields don't double-count)
        seen: set[str] = set()
        for err in errors:
            loc = err.get("loc") or ()
            field = str(loc[0]) if loc else "__model__"
            if field in seen:
                continue
            seen.add(field)
            self.stats.inc_value(f"validation/failed_field/{field}")

        run_id = getattr(spider, "run_id", None)
        logger.warning(
            "item failed validation",
            extra={
                "run_id": run_id,
                "platform": payload.get("platform"),
                "brn": payload.get("brn"),
                "agent_url": payload.get("agent_url"),
                "errors": [
                    {
                        "loc": list(err.get("loc", ())),
                        "msg": err.get("msg"),
                        "type": err.get("type"),
                    }
                    for err in errors
                ],
            },
        )

        # Phase 3's Postgres pipeline drains this on close_spider
        bad_items = getattr(spider, "bad_items", None)
        if bad_items is None:  # defensive: extension should have set it
            spider.bad_items = bad_items = []
        bad_items.append(
            {
                "run_id": run_id,
                "platform": payload.get("platform", "propertyfinder"),
                "reason": "; ".join(_format_error(e) for e in errors[:3]),
                "payload": {"item": payload, "errors": errors},
            }
        )
