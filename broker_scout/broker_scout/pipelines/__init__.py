"""Item pipelines for broker_scout.

Contract — items in flight:

Pipelines exchange items as plain ``dict``s starting at priority 200
(``ValidationPipeline``). The first pipeline to receive a
``PropertyFinderBrokerItem`` dataclass converts it via
``dataclasses.asdict`` (or ``model.model_dump()`` after pydantic
validation). Every later pipeline — Postgres, Sheets, Drive — must
accept ``dict`` input, not the dataclass.

Bad-items buffer:

Validation failures are appended to ``spider.bad_items`` (initialized
by ``RunIdExtension`` on ``spider_opened``). Each entry is a dict::

    {"run_id": str, "platform": str, "reason": str, "payload": dict}

The Phase 3 Postgres pipeline drains this buffer into the ``bad_items``
table on ``close_spider``.
"""
