"""Smoke tests — replaced with real schema/pipeline tests in Phase 2.4."""

from __future__ import annotations


def test_package_imports() -> None:
    import broker_scout
    from broker_scout import pipelines

    assert broker_scout is not None
    assert pipelines.__doc__ is not None
    assert "dict" in pipelines.__doc__


def test_settings_has_item_pipelines() -> None:
    from broker_scout import settings

    assert hasattr(settings, "ITEM_PIPELINES")
    assert isinstance(settings.ITEM_PIPELINES, dict)
