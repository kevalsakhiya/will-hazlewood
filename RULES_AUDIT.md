# RULES.md compliance audit

Audit date: 2026-05-08
Scope: `broker_scout/broker_scout/**/*.py`, `sql/migrations/*.sql`, `.env.example`
Reference: [RULES.md](RULES.md)

**Status: all findings resolved.** 376 tests passing.

## Resolution summary

| # | Rule | Files | Resolution |
|---|---|---|---|
| 1 | §7.1 — env var read outside allow-list | `pipelines/gdrive_csv.py`, `common/db.py`, `common/sheets_repo.py` | `gdrive_csv` now reads `GDRIVE_CSV_FOLDER_ID` from `crawler.settings` (added to [settings.py](broker_scout/broker_scout/settings.py)); `db.py` and `sheets_repo.py` documented in [RULES.md §7.1](RULES.md) as cross-cutting CLI-callable plumbing (same exemption category as `gauth.py` / `notifiers.py`). |
| 2 | §9.2 — `spider.logger` outside spider class | `extensions.py`, `middlewares.py` | `extensions.py` now uses `logger = logging.getLogger(__name__)`; `middlewares.py` deleted (was unused boilerplate). |
| 3 | §1 / §17.5 / §23 — dead boilerplate | `middlewares.py` | Deleted. Closed findings 3, 4 (partial), 10, 11 in one move. |
| 4 | §17.1 — restating-the-line comments | `middlewares.py`, `spiders/__init__.py` | `middlewares.py` deleted; `spiders/__init__.py` emptied. |
| 5 | §17.3 — legacy `Optional[...]` outside Pydantic | `items.py`, `common/matching.py` | All converted to `X \| None`; `from typing import Optional` removed from both. |
| 6 | §17.3 — missing return types | `spiders/base.py:95`, `spiders/agent_spider.py:79` | Both `parse_search_results` now annotated `-> Iterable[Request \| dict]`. |
| 7 | §4.3 — `.clear()` instead of rebinding | `pipelines/postgres.py:183`, `common/dld_repo.py:80` | Both rebind via `= []`. The `test_spider_closed_drains_bad_items` test was relying on the exact mutation behaviour the rule warns against — fixed to capture the list reference before the call. |
| 8 | §1 — non-empty `__init__.py` | `spiders/__init__.py` | Emptied. |
| 9 | §17.2 — missing module docstrings | `items.py`, `spiders/agent_spider.py` | Both have docstrings stating role + key design choices. |
| 10 | §23 — bare `except: pass` | `middlewares.py:44,97` | Closed by deleting the file. |
| 11 | §9.1 — `%`-formatted log messages | `middlewares.py:53,100` | Closed by deleting the file. |

## Files changed

- **Deleted**: [`broker_scout/broker_scout/middlewares.py`](broker_scout/broker_scout/middlewares.py) — unused Scrapy template.
- **Emptied**: [`broker_scout/broker_scout/spiders/__init__.py`](broker_scout/broker_scout/spiders/__init__.py).
- **Modified**:
  - [`extensions.py`](broker_scout/broker_scout/extensions.py) — module logger.
  - [`pipelines/postgres.py`](broker_scout/broker_scout/pipelines/postgres.py) — rebind buffer.
  - [`pipelines/gdrive_csv.py`](broker_scout/broker_scout/pipelines/gdrive_csv.py) — `folder_id` via constructor / `crawler.settings`.
  - [`common/dld_repo.py`](broker_scout/broker_scout/common/dld_repo.py) — rebind batch.
  - [`common/matching.py`](broker_scout/broker_scout/common/matching.py) — modern typing.
  - [`items.py`](broker_scout/broker_scout/items.py) — module docstring + modern typing.
  - [`spiders/base.py`](broker_scout/broker_scout/spiders/base.py) — `Iterable` from `collections.abc`, return annotation.
  - [`spiders/agent_spider.py`](broker_scout/broker_scout/spiders/agent_spider.py) — module docstring, `from __future__ import annotations`, `collections.abc`, return annotation.
  - [`settings.py`](broker_scout/broker_scout/settings.py) — added `GDRIVE_CSV_FOLDER_ID`.
  - [`tests/test_postgres_pipeline.py`](tests/test_postgres_pipeline.py) — capture list pre-call (decoupled from rebind).
  - [`tests/test_gdrive_csv_pipeline.py`](tests/test_gdrive_csv_pipeline.py) — `env` fixture injects `_folder_id` directly.
  - [`RULES.md`](RULES.md) — §7.1 expanded allow-list with `db.py` and `sheets_repo.py` justifications.

## Verification

- `poetry run pytest tests/ -q` → **376 passed**.
- `grep -rn "os.getenv\|os.environ" broker_scout/broker_scout --include="*.py"` → only allow-listed modules (settings, notifiers, gauth, db, sheets_repo, oauth_setup tool).
- `grep -rn "spider\.logger\|\.clear()" broker_scout/broker_scout --include="*.py"` → no matches.
- `grep -rn "from typing import.*Optional" broker_scout/broker_scout --include="*.py"` → only `schemas.py` (Pydantic Field exemption per §17.3).
