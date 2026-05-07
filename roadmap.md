# Roadmap — DLD × PropertyFinder × Bayut

Step-by-step build plan. Pipelines before monitors; monitors layer on top once history exists. See `plan.md` for architecture.

Phase numbering is execution order. Each phase has discrete checkpoints — finish a phase before starting the next so we always have a working scraper at HEAD.

---

## Phase 0 — Foundations *(prerequisite for everything)*

- [x] Repo layout matches `plan.md` §3 (project package: `broker_scout`)
- [x] `pyproject.toml` with pinned deps (`scrapy`, `httpx`, `psycopg[binary,pool]`, `pydantic>=2`, `gspread`/`google-api-python-client`, `tenacity`, `python-json-logger`, `spidermon`)
- [x] `.env.example` with `RUN_ENV`, `POSTGRES_*`, `GSHEET_PF_ID`, `GSHEET_BAYUT_ID`, `GDRIVE_FOLDER_ID`, `GOOGLE_CHAT_WEBHOOK_URL`, `SERVICE_ACCOUNT_JSON_PATH`, `PROXY_URL`
- [x] `docker-compose.yml` with Postgres for local dev
- [x] `utils/logging_setup.py` — JSON logs, `run_id` injected
- [x] `extensions.py` — generates `run_id = uuid4()` on spider open, propagates via `spider.run_id`, `crawler.stats`, and a `RunContext` contextvar (Scrapy settings freeze post-init, so we don't mutate them)
- [x] `common/run_context.py` — single source for `run_id`, `scrape_date` (UTC), `spider_label`

---

## Phase 1 — DLD client + `dld_brokers` table *(unblocks both spiders)*

- [x] `sql/migrations/001_dld_brokers.sql` — table schema with `brn` PK, parsed DLD fields, `first_seen_at`, `last_seen_at`, `last_seen_run_id`
- [x] `common/db.py` — `psycopg_pool.ConnectionPool` from env (reused by Phase 3 pipeline)
- [x] `tools/migrate.py` — minimal forward-only migration runner; tracks applied files in `_migrations`
- [x] `common/dld_models.py` — `DLDBroker` dataclass + `from_api()` mapping
- [x] `common/normalizers.py` — phone E.164, date, email, str helpers
- [x] `common/dld_client.py` — `fetch_all()` with gzip/br + tenacity retry; `write_snapshot()` to `dld_snapshots/{run_id}.jsonl`
- [x] `common/dld_repo.py` — `upsert_brokers(records, run_id)` with `ON CONFLICT (brn) DO UPDATE`; preserves `first_seen_at`
- [x] `tools/fetch_dld.py` — CLI: generate `run_id`, set `RunContext`, fetch, write JSONL, upsert to Postgres, log summary

Run weekly via cron: `python -m broker_scout.tools.fetch_dld` (separate from any spider).

---

## Phase 2 — Validation layer *(no DB write yet)*

Goal: catch bad rows at the boundary, isolate them, keep good rows flowing.

### 2.1 Plumbing prerequisites

- [x] Convert `broker_scout/pipelines.py` (single-file placeholder) into a `pipelines/` package with `__init__.py`. Delete the no-op `PropertyfinderPipeline`.
- [x] Add `ITEM_PIPELINES = {}` dict to `settings.py` (currently absent) — slots get filled phase by phase.
- [x] Lock the contract: `ValidationPipeline.process_item` returns a **dict**, not the dataclass. All later pipelines (Phase 3+) consume dicts. Document this in `pipelines/__init__.py`.
- [x] Define the bad-items buffer shape now so Phase 3 can drain without refactor: `spider.bad_items: list[dict]` where each entry is `{run_id, platform, reason, payload}`. Initialized in `RunIdExtension.spider_opened`.
- [x] ~~Add `pytest` to dev deps in `pyproject.toml`~~ (already pinned) and create `tests/` dir at repo root.

### 2.2 `schemas.py` — pydantic model `PropertyFinderBrokerSchema`

**Identity / provenance**
- [x] All fields `Optional[...]` by default unless noted
- [x] `platform`: `Literal["propertyfinder"]`
- [x] `scrape_date`: ISO date string, parses as date, within ±1 day of today UTC (avoids midnight-crossing flakes)
- [x] `agent_url`: starts with `https://www.propertyfinder.ae/`
- [x] `broker_name`: non-empty string ≤ 200 chars
- [x] `brn`: non-empty string when present — **no regex** (DLD is source of truth)
- [x] `nationality`: string ≤ 100 chars
- [x] `agent_specialization`: string ≤ 100 chars
- [x] `experience_since`: 1980 ≤ year ≤ `date.today().year` *(computed at validation time, not import)*
- [x] `whatsapp_response_time`: 0 ≤ x ≤ 86_400, or null
- [x] `is_superagent`: strict `bool` or null

**Agency**
- [x] `agency_url`: starts with `https://www.propertyfinder.ae/` when present
- [x] `agency_registration_number`: non-empty string ≤ 100 chars when present

**Listing counts**
- [x] `listings_for_sale`, `listings_for_rent`: 0 ≤ x ≤ 5000
- [x] `listings_total` equals `(sale or 0) + (rent or 0)` — null only if both inputs null
- [x] `listings_with_marketing_spend`: 0 ≤ x ≤ `listings_total` (cross-field)

**Listing prices / ages**
- [x] `average_listing_price_sale`, `average_listing_price_rent`: 0 ≤ x ≤ 10⁹
- [x] `average_listing_age_days_sale`, `average_listing_age_days_rent`: 0 ≤ x ≤ 36_500
- [x] `most_recent_listing_date_sale`, `most_recent_listing_date_rent`: parse as date, ≥ 2000-01-01, ≤ today UTC

**Closed transactions / deals**
- [x] `closed_transaction_sale`, `closed_transaction_rent`: ≥ 0
- [x] `closed_deals_total` equals `(sale or 0) + (rent or 0)` — null only if both inputs null
- [x] `closed_transaction_deal_value`: 0 ≤ x ≤ 10⁹
- [x] `closed_transaction_sale_total_amount`, `closed_transaction_rent_total_amount`: 0 ≤ x ≤ 10⁹
- [x] `closed_transaction_sale_avg_amount`, `closed_transaction_rent_avg_amount`: 0 ≤ x ≤ 10⁹
- [x] `most_recent_deal_date_sale`, `most_recent_deal_date_rent`: parse as date, ≥ 2000-01-01, ≤ today UTC
- [x] `average_monthly_deal_volume_sale`, `average_monthly_deal_volume_rent`: ≥ 0

### 2.3 `pipelines/validation.py`

- [x] Run `PropertyFinderBrokerSchema.model_validate(asdict(item))`
- [x] On success: pass normalized dict (`model.model_dump(mode="json")`) downstream
- [x] On `ValidationError`:
  - [x] Drop the item (`raise DropItem`)
  - [x] Log structured error with `run_id`, `brn`, `agent_url`, `errors=[{loc, msg, type}, ...]`
  - [x] Increment `validation/failed_total`
  - [x] Increment `validation/failed_field/{field}` for each failing field
  - [x] Append to `spider.bad_items` buffer with `{run_id, platform, reason, payload}` (Phase 3 drains this)
- [x] On success: increment `validation/passed_total` (denominator for the Phase 9.1 failure-rate monitor)
- [x] Wire `ValidationPipeline` at priority `200` in `ITEM_PIPELINES`

### 2.4 Tests

- [x] `tests/test_schemas.py` — table-driven tests covering:
  - [x] Happy path: a fully-populated valid item passes
  - [x] Each field-level rule rejects its bad input and accepts a valid one
  - [x] Cross-field rules (`listings_total`, `closed_deals_total`, `listings_with_marketing_spend`) reject mismatches
  - [x] All-null item passes (everything is `Optional`)
- [x] `tests/test_validation_pipeline.py`
  - [x] Valid item → returned as dict, `validation/passed_total` incremented
  - [x] Invalid item → `DropItem` raised, `failed_total` + `failed_field/*` incremented, buffer appended

---

## Phase 3 — Postgres pipeline *(authoritative store)*

- [x] `sql/migrations/002_brokers.sql` *(was `001_init.sql` in this doc; renumbered because Phase 1 already shipped `001_dld_brokers.sql`)*
  - [x] `scrape_runs` (run_id, spider, started_at, finished_at, status, items_scraped, items_dropped, stats JSONB)
  - [x] `brokers` (run_id, scrape_date, platform, brn, match_status, match_confidence, all item fields, raw JSONB; UNIQUE on (run_id, platform, brn))
  - [x] `bad_items` (run_id, platform, reason, payload JSONB, created_at)
  - [x] `alert_log` (run_id, level, title, body, sent_at)
  - [x] Indexes on `brokers(brn)`, `brokers(scrape_date)`, `bad_items(run_id)`
- [x] `common/brokers_repo.py` — `open_run` / `insert_brokers` / `insert_bad_items` / `close_run` (mirrors `dld_repo.py` pattern)
- [x] `pipelines/postgres.py`
  - [x] Connection pool from env (via `common/db.py`)
  - [x] `open_spider`: insert row in `scrape_runs` with status `running`
  - [x] Per-item: buffer in memory
  - [x] Every 500 items + on `spider_closed`: bulk insert via `executemany` with `ON CONFLICT (run_id, platform, brn) DO NOTHING`
  - [x] Flush `bad_items` buffer too
  - [x] `spider_closed`: update `scrape_runs` status `ok`/`failed`, write final `items_scraped`, `items_dropped`, full Scrapy stats blob (datetime-coerced) into `stats` JSONB
- [x] ~~`pipelines/stats_writer.py`~~ — folded into `PostgresPipeline.spider_closed`; a separate extension would write the same blob to the same row.
- [x] Wire `PostgresPipeline` at priority `400`

(`tools/migrate.py` and `common/db.py` already shipped in Phase 1; reused here.)

---

## Phase 4 — Google Sheets pipeline *(monthly auto-rotation per platform)*

**Design baseline (locked):** the 10M cells/spreadsheet limit is the binding constraint. At ~30k rows × ~45 cols = 1.35M cells/run, one spreadsheet holds ~7 weekly runs. We rotate to a **new spreadsheet file every month** (~4 runs/month ≈ 5.4M cells, ~54% utilization, leaves headroom for column drift). A new tab inside the same file does **not** help — the 10M cap is per file, not per tab.

Rotation is fully automated end-to-end via the Sheets + Drive APIs (Trigger A: pipeline-driven on `spider_opened`, no separate cron). The operator's only manual step is the **one-time bootstrap**: create one template spreadsheet per platform (with header row pre-populated), share it + a Drive folder with the service account email, set `.env` vars.

### 4.1 Plumbing prerequisites

- [x] `sql/migrations/003_sheet_registry.sql` — new table:
  ```sql
  CREATE TABLE sheet_registry (
      id          BIGSERIAL PRIMARY KEY,
      platform    TEXT NOT NULL,                   -- 'propertyfinder' | 'bayut'
      period      TEXT NOT NULL,                   -- 'YYYY-MM', e.g. '2026-05'
      sheet_id    TEXT NOT NULL,                   -- Google Sheets file id
      is_active   BOOLEAN NOT NULL DEFAULT TRUE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (platform, period)
  );
  CREATE INDEX ON sheet_registry (platform, is_active);
  ```
- [x] `.env.example` — add:
  - `GSHEET_TEMPLATE_PF_ID`   — template spreadsheet for PF (replaces old `GSHEET_PF_ID`)
  - `GSHEET_TEMPLATE_BAYUT_ID` — template spreadsheet for Bayut
  - `GSHEET_PF_FOLDER_ID`     — Drive folder where rotated PF spreadsheets land
  - `GSHEET_BAYUT_FOLDER_ID`  — same for Bayut
  - `GSHEET_VIEWER_EMAILS`    — comma-separated emails to auto-share each new file with
- [x] `utils/gauth.py` — service account loader. Loads JSON from `SERVICE_ACCOUNT_JSON_PATH`, returns Sheets + Drive clients with scopes `spreadsheets` + `drive.file`. Single source of credentials (Phase 5 reuses it).

### 4.2 `common/sheets_repo.py`

Mirrors the column-list / SQL-template pattern from `brokers_repo.py`. Public functions:

- [x] `get_or_create_active_sheet(platform: str) -> str` — returns the active spreadsheet ID for the current period (`datetime.now(UTC).strftime("%Y-%m")`).
  - [x] Look up `(platform, period, is_active=TRUE)` in `sheet_registry`. If found: return.
  - [x] If missing: call `drive.files.copy(template_id, body={name: "PropertyFinder Brokers — 2026-05", parents: [folder_id]})`.
  - [x] Share the new file: `drive.permissions.create` for each address in `GSHEET_VIEWER_EMAILS` (role=`reader`, type=`user`).
  - [x] Insert new registry row (handles concurrent-creation race via `ON CONFLICT DO NOTHING` + re-SELECT), mark prior periods `is_active = FALSE` for this platform.
  - [x] Return new sheet ID.
- [x] `append_rows(sheet_id: str, rows: list[list]) -> int` — wrapped in `tenacity` retry (5 attempts, exponential backoff up to 60s, only on 5xx/429). Calls `spreadsheets.values.append` with `valueInputOption=RAW`, `insertDataOption=INSERT_ROWS`, `includeValuesInResponse=False`. Returns rows-sent count on success.
- [x] `pre_flight_capacity_check(sheet_id: str, expected_run_cells: int) -> None` — calls `spreadsheets.get` with `fields=sheets/properties/gridProperties` once per run. Raises `SheetsCapacityError` if `expected_run_cells > remaining * 0.9` (10% safety margin). Operator action then: investigate column drift, manually rotate, or shorten rotation cadence.

Constants: `_SHEET_COLUMNS` tuple drives column order. **Excludes** the `raw` JSONB column (Sheets gets the flat view; Postgres keeps the blob). Period format `%Y-%m` is centralized as a constant.

### 4.3 `pipelines/gsheets.py` — `GSheetsBatchPipeline` (priority 500)

Lifecycle mirrors `PostgresPipeline`: signal-based open/close, in-memory buffer, rebind-on-success flush.

- [x] `from_crawler` — subscribe `spider_opened` + `spider_closed` (not auto-wired methods, same Phase 3 reasoning).
- [x] `spider_opened`:
  - [x] Read `spider.platform` (defaults to `"propertyfinder"` until Phase 6 sets it on the base spider).
  - [x] `self._sheet_id = sheets_repo.get_or_create_active_sheet(platform)` — auto-creates and shares the monthly file if missing.
  - [x] Set `crawler.stats["gsheets/sheet_id"]` (Phase 11 alert links use this).
- [x] `process_item`:
  - [x] Convert dict → flat row via `sheets_repo.to_row(item)` (column order = `_SHEET_COLUMNS`).
  - [x] `self._buffer.append(row)`. Flush at `len >= 2000`.
- [x] `_flush(spider)`:
  - [x] On first call of run: `pre_flight_capacity_check(sheet_id, projected_run_cells)`.
  - [x] `rows = self._buffer; self._buffer = []; append_rows(...)` — rebind before handoff; on failure restore the buffer so retry covers it.
  - [x] `crawler.stats.inc_value("gsheets/rows_appended", len(rows))`.
- [x] `spider_closed`:
  - [x] Final `_flush()` to drain the < 2000 tail.
  - [x] On flush failure: log, swallow, set `gsheets/flush_failed = 1`. Do **not** re-raise (Postgres + Drive CSV must finish their close handlers; Phase 12 `tools/replay_run.py` is the recovery path).

Stats emitted (consumed by Phase 9 `PipelineFailureMonitor`):
- `gsheets/sheet_id` — string, set on first sheet open.
- `gsheets/rows_appended` — int, must equal `item_scraped_count` for a healthy run.
- `gsheets/flush_failed` — 0 or 1.

### 4.4 Tests

- [x] `tests/test_sheets_repo.py` — registry + creation flows mocked at the Drive/Sheets client level:
  - [x] `get_or_create_active_sheet` returns existing row when `(platform, period, is_active=TRUE)` exists.
  - [x] When missing: calls `drive.files.copy`, then `drive.permissions.create` per viewer, then registers, then deactivates prior periods.
  - [x] Concurrent-creation race: orphans the Drive copy and falls back to the winner's sheet ID.
  - [x] `append_rows` retries 5xx/429 transient errors, gives up after 5 attempts; does not retry 4xx.
  - [x] `pre_flight_capacity_check` raises when projected usage exceeds 90% of remaining cells; aggregates across tabs.
- [x] `tests/test_gsheets_pipeline.py` — lifecycle (`sheets_repo` mocked at the pipeline's import path):
  - [x] `spider_opened` resolves and caches `sheet_id`; idempotent on repeat call.
  - [x] `process_item` buffers below threshold, flushes at threshold.
  - [x] Pre-flight capacity check runs only on first flush; failure propagates loudly.
  - [x] On flush success: buffer rebound to empty.
  - [x] On flush failure: buffer retained for retry.
  - [x] `spider_closed` final flush + final-flush-failure does not re-raise; sets `flush_failed` stat.

### 4.5 Bootstrap docs

- [x] README — one-time setup section:
  - [x] Create one template spreadsheet per platform with the column headers in row 1 (matching `_SHEET_COLUMNS`).
  - [x] Create one Drive folder per platform for rotated spreadsheets.
  - [x] Share both the template and the folder with the service account email (`xxx@xxx.iam.gserviceaccount.com`) as Editor.
  - [x] Set `.env` vars listed in §4.1.

### 4.6 Wire pipeline

- [x] `ITEM_PIPELINES` adds `"broker_scout.pipelines.gsheets.GSheetsBatchPipeline": 500`.

---

## Phase 5 — Google Drive CSV pipeline

- [x] `pipelines/gdrive_csv.py`
  - [x] Per-item write to `out/{spider}_{run_id}.csv`, flushed per row so a mid-run crash leaves a partial file we can recover from.
  - [x] On `spider_closed`: upload via Drive API, name `{spider}_{YYYYMMDD-HHMMSS}.csv` (UTC).
  - [x] Resumable upload for files > 5 MB; simple upload below.
  - [x] Local copy retained 7 days (cron purge — separate).
  - [x] Header row + data row use the same column order as Sheets (`sheets_repo.template_header_row()` / `to_row`) so a CSV row is byte-identical to a Sheet row — trivial replay.
- [x] ~~README: instructions to share Drive folder with the service account email~~ — N/A under OAuth user creds; the user already owns the folder.
- [x] Wire `GDriveCsvPipeline` at priority `600`.

Stats counters emitted (consumed by Phase 9 `PipelineFailureMonitor`):
- `gdrive_csv/upload_status` — `ok` | `failed` | `skipped` (zero-row run).
- `gdrive_csv/file_id` — Drive file ID, used by Phase 11 alerts to link the CSV.
- `gdrive_csv/rows_uploaded` — must equal `item_scraped_count` for a healthy run.

---

## Phase 6 — DLD-seeded spider refactor

Goal: replace the hardcoded `"DHARAM VIR JUNEJA"` lookup with a run that drives off the full DLD broker list and emits one validated item per DLD broker, classified by match status. After this phase ships, the system scrapes ~30k brokers per weekly run instead of 1.

Existing state (already in place from Phases 1 + 3):
- `dld_brokers` table is populated by `tools/fetch_dld` (~30k rows).
- `brokers.match_status` (default `'unknown'`) and `brokers.match_confidence` columns already exist.
- `brokers_repo._BROKER_COLUMNS` already references those two.

What's missing (to be added in this phase):
- `dld_brn`, `dld_broker_name`, `agency_name` columns on `brokers`.
- Same fields on `PropertyFinderBrokerItem`, `PropertyFinderBrokerSchema`, `_BROKER_COLUMNS`, `_SHEET_HEADERS`.
- `match_status` as a typed `Literal` in the schema (currently free-form `TEXT` in DB).
- A DLD-snapshot loader for the spider's `start_requests`.
- The matching logic itself.
- A `BaseBrokerSpider` extracted out of `agent_spider`.

### 6.1 Schema, items, migration, repo + sheet wiring (additive, no spider change)

- [x] `sql/migrations/004_match_columns.sql` — add three columns to `brokers`:
  - `dld_brn TEXT` *(DLD ground truth — useful for forensics: detect when PF's BRN disagrees with DLD)*
  - `dld_broker_name TEXT` *(DLD ground truth name, before normalization)*
  - `agency_name TEXT` *(from DLD `OfficeNameEn`; PF's agency is the URL only)*
- [x] Add the three fields to [PropertyFinderBrokerItem](broker_scout/broker_scout/items.py) under a new `# --- match / DLD ground truth ---` block alongside `match_status` and `match_confidence` (which were not on the dataclass before).
- [x] Update [PropertyFinderBrokerSchema](broker_scout/broker_scout/schemas.py):
  - [x] `match_status: Optional[Literal["exact_brn", "name_unique", "name_fuzzy", "ambiguous", "not_found", "unknown"]]` (typed alias `MatchStatusType` + runtime `MATCH_STATUSES` tuple).
  - [x] `match_confidence: Optional[float]` with `0 ≤ x ≤ 1`.
  - [x] `dld_brn: Optional[str]` non-empty when present (no regex).
  - [x] `dld_broker_name: Optional[str]` ≤ 200 chars.
  - [x] `agency_name: Optional[str]` ≤ 200 chars.
- [x] `brokers_repo._ITEM_COLUMNS` += three new fields (so the per-item INSERT pulls them through).
- [x] `sheets_repo._SHEET_HEADERS` += three new fields **appended at the end** so existing data rows in deployed spreadsheets stay column-aligned. Operator action documented in §6.7.
- [x] `tests/test_schemas.py` += rows in the rejection/acceptance tables + a guard test that catches drift in `MATCH_STATUSES`.
- [x] Migration applied to live DB, 208 tests pass, end-to-end spider run still green across all 4 sinks.

### 6.2 Matching layer

- [ ] Add `rapidfuzz = "^3.10"` to [pyproject.toml](pyproject.toml). Pure C, fast, well-maintained; standard for fuzzy string matching in Python.
- [ ] `common/matching.py`:
  - [ ] `MatchResult` dataclass: `status` (Literal of statuses above), `confidence` (float 0..1), `candidate_url` (str | None), `candidate_brn` (str | None).
  - [ ] `_normalize_name(s: str) -> str` — lowercase, collapse whitespace, strip punctuation. DLD names are uppercase, PF names are title case; normalize both before comparison.
  - [ ] `match_candidates(dld_broker: DLDBroker, candidates: list[Candidate]) -> MatchResult`:
    - [ ] **BRN match** (highest confidence: 1.0): if any candidate exposes a BRN equal to `dld_broker.brn`, pick it. PF rarely exposes BRN in search results — usually a profile-fetch is needed. Pragmatic: skip BRN at the search-result stage; only confirm BRN match after the agent profile has been fetched (post-`parse_agent`). Status promotes from `name_unique`/`name_fuzzy` → `exact_brn` if the parsed BRN matches.
    - [ ] **Name-unique** (confidence 0.95): exactly one candidate, normalized name equals normalized DLD name.
    - [ ] **Name-fuzzy** (confidence = ratio/100): exactly one candidate above the configurable `MATCH_FUZZY_THRESHOLD` (default 90); compute via `rapidfuzz.fuzz.token_set_ratio`.
    - [ ] **Ambiguous**: multiple candidates above the threshold — emit stub, do not pick.
    - [ ] **Not found**: zero candidates — emit DLD-only stub.
- [ ] `tests/test_matching.py`:
  - [ ] BRN tie-break beats name match.
  - [ ] Name-unique exact match → `confidence=0.95`.
  - [ ] Token-set ratio works for "Dharam V. Juneja" vs "DHARAM VIR JUNEJA".
  - [ ] Multiple high-ratio candidates → ambiguous, not picked.
  - [ ] Empty candidates → not_found.
  - [ ] Configurable threshold respected (test override).

### 6.3 DLD snapshot loader

- [ ] `common/dld_repo.py` += `iter_active_brokers() -> Iterator[DLDBroker]` reading every row from the `dld_brokers` table. Postgres is the source of truth; the JSONL snapshots are an audit trail / backup, not the spider's input.
- [ ] Optionally (defer if not needed): load only rows where `last_seen_run_id` is the most-recent run. Default behavior reads all rows — DLD is broker license registry, expired brokers are still meaningful.
- [ ] `tests/test_dld_repo.py` += a test for `iter_active_brokers` with mocked cursor returning multiple rows.

### 6.4 `BaseBrokerSpider` + `agent_spider` refactor

- [ ] `spiders/base.py` — `BaseBrokerSpider(Spider)`:
  - [ ] Class-level `platform: str = ""` (subclass overrides; `agent_spider` sets `"propertyfinder"`).
  - [ ] `start_requests`: iterate `dld_repo.iter_active_brokers()`, call `self.search_for_broker(dld_broker)` per row, yield from each.
  - [ ] Abstract method `search_for_broker(self, dld_broker: DLDBroker) -> Iterable[Request]` — subclass implements platform-specific search URL.
  - [ ] Abstract method `parse_search_results(self, response, dld_broker)` — receives `dld_broker` via `cb_kwargs`. Subclass extracts candidate list, calls `matching.match_candidates(dld_broker, candidates)`, emits one of:
    - matched → request for the candidate's profile, carrying `dld_broker` + `match_result` via `cb_kwargs`.
    - ambiguous → emit stub item with `match_status="ambiguous"`.
    - not_found → emit stub item with `match_status="not_found"`.
- [ ] Refactor [spiders/agent_spider.py](broker_scout/broker_scout/spiders/agent_spider.py):
  - [ ] Inherit from `BaseBrokerSpider`; remove the hardcoded name + start_urls.
  - [ ] `search_for_broker(dld_broker)` builds the PF search URL from `dld_broker.broker_name_en` (or `_ar` fallback).
  - [ ] `parse_search_results(response, dld_broker)` extracts all candidates from the `<div data-testid="AgentList">`, builds a `Candidate(name, url, brn=None)` list, calls `matching.match_candidates(...)`, and either:
    - yields the match's profile request → `parse_agent`, carrying `(dld_broker, match_result)`.
    - yields an ambiguous/not_found stub item directly.
  - [ ] `parse_agent` receives `dld_broker` + `match_result` from `cb_kwargs`. Sets:
    - `item.match_status = match_result.status` (may be promoted to `exact_brn` here if PF's BRN matches DLD's).
    - `item.match_confidence = match_result.confidence`.
    - `item.dld_brn`, `item.dld_broker_name`, `item.agency_name` from `dld_broker`.
  - [ ] Stub-item helper (`_emit_dld_stub(dld_broker, status)`) for ambiguous + not_found rows so they go through the same validation pipeline.
- [ ] Existing parse chain (`parse_agency`, `parse_property`, etc.) untouched aside from threading `dld_broker` + `match_result` through cb_kwargs.

### 6.5 Tests + verification

- [ ] `tests/test_base_spider.py`:
  - [ ] `start_requests` calls `iter_active_brokers` and routes each broker through `search_for_broker`.
  - [ ] Stub-item helper produces a dict that passes `PropertyFinderBrokerSchema`.
- [ ] Refactor existing live smoke: replace `CLOSESPIDER_ITEMCOUNT=1` with a small fixed DLD subset (e.g. set `DLD_LIMIT=5` env var, spider tops out start_requests). Validates the matched/ambiguous/not_found distribution lands in Postgres + Sheets + Drive correctly.
- [ ] Confirm at the DB level: `SELECT match_status, count(*) FROM brokers WHERE run_id=...GROUP BY match_status;` shows the expected mix.

### 6.6 Settings exposed

- [ ] `MATCH_FUZZY_THRESHOLD` in `settings.py` (default 90), surfaced via `crawler.settings.getfloat(...)` in `matching.py`.
- [ ] `DLD_LIMIT` (optional int) — when set, `BaseBrokerSpider.start_requests` stops after that many DLD brokers. For dev / smoke testing without scraping the full 30k.

### 6.7 Operator notes (after Phase 6 ships)

- [ ] **Re-paste headers** in both Sheet templates: 6.1 added 3 columns, so 37 → 40. Run `poetry run python -c "from broker_scout.common.sheets_repo import template_header_row; print(','.join(template_header_row()))"`, paste into A1, Split-text-to-columns. Existing data rows under the old headers stay aligned (we appended new columns to the right end of `_SHEET_HEADERS`'s "Provenance" group, so old data is still in the right place; new columns will just be empty for past rows).
- [ ] First post-deploy run must follow a `python -m broker_scout.tools.fetch_dld` so `dld_brokers` has the latest snapshot.

---

## Phase 7 — Extraction-health stats counters *(do alongside earlier phases)*

These are one-line `self.crawler.stats.inc_value(...)` calls. Cost nothing, accumulate signal for the monitor work in Phase 9.

- [ ] `extract/next_data/missing` — `__NEXT_DATA__` script tag absent on agent page
- [ ] `extract/agent_data/missing` — `props.pageProps.agent` is null
- [ ] `extract/brn/fallback_used` — HTML fallback fired (already wired)
- [ ] `extract/agency_license/missing` — license XPath returned nothing
- [ ] `extract/listings_api/non_json` — JSON decode failed (already wired)
- [ ] `extract/listings_api/empty` — listings array empty when `total_page_count > 0`

---

## Phase 8 — Bayut spider

- [ ] `spiders/bayut.py` extending `BaseBrokerSpider`
- [ ] Reuse DLD client, matching, all pipelines, validation schema (or platform-specific subclass)
- [ ] Bayut-specific extraction logic
- [ ] Field parity check vs PF — flag any field unavailable on Bayut

---

## Phase 9 — Spidermon wiring + monitors

`extensions.py` wires Spidermon. Three suites: validation, periodic, close.

### 9.1 Validation suite (per-item, runs in pipeline)
*Already producing data from Phase 2.*

- [ ] **`ValidationFailureRateMonitor`** *(custom)* — fail if `validation/failed_total / item_scraped_count > 5%`
- [ ] **`ValidationFailureByFieldMonitor`** *(custom)* — fail if any single field > 10% failure rate (catches PF schema drift)

### 9.2 Periodic suite (60s interval — circuit breakers)

- [ ] **Periodic `ErrorCountMonitor`** *(built-in)* — kill spider if errors > 500
- [ ] **Periodic `UnwantedHTTPCodesMonitor`** *(built-in)* — kill if `429` count > 50 (rate-limited)
- [ ] On trigger: send single Google Chat alert + `crawler.engine.close_spider("circuit_breaker")` — no spam

### 9.3 Close suite (run once at spider end)

**Volume**
- [ ] **`ItemCountMonitor`** *(built-in)* — minimum item count threshold
- [ ] **`ItemCountIncreaseMonitor`** *(built-in)* — minimum increase vs prior run
- [ ] **`ZeroItemsMonitor`** *(custom)* — fail loudly when 0 items

**Field coverage** — `FieldCoverageMonitor` *(built-in)*, three tiers:
- [ ] **Critical (≥ 95%)**: `broker_name`, `agent_url`, `scrape_date`, `platform`
- [ ] **High (≥ 80%)**: `listings_total`, `experience_since`, `nationality`, `agency_url`
- [ ] **Medium (≥ 50%)**: `whatsapp_response_time`, `is_superagent`, `agent_specialization`, `agency_registration_number`
- [ ] **Informational** (track-only, no threshold): all `closed_transaction_*`, all `average_listing_*`, `most_recent_*_date`, `listings_with_marketing_spend`
- [ ] Tier definitions live in `monitors/coverage_tiers.py`

**HTTP / network**
- [ ] **`ErrorCountMonitor`** *(built-in)* — total errors < 50
- [ ] **`UnwantedHTTPCodesMonitor`** *(built-in)* — `403` < 20, `429` < 10, `503` < 5
- [ ] **`RetryRateMonitor`** *(custom)* — `retry_count / request_count > 15%` → warning

**Runtime**
- [ ] **`RuntimeMonitor`** *(built-in)* — runtime within 50–200% of 4-week median
- [ ] **`FinishReasonMonitor`** *(built-in)* — must equal `finished`

**Pipeline health**
- [ ] **`PipelineFailureMonitor`** *(custom)*
  - [ ] `postgres/items_inserted` equals `item_scraped_count`
  - [ ] `gsheets/rows_appended` equals `item_scraped_count`
  - [ ] `gdrive/upload_status` equals `ok`
  - [ ] Any mismatch → critical

**Extraction health** — reads counters from Phase 7
- [ ] **`ExtractionFailureMonitor`** *(custom)*
  - [ ] `extract/next_data/missing` > 1% of agent pages → critical
  - [ ] `extract/brn/fallback_used` > 20% → warning
  - [ ] `extract/listings_api/non_json` > 5% → critical
  - [ ] `extract/agency_license/missing` > 30% → warning

**Match coverage** *(post-Phase 6)*
- [ ] **`MatchStatusDistributionMonitor`** *(custom)* — `exact_brn + name_unique` ≥ 60% of DLD brokers
- [ ] **`NotFoundRateMonitor`** *(custom)* — `match=not_found` < 50%
- [ ] **`AmbiguousRateMonitor`** *(custom)* — `match=ambiguous` < 5%

---

## Phase 10 — Cross-run drift monitors *(after 3–4 weekly runs of history)*

Read from `scrape_runs.stats` JSONB. Run as part of close suite.

- [ ] Item count drop vs last run > 20% → critical
- [ ] Item count drop vs 4-week median > 30% → critical
- [ ] Matched-broker count drop > 10% → warning
- [ ] Median `listings_total` drift > 30% → warning
- [ ] Median `whatsapp_response_time` drift > 50% → warning
- [ ] Agency-coverage drop (% rows with `agency_url`) > 10% → warning
- [ ] `not_found` rate jump > 10pp vs last run → critical

---

## Phase 11 — Alert system

### 11.1 Notifier interface

- [ ] `monitors/actions.py` — `Notifier` protocol with `send(level, title, body, run_id)`
- [ ] **`GoogleChatNotifier`** — POST to webhook, formatted as Chat card
  - [ ] Severity → header colour: green (ok), yellow (warning), red (critical)
  - [ ] Card body includes: spider name, run_id, runtime, items scraped, match-status breakdown, validation failure rate, top 3 monitor failures, links to Sheet + Drive CSV + Postgres run row
- [ ] **`LogOnlyNotifier`** — for local dev (no webhook calls)

### 11.2 End-of-run summary message

Always sent at `spider_closed` regardless of pass/fail. Example:

```
PropertyFinder weekly scrape — OK
Run: 2026-04-29 02:14 UTC  (run_id: a1b2c3...)
Items: 28,431 (matched 24,102 · ambiguous 311 · not_found 4,018)
Validation failures: 142 (0.5%)
Runtime: 1h 47m
Sheet: <link>  Drive CSV: <link>
```

### 11.3 Mid-run critical-only alerts

- [ ] Circuit breakers from Phase 9.2 send a single message + close the spider
- [ ] No mid-run warnings — too noisy

### 11.4 Anti-spam

- [ ] `alert_log` table tracks every alert sent
- [ ] Suppress identical alert (same level + title) within 30 min
- [ ] Config: `ALERT_MIN_LEVEL=warning` for early runs, `critical` once stabilized

### 11.5 Configurable backend

- [ ] `.env`: `ALERT_BACKEND=google_chat` (default), `GOOGLE_CHAT_WEBHOOK_URL=...`
- [ ] Easy to add `WhatsAppNotifier` (Twilio/CallMeBot) or `SlackNotifier` later — same protocol

---

## Phase 12 — Production deploy

- [ ] `Dockerfile` for spider + scheduler
- [ ] Crontab on Will's server: weekly run, PF and Bayut staggered by 2h to share proxies
- [ ] Postgres on Will's server (version, backup policy → confirm with Will)
- [ ] Service account JSON deployed via Vault / file mount (never committed)
- [ ] `tools/replay_run.py` — replay Sheets/Drive flush from Postgres if those sinks fail mid-run
- [ ] `tools/dry_run.py` — `--no-write` mode for testing matching logic without polluting sinks
- [ ] Smoke test: trigger one full run end-to-end on a 100-broker subset

---

## Outstanding decisions for Will

- [ ] Postgres hosting specifics (version, backup, location on his server)
- [ ] Match thresholds — fuzzy ratio cutoff
- [ ] Should `match=not_found` rows write to Sheets, or only Postgres?
- [x] ~~Sheets rollover policy~~ — **decided: monthly auto-rotation** (one new spreadsheet file per `YYYY-MM`, ~5.4M cells, 54% utilization with headroom for column drift). Pipeline-driven via `sheet_registry` table; no manual operator action after one-time bootstrap.
- [ ] Drive folder layout — single vs per-spider folders
- [ ] Confirm `whatsapp_response_time` rename is acceptable downstream (was `response_time`)
