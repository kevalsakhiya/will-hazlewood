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

- [ ] Run `PropertyFinderBrokerSchema.model_validate(asdict(item))`
- [ ] On success: pass normalized dict (`model.model_dump(mode="json")`) downstream
- [ ] On `ValidationError`:
  - [ ] Drop the item (`raise DropItem`)
  - [ ] Log structured error with `run_id`, `brn`, `agent_url`, `errors=[{loc, msg, type}, ...]`
  - [ ] Increment `validation/failed_total`
  - [ ] Increment `validation/failed_field/{field}` for each failing field
  - [ ] Append to `spider.bad_items` buffer with `{run_id, platform, reason, payload}` (Phase 3 drains this)
- [ ] On success: increment `validation/passed_total` (denominator for the Phase 9.1 failure-rate monitor)
- [ ] Wire `ValidationPipeline` at priority `200` in `ITEM_PIPELINES`

### 2.4 Tests

- [ ] `tests/test_schemas.py` — table-driven tests covering:
  - [ ] Happy path: a fully-populated valid item passes
  - [ ] Each field-level rule rejects its bad input and accepts a valid one
  - [ ] Cross-field rules (`listings_total`, `closed_deals_total`, `listings_with_marketing_spend`) reject mismatches
  - [ ] All-null item passes (everything is `Optional`)
- [ ] `tests/test_validation_pipeline.py`
  - [ ] Valid item → returned as dict, `validation/passed_total` incremented
  - [ ] Invalid item → `DropItem` raised, `failed_total` + `failed_field/*` incremented, buffer appended

---

## Phase 3 — Postgres pipeline *(authoritative store)*

- [ ] `sql/migrations/001_init.sql`
  - [ ] `scrape_runs` (run_id, spider, started_at, finished_at, status, items_scraped, items_dropped, stats JSONB)
  - [ ] `brokers` (run_id, scrape_date, platform, brn, match_status, all fields, raw JSONB; UNIQUE on (run_id, platform, brn))
  - [ ] `bad_items` (run_id, platform, reason, payload JSONB, created_at)
  - [ ] `alert_log` (run_id, level, title, body, sent_at)
  - [ ] Indexes on `brokers(brn)`, `brokers(scrape_date)`
- [ ] `pipelines/postgres.py`
  - [ ] Connection pool from env
  - [ ] `open_spider`: insert row in `scrape_runs` with status `running`
  - [ ] Per-item: buffer in memory
  - [ ] Every 500 items + on `close_spider`: bulk insert via `psycopg.copy` or `execute_values` with `ON CONFLICT (run_id, platform, brn) DO NOTHING`
  - [ ] Flush `bad_items` buffer too
  - [ ] `close_spider`: update `scrape_runs` status `ok`/`failed`, write final `items_scraped`, `items_dropped`, full Scrapy stats blob into `stats` JSONB
- [ ] `pipelines/stats_writer.py` — small extension to copy spider stats into `scrape_runs.stats` for cross-run monitor reads
- [ ] Wire `PostgresPipeline` at priority `400`

(`tools/migrate.py` and `common/db.py` already shipped in Phase 1; reuse here.)

---

## Phase 4 — Google Sheets pipeline *(separate sheet per website)*

- [ ] `utils/gauth.py` — load service account JSON for Sheets + Drive
- [ ] `pipelines/gsheets.py`
  - [ ] Spreadsheet ID per platform (`GSHEET_PF_ID`, `GSHEET_BAYUT_ID`)
  - [ ] Worksheets: `brokers`, `_runs`, `_coverage`
  - [ ] Buffer items in memory
  - [ ] Flush every 2000 items + `close_spider` via `spreadsheets.values.append` (`valueInputOption=RAW`, `insertDataOption=INSERT_ROWS`)
  - [ ] Header row written once on first run if sheet is empty
  - [ ] On `close_spider`: append summary row to `_runs` (mirrors Postgres `scrape_runs`)
- [ ] README: instructions to share each sheet with the service account email
- [ ] Wire `GSheetsBatchPipeline` at priority `500`

---

## Phase 5 — Google Drive CSV pipeline

- [ ] `pipelines/gdrive_csv.py`
  - [ ] Per-item write to `out/{spider}_{run_id}.csv`
  - [ ] On `close_spider`: upload via Drive API, name `{spider}_{YYYYMMDD-HHMM}.csv`
  - [ ] Resumable upload for files > 5 MB
  - [ ] Local copy retained 7 days (cron purge — separate)
- [ ] README: instructions to share Drive folder with the service account email
- [ ] Wire `GDriveCsvPipeline` at priority `600`

---

## Phase 6 — DLD-seeded spider refactor

- [ ] `spiders/base.py` — `BaseBrokerSpider` with DLD-driven `start_requests`
- [ ] `common/matching.py`
  - [ ] BRN-first match (when PF exposes it)
  - [ ] Fall back to name-unique, then fuzzy (token-set ratio ≥ 90)
  - [ ] Return `MatchResult(status, confidence, candidate_url)`
- [ ] Refactor `agent_spider.py`:
  - [ ] Drive from DLD snapshot, not a hardcoded name
  - [ ] PF search → match → pick result → existing parse chain
  - [ ] Always emit one item per DLD broker, including `not_found` stubs
  - [ ] Carry `dld_record` and `match_status` through `cb_kwargs`
- [ ] Add `match_status`, `match_confidence`, `dld_brn`, `dld_broker_name`, `agency_name` (from DLD) to item dataclass

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
- [ ] Sheets rollover policy (annual? trim to last N runs?)
- [ ] Drive folder layout — single vs per-spider folders
- [ ] Confirm `whatsapp_response_time` rename is acceptable downstream (was `response_time`)
