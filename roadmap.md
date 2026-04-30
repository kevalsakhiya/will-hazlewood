# Roadmap — DLD × PropertyFinder × Bayut

Step-by-step build plan. Pipelines before monitors; monitors layer on top once history exists. See `plan.md` for architecture.

Phase numbering is execution order. Each phase has discrete checkpoints — finish a phase before starting the next so we always have a working scraper at HEAD.

---

## Phase 0 — Foundations *(prerequisite for everything)*

- [ ] Repo layout matches `plan.md` §3 (project package: `broker_scout`)
- [ ] `pyproject.toml` with pinned deps (`scrapy`, `httpx`, `psycopg[binary,pool]`, `pydantic>=2`, `gspread`/`google-api-python-client`, `tenacity`, `python-json-logger`, `spidermon`)
- [ ] `.env.example` with `RUN_ENV`, `POSTGRES_*`, `GSHEET_PF_ID`, `GSHEET_BAYUT_ID`, `GDRIVE_FOLDER_ID`, `GOOGLE_CHAT_WEBHOOK_URL`, `SERVICE_ACCOUNT_JSON_PATH`, `PROXY_URL`
- [ ] `docker-compose.yml` with Postgres for local dev
- [ ] `utils/logging_setup.py` — JSON logs, `run_id` injected
- [ ] `extensions.py` — generates `run_id = uuid4()` on spider open, sets `crawler.settings["RUN_ID"]`, propagates to logs and items
- [ ] `common/run_context.py` — single source for `run_id`, `scrape_date` (UTC), `spider_label`

---

## Phase 1 — DLD client *(unblocks both spiders)*

- [ ] `common/dld_client.py`
  - [ ] `fetch_all_brokers(run_id) -> Iterator[DLDBroker]` — single `pageSize=50000` call
  - [ ] `Accept-Encoding: gzip, br` header
  - [ ] `tenacity` retry: 3 attempts, exponential backoff, 2-min timeout
  - [ ] Snapshot to `dld_snapshots/{run_id}.jsonl` immediately after fetch
  - [ ] `load_snapshot(run_id)` helper for spider re-runs
- [ ] `common/normalizers.py` — phone E.164, name trim/casefold, BRN as string, AED `Decimal`
- [ ] Define `DLDBroker` dataclass: `brn`, `broker_name`, `agency_name`, `phone`, `dld_status`, `dld_raw` (full payload preserved)

---

## Phase 2 — Validation layer *(no DB write yet)*

Goal: catch bad rows at the boundary, isolate them, keep good rows flowing.

- [ ] `schemas.py` — pydantic model `PropertyFinderBrokerSchema`
  - [ ] All fields `Optional[...]` by default
  - [ ] **No BRN regex** (DLD is the source of truth)
  - [ ] `broker_name` non-empty string ≤ 200 chars
  - [ ] `agent_url` starts with `https://www.propertyfinder.ae/`
  - [ ] `experience_since`: 1980 ≤ year ≤ current year
  - [ ] `whatsapp_response_time`: 0 ≤ x ≤ 86_400, or null
  - [ ] `listings_for_sale`, `listings_for_rent`: 0 ≤ x ≤ 5000
  - [ ] `listings_total` equals `(sale or 0) + (rent or 0)`
  - [ ] `closed_transaction_*` ≥ 0
  - [ ] `closed_transaction_*_avg_amount`: 0 ≤ x ≤ 10⁹
  - [ ] All date fields parse as date, not in future, not before 2000
  - [ ] `is_superagent`: strict `bool` or null
  - [ ] `scrape_date` equals today (UTC)
- [ ] `pipelines/validation.py`
  - [ ] Run `Schema.model_validate(asdict(item))`
  - [ ] On success: pass normalized dict downstream
  - [ ] On `ValidationError`: drop, log, increment `validation/failed_total` and `validation/failed_field/{field}`
  - [ ] Buffer failures in memory; flush to `bad_items` table once Postgres pipeline lands (Phase 3)
- [ ] Wire `ValidationPipeline` at priority `200` in `ITEM_PIPELINES`

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
- [ ] Migration runner script (`tools/migrate.py`) — apply versioned `.sql` files
- [ ] Wire `PostgresPipeline` at priority `400`

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
