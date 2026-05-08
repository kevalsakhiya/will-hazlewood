# Data flow

The cross-layer contracts: what shape an item is at each step, what status it can have, and what stats are emitted along the way.

---

## Item shape lifecycle

```
PF JSON              ──►  PropertyFinderBrokerItem      ──►  dict                     ──►  DB row
(__NEXT_DATA__       PF   @dataclass(slots=True),       VAL  asdict / model_dump      PG   brokers row +
 + listings API)     extr defaults None per field       pipe pydantic-validated       pipe `raw` JSONB
                                                                                            │
                                                                                            └──►  Sheets row
                                                                                                 (column-mapped,
                                                                                                  None → "")
                                                                                                 +
                                                                                            └──►  Drive CSV row
                                                                                                 (same shape)
```

| Stage | Type | Carried where | Mutable? |
|---|---|---|---|
| Raw PF JSON | `dict` | Inside spider callback | Yes (we read fields off it) |
| Spider emits | `PropertyFinderBrokerItem` | Yielded to ITEM_PIPELINES | Yes (set fields during extraction) |
| ValidationPipeline output | `dict` | Forwarded to next pipeline | No (treat as immutable) |
| PostgresPipeline buffer | `list[dict]` | In-memory buffer until flush | Append-only |
| Postgres `brokers` row | columns + JSONB `raw` | Persisted | No (`ON CONFLICT DO NOTHING`) |
| GSheets row | `list` (column-ordered) | Append batch | No |
| Drive CSV row | `list` (same column order as Sheets) | Single CSV file | No |

### The dict contract

After `ValidationPipeline` (priority 200), items are **plain `dict`s**. Every later pipeline accepts dicts. A pipeline that receives a `PropertyFinderBrokerItem` dataclass must convert via `dataclasses.asdict()` or `model.model_dump()` before forwarding (RULES §3.3 / §4.5).

### Why two shapes (`items.py` + `schemas.py`)

| Concern | Lives in `items.py` | Lives in `schemas.py` |
|---|---|---|
| What fields exist | ✓ | ✓ (mirror) |
| Default values | ✓ (`None`) | – |
| Field bounds (max length, ranges) | – | ✓ |
| Cross-field invariants | – | ✓ (`model_validator`) |
| Time-sensitive checks | – | ✓ (`field_validator(mode="after")`) |
| `extra="forbid"` drift detection | – | ✓ |

The dataclass is the wire format the spider produces; the schema is the rules layer the validation pipeline runs. Drift between them (typo'd field, removed field) fails loudly via `extra="forbid"`.

---

## Match-status state machine

Every emitted item has exactly one `match_status`. Set in `match_candidates` then optionally upgraded in `parse_agent` via `promote_to_brn_match`.

```
                                       ┌──────────────────┐
DLD broker  ──►  search_for_broker ──► │ candidates list  │
                                       └──────────────────┘
                                              │
                                              ▼
                                    candidates is empty?
                                       │            │
                                      yes           no
                                       │            │
                                       ▼            ▼
                                  not_found    BRN match present?
                                                  │            │
                                                 yes           no
                                                  │            │
                                                  ▼            ▼
                                             exact_brn    exact normalized name?
                                                                │           │
                                                              yes (1)      no
                                                                │           │
                                                                ▼           ▼
                                                          name_unique   fuzzy match?
                                                                          │      │
                                                                       1 above  ≥2 above
                                                                       threshold threshold
                                                                          │      │
                                                                          ▼      ▼
                                                                    name_fuzzy ambiguous
                                                                                │
                                                                                ▼
                                                                       walk plausibles
                                                                       by BRN; first match
                                                                       upgrades to exact_brn
                                                                       (parse_disambiguating_profile)
                                                                                │
                                                                                ▼
                                                                  exhausted? → ambiguous stub
```

Then in `parse_agent`:

```
match_status = name_unique or name_fuzzy
PF profile BRN equals DLD BRN?
   yes → match_status = exact_brn
         match/promoted_to_exact_brn += 1
   no, both BRNs present → match/brn_drift += 1
```

| Status | Confidence | Means |
|---|---|---|
| `exact_brn` | 1.0 | Regulator's BRN agrees PF↔DLD. Strongest signal. |
| `name_unique` | 0.95 | One candidate, exact normalized name match. Very high. |
| `name_fuzzy` | 0.0..1.0 (token-set ratio) | One candidate above the fuzzy threshold. Weaker. |
| `ambiguous` | 0.0 | ≥2 plausible candidates and BRN walk didn't disambiguate. We don't pick. |
| `not_found` | 0.0 | Zero candidates, or PF returned 404. |
| `unknown` | NULL | Pre-Phase-6 legacy default, should never appear in modern runs. |

`MATCHED_STATUSES = ("exact_brn", "name_unique", "name_fuzzy")` — the tuple in `brokers_repo.py` that drives matched-row coverage queries.

---

## Stat namespaces

Every counter is part of the public contract — monitors and the chat summary card read them by exact name. Adding a new top-level category requires updating `monitors/monitors.py` AND this doc AND `roadmap.md` §7 (RULES §16.2).

### `validation/`

Set in `pipelines/validation.py` per item.

| Stat | Type | Meaning |
|---|---|---|
| `validation/passed_total` | counter | Items that passed schema validation. |
| `validation/failed_total` | counter | Items dropped by validation. |
| `validation/failed_field/{field}` | counter | Per-field rejection count (one stat per failing field per dropped item). |

### `match/`

Set in `agent_spider.py` callbacks.

| Stat | Type | Meaning |
|---|---|---|
| `match/exact_brn` | counter | Items resolved by BRN match. |
| `match/name_unique` | counter | Items resolved by single exact-name match. |
| `match/name_fuzzy` | counter | Items resolved by single fuzzy match above threshold. |
| `match/ambiguous` | counter | Items where ≥2 candidates were plausible. |
| `match/not_found` | counter | Items with no plausible PF candidate. |
| `match/ambiguous_disambiguated` | counter | Ambiguous walks that found the right candidate by BRN. |
| `match/ambiguous_exhausted` | counter | Ambiguous walks that exhausted the plausibles list. |
| `match/promoted_to_exact_brn` | counter | Items whose name match upgraded to `exact_brn` after profile fetch. |
| `match/brn_drift` | counter | Items where PF BRN ≠ DLD BRN (both present). Real signal, not bug. |

### `extract/`

Set in `_pf_extractors.py` and `agent_spider.parse_*`. Increments are *always* paired with a fallback path so the spider keeps producing items.

| Stat | Type | Meaning |
|---|---|---|
| `extract/next_data/missing` | counter | `__NEXT_DATA__` script tag not found on profile page. |
| `extract/next_data/bad_json` | counter | `__NEXT_DATA__` content failed `json.loads`. |
| `extract/agent_data/missing` | counter | `props.pageProps.agent` empty inside parsed JSON. |
| `extract/search_json/fallback_used` | counter | Search-page JSON missing → HTML fallback used (no BRN). |
| `extract/brn/fallback_used` | counter | Profile-page JSON missing BRN → HTML table fallback used. |
| `extract/listings_api/non_json` | counter | Listings API returned non-JSON (rare but seen on errors). |
| `extract/listings_api/empty` | counter | Page-1 listings empty when `total_page_count > 0`. |
| `extract/agency_license/missing` | counter | Agency page lacked `data-testid="license-content"`. |

### `postgres/`

Set in `pipelines/postgres.py`.

| Stat | Type | Meaning |
|---|---|---|
| `postgres/brokers_inserted` | counter | Rows inserted into `brokers` (sums across batches). |
| `postgres/bad_items_inserted` | counter | Rows inserted into `bad_items` (drained from `spider.bad_items`). |

### `gsheets/`

Set in `pipelines/gsheets.py`.

| Stat | Type | Meaning |
|---|---|---|
| `gsheets/sheet_id` | value (string) | Active spreadsheet ID for this run. Used by chat summary link. |
| `gsheets/rows_appended` | counter | Rows appended (sums across batches). |
| `gsheets/flush_failed` | value (0 or 1) | 1 = final flush raised; pipeline swallowed it. |

### `gdrive_csv/`

Set in `pipelines/gdrive_csv.py`.

| Stat | Type | Meaning |
|---|---|---|
| `gdrive_csv/upload_status` | value (`ok`/`skipped`/`failed`) | Final upload outcome. `skipped` = zero rows. |
| `gdrive_csv/file_id` | value (string) | Drive file ID on success. Used by chat summary link. |
| `gdrive_csv/rows_uploaded` | counter | Rows in the uploaded CSV. |

### `pipeline/` (reserved)

`pipeline/flush_failed` and friends are reserved for non-success paths the close-suite monitor reads. Currently emitted as part of `gsheets/flush_failed`; new categories should follow the same `<sink>/<event>` shape.

### Top-level Scrapy stats we read

These come from Scrapy core, not us. Listed here because monitors depend on them.

| Stat | Source | Read by |
|---|---|---|
| `item_scraped_count` | Scrapy core | Most rate monitors. |
| `item_dropped_count` | Scrapy core (DropItem) | `PipelineFailureMonitor` (parity check). |
| `downloader/request_count` | Scrapy core | `RetryRateMonitor`. |
| `downloader/response_status_count/{code}` | Scrapy core | `PeriodicRateLimitMonitor` (429), close-suite `UnwantedHTTPCodesMonitor`. |
| `retry/count` | Scrapy core | `RetryRateMonitor`. |
| `log_count/ERROR` | Scrapy core | `ErrorCountMonitor` (Spidermon built-in). |
| `finish_reason` | Scrapy core | `FinishReasonMonitor` (Spidermon built-in). |
| `start_time`, `finish_time`, `elapsed_time_seconds` | Scrapy core | Chat summary runtime. |

---

## Database schema

Migrations are forward-only (`sql/migrations/NNN_*.sql`). Re-running `tools/migrate.py` is idempotent — every `CREATE` uses `IF NOT EXISTS`.

```
dld_brokers           ◄─ tools/fetch_dld.py upserts; spider iterates active rows
   brn (PK)
   broker_name_en, broker_name_ar
   office_name_en, office_name_ar
   first_seen_run_id, last_seen_run_id
   active (bool)

scrape_runs            ◄─ PostgresPipeline.spider_opened opens; close_run + engine_stopped re-snap finalize
   run_id (PK, uuid)
   spider, started_at, finished_at
   status (running | ok | failed)
   items_scraped, items_dropped
   stats (jsonb)        ◄─ full Scrapy stats blob, post-flush

brokers                ◄─ PostgresPipeline batched insert; ON CONFLICT (run_id, platform, brn) DO NOTHING
   id (PK), run_id (FK)
   scrape_date, platform, brn
   match_status (default 'unknown'), match_confidence
   broker_name, dld_brn, dld_broker_name, agency_name, agency_url, agency_registration_number
   nationality, agent_specialization, experience_since, whatsapp_response_time, is_superagent
   listings_for_sale, listings_for_rent, listings_total, listings_with_marketing_spend
   average_listing_price_*, average_listing_age_days_*, most_recent_listing_date_*
   closed_transaction_*, closed_deals_total, most_recent_deal_date_*
   average_monthly_deal_volume_*
   raw (jsonb)          ◄─ full unflattened item, for forensics

bad_items              ◄─ drained from spider.bad_items by PostgresPipeline.spider_closed
   id (PK), run_id (FK), platform
   reason, payload (jsonb)

sheet_registry         ◄─ sheets_repo monthly-rotation pointer
   id (PK), platform, period (YYYY-MM), sheet_id
   is_active (bool), created_at
   UNIQUE (platform, period)

alert_log              ◄─ Phase 11 anti-spam dedupe
   id (PK), run_id, level, title, body, sent_at
```

### Idempotency rules per table

| Table | Strategy | Re-run safe? |
|---|---|---|
| `dld_brokers` | `ON CONFLICT (brn) DO UPDATE` (last_seen_run_id, active) | Yes |
| `scrape_runs` | INSERT (PK = uuid run_id) — never collides | Yes |
| `brokers` | `ON CONFLICT (run_id, platform, brn) DO NOTHING` | Yes within a run |
| `bad_items` | INSERT — append-only, no conflict key | Yes (drains from in-memory buffer) |
| `sheet_registry` | `ON CONFLICT (platform, period) DO NOTHING` | Yes (race-safe, see RULES §14.2) |
| `alert_log` | INSERT — append-only | Yes |

### JSONB blobs

`brokers.raw` and `bad_items.payload` store the full unflattened item via `Jsonb(item)`. Used for forensics (diff what we extracted vs. what's in the DB). **Never query into JSONB as if it were structured data** — if you need to filter on a field, promote it to a real column (RULES §13.4).

---

## Schema evolution rules

When adding a field to PF / Bayut output:

1. Add the attribute to `items.py` (`field: T | None = None`).
2. Add the validation rule to `schemas.py`.
3. Write a forward-only migration: `sql/migrations/NNN_add_field.sql` with `ALTER TABLE brokers ADD COLUMN IF NOT EXISTS field T`.
4. Add the column name to `_BROKER_COLUMNS` in `brokers_repo.py`.
5. Add the column to `_SHEET_HEADERS` in `sheets_repo.py` (RULES §14.3 says new columns go at the right edge so old rows in deployed spreadsheets stay column-aligned).
6. Add the field to a tier in `monitors/coverage_tiers.py` (the module-load assertion fails until you do).

Removing a field: never rename in place if any committed code reads the old name. Add the new column + drop the old in two separate migrations (RULES §13.5).

---

## Run lifecycle

The order matters; this is the load-bearing dance that lets monitors read post-flush stats.

```
T0  spider startup
       ↳ settings.py loaded, configure_logging() installs StreamHandler
       ↳ extensions.from_crawler — RunIdExtension wires signals, Spidermon wires its suites
       ↳ pipelines.from_crawler — Postgres/Sheets/Drive wire signals

T1  spider_opened signal fires (in registration order)
    1. RunIdExtension.spider_opened
       ↳ uuid4 → spider.run_id, contextvar, stats
       ↳ prune logs/, attach FileHandler at logs/{spider}_{run_id}.log
    2. PostgresPipeline.spider_opened
       ↳ brokers_repo.open_run → INSERT scrape_runs row
    3. GSheetsBatchPipeline.spider_opened
       ↳ resolve / create monthly sheet
    4. GDriveCsvPipeline.spider_opened
       ↳ open out/{spider}_{run_id}.csv, write header

T2  spider runs — items flow through pipelines
       ↳ ValidationPipeline → PostgresPipeline (buffered) → GSheetsBatchPipeline (buffered) → GDriveCsvPipeline (per-item flush)

T3  Periodic monitor suite ticks every 60s
       ↳ ErrorCountMonitor, PeriodicRateLimitMonitor
       ↳ on failure: SendCriticalChatAlertAction + CloseSpiderAction (idempotent)

T4  spider_closed signal fires (in registration order, opposite of open)
    1. PostgresPipeline.spider_closed
       ↳ flush remaining brokers + drain spider.bad_items + close_run (status, counts, stats blob)
    2. GSheetsBatchPipeline.spider_closed
       ↳ flush remaining rows; on failure swallow + gsheets/flush_failed=1
    3. GDriveCsvPipeline.spider_closed
       ↳ upload CSV to Drive; on failure swallow + gdrive_csv/upload_status=failed

T5  engine_stopped signal fires (after every spider_closed handler completes)
    1. PostgresPipeline.engine_stopped
       ↳ re-snapshot stats blob (captures gsheets/* and gdrive_csv/* counters set in T4)
       ↳ brokers_repo.update_run_stats — narrow UPDATE on scrape_runs.stats
    2. SpiderCloseMonitorSuite runs
       ↳ all stats are final by now
       ↳ on any failure: SendChatSummaryAction posts the card

T6  RunIdExtension.spider_closed
       ↳ logs "run finished"; detach FileHandler (after the line lands in the file); clear contextvar
```

The critical invariants:

- `engine_stopped` is the only place stats are guaranteed post-flush.
- Pipelines never raise from `engine_stopped` — the reactor is shutting down; raising surfaces as "unhandled error in deferred" (RULES §4.4).
- The log file's "run finished" line lands *in the file* because detach happens *after* the log call.
