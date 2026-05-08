# Monitors reference

Spidermon monitors that ship with broker_scout, what each one checks, what triggers a failure, and how to react when one alerts. Settings names + defaults are pulled directly from `monitors/monitors.py` and `settings.py`.

## Suites

| Suite | Hook | Frequency | Lives in |
|---|---|---|---|
| `PeriodicMonitorSuite` | every 60s during a run | mid-run | `SPIDERMON_PERIODIC_MONITORS` |
| `SpiderCloseMonitorSuite` | `engine_stopped` (post all pipeline flushes) | once per run | `SPIDERMON_ENGINE_STOP_MONITORS` |

> Critical: the close suite must use `SPIDERMON_ENGINE_STOP_MONITORS`, **not** `SPIDERMON_SPIDER_CLOSE_MONITORS`. The latter races our pipelines, which also hook `spider_closed`. See [`architecture.md`](architecture.md) and RULES §11.1.

## Severity model

Every custom monitor inherits from `_BrokerScoutMonitor` and defaults `severity = "critical"`. The chat summary card colour-codes by the worst severity present at or above `ALERT_MIN_LEVEL` (default `warning`). Demote a specific monitor to `"warning"` only after it's empirically too noisy.

Spidermon's built-in monitors don't expose `severity` — `actions.py` reads it via `getattr(monitor, "severity", "critical")` so missing values fall back safely.

## Periodic suite — circuit breakers

These run every 60s. Failures trigger:
1. `LogOnlyAction` → log line at ERROR.
2. `SendCriticalChatAlertAction` → mid-run "Circuit breaker tripped" Discord/Chat card. Idempotent (class-level `_fired` flag); deduped against `alert_log` within `_DEDUPE_WINDOW_MINUTES` (30).
3. `CloseSpiderAction` → calls `engine.close_spider(spider, "circuit_breaker")`. Idempotent.

### `ErrorCountMonitor` (Spidermon built-in)

| | |
|---|---|
| **Reads** | `log_count/ERROR` |
| **Threshold** | `SPIDERMON_MAX_ERRORS` = 500 |
| **Fires when** | the run accumulates >500 ERROR-level log records. |
| **Means** | Something is broken at scale — pipeline crashes, repeated extraction errors, etc. |
| **Action** | Look for the actual ERROR lines in `logs/{spider}_{run_id}.log`. Common causes: PF schema change (lots of `extract/*` warnings escalated), Postgres pool exhausted, Drive permissions revoked. |

### `PeriodicRateLimitMonitor` (custom)

| | |
|---|---|
| **Reads** | `downloader/response_status_count/429` |
| **Threshold** | `PERIODIC_429_THRESHOLD` = 50 |
| **Fires when** | mid-run 429 count exceeds 50. |
| **Means** | We're being rate-limited by PF. Continuing risks proxy bans or account-level blocks. |
| **Action** | Stop, then either rotate proxies, lower `CONCURRENT_REQUESTS_PER_DOMAIN`, raise `DOWNLOAD_DELAY`, or wait. Don't bypass by raising the threshold. |

## Close suite — final-state checks

Runs once at `engine_stopped`. Failures trigger:
1. `LogOnlyAction` → INFO/ERROR per outcome.
2. `SendChatSummaryAction` → end-of-run summary card (always fires, regardless of pass/fail).

Card colour reflects the worst monitor severity at or above `ALERT_MIN_LEVEL`. Card body lists the failure list capped at `_FAILURE_LIST_LIMIT` (10).

### `FinishReasonMonitor` (Spidermon built-in)

| | |
|---|---|
| **Reads** | `finish_reason` |
| **Threshold** | `SPIDERMON_EXPECTED_FINISH_REASONS` = `("finished", "closespider_itemcount", "closespider_pagecount", "closespider_timeout")` |
| **Fires when** | `finish_reason` is anything else (`shutdown`, `cancelled`, `closespider_errorcount`, …). |
| **Means** | The spider didn't reach a clean stop. Often paired with other failures. |
| **Action** | Same set is mirrored in `pipelines/postgres.SUCCESSFUL_REASONS` so `scrape_runs.status` and the monitor agree. |

### `ValidationFailureRateMonitor`

| | |
|---|---|
| **Reads** | `validation/passed_total`, `validation/failed_total` |
| **Threshold** | `VALIDATION_FAILURE_RATE_THRESHOLD` = 0.05 (5%) |
| **Fires when** | `failed / (passed + failed)` > 5%. |
| **Skips** | when no items reached the validator (`ZeroItemsMonitor` covers that). |
| **Means** | Systemic schema issue — many items don't fit the rules. |
| **Action** | Tail `validation/failed_field/{field}` — usually a single field is responsible. See `ValidationFailureByFieldMonitor`. |

### `ValidationFailureByFieldMonitor`

| | |
|---|---|
| **Reads** | every `validation/failed_field/{field}` counter |
| **Threshold** | `VALIDATION_FIELD_FAILURE_RATE_THRESHOLD` = 0.10 (10%) per field |
| **Fires when** | any single field rejects >10% of items. |
| **Skips** | when `item_scraped_count == 0`. |
| **Means** | PF schema drift — that field changed shape (string instead of int, bigger value range, etc.) and our schema rejects every item with it. |
| **Action** | Look at the `validation/failed_field/{x}` counter that exceeded. Inspect a `bad_items.payload` row for that field. Usually a `schemas.py` rule needs to relax (or a real upstream change to handle). |

### `ZeroItemsMonitor`

| | |
|---|---|
| **Reads** | `item_scraped_count` |
| **Threshold** | must be `> 0` |
| **Fires when** | the run produced zero items. |
| **Means** | Spider crashed before yielding anything — DLD load failed, spider class import error, warmup 404, etc. |
| **Action** | Check the run's log file for the first ERROR. Likely no need to look further down — the cause is at the top. |

### `RetryRateMonitor`

| | |
|---|---|
| **Reads** | `retry/count`, `downloader/request_count` |
| **Threshold** | `RETRY_RATE_THRESHOLD` = 0.15 (15%) |
| **Fires when** | retries / total requests > 15%. |
| **Means** | Flakey upstream — proxies failing, network issues, PF intermittently 5xx. |
| **Action** | Check proxy health, look at `downloader/response_status_count/{code}` for the dominant retry-trigger. |

### `UnwantedHTTPCodesMonitor` (Spidermon built-in)

| | |
|---|---|
| **Reads** | `downloader/response_status_count/{code}` |
| **Threshold** | `SPIDERMON_UNWANTED_HTTP_CODES` = `{403: 20, 429: 100, 503: 5}` |
| **Fires when** | absolute count of any listed code exceeds its budget. |
| **Note** | 404 is **deliberately not in the dict** — empirically PF returns 404 for many DLD names that aren't on PF; it's a normal `not_found` outcome via `match_status`, not a failure. Don't add 404 here. |
| **Means** | Sustained upstream issues at codes that should be rare. |

### `PipelineFailureMonitor`

Five test methods — every sink must process every scraped item.

| Test | Asserts | Skips when |
|---|---|---|
| `test_postgres_inserted_all_items` | `postgres/brokers_inserted == item_scraped_count` | scraped == 0 |
| `test_gsheets_appended_all_items` | `gsheets/rows_appended == item_scraped_count` | scraped == 0 |
| `test_gsheets_no_flush_failure` | `gsheets/flush_failed == 0` | never |
| `test_gdrive_csv_upload_status_ok` | `gdrive_csv/upload_status == "ok"` | scraped == 0 |
| `test_gdrive_csv_rows_uploaded_match` | `gdrive_csv/rows_uploaded == item_scraped_count` | scraped == 0 |

**Means** one or more sinks didn't agree on item counts.

**Action**:
- Postgres mismatch → check `bad_items` (validation drops still increment scraped but not inserted). Confirm with `SELECT COUNT(*) FROM brokers WHERE run_id = '<id>'`.
- Sheets mismatch → check the run log for `gsheets pipeline ready` + any `final gsheets flush failed`. Verify with the Sheet directly.
- Drive CSV failed → check the run log for the actual upload exception. Local CSV in `out/` is preserved for replay (Phase 12).

### `ExtractionFailureMonitor`

Eight per-counter test methods. Thresholds in `DEFAULT_EXTRACTION_THRESHOLDS`:

| Test | Stat | Threshold | Type |
|---|---|---|---|
| `test_next_data_missing_rate` | `extract/next_data/missing` | 0.01 | rate |
| `test_next_data_bad_json_count` | `extract/next_data/bad_json` | 0 | absolute |
| `test_agent_data_missing_rate` | `extract/agent_data/missing` | 0.01 | rate |
| `test_search_json_fallback_rate` | `extract/search_json/fallback_used` | 0.05 | rate |
| `test_brn_fallback_rate` | `extract/brn/fallback_used` | 0.20 | rate |
| `test_listings_api_non_json_rate` | `extract/listings_api/non_json` | 0.05 | rate |
| `test_listings_api_empty_rate` | `extract/listings_api/empty` | 0.10 | rate |
| `test_agency_license_missing_rate` | `extract/agency_license/missing` | 0.30 | rate |

**Skips** when `item_scraped_count == 0`.

**Means** PF schema or HTML changed — the JSON path stopped working enough that fallback is dominating.

**Action** which counter exceeded narrows the change:
- `next_data/*` → PF dropped `__NEXT_DATA__` from the page entirely or changed its structure.
- `search_json/fallback_used` → search results page no longer exposes `props.pageProps.agents.data` or that path moved.
- `brn/fallback_used` → profile page's `compliances` array shape changed.
- `listings_api/*` → PF's pwa property API broke or changed shape.
- `agency_license/missing` → agency page lost its `data-testid="license-content"`.

The fix is in the spider's selectors / JMESPath queries (`spiders/_pf_extractors.py` for shape transforms, `spiders/agent_spider.py` for selectors). Don't lower the threshold to silence — that hides the next regression.

### `MatchStatusDistributionMonitor`

| | |
|---|---|
| **Reads** | `match/exact_brn`, `match/name_unique`, `item_scraped_count` |
| **Threshold** | `MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD` = 0.60 (60%) |
| **Fires when** | `(exact_brn + name_unique) / item_scraped_count` < 60%. |
| **Skips** | when `item_scraped_count == 0`. |
| **Means** | We've stopped getting strong matches — usually because PF stopped exposing BRN in search-page JSON, so we're falling back to fuzzy. Check `extract/search_json/fallback_used` simultaneously. |
| **Action** | Often correlates with `ExtractionFailureMonitor.test_search_json_fallback_rate` failing. Fix the same drift. |

### `NotFoundRateMonitor`

| | |
|---|---|
| **Reads** | `match/not_found`, `item_scraped_count` |
| **Threshold** | `NOT_FOUND_RATE_THRESHOLD` = 0.50 (50%) |
| **Fires when** | `not_found / item_scraped_count` ≥ 50%. |
| **Means** | More than half our DLD seeds didn't match anything on PF — likely the search step is broken (PF rejecting the query format, name normalization off, search URL changed). |
| **Action** | Pull a sample DLD broker known to be on PF, run the search URL manually, see what the page returns. |

### `AmbiguousRateMonitor`

| | |
|---|---|
| **Reads** | `match/ambiguous`, `item_scraped_count` |
| **Threshold** | `AMBIGUOUS_RATE_THRESHOLD` = 0.05 (5%) |
| **Fires when** | `ambiguous / item_scraped_count` ≥ 5%. |
| **Means** | BRN-first match is failing across the board → the search-page JSON probably stopped exposing BRN and name fuzzy is producing ≥2 plausible candidates per query. |
| **Action** | Same fix-shape as `MatchStatusDistributionMonitor` — check search-page JSON shape. |

### `BRNDriftMonitor`

| | |
|---|---|
| **Reads** | `match/brn_drift` |
| **Threshold** | `BRN_DRIFT_THRESHOLD` = 0 (any drift fires) |
| **Fires when** | any item had `item.brn != dld_broker.brn` and both were present. |
| **Means** | PF and DLD disagree about the regulator's BRN for some broker. *Do not silence* — the matching layer deliberately doesn't promote disagreeing pairs to `exact_brn` because they're real-world data signals worth investigating (could be a recently re-licensed broker, could be a PF data bug, could be an honest mismatch). |
| **Action** | `SELECT brn, dld_brn, broker_name, dld_broker_name FROM brokers WHERE run_id = '<id>' AND brn IS NOT NULL AND dld_brn IS NOT NULL AND brn != dld_brn;` Look at the cases. Decide per-row. |

### `MatchedRowFieldCoverageMonitor`

Three test methods, one per tier. Reads coverage from Postgres directly — not from in-memory stats — so it can filter to matched rows only (the native Spidermon `FieldCoverageMonitor` averages over every item including stubs, which dilutes the rate).

Tiers + thresholds (defined in `monitors/coverage_tiers.py`):

| Tier | Fields | Threshold setting | Default |
|---|---|---|---|
| Critical | `PF_CRITICAL_FIELDS` (broker_name, agent_url, brn, …) | `FIELD_COVERAGE_CRITICAL_THRESHOLD` | 0.95 |
| High | `PF_HIGH_FIELDS` (listings + closed-deal counts, …) | `FIELD_COVERAGE_HIGH_THRESHOLD` | 0.80 |
| Medium | `PF_MEDIUM_FIELDS` (averages, agency fields, …) | `FIELD_COVERAGE_MEDIUM_THRESHOLD` | 0.50 |

| | |
|---|---|
| **Reads** | `brokers_repo.matched_field_coverage(run_id, fields)` — SQL query against `brokers` filtered to `match_status IN MATCHED_STATUSES` |
| **Skips** | when no matched rows exist, or DB is unreachable, or `run_id` missing from stats |
| **Means** | A field that should be populated for matched brokers is regularly NULL — usually a PF JSON path broke for that field. |
| **Action** | The error message lists every offender (`field=Xpct` for each). Look at `brokers WHERE run_id = '<id>' AND match_status IN ('exact_brn', 'name_unique', 'name_fuzzy') AND <field> IS NULL` to see real examples. |

The `Provenance` and `Informational` tiers are not enforced — they're documented in `coverage_tiers.py` for completeness but pure-data fields like `match_status` are always set, and `Informational` ones (whatsapp response time, …) aren't worth alerting on.

## Adding a new monitor

1. Subclass `_BrokerScoutMonitor` (gets `severity = "critical"` + `__test__ = False`).
2. One test method = one assertion. Multiple `assertX(...)` in one method become one combined error message — split them (RULES §11.3).
3. `self.skipTest(...)` when input data isn't there (`item_scraped_count == 0`, no `run_id`, etc.). Don't fail without signal.
4. Use `assertLessEqual` / `assertGreaterEqual` so boundary values pass — 5% failure rate at threshold 5% is OK.
5. Read thresholds from settings with the default constant pattern (see RULES §7.2).
6. Add to the `monitors` list of the relevant suite in `monitors/monitors.py`.
7. Add to this doc.
8. Add to `roadmap.md` §7 if it reads a stat that wasn't there before.

## Tuning thresholds

Every threshold lives in `settings.py` AND has a default in `monitors/monitors.py`. Override via `.env` if your runs are systematically different:

```ini
RETRY_RATE_THRESHOLD=0.10
NOT_FOUND_RATE_THRESHOLD=0.40
```

A test asserts default ↔ settings parity to catch silent drift.
