# Operator runbook

For people running the spider, watching alerts, and triaging failures. New-machine setup is in [`README.md`](../README.md); this doc covers the day-to-day.

## Quick reference

| Question | Answer |
|---|---|
| Where do logs go? | `logs/{spider}_{run_id}.log` (always JSON) + stderr (format depends on `LOG_FORMAT`). |
| Where do CSV archives go? | `out/{spider}_{run_id}.csv` (locally, retained 7 days) + Drive folder `GDRIVE_CSV_FOLDER_ID`. |
| Where's the canonical data? | Postgres `brokers` table. Always Postgres-first. |
| Where's the active spreadsheet? | `SELECT sheet_id FROM sheet_registry WHERE platform=? AND is_active=TRUE;` |
| What's the run_id? | `crawler.stats.get_value("run_id")`, or any line of the run log file. |

## Running a spider

```bash
# Production-like run
poetry run scrapy crawl agent_spider

# Smoke test (1 broker, 1 item)
poetry run scrapy crawl agent_spider -s CLOSESPIDER_ITEMCOUNT=1 -s DLD_LIMIT=1

# Run only specific BRNs (debug a single broker)
DLD_BRN_FILTER=12345,67890 poetry run scrapy crawl agent_spider

# Pretty terminal output during dev
LOG_FORMAT=pretty poetry run scrapy crawl agent_spider
```

## Debugging a failed run

The order to check, fastest signal first:

### 1. Read the alert (if Discord/Chat is wired)

The summary card body packs the most actionable info: item count, validation rate, match-status breakdown, pipeline ✓/✗ marks, finish reason, runtime, top 10 monitor failures with the first line of each error. Most issues are diagnosed from the card alone.

### 2. Read `scrape_runs`

```sql
SELECT run_id, started_at, finished_at, status, items_scraped, items_dropped,
       stats->>'finish_reason'         AS finish_reason,
       stats->>'log_count/ERROR'       AS errors,
       stats->>'item_scraped_count'    AS scraped,
       stats->>'match/exact_brn'       AS exact_brn,
       stats->>'match/not_found'       AS not_found,
       stats->>'gsheets/flush_failed'  AS sheets_failed
FROM scrape_runs
ORDER BY started_at DESC
LIMIT 5;
```

`status='failed'` = either `finish_reason` not in `SUCCESSFUL_REASONS` or a flush exception. The `stats` JSONB has every counter the run produced.

### 3. Read the run log file

```bash
# Tail the most recent run, levels ≥ WARNING
jq -c 'select(.level == "WARNING" or .level == "ERROR")' \
   logs/agent_spider_*.log | tail -50

# Or grep by run_id if you know which one
jq -c 'select(.run_id == "abc123...")' logs/agent_spider_abc123*.log
```

JSON in the file regardless of terminal `LOG_FORMAT` — `jq`-friendly always.

### 4. Inspect bad items

```sql
SELECT reason, payload
FROM bad_items
WHERE run_id = '<run_id>'
LIMIT 10;
```

`payload` is the full unflattened item that failed validation. Usually one or two fields are the culprit.

### 5. Inspect a specific item

```sql
SELECT brn, broker_name, match_status, match_confidence, raw
FROM brokers
WHERE run_id = '<run_id>' AND brn = '<brn>';
```

`raw` is the JSONB blob of everything we extracted — diff against what's currently on PF if you suspect bad parsing.

## Alerts — what they mean

Each alert title corresponds to a monitor or set of monitors. Severity colours: red = critical, yellow = warning, green = ok.

| Alert title | Likely cause | Where to look |
|---|---|---|
| `<spider> — OK` (green) | clean run | Card itself. Nothing to do. |
| `<spider> — WARNING` | a warning-severity monitor failed | Card lists which. Most often `extract/*` rates above warning threshold. |
| `<spider> — CRITICAL` | a critical-severity monitor failed | Card lists which. See `monitors.md` for per-monitor playbooks. |
| `Circuit breaker tripped` (mid-run) | error count or 429 count exceeded threshold during run | The card body has the live counters. Run was force-closed; investigate before restarting. |

The summary card always fires once per run (regardless of pass/fail) — green cards are the "all clear" signal, not noise. If you've been getting reds and then a green lands, the issue is fixed.

### Common alert → action mappings

| Card lists this | Read this section in [`monitors.md`](monitors.md) |
|---|---|
| `ZeroItemsMonitor` | Spider crashed before extracting anything. First ERROR line in the log file is the cause. |
| `ExtractionFailureMonitor.test_*_fallback_rate` | PF schema drift. The exact counter narrows it down — see [`monitors.md`](monitors.md). |
| `ValidationFailureByFieldMonitor` | A specific field stopped validating. Check `bad_items.payload` for that field. |
| `PipelineFailureMonitor.test_postgres_*` | Validation drops likely. Confirm against `bad_items`. |
| `PipelineFailureMonitor.test_gsheets_*` | Look at the `gsheets/*` stats and the run log for `gsheets pipeline ready` + flush errors. |
| `PipelineFailureMonitor.test_gdrive_csv_*` | Drive permissions or quota. Local CSV in `out/` is the recovery source (Phase 12 will replay). |
| `MatchStatusDistributionMonitor` + `AmbiguousRateMonitor` | PF stopped exposing BRN in search-page JSON. Check `extract/search_json/fallback_used`. |
| `BRNDriftMonitor` | Real-world disagreement PF↔DLD. Don't silence — investigate per-row (see [`monitors.md`](monitors.md)). |
| `MatchedRowFieldCoverageMonitor.test_*_field_coverage` | One or more fields stopped extracting. Card lists offenders with rates. |

## Bootstrap operations

### Run a new migration

```bash
poetry run python -m broker_scout.tools.migrate
```

Idempotent — re-running already-applied migrations is a no-op (every `CREATE` uses `IF NOT EXISTS`).

### Refresh DLD broker list

```bash
poetry run python -m broker_scout.tools.fetch_dld
```

Run this on a weekly cron (currently manual). Spider's seeded broker list comes from `dld_brokers WHERE active = TRUE`.

### Bootstrap Google OAuth (one-time, dev machine with browser)

```bash
poetry run python -m broker_scout.tools.oauth_setup
```

Writes `secrets/oauth_token.json`. Copy that file (along with `secrets/oauth_client.json`) to any server you deploy to — never re-run on a headless server.

Detailed Google setup is in [`README.md`](../README.md).

### Add an OAuth viewer to monthly Sheets

`GSHEET_VIEWER_EMAILS=alice@example.com,bob@example.com` in `.env`. Auto-shares (role: reader) at next monthly rotation. Viewers added after a sheet is created have to be granted manually in the Drive UI for that month.

## Reading run output

### Did the run actually succeed?

```sql
SELECT run_id, status, items_scraped, items_dropped,
       stats->>'finish_reason' AS finish_reason
FROM scrape_runs
WHERE started_at > now() - interval '1 day'
ORDER BY started_at DESC;
```

`status='ok'` AND `finish_reason='finished'` AND `items_dropped=0` is the green case.

### How many items in each match bucket?

```sql
SELECT match_status, COUNT(*)
FROM brokers
WHERE run_id = '<run_id>'
GROUP BY match_status
ORDER BY 2 DESC;
```

Healthy distribution: `exact_brn` and `name_unique` together ≥60% of items, `not_found` < 50%, `ambiguous` < 5% (mirrors monitor thresholds).

### Which fields are dropping coverage?

```sql
WITH matched AS (
  SELECT * FROM brokers WHERE run_id = '<run_id>' AND match_status IN ('exact_brn', 'name_unique', 'name_fuzzy')
)
SELECT
  COUNT(*) FILTER (WHERE broker_name IS NOT NULL) * 1.0 / COUNT(*) AS broker_name,
  COUNT(*) FILTER (WHERE listings_total > 0)      * 1.0 / COUNT(*) AS listings_total,
  COUNT(*) FILTER (WHERE closed_deals_total > 0)  * 1.0 / COUNT(*) AS closed_deals_total
FROM matched;
```

Run this when `MatchedRowFieldCoverageMonitor` alerts to see live numbers.

## Common failure modes

### "Spider returns 404 for everything"

PF rejects bare `/search?text=...` without session cookies. `BaseBrokerSpider.warmup_url` is supposed to seed those cookies on the first request. If the warmup itself returns non-200, every subsequent search gets 404.

Check:
```bash
curl -I https://www.propertyfinder.ae/en/find-agent
```

If that returns a non-200 from your IP, you may need a proxy. Set `PROXY_URL` in `.env`.

### "GSheets capacity error"

```
SheetsCapacityError: sheet '<id>' has insufficient capacity: used=9,500,000 …
```

The active monthly spreadsheet is filling. Sheets cap is 10M cells; `pre_flight_capacity_check` raises when projected run size exceeds 90% of remaining capacity. Either:
- Wait for next month's rotation.
- Manually trigger rotation by inserting a row into `sheet_registry` for next period (deactivate current).
- Reduce columns in `_SHEET_HEADERS` (rare; that's a schema change).

### "Discord/Chat alert never arrived"

Check in order:
1. `ALERT_BACKEND` in `.env` — explicit selection wins; without it, presence of `DISCORD_WEBHOOK_URL` (preferred, works on personal accounts) → `GOOGLE_CHAT_WEBHOOK_URL` → LogOnly.
2. Webhook URL valid? `curl -X POST -H 'Content-Type: application/json' --data '{"content":"test"}' "$DISCORD_WEBHOOK_URL"` should produce a message.
3. Run log for `alert_log insert failed` or `webhook URL unset`.
4. `SELECT level, title, sent_at FROM alert_log ORDER BY sent_at DESC LIMIT 5;` — if rows exist, the post succeeded; the issue is on Discord's side.
5. Anti-spam: if `(level, title)` was sent in the last 30 minutes, `recent_alert_exists` returns True and the action no-ops. This is intended for circuit-breaker dedupe.

### "Old log files filling disk"

`logs/` is auto-pruned at the start of every run via `LOG_RETENTION_DAYS` (default 30). If you've been disabling pruning (`LOG_RETENTION_DAYS=0`) you have to clean up by hand:

```bash
find logs/ -name '*.log' -mtime +30 -delete
```

Re-enable pruning by removing the override.

### "Spider hangs indefinitely on a single broker"

Listings API for a high-volume broker can paginate to hundreds of pages. A clean `CLOSESPIDER_TIMEOUT` setting in `.env` is your safety net — set to a number of seconds your run shouldn't exceed.

## Maintenance jobs

| Job | Frequency | Command |
|---|---|---|
| DLD ingest | Weekly | `poetry run python -m broker_scout.tools.fetch_dld` |
| Spider run | Weekly | `poetry run scrapy crawl agent_spider` |
| Schema migrations | On deploy | `poetry run python -m broker_scout.tools.migrate` |
| `out/` CSV cleanup | Operator cron, ad-hoc | `find out/ -name '*.csv' -mtime +7 -delete` |
| `logs/` cleanup | Built-in (auto-prune) | (auto, controlled by `LOG_RETENTION_DAYS`) |
| Sheet rotation | Auto (monthly) | (no action — pipeline handles) |

## When something breaks at 3am

A short triage sequence:

1. **Was the run started?** `SELECT MAX(started_at) FROM scrape_runs;` — if old, check the cron / scheduler, not the spider.
2. **Did it finish?** `SELECT finished_at, status FROM scrape_runs ORDER BY started_at DESC LIMIT 1;`
3. **Pipelines OK?** Look at `stats->>'gsheets/flush_failed'`, `stats->>'gdrive_csv/upload_status'`. If these are non-OK but Postgres is fine, **the data is safe**. Replay (Phase 12) can re-fill Sheets/Drive from `out/*.csv`.
4. **Postgres OK?** `SELECT items_scraped, items_dropped FROM scrape_runs ORDER BY started_at DESC LIMIT 1;` matched against the spider's `match/*` total.
5. **Read the alert.** It will tell you which monitor failed; match against [`monitors.md`](monitors.md).
6. **Read the run log.** First ERROR is usually the actual cause; everything below it is consequences.

If you can't tell what went wrong from those steps, the spider's lifecycle has a deeper issue — escalate to whoever wrote the spider (or open a roadmap entry).
