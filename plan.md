# Project Architecture — DLD × PropertyFinder × Bayut

Broker intelligence pipeline: pull the licensed-broker list from DLD, search each broker on PropertyFinder and Bayut, persist enriched records to Postgres + Google Sheets + Google Drive, and monitor everything with Spidermon + Google Chat alerts.

---

## 1. Decisions locked in

| Topic | Decision |
|---|---|
| Alert channel | **Google Chat webhooks** (single channel; severity in card colour) |
| DLD source | **DLD JSON API** (returns broker list directly; no HTML rendering) |
| Coverage model | DLD list is the universe. Each broker has a **BRN** that uniquely identifies them. After every run we report: found-on-PF, found-on-Bayut, found-on-both, found-on-neither. |
| Run cadence | **Weekly** |
| Database hosting | **Will's server in production**, local Postgres for dev/testing |
| Sheets layout | **Separate Google Sheet per website** (one for PF, one for Bayut) |
| Postgres hosting details | TBD — discuss with Will |
| Matching thresholds | TBD — set sensible defaults, refine after seeing real data |

---

## 2. High-level workflow

```
                    ┌──────────────────────────────────┐
                    │  DLD broker list (JSON API)      │   one fetch per run, cached
                    │  ~30k brokers, each has a BRN    │
                    └────────────────┬─────────────────┘
                                     │
                  ┌──────────────────┴──────────────────┐
                  ▼                                     ▼
        PropertyFinderSpider                       BayutSpider
        search by name → match on BRN              search by name → match on BRN
                  │                                     │
                  ▼                                     ▼
        Item per DLD broker                       Item per DLD broker
        (matched | ambiguous | not_found)         (matched | ambiguous | not_found)
                  │                                     │
                  ▼                                     ▼
        Postgres + PF Sheet + PF CSV              Postgres + Bayut Sheet + Bayut CSV
```

Both spiders consume the **same** DLD snapshot — coverage numbers across PF and Bayut are therefore directly comparable by BRN.

---

## 3. Project structure

```
will_hazlewood_scraper/
├── scrapy.cfg
├── pyproject.toml                    # poetry/uv — pinned deps
├── .env.example                      # DB creds, GSheet IDs, webhook URL, proxy
├── docker-compose.yml                # postgres for local dev
├── README.md
├── deploy/
│   ├── Dockerfile
│   └── crontab                       # weekly schedule on Will's server
├── sql/
│   └── migrations/                   # versioned schema
│       ├── 001_init.sql
│       └── 002_monitoring.sql
└── broker_scout/
    ├── __init__.py
    ├── settings.py
    ├── items.py                      # dataclass-based Scrapy items
    ├── schemas.py                    # pydantic validation models
    ├── middlewares.py                # proxy rotation, playwright, retry tweaks
    ├── extensions.py                 # spidermon hookup, run-id, stats persist
    ├── common/
    │   ├── dld_client.py             # DLD API client — shared by both spiders
    │   ├── dld_cache.py              # snapshot per run (one fetch reused)
    │   ├── normalizers.py            # phone, name, BRN, AED, dates
    │   ├── matching.py               # BRN-first match, name-fallback, confidence
    │   └── run_context.py            # run_id, scrape_date, spider_label
    ├── spiders/
    │   ├── __init__.py
    │   ├── base.py                   # BaseBrokerSpider — DLD seeding, common hooks
    │   ├── propertyfinder.py
    │   └── bayut.py
    ├── pipelines/
    │   ├── __init__.py
    │   ├── normalization.py
    │   ├── validation.py             # pydantic; bad rows → bad_items table
    │   ├── dedupe.py
    │   ├── postgres.py               # batched COPY/executemany
    │   ├── gsheets.py                # buffered, flushed in close_spider
    │   ├── gdrive_csv.py             # writes CSV → uploads on close_spider
    │   └── stats_writer.py           # persist run stats to postgres
    ├── monitors/
    │   ├── __init__.py
    │   ├── monitors.py               # Spidermon Monitor classes
    │   └── actions.py                # GoogleChatNotifier action
    └── utils/
        ├── gauth.py                  # service account loader (sheets+drive)
        ├── logging_setup.py          # JSON logs with run_id
        └── retry.py                  # tenacity wrappers
```

**Why this shape**

- `common/` is the reusable DLD layer — both spiders import from it.
- `pipelines/` is split per sink so each can be enabled/disabled cleanly.
- `monitors/` lives in-repo (Spidermon convention), wired in `extensions.py`.
- `schemas.py` separates validation rules from the Item plumbing.

---

## 4. DLD layer (shared)

### 4.1 `common/dld_client.py`

- One entry point: `fetch_all_brokers(run_id) -> Iterator[DLDBroker]`.
- Internally paginates the DLD API, retries with `tenacity`, persists a snapshot to `dld_snapshots/{run_id}.jsonl` so re-running PF or Bayut in the same session does not re-hit DLD.
- Helper: `load_snapshot(run_id) -> Iterator[DLDBroker]` — used by spiders.

### 4.2 `spiders/base.py` — `BaseBrokerSpider`

```python
class BaseBrokerSpider(Spider):
    platform: str = ""  # "propertyfinder" | "bayut"

    def start_requests(self):
        run_id = self.settings["RUN_ID"]
        for broker in dld_client.iter_brokers(run_id):
            yield from self.search_for_broker(broker)

    def search_for_broker(self, dld_broker): ...   # abstract
```

The DLD record rides along on `cb_kwargs` / `meta` so the final item carries both the DLD ground truth and the platform-specific extracts. The `BRN` is the linking key end-to-end.

### 4.3 Matching (`common/matching.py`)

A name search returns 0, 1, or several profiles. Resolve to one of:

- `match=exact_brn` — BRN found on profile and equals DLD BRN. (Highest confidence.)
- `match=name_unique` — single result, normalized name equals DLD name.
- `match=name_fuzzy` — single result, token-set ratio ≥ threshold.
- `match=ambiguous` — >1 plausible result; emit item with `match_status=ambiguous`, do not pick.
- `match=not_found` — emit a "DLD-only" stub row so coverage is auditable.

**Always emit an item per DLD broker per platform**, even on `not_found`. Coverage = count by `match_status` per platform per run.

---

## 5. Items, schemas, validation

Two layers, intentionally separated.

**Layer A — Scrapy Item (`items.py`)**
A flat `@dataclass` Item per platform: `PropertyFinderBrokerItem`, `BayutBrokerItem`. Includes DLD fields + platform fields + provenance: `run_id`, `scrape_date`, `source_url`, `match_status`, `match_confidence`.

**Layer B — Pydantic schema (`schemas.py`)**
Same fields, with validators: BRN regex, phone E.164 normalization, AED-as-`Decimal`, date parsing, enums for `match_status`. The validation pipeline runs `Model.model_validate(dict(item))` and either:

- passes the dict downstream (with normalized values), or
- on `ValidationError`: drops + logs to a `bad_items` table with the reason. Spidermon picks this up.

**Field-level rules to encode**

- `brn`: required for matched rows, regex (verify against DLD samples).
- `whatsapp_response_time`: int ≥ 0 OR null (kills the `-22` bug we saw in `output.json`).
- `listings_for_sale`, `listings_for_rent`: int ≥ 0.
- `experience_since`: 4-digit year, ≥ 1990.
- monetary fields: `Decimal`, ≥ 0.
- `phone`: E.164 (`+9715…`), normalized in `normalizers.py`.

---

## 6. Pipelines

`ITEM_PIPELINES` order:

```
NormalizationPipeline    100   trim, lowercase emails, parse dates
ValidationPipeline       200   pydantic; DropItem on hard failures
DedupePipeline           300   in-memory set keyed on (platform, brn)
PostgresPipeline         400   primary store, batched writes
GSheetsBatchPipeline     500   buffered, flushed in close_spider
GDriveCsvPipeline        600   writes CSV during run, uploads in close_spider
```

### 6.1 Postgres

- Driver: `psycopg[binary,pool]` v3.
- Write strategy: `COPY FROM` via `psycopg.copy` for bulk, or `executemany` with `execute_values` in batches of 500–1000. Never one row at a time.
- Local Postgres in Docker for dev; production runs on Will's server.

**Schema sketch**

```sql
CREATE TABLE scrape_runs (
  run_id        UUID PRIMARY KEY,
  spider        TEXT NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  finished_at   TIMESTAMPTZ,
  status        TEXT,            -- running|ok|failed
  items_scraped INT,
  items_dropped INT,
  stats         JSONB
);

CREATE TABLE brokers (             -- append-only; one row per (run, brn, platform)
  id            BIGSERIAL PRIMARY KEY,
  run_id        UUID REFERENCES scrape_runs(run_id),
  scrape_date   DATE NOT NULL,
  platform      TEXT NOT NULL,    -- 'propertyfinder' | 'bayut'
  brn           TEXT,
  match_status  TEXT NOT NULL,    -- exact_brn|name_unique|name_fuzzy|ambiguous|not_found
  -- … all platform fields …
  raw           JSONB,             -- unflattened payload for forensics
  UNIQUE (run_id, platform, brn)
);
CREATE INDEX ON brokers (brn);
CREATE INDEX ON brokers (scrape_date);

CREATE TABLE bad_items (           -- validation failures land here
  id BIGSERIAL PRIMARY KEY,
  run_id UUID, platform TEXT, reason TEXT, payload JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE alert_log (           -- dedupe noisy alerts
  id BIGSERIAL PRIMARY KEY,
  run_id UUID, level TEXT, title TEXT, body TEXT,
  sent_at TIMESTAMPTZ DEFAULT now()
);
```

Postgres is **append-only**, so weekly snapshots stack up cleanly. `(run_id, platform, brn)` unique constraint makes flush retries idempotent (`ON CONFLICT DO NOTHING`).

### 6.2 Google Sheets — separate sheet per website

Two spreadsheets, owned by the same service account:

- `PropertyFinder Brokers` (sheet ID in `.env` as `GSHEET_PF_ID`)
- `Bayut Brokers` (sheet ID in `.env` as `GSHEET_BAYUT_ID`)

Each sheet has worksheets:

- `brokers` — one row per (run, BRN). Append-only.
- `_runs` — one row per scrape run (mirrors `scrape_runs`).
- `_coverage` — per-run coverage roll-up (matched / ambiguous / not_found / total).

**Batch model**

- Pipeline buffers items in memory.
- Flush every 2000 items and once at `close_spider` via `spreadsheets.values.append` (`valueInputOption=RAW`, `insertDataOption=INSERT_ROWS`).
- One API call per flush. 30k rows / 2k batch = 15 API calls per run — well under quota.

**Auth**

- Single service account JSON, shared with both Sheets and the Drive folder.
- README must include: "share each Sheet AND the Drive folder with `xxx@xxx.iam.gserviceaccount.com`."

**Capacity note**

- Sheets cell limit is 10M per spreadsheet. 30k rows × ~50 cols × weekly runs will hit this in roughly a year. Plan for an annual rollover sheet, or treat Sheets as a "recent runs" view and use Postgres for full history. Decide before we ship.

### 6.3 Google Drive CSV

- During the run, write rows to `out/{spider}_{run_id}.csv`.
- On `close_spider`: upload via Drive API to a configured folder, named `{spider}_{YYYYMMDD-HHMM}.csv`.
- Resumable upload for files > 5 MB.
- Keep local copy for 7 days (cron purge).

---

## 7. Spidermon — monitoring

Wired as a Scrapy extension in `extensions.py`. Two layers of monitors.

### 7.1 Per-run monitors (run on `spider_closed`, plus periodic during run)

| Monitor | Threshold | Severity |
|---|---|---|
| `ItemCountMonitor` | ≥ 80% of last-4-weeks median | critical |
| `FieldCoverageMonitor` (`brn`, `broker_name`) | ≥ 95% present | critical |
| `FieldCoverageMonitor` (`listings_total`, `nationality`) | ≥ 70% present | warning |
| `ValidationFailureMonitor` (bad_items rate) | < 5% of total items | critical |
| `ErrorCountMonitor` | < 50 errors | warning |
| `HTTP4xxMonitor` (excl. 404) | < 100 | warning |
| `HTTP5xxMonitor` | < 50 | critical |
| `RuntimeMonitor` | within 50%–200% of 4-week median | warning |
| `PipelineFailureMonitor` (custom) | 0 failures across postgres/sheets/drive | critical |
| `MatchCoverageMonitor` (custom) | matched rate ≥ 80% of last run | warning |

### 7.2 Cross-run monitor (queries `scrape_runs` history)

- Item count drop > 20% vs prior run → critical.
- Matched-broker count drop > 10% vs prior run → warning (matching may have regressed).
- Per-field median drift > 30% (e.g. `listings_total`) → warning.

The 4-week medians are stored in `scrape_runs.stats` (JSONB). `pipelines/stats_writer.py` writes that on `spider_closed`; monitors read it.

---

## 8. Alerts — Google Chat webhook

Single notifier interface:

```python
class Notifier(Protocol):
    def send(self, level: str, title: str, body: str, run_id: str) -> None: ...
```

Default implementation: `GoogleChatNotifier` — POST to webhook, formatted as a Chat card with severity-coloured header.

Config (`.env`):

```
ALERT_BACKEND=google_chat
GOOGLE_CHAT_WEBHOOK_URL=https://chat.googleapis.com/...
ALERT_MIN_LEVEL=warning   # promote to critical-only after stabilization
```

**Anti-spam**

- Dedupe identical alerts within 30 minutes via `alert_log` table.
- One **summary message at end of run** (success or failure), plus critical-only mid-run alerts.

**Example end-of-run message**

```
PropertyFinder weekly scrape — OK
Run: 2026-04-29 02:14 UTC
Items: 28,431 (matched 24,102 · ambiguous 311 · not_found 4,018)
Validation failures: 142 (0.5%)
Runtime: 1h 47m
Sheet: <link>  Drive CSV: <link>
```

---

## 9. Reliability — operational concerns

- **Run identity** — `run_id = uuid4()` generated in `extensions.py`, propagated via `crawler.settings.set("RUN_ID", …)` and into every log line, item, DB row.
- **Proxies** — Smartproxy residential, via `HttpProxyMiddleware` + custom rotating middleware. Per-request retry on `407/403/429/5xx` with exponential backoff. Don't share one proxy across the whole run.
- **Concurrency tuning** — `CONCURRENT_REQUESTS_PER_DOMAIN=4`, `DOWNLOAD_DELAY=0.5`, `AUTOTHROTTLE_ENABLED=True`. Conservative — anti-bot triggers fast on PF and Bayut.
- **Resumable runs** — `JOBDIR=jobs/{spider}` so a killed run resumes. Important at 30k brokers.
- **Idempotent flushes** — Postgres unique constraint makes batch retries safe. Sheets/Drive failures: alert and replay from Postgres via `tools/replay_run.py`.
- **Source-of-truth model** — Postgres is authoritative. Sheets and Drive are derivative views.
- **Logging** — structured JSON (`python-json-logger`), `run_id` and `spider` on every record.
- **Secrets** — `.env` locally; Vault / GitHub secrets in deploy. Never commit `service_account.json`.
- **Schedule** — cron on Will's server, weekly. Two cron lines (PF and Bayut) staggered by a few hours so they don't fight for proxies.

---

## 10. Improvements proposed beyond brief

1. **Postgres as source of truth, Sheets as the view layer.** Sheets alone won't survive multi-month accumulation at 30k rows/week. Frame this for Will as "Sheets is what you read; Postgres is what powers it."
2. **Coverage report, not just data dump.** `_coverage` worksheet showing matched / ambiguous / not_found per run turns this into a usable funnel.
3. **Diff alerts.** Once we have history, weekly "23 new brokers, 4 brokers disappeared from PF, 12 changed agency" is far more valuable than "scrape succeeded".
4. **Dry-run mode** (`--no-write`) for testing matching logic without polluting Sheets/Postgres.
5. **DLD snapshot retention** — keeps DLD-side changes auditable independent of platform scraping.

---

## 11. Open items still to confirm with Will

1. **Postgres hosting details** — where exactly on his server, version, backup policy.
2. **Match thresholds** — fuzzy ratio cutoff, whether `not_found` rows should also write to Sheets or only Postgres.
3. **Sheet rollover policy** — annual rollover, or trim Sheets to last N runs.
4. **Drive folder layout** — single folder, or per-spider folders.

---

## 12. Suggested build order

1. `common/dld_client.py` + DLD snapshot caching.
2. Postgres schema migration (`001_init.sql`) + `pipelines/postgres.py`.
3. `BaseBrokerSpider` + refactor existing PF spider to consume DLD seeds.
4. Matching layer + `match_status` instrumentation.
5. Validation pipeline (pydantic) + `bad_items` sink.
6. Google Sheets batch pipeline (PF sheet first).
7. Google Drive CSV pipeline.
8. Spidermon monitors + Google Chat notifier.
9. Bayut spider — reuses everything above.
10. Cron + deploy on Will's server.
