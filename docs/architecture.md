# Architecture

## What this system does

Pull DLD's licensed-broker registry, search each broker on PropertyFinder (and Bayut, Phase 8), match candidates back to the regulator's BRN, enrich with listings + closed-deal stats, and persist the result to three sinks:

1. **Postgres** — authoritative, queryable, retains the raw JSONB blob for forensics.
2. **Google Sheets** — monthly per-platform spreadsheet for human browsing.
3. **Google Drive CSV** — per-run archive for replay / forensics.

Spidermon watches the run end-to-end; failures trigger Discord or Google Chat alerts.

## Data flow

```
                  ┌─────────────────────────────────────────────────────────┐
                  │  tools/fetch_dld.py   (separate, weekly cron)           │
                  │  DLD registry API ──► dld_brokers table                  │
                  └─────────────────────────────────────────────────────────┘
                                            │
                                            ▼
   ┌────────────────────────────────────────────────────────────────────────┐
   │                       broker_scout (Scrapy spider)                      │
   │                                                                         │
   │  RunIdExtension (pri 100)                                              │
   │     · uuid4 → spider.run_id, contextvar, log file attach                │
   │     · prune logs older than LOG_RETENTION_DAYS                          │
   │                                                                         │
   │  BaseBrokerSpider.start_requests                                        │
   │     · warmup GET → seed cookies                                         │
   │     · iter_active_brokers from dld_brokers                              │
   │     · for each: search_for_broker(dld_broker)                           │
   │                                                                         │
   │  AgentSpider (PropertyFinder)                                           │
   │     parse_search_results                                                │
   │       · extract_candidates (JSON-first, HTML fallback)                  │
   │       · match_candidates → exact_brn | name_unique | name_fuzzy |       │
   │                            ambiguous | not_found                        │
   │     parse_disambiguating_profile (only when status='ambiguous')         │
   │     parse_agent  → parse_agency  → parse_property  (paginated)          │
   │                                                                         │
   │  → emits dict items (always one per DLD broker, even on no-match)       │
   │                                                                         │
   │                                  │                                      │
   │  ITEM_PIPELINES (priority order) ▼                                      │
   │                                                                         │
   │  200 ValidationPipeline      pydantic validate; bad → spider.bad_items  │
   │  400 PostgresPipeline        buffer 500, flush; engine_stopped re-snap  │
   │  500 GSheetsBatchPipeline    buffer 2000, append to monthly sheet       │
   │  600 GDriveCsvPipeline       per-run CSV → Drive folder on close        │
   │                                                                         │
   │  Spidermon (pri 500)                                                    │
   │     · PeriodicMonitorSuite — every 60s, circuit-breakers (errors, 429s) │
   │     · SpiderCloseMonitorSuite — engine_stopped, final-state checks      │
   │       fail → SendChatSummaryAction → Discord or Google Chat             │
   │                                                                         │
   └────────────────────────────────────────────────────────────────────────┘
```

## Layered responsibilities

Each layer is allowed to fail in a bounded way without breaking the layer below it (RULES §0 "defence-in-depth").

| Layer | Responsibility | If it fails |
|---|---|---|
| DLD client (`common/dld_client.py`) | Pull broker list from DLD API. | Tenacity retry; tools/fetch_dld.py exits non-zero. |
| DLD repo (`common/dld_repo.py`) | Upsert `dld_brokers` table. Becomes the seed list. | UNIQUE on `brn` makes re-runs idempotent. |
| Spider (`spiders/agent_spider.py`) | One DLD broker → one item, **always**. Even on 404, ambiguous, no-match. | Stub item with `match_status='not_found'/'ambiguous'`; `extract/*` counter. |
| PF extractors (`spiders/_pf_extractors.py`) | JSON-shape transforms only. No requests, no state. | HTML fallback path increments `extract/*/fallback_used`. |
| Matching (`common/matching.py`) | Disambiguate PF candidates against DLD ground truth. BRN-first, then exact name, then fuzzy. | Returns `ambiguous` or `not_found` rather than guessing. |
| Validation (`pipelines/validation.py`) | Run pydantic schema; route bad items to `spider.bad_items`. | DropItem on rejection; `validation/failed_field/{f}` counter. |
| Postgres pipeline (`pipelines/postgres.py`) | Authoritative store. Buffer + batch flush + bad_items drain. | `engine_stopped` swallow; `pipeline/flush_failed` counter. |
| GSheets pipeline (`pipelines/gsheets.py`) | Append to active monthly spreadsheet. | Final flush failure swallowed + logged + `gsheets/flush_failed=1`. Postgres + Drive CSV continue. |
| GDrive CSV pipeline (`pipelines/gdrive_csv.py`) | Per-run CSV → Drive. | Upload failure swallowed + `gdrive_csv/upload_status=failed`. Local CSV retained for replay. |
| Monitors (`monitors/monitors.py`) | Read final stats; assert thresholds; surface failures. | LogOnlyAction logs; SendChatSummaryAction posts the card; SendCriticalChatAlertAction fires mid-run. |

## Why these specific choices

### Postgres-as-truth

Sheets and Drive CSV are derivative views. Anything that affects downstream behaviour (status, counts, match outcomes) must be in Postgres before it's announced (RULES §0.2). This is enforced by the priority order: 400 (Postgres) before 500 (Sheets) before 600 (Drive CSV). A failed Postgres write means the item never reaches the other sinks.

### One item per DLD broker, always

The match-status state machine produces one of `exact_brn | name_unique | name_fuzzy | ambiguous | not_found | unknown` for every DLD row. This invariant lets monitors compute meaningful rates (`match/{status}/scraped`) and lets coverage SQL filter to matched rows in one place (RULES §11.4). Without it, a "broker missing from PF" looks identical to "spider crashed mid-extract."

### `engine_stopped`, not `spider_closed`

Pipelines hook `spider_closed` to flush their final batches. Monitors must read post-flush stats. `engine_stopped` fires once every `spider_closed` handler completes, so by then every `postgres/`, `gsheets/`, and `gdrive_csv/` counter is final. Using `spider_closed` for monitors races the pipelines (RULES §11.1).

### BRN-first matching

The regulator's BRN is the strongest unique key. PF's search-page JSON exposes each candidate's BRN inline (`compliances[?type=='brn'].value`), so we can confirm the regulator's identifier without ever fetching a profile. Name-based matching (exact → fuzzy) is the fallback when no candidate's BRN matches.

### Per-run file logging + auto-prune

Every spider run writes a JSON log to `logs/{spider}_{run_id}.log`. `RunIdExtension` attaches the FileHandler at `spider_opened`, detaches at `spider_closed`. Files older than `LOG_RETENTION_DAYS` are pruned at the start of each run. Mirrors the per-run `out/*.csv` archive pattern (RULES §14.5).

## Component map

```
broker_scout/broker_scout/
├── settings.py            EXTENSIONS, ITEM_PIPELINES, all env-var reads
├── extensions.py          RunIdExtension (run_id, log file lifecycle)
├── items.py               @dataclass items (spider-emitted shape)
├── schemas.py             pydantic schemas (validation rules)
├── common/
│   ├── db.py              lazy psycopg connection pool
│   ├── dld_client.py      DLD HTTP client with tenacity retry
│   ├── dld_repo.py        dld_brokers upsert + iter_active_brokers
│   ├── brokers_repo.py    scrape_runs / brokers / bad_items / alert_log + matched-row coverage
│   ├── sheets_repo.py     monthly-rotation Sheet resolver + append + capacity guard
│   ├── matching.py        Candidate / MatchResult / match_candidates / promote_to_brn_match
│   ├── normalizers.py     name normalization (used by matching)
│   ├── dld_models.py      DLDBroker dataclass
│   └── run_context.py     RunContext contextvar (run_id / scrape_date / spider)
├── monitors/
│   ├── monitors.py        12 custom monitors + 2 suites
│   ├── actions.py         LogOnlyAction, CloseSpiderAction, SendChatSummaryAction, SendCriticalChatAlertAction
│   ├── notifiers.py       Notifier protocol + Discord, Google Chat, LogOnly
│   └── coverage_tiers.py  Provenance / Critical / High / Medium / Informational field tiers
├── pipelines/
│   ├── validation.py      pydantic check → dict | spider.bad_items
│   ├── postgres.py        buffered insert + bad_items drain + engine_stopped re-snap
│   ├── gsheets.py         monthly-rotation append + capacity guard
│   └── gdrive_csv.py      per-run CSV → Drive
├── spiders/
│   ├── base.py            BaseBrokerSpider (DLD seeding, stub emission)
│   ├── agent_spider.py    PropertyFinder spider — callbacks + request flow
│   └── _pf_extractors.py  PropertyFinder JSON-shape transforms
├── tools/
│   ├── fetch_dld.py       weekly DLD ingest CLI
│   ├── migrate.py         idempotent migration runner
│   └── oauth_setup.py     one-time OAuth bootstrap
└── utils/
    ├── gauth.py           OAuth user creds + Sheets/Drive clients (lazy singletons)
    └── logging_setup.py   JSON / pretty formatters + per-run FileHandler + prune
```

## Extending with a new platform

The DLD client, matching layer, validation pipeline, Postgres pipeline, and monitors are platform-agnostic. Adding Bayut (Phase 8) is mostly:

1. New file `spiders/bayut.py` — subclass `BaseBrokerSpider`, set `name`/`platform`/`warmup_url`/`handle_httpstatus_list`, implement `search_for_broker` + `parse_search_results` + a `parse_agent` that yields `BayutBrokerItem` (or extends `PropertyFinderBrokerItem` if fields overlap).
2. New sibling `spiders/_bayut_extractors.py` — pure JSON-shape transforms, never imported outside the bayut spider. Same shape as `_pf_extractors.py`.
3. If new fields land in items: add to `items.py`, `schemas.py`, a new migration, `_BROKER_COLUMNS` in `brokers_repo.py`, `_SHEET_HEADERS` in `sheets_repo.py`, and a tier in `coverage_tiers.py` (the integrity assert will fail loudly until you do).
4. Add the platform to `_PLATFORM_CONFIG` in `sheets_repo.py` for monthly Sheet rotation.

Full checklist: [`RULES.md` §19](../RULES.md).

## Cross-cutting invariants worth knowing

These hold across every layer; breaking one causes correctness drift, not loud failures:

- Every emitted item is a plain `dict` from `ValidationPipeline` onward. Pipelines that follow accept dicts only, never the dataclass.
- Every emitted item has `match_status` set. Stubs (DLD-only, no PF profile) use one of `not_found`, `ambiguous`, or `unknown`.
- Every spider run produces exactly one `scrape_runs` row. The pipeline opens it on first item OR `spider_opened`, whichever fires first.
- Every `extract/*` and `match/*` counter is part of the public contract — monitors and the chat summary card read them by exact name.
