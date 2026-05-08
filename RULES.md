# Project Rules — broker_scout

This document captures **how we write code in this repo**. Read it before contributing. When something here disagrees with what's in the codebase, fix one or the other and update this document.

The rules are not aspirational — they describe the conventions already in place across Phases 0–11. New code should follow them. Deviations need a written reason in the commit message.

---

## 0. Working principles

Three durable preferences that override most micro-decisions:

1. **Loud failure beats silent wrong-data.** If we can't be certain, emit a stub with explicit status (`match_status='not_found'`, `gdrive_csv/upload_status='failed'`) rather than fabricating or skipping. Monitors then surface the signal. Never paper over a missing field by guessing.
2. **Postgres is the source of truth.** Sheets and Drive CSV are derivative views. Anything that could meaningfully change downstream behavior (status, counts, match outcomes) must be persisted to Postgres before being announced.
3. **Defence-in-depth at module boundaries.** Each layer (spider, pipeline, monitor) should still produce a usable result if the layer above it gave it incomplete input — but should also count the degradation via stats, so monitors can alert.

Concrete examples already in the code: `_make_dld_stub` in `BaseBrokerSpider` (loud-fail invariant), `engine_stopped` fix in `PostgresPipeline` (Postgres-as-truth), `_extract_profile_brn` HTML fallback (defence-in-depth) — all match counters surface the degradation through `extract/*` and `match/*` stats.

---

## 1. Project structure

```
broker_scout/
├── scrapy.cfg
├── pyproject.toml                     # Poetry-pinned deps
├── .env.example                       # all configurable env vars; secrets blank
├── docker-compose.yml                 # Postgres for local dev
├── README.md                          # operator setup
├── RULES.md                           # this file
├── plan.md                            # architecture decisions + rationale
├── roadmap.md                         # ordered work plan with checkboxes
├── sql/
│   └── migrations/                    # numbered `NNN_description.sql`, forward-only
├── tests/                             # pytest suite, mirrors source layout
└── broker_scout/
    └── broker_scout/
        ├── settings.py                # ALL env-var reading + ITEM_PIPELINES + EXTENSIONS
        ├── extensions.py              # Scrapy extensions (RunIdExtension, etc.)
        ├── items.py                   # @dataclass Items
        ├── schemas.py                 # pydantic validation models
        ├── middlewares.py             # Scrapy middlewares
        ├── pipelines.py / pipelines/  # use a package (directory) once >1 pipeline
        ├── common/                    # platform-agnostic helpers + repos + clients
        ├── monitors/                  # Spidermon suites + actions + notifiers + tiers
        ├── spiders/                   # one file per spider, plus `base.py`
        ├── tools/                     # CLI scripts (migrate, fetch_dld, oauth_setup)
        └── utils/                     # cross-cutting, no business logic (logging, gauth)
```

**Rules:**

- **One concern per file.** `common/dld_repo.py` does DLD persistence and nothing else. If a file's name no longer describes what's in it, split it.
- **Directories are packages.** Every directory under `broker_scout/` has an `__init__.py`. Keep them empty unless there's a real reason to expose something at package level.
- **`tools/` is for human or cron invocation.** Scripts there have `if __name__ == "__main__":` and a `main()` function. They never get imported by other code.
- **`utils/` is for code with no business meaning.** Logging setup, Google auth helpers — anything that could be lifted into another project unchanged. If it knows what a "broker" or "DLD" is, it doesn't belong here.
- **`common/` is for project-specific helpers shared across spiders or pipelines.** This is where DLD client, matching, repos, and normalizers live.

---

## 2. Spiders (`spiders/`)

### 2.1 Inheritance

All platform spiders extend `BaseBrokerSpider`. The base owns:
- `start_requests` (warmup → DLD seeding → `search_for_broker`)
- `_make_dld_stub` (every DLD broker emits one item, even on no-match)
- DLD limit (`DLD_LIMIT`) and BRN filter (`DLD_BRN_FILTER`) handling

A platform spider implements two abstract methods:
- `search_for_broker(dld_broker) -> Iterable[Request]`
- `parse_search_results(response, dld_broker)` → emits stub OR yields profile request

Subclasses set:
- `name` — Scrapy spider id (required by Scrapy)
- `platform` — `"propertyfinder"` | `"bayut"` (required by `_make_dld_stub`)
- `warmup_url` — landing-page GET to seed cookies (recommended; PF rejects bare `/search?` without it)
- `handle_httpstatus_list` — codes the spider must process itself (e.g. `[404]` so empty searches reach the callback rather than being dropped by `HttpErrorMiddleware`)

### 2.2 Callback state

- **Use `cb_kwargs` for new code.** It's strongly typed, documented, and the Scrapy team's recommended path forward.
- **`meta` is OK for legacy chains** (`parse_agent → parse_agency → parse_property`) where the chain is already working and changing it adds risk. Don't introduce new `meta` keys unless cb_kwargs would be awkward.

### 2.3 Always emit one item per DLD broker

Even when:
- Search returns 404 → stub with `match_status='not_found'`
- DLD broker has no name → stub with `match_status='not_found'`
- Multiple plausible candidates and BRN walk exhausts → stub with `match_status='ambiguous'`
- `parse_agent` can't extract `__NEXT_DATA__` → still emit the item with whatever fields succeeded plus DLD ground truth

This invariant is what lets monitors compute `match/{status}` rates and what makes coverage SQL queries sensible.

### 2.4 Defensive XPath / JSON parsing

Web pages change. Every selector should:
1. Tolerate missing nodes (`.get()` returns `None`, then `or fallback`).
2. Have a fallback path when reasonable (HTML → JSON, or vice versa).
3. Increment an `extract/*` counter on the fallback path so monitors can detect drift.

Pattern from `_extract_profile_brn`:
```python
brn = jmespath.search("compliances[-1].value", agent_data)
if not brn:
    fallback = response.xpath('.//td[contains(text(),"Dubai Broker License")]/following-sibling::td/text()').get()
    if fallback and fallback.strip():
        self.crawler.stats.inc_value("extract/brn/fallback_used")
        return fallback.strip()
```

### 2.5 Statistics counters in spiders

Use `self.crawler.stats.inc_value("namespace/event")` for:
- Match outcomes (`match/exact_brn`, `match/not_found`, `match/promoted_to_exact_brn`)
- Extraction degradation (`extract/next_data/missing`, `extract/brn/fallback_used`)
- Anything Phase 9 monitors need to alert on

Names use slash-separated namespaces. Stat names are part of the public contract — changing one breaks the monitors. List every emitted stat in `roadmap.md` §7 for discoverability.

---

## 3. Items + schemas + validation

### 3.1 Two-layer model (locked in since Phase 2)

| Layer | File | Purpose |
|---|---|---|
| **Item** (Scrapy plumbing) | `items.py` | `@dataclass(slots=True)`. Flat, no methods beyond `to_dict()`. Default to `None`. The spider produces these. |
| **Schema** (validation) | `schemas.py` | `pydantic.BaseModel` mirroring the item. Encodes ALL business rules: bounds, regexes, cross-field invariants. The pipeline runs `model_validate(item.to_dict())`. |

### 3.2 Schema rules

- **Every field is `Optional[...]` by default.** Required fields are explicit exceptions (e.g. `platform: Literal["propertyfinder"]` is the only truly required field).
- **`extra="forbid"`** — drift in `items.py` (typo'd field, removed field) fails validation loudly rather than silently dropping data.
- **`str_strip_whitespace=True`** — centralised stripping; spiders don't need to remember.
- **Use `Annotated[Optional[X], Field(ge=..., le=..., max_length=...)]`** for simple bounds.
- **Use `field_validator(mode="after")`** for time-sensitive checks (e.g. `experience_since` against `date.today().year`) so the cutoff is recomputed at validation time, not import time.
- **Use `model_validator(mode="after")`** for cross-field rules (`listings_total = sale + rent`, `listings_with_marketing_spend ≤ listings_total`). Raise `ValueError` with a `loc` when possible.
- **Constants for thresholds** at module level (`MAX_AED`, `MIN_DATE`, `MAX_LISTINGS_PER_BUCKET`). Tests import them so threshold tightening flows through.

### 3.3 Items in flight

After `ValidationPipeline`, items are **plain `dict`** (`model_dump(mode="json")` output). Every later pipeline must accept dicts, not the dataclass. This contract is documented in `pipelines/__init__.py` and asserted by tests.

---

## 4. Pipelines (`pipelines/`)

### 4.1 Priority order (locked)

```
200  ValidationPipeline
400  PostgresPipeline       (authoritative store)
500  GSheetsBatchPipeline   (derivative view)
600  GDriveCsvPipeline      (per-run archive)
```

Lower priority numbers run first. New sinks fit between gaps (e.g. 700 for a future warehouse export).

### 4.2 Lifecycle pattern (every pipeline that needs open/close hooks)

Use **signals**, not auto-wired methods:

```python
@classmethod
def from_crawler(cls, crawler):
    pipe = cls()
    crawler.signals.connect(pipe.spider_opened, signal=signals.spider_opened)
    crawler.signals.connect(pipe.spider_closed, signal=signals.spider_closed)
    # If post-pipeline state matters: also connect engine_stopped
    crawler.signals.connect(pipe.engine_stopped, signal=signals.engine_stopped)
    return pipe
```

Why signals over auto-wired `open_spider`/`close_spider`:
- `spider_closed` carries `reason`; the auto-wired method doesn't.
- Auto-wired `open_spider` runs **before** the `spider_opened` signal fires, so `RunIdExtension` hasn't yet set `spider.run_id`.
- `engine_stopped` is the only signal that fires AFTER all `spider_closed` handlers — required for monitors that read post-flush stats.

### 4.3 Buffering and flushing

For batched pipelines (Postgres at 500-row batches, Sheets at 2000-row batches):

```python
def _flush(self, spider):
    if not self._buffer:
        return
    rows = self._buffer
    self._buffer = []      # rebind, NOT clear()
    try:
        repo.flush(rows)
    except Exception:
        self._buffer = rows + self._buffer    # restore for retry
        raise
    spider.crawler.stats.inc_value("pipeline/rows_inserted", len(rows))
```

**Rebind, don't `clear()`.** A `clear()` mutates the list that may already be in flight to a downstream call (or test mock); rebinding decouples ownership cleanly.

### 4.4 When to swallow vs propagate

| Hook | Failure | Action |
|---|---|---|
| `process_item` | Pipeline error | Propagate. Scrapy increments `item_dropped_count`; PipelineFailureMonitor catches the mismatch. |
| Pre-`spider_closed` flush | Transient (network, rate-limit) | Retry via `tenacity`, then propagate. |
| `spider_closed` final flush | Same | Same — but log + increment a `pipeline/flush_failed` stat AND continue closing other state, so the run still records a status. Re-raise the exception only after that side-state is captured. |
| `engine_stopped` | Anything | **Swallow** + log. The Twisted reactor is shutting down; raising here surfaces as a cryptic "unhandled error in deferred" that obscures the real cause. |

### 4.5 The dict contract

Pipelines exchange items as plain `dict`s starting at priority 200. The first pipeline that receives a `PropertyFinderBrokerItem` dataclass converts via `dataclasses.asdict` (or `model.model_dump()` after pydantic validation). Every later pipeline accepts dict input only.

---

## 5. Common modules (`common/`)

### 5.1 The repo pattern

Every Postgres-touching module follows the same shape (`dld_repo.py`, `brokers_repo.py`, `sheets_repo.py`):

- **Module-level constants** declare the column tuple (`_COLUMNS`, `_BROKER_COLUMNS`).
- **Module-level SQL templates** built once from those constants (`_INSERT_SQL = f"INSERT ... ({_PLACEHOLDERS})"`).
- **Public functions** (`open_run`, `insert_brokers`, `update_run_stats`) take primitives or dataclasses, never `Cursor` or `Connection`.
- **`get_pool()`** from `common/db.py` is the only way connections are obtained. No module owns its own pool.
- **Lazy + thread-safe singletons** for connection-like resources. See `common/db.py` and `utils/gauth.py` for the exact pattern (`_lock = RLock()` + double-checked init).

### 5.2 Lazy clients

External-service clients (`get_pool`, `get_sheets_client`, `get_drive_client`) are lazy — first call constructs, subsequent calls return cached. Reasons:

1. Importing `broker_scout` in tests doesn't need to talk to Postgres or Google.
2. CLI tools (`migrate`, `fetch_dld`, `oauth_setup`) only construct the clients they actually use.
3. A `RLock` (re-entrant) is required because some constructors call each other (e.g. `get_sheets_client → _get_credentials`).

### 5.3 Identifier safety

Anywhere SQL composes column names from variables, use `psycopg.sql.Identifier`:

```python
cols = sql.SQL(", ").join(sql.Identifier(f) for f in fields)
query = sql.SQL("SELECT {cols} FROM brokers ...").format(cols=cols)
```

Even when the input is a constant from `coverage_tiers.py`, pretend it could become user-provided in the future. Same applies to `Jsonb(...)` for any dict that lands in a JSONB column.

---

## 6. Middlewares (`middlewares.py`)

We use Scrapy's defaults plus minimal extras. When a custom middleware lands:

- One responsibility per middleware. No "miscellaneous" middlewares.
- Subclass the smallest applicable base (`HttpProxyMiddleware`, `RetryMiddleware`).
- Document the priority and reasoning in the docstring — middleware ordering is a real part of the contract.

---

## 7. Settings + configuration (`settings.py`)

### 7.1 Single source of truth

`settings.py` is the only place env vars are read. Modules consume settings via `crawler.settings.getint(...)` / `getfloat(...)` / `get(...)`, never `os.getenv` directly.

The exceptions are modules that have to work outside the Scrapy spider context (CLI tools, Google API plumbing called by both pipelines and `tools/*.py`):

- `utils/gauth.py` — OAuth token path; called by `tools/oauth_setup.py` and the pipelines.
- `monitors/notifiers.py` — Discord/Chat webhook URLs; the `get_notifier()` factory runs at action-construction time without a crawler context.
- `common/db.py` — Postgres DSN; the lazy connection-pool singleton is called by `tools/migrate.py` and `tools/fetch_dld.py` outside spider runs (§5.2's lazy-singleton pattern).
- `common/sheets_repo.py` — `GSHEET_TEMPLATE_*_ID`, `GSHEET_*_FOLDER_ID`, `GSHEET_VIEWER_EMAILS`; same justification — sheet rotation is invoked from the pipeline AND from `template_header_row()` in operator setup commands.

Pipelines, monitors, and spiders **never** read env directly — they go through `crawler.settings`. New env vars added for spider-only behaviour must be declared in `settings.py` first.

### 7.2 Default + override pattern

For every tunable:

```python
# In monitors/monitors.py
DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD = 0.05

# Read at runtime
threshold = self.crawler.settings.getfloat(
    "VALIDATION_FAILURE_RATE_THRESHOLD",
    DEFAULT_VALIDATION_FAILURE_RATE_THRESHOLD,
)
```

And in `settings.py`:
```python
VALIDATION_FAILURE_RATE_THRESHOLD = 0.05
```

A test asserts `DEFAULT_*` matches the value in `settings.py` — keeps them from drifting silently.

### 7.3 Locked decisions encoded in settings

Treat the following as load-bearing — changing them needs a roadmap entry:

- `ITEM_PIPELINES` ordering (200/400/500/600)
- `EXTENSIONS` priorities (RunIdExtension at 100, Spidermon at 500)
- `SUCCESSFUL_REASONS` set in `pipelines/postgres.py` (mirrors `SPIDERMON_EXPECTED_FINISH_REASONS`)
- `MATCHED_STATUSES` tuple in `common/brokers_repo.py` (used by coverage SQL)

Whenever a setting is mirrored in two places (e.g. a default constant and a `settings.py` value), add a parity test in the same commit.

---

## 8. Secrets + environment variables

### 8.1 What goes where

- **`.env.example`** — every variable name we use, with comments. Values blank or placeholder.
- **`.env`** — gitignored. Local values. Never commit.
- **`secrets/`** — gitignored directory for credential JSON files (OAuth tokens, service account keys).

### 8.2 Reading rules

- Always provide a sensible default in `os.getenv("FOO", "default")`. Crashing on missing config is acceptable only when there's no reasonable fallback (e.g. `OAUTH_CLIENT_JSON_PATH` for the OAuth setup CLI).
- Empty string from `os.getenv` is treated as "unset" (we use `or None` patterns). Don't conflate "blank" with "explicit empty intent."
- If a variable controls behavior (`ALERT_BACKEND`, `DLD_LIMIT`), document the accepted values in `.env.example` AND in the relevant module's docstring.

### 8.3 Never log secrets

- Webhook URLs, tokens, and credentials are credentials. They go in `.env`, never in logs (even at DEBUG), never in error messages, never in stats blobs.
- If a debug message needs to confirm a credential loaded, log a fact about it (`"loaded google oauth user credentials"`) — never the value.

---

## 9. Logging

### 9.1 Format

JSON lines via `python-json-logger`, configured in `utils/logging_setup.py`. Every line includes `ts`, `level`, `logger`, `message`, plus structured `extra` fields and (when in a spider context) `run_id`, `scrape_date`, `spider`.

`LOG_FORMAT` env toggles terminal output: `json` (default, prod) or `pretty` (dev — single-line, ANSI-coloured, kv-suffixed). The pretty formatter is *terminal-only*; file output is always JSON regardless. Don't ship code that depends on a particular terminal format — pretty mode is for humans, JSON is the contract.

### 9.2 Logger names

Use `logger = logging.getLogger(__name__)` at module top, no `logger = spider.logger` anywhere outside the spider class itself. Module-named loggers make filtering with `grep "broker_scout.pipelines"` reliable.

### 9.3 LogRecord attribute collisions

`extra=` keys cannot use names already on `LogRecord`: `name`, `msg`, `args`, `levelname`, `levelno`, `lineno`, `filename`, `funcName`, `module`, `pathname`, `process`, `processName`, `thread`, `threadName`, `created`, `msecs`, `relativeCreated`, `exc_info`, `exc_text`, `stack_info`, `asctime`, `message`.

**Don't use `extra={"name": ...}`.** This is the single most common bug — Python raises `KeyError: "Attempt to overwrite 'name' in LogRecord"` and the call site explodes. We hit this in Phase 5 (`gdrive_csv` upload success log) and fixed via rename to `file_name`. Add a test for any new logging extras.

### 9.4 Level discipline

| Level | Use for |
|---|---|
| `DEBUG` | Verbose detail useful only when debugging a specific issue. |
| `INFO` | Lifecycle events: spider opened, batch flushed, alert sent. |
| `WARNING` | Degraded paths that shouldn't happen frequently: HTML fallback used, viewer email share failed. |
| `ERROR` | Things that cost data integrity: validation drop, pipeline flush failed, monitor failed. |

If you have to think about whether something is INFO or WARNING, it's INFO.

### 9.5 Spider object in `extra`

Scrapy core passes the spider OBJECT in `extra={"spider": spider}` for some log lines. Our formatter coerces it to `spider.name` automatically (see `RunContextJsonFormatter.add_fields`). Don't try to fix this at call sites — it's handled centrally.

### 9.6 Per-run log files

Every spider run writes a JSON-formatted log file at `{LOG_FILE_DIR}/{spider}_{run_id}.log` (default `logs/`). `RunIdExtension.spider_opened` attaches the FileHandler once `run_id` exists; `spider_closed` detaches it AFTER the final "run finished" line.

Retention is operator-tuneable via `LOG_RETENTION_DAYS` (default 30). The extension prunes `*.log` files older than the cutoff at the start of every run — non-`.log` files in the directory (operator notes, etc.) are left alone. Set `LOG_RETENTION_DAYS=0` to disable pruning; set `LOG_FILE_DIR=` to disable file logging entirely.

Don't add a separate `RotatingFileHandler` — per-run files plus the prune-on-open pattern is the project's retention model (mirrors `out/*.csv` per RULES.md §14.5). One source of truth.

---

## 10. Error handling

### 10.1 Where to validate

At system boundaries only:
- HTTP responses (status codes, JSON parsability)
- Database writes (UNIQUE violations, FK references)
- External API calls (rate limit, malformed response)
- User input (CLI args, env vars)

**Trust internal code.** Once an item has passed the validation pipeline, downstream pipelines accept it as-is. Re-validating wastes cycles and creates inconsistent failure semantics.

### 10.2 The four-quadrant decision

| | Recoverable | Unrecoverable |
|---|---|---|
| **Can be retried** | `tenacity` retry + log + counter | Log + counter + DropItem / propagate |
| **Cannot be retried** | Log + counter + degrade gracefully | Log + propagate |

The `tenacity` retries we use:
- 5 attempts, exponential backoff, max 30–60s wait
- Retry only on transient signals (5xx, `httpx.TransportError`, `psycopg.OperationalError`)
- Never retry 4xx — that's a config bug; failing fast surfaces it sooner

### 10.3 Don't add try/except for things that can't happen

A `psycopg.connect` failure inside our pipeline isn't going to raise `KeyError` — don't catch it just in case. Catch the specific exceptions you know can fire (`HttpError`, `JSONDecodeError`, `ValidationError`).

### 10.4 Never silently swallow

Even when we choose to swallow an error (e.g. `engine_stopped`), we log at ERROR and increment a counter. The user-facing behavior may be "continue," but the log record exists for forensics.

---

## 11. Monitoring (Spidermon)

### 11.1 Suite registration

| Suite | Setting | Hook | Used for |
|---|---|---|---|
| Periodic | `SPIDERMON_PERIODIC_MONITORS` | every 60s | Circuit breakers (errors, 429s) |
| Close | `SPIDERMON_ENGINE_STOP_MONITORS` | engine_stopped | Final-state monitors (pipeline parity, coverage, extraction health) |

**Use `SPIDERMON_ENGINE_STOP_MONITORS`, NOT `SPIDERMON_SPIDER_CLOSE_MONITORS`.** The latter races with our pipelines — they all hook `spider_closed` too, in registration order. `engine_stopped` fires once every `spider_closed` handler completes, so all stats are final.

### 11.2 Custom monitor base

```python
from broker_scout.monitors.monitors import _BrokerScoutMonitor   # private convention

class MyMonitor(_BrokerScoutMonitor):
    severity = "critical"   # or "warning"

    def test_my_invariant(self):
        ...
```

Why `_BrokerScoutMonitor`:
- `__test__ = False` keeps pytest from auto-collecting Monitor classes as test cases.
- `severity = "critical"` is the default; override per-monitor when warning is more appropriate.

Built-in Spidermon monitors don't have `severity`. Code that reads severity uses `getattr(monitor, "severity", "critical")` so missing values fall back safely.

### 11.3 Monitor test methods

- One test method = one assertion. Multiple `assertX(...)` calls in one method become one combined failure message; one method per check is clearer in the LogOnlyAction output.
- **Skip on insufficient data.** If `item_scraped_count == 0`, the rate computations are `0/0` — `self.skipTest(...)`, don't fail. `ZeroItemsMonitor` owns the broken-spider case.
- **Use `assertLessEqual` / `assertGreaterEqual` for rate checks** so the boundary value is "good." `5%` failure rate at threshold `5%` should pass.

### 11.4 Coverage (Phase 9.3.2)

`MatchedRowFieldCoverageMonitor` uses `brokers_repo.matched_field_coverage(...)` to read coverage from Postgres directly, filtering to matched rows. Don't try to compute matched-vs-stub coverage from in-memory stats — Spidermon's `FieldCoverageMonitor` measures over all items, which dilutes by stubs.

---

## 12. Alerting

### 12.1 The Notifier protocol

```python
class Notifier(Protocol):
    def send(self, level: str, title: str, body: str, run_id: str | None) -> bool:
        ...
```

Implementations: `GoogleChatNotifier`, `DiscordNotifier`, `LogOnlyNotifier`. Adding a new channel = one new class with this shape; nothing else changes.

### 12.2 Selection

`get_notifier()` factory:
1. If `ALERT_BACKEND` is set explicitly → use it (or fall back to `LogOnlyNotifier` if its URL is unset).
2. Else auto-detect: prefer Discord (works on personal accounts) → Google Chat → LogOnly.

Typo'd `ALERT_BACKEND` falls through to auto-detect rather than crashing. Missing-URL with explicit-backend is LogOnly, not silent post-to-nowhere.

### 12.3 Anti-spam

Every Chat send is followed by `brokers_repo.log_alert(...)`. Critical action checks `recent_alert_exists(level, title, 30)` before sending — global dedupe (across runs), not per-run.

### 12.4 Card content

End-of-run summary card body order (locked):
1. Run timestamp + run_id
2. Item count + validation pass rate
3. Match-status breakdown
4. Pipeline ✓/✗ marks
5. Finish reason
6. Runtime
7. Sheet link (if available)
8. Drive CSV link (if available)
9. Failures (only if any, capped at 10)

Use real data from stats, never hard-coded values. Missing stats render as `—`, not `null` or `None`.

### 12.5 Title is constant for circuit breakers

`"Circuit breaker tripped"` is the exact title — don't include run-specific data in the title. Anti-spam dedupes on `(level, title)`; varying titles defeat dedupe.

---

## 13. Database (Postgres)

### 13.1 Migration discipline

- **Numbered, forward-only.** `001_dld_brokers.sql`, `002_brokers.sql`, `003_sheet_registry.sql`, `004_match_columns.sql`. Never edit a committed migration.
- **`CREATE TABLE IF NOT EXISTS`** for new tables. **`ALTER TABLE ... IF NOT EXISTS`** for new columns (Postgres 9.6+).
- **`CREATE INDEX IF NOT EXISTS`** for indexes.
- One migration = one logical change. A migration that adds three unrelated columns is wrong; split it.
- `tools/migrate.py` is idempotent: re-running already-applied migrations is a no-op.

### 13.2 Insert / upsert idiom

```sql
INSERT INTO brokers (...) VALUES (...)
ON CONFLICT (run_id, platform, brn) DO NOTHING
```

We use `ON CONFLICT DO NOTHING` for append-only tables (`brokers`) — re-running a flush is safe. For mutable tables (`dld_brokers`), `ON CONFLICT DO UPDATE` with `last_seen_run_id` tracking.

### 13.3 Batched executemany

For >100 rows, batch in `BATCH_SIZE` chunks via `cur.executemany(SQL, [params, ...])`. See `dld_repo.upsert_brokers` and `brokers_repo.insert_brokers` for the exact pattern (rebind list, don't clear).

### 13.4 JSONB blobs

The `raw` column on `brokers` and `payload` on `bad_items` store the full unflattened item via `Jsonb(item)`. Use this for forensics; **don't query into JSONB as if it were structured data.** If you need to filter on a field, promote it to a real column.

### 13.5 Schema evolution

- Adding a nullable column is fine.
- Adding a NOT NULL column needs a default OR a backfill migration (separate from the schema change).
- Renaming a column = new migration that adds the new name + drops the old in two steps. Never rename in place if any committed code reads the old name.

---

## 14. Google Sheets + Drive

### 14.1 Auth

OAuth user credentials only (service accounts can't own files on personal Google accounts). `utils/gauth.py` is the single source. Three artifacts:
- `secrets/oauth_client.json` — downloaded once from Cloud Console
- `secrets/oauth_token.json` — produced once by `tools/oauth_setup.py`
- Refresh token inside the token file is what the running spider uses

### 14.2 Sheet rotation

Monthly rotation per platform via `sheet_registry` table. The pipeline never references a hardcoded spreadsheet ID — always queries the registry for `(platform, current_period)`.

Cell limit math: 1.35M cells/run × ~4 runs/month ≈ 5.4M < 10M. New columns push the math; if `_SHEET_HEADERS` grows past ~50, revisit the rotation cadence.

### 14.3 Column order

`monitors/coverage_tiers.py` and `common/sheets_repo.py::_SHEET_HEADERS` are the canonical sources. Module-load asserts ensure every dataclass field is tiered exactly once. Adding a new field to `items.py` will fail at import until tiered — don't suppress the assert.

### 14.4 Append idempotency

`spreadsheets.values.append` is **not idempotent.** Re-running the same flush appends duplicate rows. Avoid retrying at the application layer for Sheets the same way we do for Postgres. The `tenacity` retry only fires on 5xx where we trust the server didn't accept the write — Google's own API guarantees this for 5xx but not 4xx.

### 14.5 Drive CSV

One CSV per spider run, named `{spider}_{YYYYMMDD-HHMMSS}.csv`. Local copy retained 7 days for replay. The CSV header row matches `template_header_row()` so a row from a CSV can be re-fed into the Sheets append flow without translation (Phase 12's `tools/replay_run.py`).

---

## 15. Testing

### 15.1 Stack

`pytest` + `pytest-cov`. No `unittest` test runners outside Spidermon's internal use. No `nose`, no `tox`. Tests live in `tests/` mirroring the source layout.

### 15.2 Coverage targets

- **New modules**: ≥ 90% line coverage. Uncovered lines should be Scrapy `from_crawler` glue or defensive paths that can't be triggered without integration setup.
- **Bug fixes**: a regression test that fails on the old code, passes on the new code, in the same commit.

### 15.3 What to mock

- **External services**: always (HTTP via `httpx.Client` patching, DB via cursor mocks, Google APIs via `gauth` patches).
- **Time**: rarely. Most date logic uses `date.today()` which we let real-clock through.
- **Internal modules**: only at the seam between layers. Don't mock `brokers_repo` from inside `brokers_repo`'s own tests.

### 15.4 Fixtures

- Use `@pytest.fixture` for repeated setup, not module-level globals.
- `autouse=True` for cache-resetting fixtures (e.g. `gauth.reset_clients()` between tests so cached creds don't leak across tests).
- Name fixtures after what they produce (`mock_pool`, `pipeline_harness`, `repo_mock`), not what they do.

### 15.5 Avoid TestCase auto-collection traps

Spidermon `Monitor` extends `unittest.TestCase`, which pytest auto-collects. Set `__test__ = False` on production Monitor base classes to keep pytest from running them as tests. Spidermon's runtime uses `unittest.TestLoader` which ignores the attribute.

### 15.6 Match real shapes in tests

If you build a fake object to stand in for a real one, **mirror the real attribute names**. We hit a real bug in Phase 11 because `SimpleNamespace(error_message=...)` mocks didn't match real Spidermon failures (which use `reason` / `error`). Add a regression test using a real-shape mock for any new external interface.

### 15.7 Module-load asserts

Catch drift at import time, not at runtime:

```python
# coverage_tiers.py
assert tuple(_SHEET_HEADERS.keys()) == _SHEET_COLUMNS, (
    "_SHEET_HEADERS keys must match _SHEET_COLUMNS order exactly"
)
```

Phase 6.1 added a missing `closed_deals_total` field — caught immediately at module load, fixed in the same commit. These asserts are cheap and have zero runtime overhead.

### 15.8 Integration tests

Live Postgres / Google API tests are **not** in the unit suite. We do live verification via spider runs after a phase ships, documented in the phase's roadmap entry. Phase 12 adds proper smoke tests.

---

## 16. Naming conventions

### 16.1 Code

- **`snake_case`** for functions, variables, module names.
- **`UPPER_SNAKE_CASE`** for module-level constants (`MAX_AED`, `DEFAULT_FUZZY_THRESHOLD`).
- **`PascalCase`** for classes.
- **`_leading_underscore`** for private helpers and module attributes that aren't part of the public API.

### 16.2 Stat namespaces

`category/event` slash-separated. Existing categories:

| Category | Examples |
|---|---|
| `validation/` | `validation/passed_total`, `validation/failed_field/{field}` |
| `match/` | `match/exact_brn`, `match/promoted_to_exact_brn`, `match/brn_drift` |
| `extract/` | `extract/next_data/missing`, `extract/brn/fallback_used` |
| `postgres/` | `postgres/brokers_inserted`, `postgres/bad_items_inserted` |
| `gsheets/` | `gsheets/rows_appended`, `gsheets/sheet_id`, `gsheets/flush_failed` |
| `gdrive_csv/` | `gdrive_csv/upload_status`, `gdrive_csv/file_id`, `gdrive_csv/rows_uploaded` |

Don't invent new top-level categories without updating `roadmap.md` §7 and Phase 9 monitors.

### 16.3 SQL identifiers

- Tables: `snake_case`, plural (`brokers`, `scrape_runs`, `bad_items`, `sheet_registry`).
- Columns: `snake_case`, descriptive (`match_status`, `dld_broker_name`, not `dld_name` or `match`).
- Indexes: `<table>_<columns>_idx` (`brokers_brn_idx`).

### 16.4 Files

- `<concept>_<role>.py`: `dld_client.py`, `brokers_repo.py`, `sheets_repo.py`, `validation_pipeline.py` (well, `validation.py` inside `pipelines/` since the role is the package).
- Tests: `test_<module>.py` mirroring the source path.

---

## 17. Coding standards

### 17.1 Comments

**Default to writing no comments.** Add one only when the WHY is non-obvious — a hidden constraint, an upstream bug we work around, an empirically-discovered threshold.

Examples of comments worth keeping:
- `# Scrapy core passes the spider OBJECT in extras for some log lines` (the JSON formatter coercion)
- `# 404 deliberately NOT in the dict — empirically PF returns 404 for many DLD names that aren't on PF` (the HTTP code thresholds)
- `# RunIdExtension at priority 100 must run BEFORE Spidermon (priority 500) so spider.run_id exists` (the EXTENSIONS dict)

Don't write:
- `# initialize the buffer` above `self._buffer = []`
- `# validate the item` above `model.model_validate(...)`
- Any comment that just restates the line below in English.

### 17.2 Docstrings

Module-level: required for non-trivial modules. State the module's role + one or two key design choices. Examples worth modelling on: `common/dld_repo.py`, `monitors/notifiers.py`, `pipelines/postgres.py`.

Class-level: required for public classes. State the class's role + lifecycle (e.g. "fires on engine_stopped, AFTER all pipelines flush").

Function-level: required for public functions, optional for trivial private helpers. Don't write `Args:` / `Returns:` blocks for simple signatures — the type annotations carry that load.

### 17.3 Type hints

- **All public functions** are fully annotated.
- **Use modern syntax**: `list[int]`, `dict[str, X]`, `X | None`. Drop `from typing import List, Dict, Optional`.
- **`Optional[X]`** is OK when it pairs with `pydantic.Field` (more readable than `X | None` next to `Field(...)`).
- **Return types are required**, even when `None`. `def foo() -> None: ...`

### 17.4 Don't add features beyond the task

- A bug fix shouldn't refactor surrounding code.
- A new feature shouldn't ship "for the future" hooks that nobody calls.
- Three similar lines is better than a premature abstraction. Wait for the fourth.

### 17.5 Don't write half-finished code

If a function isn't ready, don't merge it with `# TODO: implement X` and a `pass` body. Either:
- Ship a working minimal version that's honest about its scope.
- Don't ship the call site that needs the unwritten function.

### 17.6 Errors are not control flow

`except` is for handling failures, not for branching. If you find yourself catching a specific exception to make a normal-path decision, restructure to avoid the throw.

---

## 18. Git + commits

### 18.1 Commit per sub-phase

We commit per roadmap sub-phase (e.g. Phase 9.0, 9.1, 9.2, 9.3 each get one commit). The commit message:
1. Summarizes the change in one line.
2. Has a body explaining WHY the change is being made — what problem it solves, what it unblocks.
3. Lists files touched + the rationale for each.
4. Notes any bugs caught during implementation (these are the most useful parts of the message in retrospect).
5. Records the live verification result.

Bug-fix commits explicitly mention the regression test added in the same commit.

### 18.2 Never co-author

The user has explicitly asked that commits not include co-author trailers (`Co-Authored-By:`). Don't add them.

### 18.3 No skipped hooks

Don't pass `--no-verify` to `git commit` or `git push` unless the user explicitly asked. If a pre-commit hook fails, fix the underlying issue.

### 18.4 Atomic commits

A commit should leave the repo in a working state. Tests pass. The roadmap entry for the sub-phase ticks complete. Don't commit half a feature with the rest "coming next."

---

## 19. Adding a new platform spider

Concrete checklist when adding Bayut (Phase 8) or any future platform:

1. **Subclass `BaseBrokerSpider`** in `spiders/<platform>.py`.
2. Set `name`, `platform`, `warmup_url`, `handle_httpstatus_list`.
3. Implement `search_for_broker(dld_broker)` — build the platform-specific search URL.
4. Implement `parse_search_results(response, dld_broker)` — extract candidates, run `match_candidates`, route.
5. Implement `parse_agent` (or whatever the platform calls a profile fetch) — set `match_status`, `match_confidence`, DLD ground-truth fields.
6. **If the platform's items have new fields**:
   - Add to `items.py` (or create a sibling dataclass if fields diverge significantly).
   - Add validation rules in `schemas.py`.
   - Add columns to `brokers` table in a new migration.
   - Add to `_BROKER_COLUMNS` in `brokers_repo.py`.
   - Add to `_SHEET_HEADERS` in `sheets_repo.py`.
   - Add to a tier in `coverage_tiers.py` (the integrity assert will fail loudly until you do).
7. Add the platform to `_PLATFORM_CONFIG` in `sheets_repo.py` with template + folder env-var names.
8. Add env vars to `.env.example`.
9. Add a `Literal` member to `MatchStatusType` only if the new platform has new match outcomes (rare).
10. Tests:
   - Spider unit tests under `tests/test_<platform>_spider.py`.
   - Schema rejection/acceptance rows for any new fields.
   - Coverage tier integrity (already auto-tested).

The DLD client, matching layer, validation pipeline, Postgres pipeline, and monitors are all platform-agnostic and need no changes for a new spider.

---

## 20. Handling website structure changes

PF and Bayut update their HTML/JSON without notice. Our defensive layers:

1. **Stat counters** — every fallback path increments `extract/*`. A spike on a counter in the next run is the first signal.
2. **Validation schema** — `extra="forbid"` catches new fields silently appearing. Drift in field shapes (e.g. `whatsapp_response_time` returning a string instead of int) becomes a `validation/failed_field/<field>` counter spike.
3. **Phase 9 monitors** — `ExtractionFailureMonitor` fails the run when any `extract/*` counter exceeds its threshold; `ValidationFailureByFieldMonitor` fails when any single field rejects >10% of items.
4. **HTML XPath fallbacks** — when the JSON path breaks, the HTML path keeps working at lower fidelity (`extract/search_json/fallback_used` increments). Buys time to fix.
5. **Per-monitor severity** — a single field falling to High coverage isn't critical; the monitor severity classifies how loudly we alert.

When you fix a structure change:
- The fix goes in the spider's selectors / JMESPath queries.
- Add or adjust the `extract/*` counter for the new failure shape.
- If the fix changes the schema, add a regression test that asserts the new shape parses correctly.
- Don't lower a monitor threshold to silence the alert — that hides future regressions.

---

## 21. Maintainability checklist (before merging anything)

A change is ready to merge when:

- [ ] All tests pass locally (`poetry run pytest tests/ -q`).
- [ ] Coverage on new modules is ≥ 90% (or has explicit reasoning in the commit body).
- [ ] No `# TODO`, no `# FIXME`, no `print(...)` in production code.
- [ ] No half-implemented functions.
- [ ] All new env vars are in `.env.example` with comments.
- [ ] All new stat counters are listed in `roadmap.md` §7 (Phase 7).
- [ ] All new migrations are numbered correctly + idempotent.
- [ ] The roadmap entry for the sub-phase is ticked.
- [ ] Live verification ran (or has explicit reasoning if it can't yet).
- [ ] Commit message explains WHY, not just WHAT.

---

## 22. Phase / roadmap discipline

`roadmap.md` is the source of truth for what's planned and what's done. Rules:

- **Tick boxes only when shipped.** Don't tick before the commit lands.
- **Update inline when reality diverges.** If `RuntimeMonitor` (built-in) doesn't exist in our Spidermon version, the roadmap entry says so — don't leave the original spec as if it would just work.
- **Sub-phase numbering** (`9.0`, `9.1`, etc.) corresponds to commit boundaries. One sub-phase = one commit.
- **Audit before implementing.** Before starting a phase, re-read its roadmap entry against the current codebase. Stat names will have drifted, fields will have been added. Update the spec, then write the code.

---

## 23. Things we deliberately avoid

A short list of patterns that look reasonable but cause problems for this project:

- **Catch-and-pass exceptions.** Always either handle (specific exception, log, recover) or propagate. `except Exception: pass` makes failures invisible.
- **Magic field paths.** Don't `agent_data["compliances"][0]["value"]` — use `jmespath.search("compliances[?type=='brn'].value | [0]", ...)` and provide a fallback. Indexing into JSON without `jmespath` falls over the moment the structure changes.
- **Conditional imports for "performance."** All imports go at module top. Lazy imports are reserved for the lazy-singleton pattern in connection clients (`gauth.py`, `db.py`).
- **Globally mutable state.** Module-level mutable dicts/lists that get edited at runtime are a recipe for test pollution. The few places we have it (`gauth._creds`, `CloseSpiderAction._fired`) are documented and have `reset()` helpers.
- **Wide try blocks.** `try` should wrap the smallest possible call. `try/except` around a 30-line function obscures which exact line raised.
- **Skip-the-test-and-comment-it-out.** If a test is wrong, fix it. If it's flaky, fix the flake. If it's no longer valid, delete it. Never comment out a test.

---

## 24. When in doubt

- **Match an existing pattern.** Most of what you'll write is similar to something already in the codebase. `dld_repo.py` is the model for repos. `validation.py` for pipelines. `match_candidates` for matching logic. `MatchedRowFieldCoverageMonitor` for SQL-backed monitors. Copy the shape, not the substance.
- **Read the surrounding tests.** They show what the contract is supposed to be. If the test for the function you're modifying doesn't exist, write one before changing behavior.
- **Update this file** when a new convention emerges. The doc only stays useful if it tracks reality.
