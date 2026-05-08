# will-hazlewood — broker intelligence pipeline

Dubai broker intelligence: pull DLD's licensed-broker registry, search each broker on PropertyFinder (Bayut next), match candidates back to the regulator's BRN, persist enriched records to **Postgres + Google Sheets + Google Drive**, monitor end-to-end with Spidermon, alert via **Discord or Google Chat** (configurable).

| Read this | When |
|---|---|
| [`docs/`](docs/index.md) | You want to understand or operate the system. Five reference docs: architecture, data flow, monitors, operator runbook, index. |
| [`plan.md`](plan.md) | Architecture decisions and the rationale behind them. |
| [`roadmap.md`](roadmap.md) | What's shipped, what's next. |
| [`RULES.md`](RULES.md) | Coding conventions. New code must match. |
| This file | First-time setup on a fresh machine. |

## Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)
- Docker (for local Postgres)
- A Google account (any personal Gmail works — service accounts won't, see [Google integration](#one-time-google-integration-setup) below)

## Quick start

The end state is: Postgres running, schema applied, DLD broker list loaded, OAuth tokens in place, spider produces a clean smoke-test run.

```bash
# 1. Install + env file
poetry install
cp .env.example .env                             # edit afterwards (see below)

# 2. Postgres
docker compose up -d postgres

# 3. Schema
poetry run python -m broker_scout.tools.migrate  # idempotent; safe to re-run

# 4. Google integration (one-time, machine with browser) — see section below
poetry run python -m broker_scout.tools.oauth_setup

# 5. Seed the DLD broker registry
poetry run python -m broker_scout.tools.fetch_dld

# 6. Smoke test (1 DLD broker → 1 item)
poetry run scrapy crawl agent_spider -s DLD_LIMIT=1 -s CLOSESPIDER_ITEMCOUNT=1

# 7. First real run
poetry run scrapy crawl agent_spider
```

After step 6 you should see:

- `logs/agent_spider_<run_id>.log` (JSON, one record per line)
- `out/agent_spider_<run_id>.csv` (one row + header)
- A new row in Postgres `scrape_runs` with `status='ok'` (or, if the broker isn't on PF, `match_status='not_found'`)
- A new row in the active monthly Google Sheet
- A new file in your Drive CSV folder
- (If Discord/Chat is wired) a green summary card

If anything went wrong, see [`docs/operators.md`](docs/operators.md#debugging-a-failed-run).

## `.env` reference

`.env.example` lists every variable. Required-to-run vs. optional:

### Required for any run

```ini
# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5433                    # 5433 to avoid clashing with a host-installed Postgres
POSTGRES_HOST_PORT=5433               # docker-compose host-port binding
POSTGRES_DB=broker_scout
POSTGRES_USER=root
POSTGRES_PASSWORD=root
```

### Required once Google integration is set up (step 4 onwards)

```ini
OAUTH_CLIENT_JSON_PATH=./secrets/oauth_client.json
OAUTH_TOKEN_JSON_PATH=./secrets/oauth_token.json

GSHEET_TEMPLATE_PF_ID=<spreadsheet id from PF template URL>
GSHEET_PF_FOLDER_ID=<folder id from PF folder URL>
GDRIVE_CSV_FOLDER_ID=<folder id from "Broker Scout — CSVs" folder URL>

# Bayut (Phase 8 — leave blank for now)
GSHEET_TEMPLATE_BAYUT_ID=
GSHEET_BAYUT_FOLDER_ID=

# Optional: auto-share each monthly file with viewers (role: reader).
GSHEET_VIEWER_EMAILS=
```

### Logging

```ini
LOG_LEVEL=INFO
LOG_FORMAT=pretty                     # `json` (prod) or `pretty` (dev terminal)
LOG_FILE_DIR=logs                     # blank to disable file logging
LOG_RETENTION_DAYS=30                 # 0 to disable auto-prune
```

`LOG_FORMAT=pretty` gives you ANSI-coloured single-line output in the terminal. The file at `logs/{spider}_{run_id}.log` is always JSON regardless. Old log files get auto-pruned at the start of each run.

### Alerts (optional)

```ini
# Auto-detect: if both URLs are set, Discord wins (works on personal Google accounts).
# Set ALERT_BACKEND=google_chat to force, or ALERT_BACKEND=discord.
ALERT_BACKEND=
DISCORD_WEBHOOK_URL=
GOOGLE_CHAT_WEBHOOK_URL=

# `warning` sends warning + critical; `critical` sends critical only.
ALERT_MIN_LEVEL=warning
```

Without a webhook URL the run still works — alerts fall back to `LogOnlyNotifier` (logged at WARNING/ERROR depending on severity).

### Run-time tunables

```ini
# Smoke testing
DLD_LIMIT=0                           # 0 = all DLD brokers; >0 caps the run
DLD_BRN_FILTER=                       # comma-separated BRNs; runs only those (replay / focused dev)

# Matching
MATCH_FUZZY_THRESHOLD=90              # rapidfuzz token-set ratio cutoff for name_fuzzy
```

Monitor thresholds (extraction/match/coverage rates) are also overridable; see [`docs/monitors.md`](docs/monitors.md#tuning-thresholds).

---

## One-time Google integration setup

The Sheets pipeline auto-rotates spreadsheets monthly: every month it copies a per-platform **template spreadsheet** into the configured Drive folder. We authenticate as a **real Google user via OAuth** (not a service account) so the rotated files are owned by your account and count against your 15 GB Drive quota — service accounts have zero quota and can't own files on personal Google accounts.

### 1. Cloud project + APIs

1. In **<https://console.cloud.google.com/>**, create or pick a project.
2. **APIs & Services → Library**, enable **Google Sheets API** and **Google Drive API**.

### 2. OAuth consent screen

1. **APIs & Services → OAuth consent screen** → **External** → **Create**.
2. Fill App name, support email, developer email. Skip logo.
3. **Scopes** screen — click **Save and continue** without adding anything (scopes are requested at runtime).
4. **Test users** screen — add **your own Gmail address** as a test user. This is what gives you an indefinite-lifetime refresh token. Add any teammates whose accounts you also want to authorize.

### 3. OAuth client credentials

1. **APIs & Services → Credentials → + Create Credentials → OAuth client ID**.
2. Application type: **Desktop app**. Name it. Create.
3. **Download JSON** from the dialog.
4. Save it in the repo:
   ```bash
   mkdir -p secrets
   mv ~/Downloads/client_secret_*.googleusercontent.com.json secrets/oauth_client.json
   ```

### 4. One-time consent flow (on a machine with a browser)

```bash
poetry run python -m broker_scout.tools.oauth_setup
```

This opens your browser, you sign in and click **Allow**, and the script writes `secrets/oauth_token.json`. That token contains a long-lived refresh token; the running spider uses it forever after — no browser needed at runtime.

If you're deploying to a server later, you copy `secrets/oauth_token.json` along with the rest of the repo. The server never opens a browser.

### 5. Templates + folders

1. Generate the canonical header row:
   ```bash
   poetry run python -c "from broker_scout.common.sheets_repo import template_header_row; print(','.join(template_header_row()))"
   ```
2. Create two new spreadsheets in your Drive: **PropertyFinder Brokers — TEMPLATE** and **Bayut Brokers — TEMPLATE**. In each, rename the first tab to **`brokers`** (lowercase, exact). Click cell A1, paste the header line from step 1, then **Data → Split text to columns** (delimiter: comma). Apply any frozen-row / conditional formatting you want; it persists through monthly rotations.
3. Create two folders in your Drive: **PropertyFinder Brokers** and **Bayut Brokers**. Each month's rotated spreadsheet lands here.
4. Create one more folder in your Drive: **Broker Scout — CSVs**. The Drive CSV pipeline uploads one CSV file per spider run here as a per-run archive (separate from the monthly Sheets).
5. No share dialogs needed — your OAuth user already owns these files.

### 6. Populate `.env`

Paste each ID / folder URL fragment into the matching variable. See [`.env` reference](#env-reference) above.

After this bootstrap, no manual sheet creation, sharing, or rotation is ever required. The pipeline copies the template into a new monthly file on the first run of each month, registers it in `sheet_registry`, and writes there until the next month.

---

## Optional: alerts (Discord or Google Chat)

Pick whichever you have access to. If you have a personal Google account, prefer Discord — Google Chat webhooks are restricted on consumer accounts.

### Discord

1. In your Discord server, **Server Settings → Integrations → Webhooks → New Webhook**. Pick a channel, copy the URL.
2. In `.env`:
   ```ini
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
   ALERT_BACKEND=                       # leave blank for auto-detect, or set to "discord"
   ```

### Google Chat (Workspace only)

1. In a Google Chat space, **Manage webhooks → Add webhook**, copy the URL.
2. In `.env`:
   ```ini
   GOOGLE_CHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/.../messages?...
   ALERT_BACKEND=google_chat            # explicit selection (otherwise Discord wins if both set)
   ```

After your next spider run, you should see a green "OK" summary card. Failures get a red/yellow card with the failure list. Mid-run circuit-breakers (errors, 429 rate limits) get a separate critical card.

---

## Daily operation

For the day-to-day — running spiders, debugging failures, reading alerts, refreshing DLD, sheet rotation behaviour — see [`docs/operators.md`](docs/operators.md).

For the system architecture and what every layer does, see [`docs/architecture.md`](docs/architecture.md).

For every emitted stat / counter, see [`docs/data-flow.md`](docs/data-flow.md#stat-namespaces).

For every Spidermon monitor (what it checks, threshold, what an alert means), see [`docs/monitors.md`](docs/monitors.md).
