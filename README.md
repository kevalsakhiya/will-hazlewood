# will-hazlewood — broker intelligence pipeline

Dubai broker intelligence: pull DLD's licensed-broker list, search each broker on PropertyFinder and Bayut, persist enriched records to Postgres + Google Sheets + Google Drive, monitor with Spidermon, alert via Google Chat.

Architecture: see [`plan.md`](plan.md).
Phased build plan: see [`roadmap.md`](roadmap.md).

## Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)
- Docker (for local Postgres)

## Setup

```bash
poetry install
cp .env.example .env          # then fill in real values
docker compose up -d postgres
```

## Run a spider

```bash
poetry run scrapy crawl agent_spider
```

Logs are emitted as JSON, one object per line, each tagged with the per-run `run_id`.

## One-time Google integration setup

The Sheets pipeline (Phase 4) auto-rotates spreadsheets monthly: every month it copies a per-platform **template spreadsheet** into the configured Drive folder. The operator only sets things up once.

1. **Create the Google Cloud service account** and download its JSON key. Save it locally as `secrets/service_account.json` (or set `SERVICE_ACCOUNT_JSON_PATH` to a different path in `.env`).
2. **Enable APIs** on the project: Google Sheets API + Google Drive API.
3. **Create one template spreadsheet per platform** (PropertyFinder, Bayut). Open each, name the first tab `brokers`, fill row 1 with the column headers (matching the order in `broker_scout/broker_scout/common/sheets_repo.py::_SHEET_COLUMNS`), and apply any conditional formatting / frozen-row styling you want preserved across rotations.
4. **Create one Drive folder per platform** for the rotated monthly spreadsheets to land in.
5. **Share both** the template spreadsheets *and* the Drive folders with the service account email (visible inside the JSON key as `client_email`) — role: **Editor**.
6. **Populate `.env`**:
   - `GSHEET_TEMPLATE_PF_ID` / `GSHEET_TEMPLATE_BAYUT_ID` — the template spreadsheet IDs (from each template's URL).
   - `GSHEET_PF_FOLDER_ID` / `GSHEET_BAYUT_FOLDER_ID` — the Drive folder IDs.
   - `GSHEET_VIEWER_EMAILS` — comma-separated list of teammate emails to auto-share each new monthly file with (role: reader). Leave empty to keep files private to the service account only.

After this bootstrap, no manual sheet creation, sharing, or rotation is ever required. The pipeline copies the template into a new monthly file on the first run of each month, registers it in `sheet_registry`, and writes there until the next month.
