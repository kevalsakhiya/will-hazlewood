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

The Sheets pipeline auto-rotates spreadsheets monthly: every month it copies a per-platform **template spreadsheet** into the configured Drive folder. We authenticate as a real Google user via OAuth (not a service account) so the rotated files are owned by your account and count against your 15 GB Drive quota — service accounts have zero quota and can't own files on personal Google accounts.

### 1. Cloud project + APIs

1. In **https://console.cloud.google.com/**, create or pick a project.
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

```ini
OAUTH_CLIENT_JSON_PATH=./secrets/oauth_client.json
OAUTH_TOKEN_JSON_PATH=./secrets/oauth_token.json

GSHEET_TEMPLATE_PF_ID=<spreadsheet id from PF template URL>
GSHEET_TEMPLATE_BAYUT_ID=<spreadsheet id from Bayut template URL>

GSHEET_PF_FOLDER_ID=<folder id from PF folder URL>
GSHEET_BAYUT_FOLDER_ID=<folder id from Bayut folder URL>

GDRIVE_CSV_FOLDER_ID=<folder id from "Broker Scout — CSVs" folder URL>

# Optional: comma-separated emails to auto-share each monthly file with (role: reader).
GSHEET_VIEWER_EMAILS=
```

After this bootstrap, no manual sheet creation, sharing, or rotation is ever required. The pipeline copies the template into a new monthly file on the first run of each month, registers it in `sheet_registry`, and writes there until the next month.
