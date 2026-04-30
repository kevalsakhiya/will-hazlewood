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
