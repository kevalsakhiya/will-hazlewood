-- Phase 3: authoritative store for scrape outputs.
--
-- Four tables:
--   * scrape_runs — one row per spider run; status, item counts, stats blob
--   * brokers     — append-only; one row per (run, platform, brn); raw JSONB kept
--   * bad_items   — drained from spider.bad_items by PostgresPipeline.close
--   * alert_log   — Phase 11 dedupe; created now to avoid a later migration
--
-- match_status / match_confidence are populated by Phase 6's matching layer.
-- Until then the pipeline writes the column default ('unknown') and NULL.

CREATE TABLE IF NOT EXISTS scrape_runs (
    run_id        UUID PRIMARY KEY,
    spider        TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running',  -- running | ok | failed
    items_scraped INT,
    items_dropped INT,
    stats         JSONB
);

CREATE TABLE IF NOT EXISTS brokers (
    id                                     BIGSERIAL PRIMARY KEY,
    run_id                                 UUID NOT NULL REFERENCES scrape_runs(run_id),
    scrape_date                            DATE NOT NULL,
    platform                               TEXT NOT NULL,
    brn                                    TEXT,
    match_status                           TEXT NOT NULL DEFAULT 'unknown',
    match_confidence                       NUMERIC,
    agent_url                              TEXT,
    broker_name                            TEXT,
    nationality                            TEXT,
    agent_specialization                   TEXT,
    experience_since                       INT,
    whatsapp_response_time                 INT,
    is_superagent                          BOOLEAN,
    agency_url                             TEXT,
    agency_registration_number             TEXT,
    listings_for_sale                      INT,
    listings_for_rent                      INT,
    listings_total                         INT,
    listings_with_marketing_spend          INT,
    average_listing_price_sale             NUMERIC,
    average_listing_price_rent             NUMERIC,
    average_listing_age_days_sale          NUMERIC,
    average_listing_age_days_rent          NUMERIC,
    most_recent_listing_date_sale          DATE,
    most_recent_listing_date_rent          DATE,
    closed_transaction_sale                INT,
    closed_transaction_rent                INT,
    closed_deals_total                     INT,
    closed_transaction_deal_value          NUMERIC,
    closed_transaction_sale_total_amount   NUMERIC,
    closed_transaction_rent_total_amount   NUMERIC,
    closed_transaction_sale_avg_amount     NUMERIC,
    closed_transaction_rent_avg_amount     NUMERIC,
    most_recent_deal_date_sale             DATE,
    most_recent_deal_date_rent             DATE,
    average_monthly_deal_volume_sale       NUMERIC,
    average_monthly_deal_volume_rent       NUMERIC,
    raw                                    JSONB,
    UNIQUE (run_id, platform, brn)
);
CREATE INDEX IF NOT EXISTS brokers_brn_idx         ON brokers (brn);
CREATE INDEX IF NOT EXISTS brokers_scrape_date_idx ON brokers (scrape_date);

CREATE TABLE IF NOT EXISTS bad_items (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID,
    platform    TEXT,
    reason      TEXT,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bad_items_run_id_idx ON bad_items (run_id);

CREATE TABLE IF NOT EXISTS alert_log (
    id        BIGSERIAL PRIMARY KEY,
    run_id    UUID,
    level     TEXT,
    title     TEXT,
    body      TEXT,
    sent_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
