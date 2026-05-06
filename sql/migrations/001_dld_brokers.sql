-- Phase 1: DLD broker registry table.
--
-- Source of truth for the licensed-broker list. Refreshed weekly via
-- `python -m broker_scout.tools.fetch_dld`. UPSERT on (brn) so DLD
-- field changes (agency moves, expiry dates, etc.) propagate.

CREATE TABLE IF NOT EXISTS dld_brokers (
    brn                     TEXT PRIMARY KEY,        -- DLD CardNumber, unique per broker
    office_license_number   TEXT,                    -- DLD LicenseNumber (shared across brokers in the same office)
    broker_name_en          TEXT,
    broker_name_ar      TEXT,
    phone               TEXT,
    mobile              TEXT,
    email               TEXT,
    real_estate_number  TEXT,
    office_name_en      TEXT,
    office_name_ar      TEXT,
    card_issue_date     DATE,
    card_expiry_date    DATE,
    office_issue_date   DATE,
    office_expiry_date  DATE,
    photo_url           TEXT,
    office_logo_url     TEXT,
    card_rank_id        INT,
    card_rank           TEXT,
    office_rank_id      INT,
    office_rank         TEXT,
    awards_count        INT,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_run_id    UUID        NOT NULL
);

CREATE INDEX IF NOT EXISTS dld_brokers_last_seen_run_idx
    ON dld_brokers (last_seen_run_id);

CREATE INDEX IF NOT EXISTS dld_brokers_real_estate_number_idx
    ON dld_brokers (real_estate_number);
