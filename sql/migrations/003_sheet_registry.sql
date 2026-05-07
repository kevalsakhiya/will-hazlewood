-- Phase 4.1: Google Sheets monthly rotation registry.
--
-- One row per (platform, period). The Sheets pipeline asks
-- `get_or_create_active_sheet(platform)` on spider_opened; if no row
-- exists for the current YYYY-MM, the pipeline creates a new
-- spreadsheet via the Drive API and registers it here. Older periods
-- are deactivated (is_active = FALSE) so monitors / replay tools can
-- still find them by platform + period.

CREATE TABLE IF NOT EXISTS sheet_registry (
    id          BIGSERIAL PRIMARY KEY,
    platform    TEXT NOT NULL,
    period      TEXT NOT NULL,             -- 'YYYY-MM'
    sheet_id    TEXT NOT NULL,             -- Google Sheets file id
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, period)
);
CREATE INDEX IF NOT EXISTS sheet_registry_active_idx
    ON sheet_registry (platform, is_active);
