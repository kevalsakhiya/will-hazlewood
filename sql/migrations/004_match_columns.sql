-- Phase 6.1: DLD ground-truth columns on the brokers table.
--
-- match_status / match_confidence already shipped in Phase 3 (defaulted
-- to 'unknown' / NULL until the matching layer arrives). This migration
-- adds the three remaining fields the matching layer + spider refactor
-- need:
--
--   dld_brn          — DLD ground-truth BRN (the broker's CardNumber as
--                      seen at fetch time). Lets monitors detect drift
--                      between PF-reported BRN and DLD's BRN.
--   dld_broker_name  — DLD ground-truth name (CardHolderNameEn), pre-
--                      normalization. Useful for audit on `not_found`
--                      and `ambiguous` rows that lack a PF broker_name.
--   agency_name      — Office name from DLD (OfficeNameEn). PF only
--                      gives the agency URL, so this is the readable
--                      label for that broker's office.

ALTER TABLE brokers ADD COLUMN IF NOT EXISTS dld_brn         TEXT;
ALTER TABLE brokers ADD COLUMN IF NOT EXISTS dld_broker_name TEXT;
ALTER TABLE brokers ADD COLUMN IF NOT EXISTS agency_name     TEXT;
