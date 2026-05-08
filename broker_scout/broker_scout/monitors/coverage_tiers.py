"""Field-coverage tier definitions.

Data only — Phase 9.3.2's FieldCoverageMonitor wiring will consume
these. The tiering is the answer to "how do we set per-field coverage
thresholds when most items are matched but some are not_found stubs?"

Two layers:

  1. Provenance fields — set on EVERY item including stubs. Tier
     should expect ≥ 99% coverage.
  2. PF-extracted fields — populated only when match succeeds (i.e.
     `match_status` is exact_brn / name_unique / name_fuzzy). Coverage
     is bounded by the match rate, so the tier threshold (Critical /
     High / Medium) is enforced ONLY over the matched-row subset by a
     custom variant of FieldCoverageMonitor.

Integrity: every dataclass field on PropertyFinderBrokerItem must
appear in exactly one tier (or be deliberately omitted via
`OMITTED_FIELDS`). Module-load assertion catches additions in Phase 8
(Bayut) that forget to be tiered.
"""

from __future__ import annotations

from broker_scout.items import PropertyFinderBrokerItem

# Always set (spider + RunIdExtension wire these into every item,
# matched or stub). FieldCoverageMonitor enforces over ALL items.
PROVENANCE_FIELDS: tuple[str, ...] = (
    "platform",
    "scrape_date",
    "match_status",
    "dld_brn",
    "dld_broker_name",
)

# Only present on matched items. The custom matched-only variant of
# FieldCoverageMonitor enforces these over the matched subset.
PF_CRITICAL_FIELDS: tuple[str, ...] = (
    "broker_name",
    "agent_url",
    "brn",
)
PF_HIGH_FIELDS: tuple[str, ...] = (
    "listings_total",
    "experience_since",
    "nationality",
    "agency_url",
    "agency_name",
)
PF_MEDIUM_FIELDS: tuple[str, ...] = (
    "whatsapp_response_time",
    "is_superagent",
    "agent_specialization",
    "agency_registration_number",
)

# Tracked but not threshold-enforced. Built dynamically from the
# dataclass so additions in PropertyFinderBrokerItem flow through
# without manual updates here.
_INFORMATIONAL_PREFIXES = (
    "closed_transaction_",
    "average_listing_",
    "average_monthly_deal_volume_",
    "most_recent_",
)

INFORMATIONAL_FIELDS: tuple[str, ...] = (
    "match_confidence",
    "listings_for_sale",
    "listings_for_rent",
    "listings_with_marketing_spend",
    "closed_deals_total",
    *(
        f
        for f in PropertyFinderBrokerItem.__dataclass_fields__
        if f.startswith(_INFORMATIONAL_PREFIXES)
    ),
)

# Fields deliberately omitted from coverage tracking (none today; kept
# for future phases that may legitimately want a field outside any
# tier — e.g. internal-only debugging fields).
OMITTED_FIELDS: tuple[str, ...] = ()


# Integrity check: every dataclass field is tiered exactly once or
# explicitly omitted. Fails loudly at import time if Phase 8 / future
# phases add fields without tiering them.

ALL_TIERED_FIELDS: tuple[str, ...] = (
    PROVENANCE_FIELDS
    + PF_CRITICAL_FIELDS
    + PF_HIGH_FIELDS
    + PF_MEDIUM_FIELDS
    + INFORMATIONAL_FIELDS
    + OMITTED_FIELDS
)

_dataclass_fields = set(PropertyFinderBrokerItem.__dataclass_fields__)
_tiered = set(ALL_TIERED_FIELDS)

_missing = _dataclass_fields - _tiered
_unknown = _tiered - _dataclass_fields
_dupes: list[str] = [f for f in ALL_TIERED_FIELDS if list(ALL_TIERED_FIELDS).count(f) > 1]

assert not _missing, (
    f"PropertyFinderBrokerItem fields not in any tier: {_missing}. "
    f"Add them to a tier (or to OMITTED_FIELDS) in coverage_tiers.py."
)
assert not _unknown, (
    f"coverage_tiers.py references fields not on the item dataclass: {_unknown}"
)
assert not _dupes, (
    f"fields appear in multiple tiers: {set(_dupes)}"
)
