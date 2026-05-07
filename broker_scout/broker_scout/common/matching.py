"""DLD ↔ PropertyFinder broker matching layer.

Given a DLD broker (ground truth) and a list of PF search-result
candidates, produce a `MatchResult` with one of these statuses:

    exact_brn    — promoted later, in `parse_agent`, when the PF
                   profile's BRN equals the DLD BRN.
    name_unique  — exactly one candidate whose normalized name equals
                   the DLD broker's normalized name.
    name_fuzzy   — exactly one candidate's name passes the fuzzy
                   threshold (token-set ratio).
    ambiguous    — more than one candidate plausible; do not pick.
    not_found    — zero candidates plausible (or zero candidates at all).

PF rarely exposes a BRN on its search-results page, so BRN matching
is deliberately deferred: `match_candidates` returns a name-based
status, and `promote_to_brn_match` upgrades it to `exact_brn` once
the profile has been fetched.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz

from broker_scout.common.dld_models import DLDBroker
from broker_scout.schemas import MatchStatusType

DEFAULT_FUZZY_THRESHOLD = 90
EXACT_NAME_CONFIDENCE = 0.95
EXACT_BRN_CONFIDENCE = 1.0

# Anything that isn't a letter, digit, or single space is collapsed to
# a space for normalization. Captures DLD's all-caps style ("DHARAM
# VIR JUNEJA") and PF's title case + occasional initials ("Dharam V.
# Juneja") in the same key after `_normalize_name`.
_NORMALIZE_RE = re.compile(r"[^a-z0-9 ]+")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class Candidate:
    """One PF search result, as much as the search page surfaces.
    `brn` is None unless the search page exposes it (rarely)."""

    name: str
    url: str
    brn: Optional[str] = None


@dataclass(frozen=True, slots=True)
class MatchResult:
    status: MatchStatusType
    confidence: float                     # 0..1
    candidate_url: Optional[str] = None
    candidate_brn: Optional[str] = None


def _normalize_name(s: Optional[str]) -> str:
    """Lowercase, strip punctuation, collapse whitespace.

    Returns empty string for None / empty input — caller decides
    whether that's a hard-fail (no DLD name) or a soft-skip.
    """
    if not s:
        return ""
    s = s.lower()
    s = _NORMALIZE_RE.sub(" ", s)
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s


def match_candidates(
    dld_broker: DLDBroker,
    candidates: list[Candidate],
    fuzzy_threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> MatchResult:
    """Disambiguate PF search results against a DLD broker by name.

    See module docstring for status semantics.
    """
    if not candidates:
        return MatchResult(status="not_found", confidence=0.0)

    dld_norm = _normalize_name(dld_broker.broker_name_en or dld_broker.broker_name_ar)
    if not dld_norm:
        # No DLD name to match against — treat the candidate list as
        # ambiguous if multiple, or take the only one as a weak match.
        if len(candidates) == 1:
            return MatchResult(
                status="name_fuzzy",
                confidence=0.5,
                candidate_url=candidates[0].url,
                candidate_brn=candidates[0].brn,
            )
        return MatchResult(status="ambiguous", confidence=0.0)

    # Exact-name pass first — strongest pre-profile-fetch signal.
    exact_matches = [c for c in candidates if _normalize_name(c.name) == dld_norm]
    if len(exact_matches) == 1 and len(candidates) == 1:
        c = exact_matches[0]
        return MatchResult(
            status="name_unique",
            confidence=EXACT_NAME_CONFIDENCE,
            candidate_url=c.url,
            candidate_brn=c.brn,
        )

    # Fuzzy pass — token_set_ratio handles word reorderings, partials,
    # initials. Returns 0..100; we normalize to 0..1 for confidence.
    scored = [(fuzz.token_set_ratio(dld_norm, _normalize_name(c.name)), c) for c in candidates]
    above = [(score, c) for score, c in scored if score >= fuzzy_threshold]

    if not above:
        return MatchResult(status="not_found", confidence=0.0)

    if len(above) > 1:
        return MatchResult(status="ambiguous", confidence=0.0)

    score, c = above[0]
    return MatchResult(
        status="name_fuzzy",
        confidence=score / 100.0,
        candidate_url=c.url,
        candidate_brn=c.brn,
    )


def promote_to_brn_match(
    match_result: MatchResult, profile_brn: Optional[str], dld_brn: str
) -> MatchResult:
    """Upgrade a name-based match to `exact_brn` once we've fetched the
    profile and confirmed the BRN matches DLD.

    No-op (returns the input) when:
      * `match_result.status` was already `exact_brn` (idempotent).
      * `profile_brn` is missing.
      * `profile_brn` differs from `dld_brn` (legitimate name match
        with a different BRN — keep the existing status; downstream
        monitors can flag the disagreement).
    """
    if match_result.status == "exact_brn":
        return match_result
    if not profile_brn or profile_brn != dld_brn:
        return match_result
    return MatchResult(
        status="exact_brn",
        confidence=EXACT_BRN_CONFIDENCE,
        candidate_url=match_result.candidate_url,
        candidate_brn=profile_brn,
    )
