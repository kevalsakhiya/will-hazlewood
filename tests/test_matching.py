"""Coverage for `common.matching` — the DLD ↔ PF name matcher."""

from __future__ import annotations

from datetime import date

import pytest

from broker_scout.common.dld_models import DLDBroker
from broker_scout.common.matching import (
    DEFAULT_FUZZY_THRESHOLD,
    EXACT_BRN_CONFIDENCE,
    EXACT_NAME_CONFIDENCE,
    Candidate,
    MatchResult,
    _normalize_name,
    match_candidates,
    promote_to_brn_match,
)


def _dld(name: str = "DHARAM VIR JUNEJA", brn: str = "81462") -> DLDBroker:
    """Minimal DLDBroker with sensible defaults for matching tests.

    Only `broker_name_en` and `brn` matter for the matcher; the rest
    of the dataclass fields are filled with None / today's date.
    """
    return DLDBroker(
        brn=brn,
        office_license_number=None,
        broker_name_en=name,
        broker_name_ar=None,
        phone=None,
        mobile=None,
        email=None,
        real_estate_number=None,
        office_name_en=None,
        office_name_ar=None,
        card_issue_date=date(2020, 1, 1),
        card_expiry_date=None,
        office_issue_date=None,
        office_expiry_date=None,
        photo_url=None,
        office_logo_url=None,
        card_rank_id=None,
        card_rank=None,
        office_rank_id=None,
        office_rank=None,
        awards_count=None,
    )


# ============================================================ _normalize_name


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("DHARAM VIR JUNEJA", "dharam vir juneja"),
        ("Dharam Vir Juneja", "dharam vir juneja"),
        ("  Dharam   Vir  Juneja  ", "dharam vir juneja"),
        ("Dharam V. Juneja", "dharam v juneja"),
        ("Mohammed-Al Saeed", "mohammed al saeed"),
        ("Foo (LLC)", "foo llc"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_name(raw, expected):
    assert _normalize_name(raw) == expected


# ============================================================ match_candidates


def test_no_candidates_is_not_found():
    result = match_candidates(_dld(), [])
    assert result.status == "not_found"
    assert result.confidence == 0.0
    assert result.candidate_url is None


def test_single_exact_name_candidate_is_unique():
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA"),
        [Candidate(name="Dharam Vir Juneja", url="https://pf/d-v-j")],
    )
    assert result.status == "name_unique"
    assert result.confidence == EXACT_NAME_CONFIDENCE
    assert result.candidate_url == "https://pf/d-v-j"


def test_single_fuzzy_candidate_picked():
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA"),
        [Candidate(name="Dharam V. Juneja", url="https://pf/d-v-j")],
    )
    assert result.status == "name_fuzzy"
    assert result.confidence >= DEFAULT_FUZZY_THRESHOLD / 100.0
    assert result.candidate_url == "https://pf/d-v-j"


def test_one_exact_among_many_is_still_fuzzy_at_best():
    """When multiple candidates exist, name_unique requires len==1.
    With more candidates, we fall to fuzzy (which may also pick this one
    or be ambiguous, depending on scores)."""
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA"),
        [
            Candidate(name="Dharam Vir Juneja", url="https://pf/exact"),
            Candidate(name="Totally Different Person", url="https://pf/other"),
        ],
    )
    # exact + low-similarity — fuzzy picks the exact one as unique above-threshold
    assert result.status == "name_fuzzy"
    assert result.candidate_url == "https://pf/exact"


def test_multiple_above_threshold_is_ambiguous():
    """Two candidates that both look like the DLD name → don't pick."""
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA"),
        [
            Candidate(name="Dharam Vir Juneja", url="https://pf/a"),
            Candidate(name="Dharam V Juneja", url="https://pf/b"),
        ],
    )
    assert result.status == "ambiguous"
    assert result.candidate_url is None


def test_no_candidate_above_threshold_is_not_found():
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA"),
        [
            Candidate(name="Alice Smith", url="https://pf/alice"),
            Candidate(name="Bob Jones", url="https://pf/bob"),
        ],
    )
    assert result.status == "not_found"


def test_threshold_is_configurable():
    """A score that fails the default threshold but passes a lowered one
    flips not_found → name_fuzzy."""
    candidates = [Candidate(name="DV Juneja", url="https://pf/x")]
    high = match_candidates(_dld("DHARAM VIR JUNEJA"), candidates, fuzzy_threshold=95)
    low = match_candidates(_dld("DHARAM VIR JUNEJA"), candidates, fuzzy_threshold=50)
    assert high.status == "not_found"
    assert low.status == "name_fuzzy"


def test_candidate_brn_carried_through():
    """When the search page does expose a BRN, we pipe it into the result
    so post-fetch promotion can compare without re-extracting."""
    result = match_candidates(
        _dld("DHARAM VIR JUNEJA", brn="81462"),
        [Candidate(name="Dharam Vir Juneja", url="https://pf/x", brn="81462")],
    )
    assert result.candidate_brn == "81462"


def test_dld_with_no_name_falls_back_gracefully():
    """If both English and Arabic DLD names are missing (rare), we
    return a weak fuzzy match for a single candidate, ambiguous for
    multiple. Better than crashing."""
    no_name_dld = DLDBroker(
        brn="x",
        office_license_number=None,
        broker_name_en=None,
        broker_name_ar=None,
        phone=None, mobile=None, email=None,
        real_estate_number=None,
        office_name_en=None, office_name_ar=None,
        card_issue_date=None, card_expiry_date=None,
        office_issue_date=None, office_expiry_date=None,
        photo_url=None, office_logo_url=None,
        card_rank_id=None, card_rank=None,
        office_rank_id=None, office_rank=None,
        awards_count=None,
    )
    one = match_candidates(no_name_dld, [Candidate(name="X", url="https://x")])
    assert one.status == "name_fuzzy"
    assert one.confidence == 0.5

    many = match_candidates(
        no_name_dld,
        [Candidate(name="X", url="https://x"), Candidate(name="Y", url="https://y")],
    )
    assert many.status == "ambiguous"


# ============================================================ promote_to_brn_match


def test_promote_to_brn_when_match():
    base = MatchResult(
        status="name_unique",
        confidence=0.95,
        candidate_url="https://pf/x",
    )
    promoted = promote_to_brn_match(base, profile_brn="81462", dld_brn="81462")
    assert promoted.status == "exact_brn"
    assert promoted.confidence == EXACT_BRN_CONFIDENCE
    assert promoted.candidate_url == "https://pf/x"
    assert promoted.candidate_brn == "81462"


def test_promote_no_op_when_brn_missing():
    base = MatchResult(status="name_unique", confidence=0.95)
    assert promote_to_brn_match(base, None, "81462") is base


def test_promote_no_op_when_brn_disagrees():
    """Legitimate name match with a different BRN keeps the original
    status — don't silently overwrite. Phase 9 monitors flag drift."""
    base = MatchResult(status="name_unique", confidence=0.95)
    out = promote_to_brn_match(base, profile_brn="99999", dld_brn="81462")
    assert out is base  # unchanged


def test_promote_idempotent_on_already_exact_brn():
    base = MatchResult(status="exact_brn", confidence=1.0, candidate_brn="81462")
    out = promote_to_brn_match(base, profile_brn="81462", dld_brn="81462")
    assert out is base
