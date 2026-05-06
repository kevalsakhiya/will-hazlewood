"""Typed representation of one broker record from the DLD API."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date

from broker_scout.common.normalizers import (
    clean_str,
    normalize_email,
    normalize_phone,
    parse_dld_date,
    slugify_name,
    to_int,
)


def _synthesize_brn(payload: dict) -> str:
    """Stable surrogate BRN for records missing CardNumber. Keyed on
    (office license, name) so re-fetches don't create duplicates."""
    license_no = clean_str(payload.get("LicenseNumber")) or "noLicense"
    slug = slugify_name(
        payload.get("CardHolderNameEn") or payload.get("CardHolderNameAr")
    )
    return f"NOBRN:{license_no}:{slug}"


@dataclass(frozen=True, slots=True)
class DLDBroker:
    brn: str                              # DLD CardNumber (unique per broker)
    office_license_number: str | None     # DLD LicenseNumber (shared across brokers in same office)
    broker_name_en: str | None
    broker_name_ar: str | None
    phone: str | None
    mobile: str | None
    email: str | None
    real_estate_number: str | None
    office_name_en: str | None
    office_name_ar: str | None
    card_issue_date: date | None
    card_expiry_date: date | None
    office_issue_date: date | None
    office_expiry_date: date | None
    photo_url: str | None
    office_logo_url: str | None
    card_rank_id: int | None
    card_rank: str | None
    office_rank_id: int | None
    office_rank: str | None
    awards_count: int | None

    @classmethod
    def from_api(cls, payload: dict) -> DLDBroker:
        """Build a DLDBroker from one API record.

        If CardNumber is missing, synthesize a stable surrogate BRN keyed on
        (office license, name) so the broker is still searchable by name on
        PF / Bayut and doesn't get dropped.
        """
        brn = clean_str(payload.get("CardNumber")) or _synthesize_brn(payload)
        return cls(
            brn=brn,
            office_license_number=clean_str(payload.get("LicenseNumber")),
            broker_name_en=clean_str(payload.get("CardHolderNameEn")),
            broker_name_ar=clean_str(payload.get("CardHolderNameAr")),
            phone=normalize_phone(payload.get("CardHolderPhone")),
            mobile=normalize_phone(payload.get("CardHolderMobile")),
            email=normalize_email(payload.get("CardHolderEmail")),
            real_estate_number=clean_str(payload.get("RealEstateNumber")),
            office_name_en=clean_str(payload.get("OfficeNameEn")),
            office_name_ar=clean_str(payload.get("OfficeNameAr")),
            card_issue_date=parse_dld_date(payload.get("CardIssueDate")),
            card_expiry_date=parse_dld_date(payload.get("CardExpiryDate")),
            office_issue_date=parse_dld_date(payload.get("OfficeIssueDate")),
            office_expiry_date=parse_dld_date(payload.get("OfficeExpiryDate")),
            photo_url=clean_str(payload.get("CardHolderPhoto")),
            office_logo_url=clean_str(payload.get("OfficeLogo")),
            card_rank_id=to_int(payload.get("CardRankId")),
            card_rank=clean_str(payload.get("CardRank")),
            office_rank_id=to_int(payload.get("OfficeRankId")),
            office_rank=clean_str(payload.get("OfficeRank")),
            awards_count=to_int(payload.get("AwardsCount")),
        )

    def to_dict(self) -> dict:
        return asdict(self)
