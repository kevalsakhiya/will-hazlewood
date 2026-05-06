"""Pure normalization helpers for DLD payload fields."""

from __future__ import annotations

import re
from datetime import date, datetime

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def clean_str(s: str | None) -> str | None:
    """Strip whitespace; return None for empty / None input."""
    if s is None:
        return None
    s = s.strip()
    return s or None


def parse_dld_date(s: str | None) -> date | None:
    """Parse DLD's `YYYY-MM-DDT00:00:00` strings into a `date`.

    Returns None for blank input or unparseable values.
    """
    s = clean_str(s)
    if s is None:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


DEFAULT_COUNTRY_CODE = "971"  # DLD is UAE-only; bare local numbers default to 971.


def normalize_phone(s: str | None) -> str | None:
    """Normalize a DLD phone like `971|0506555800`, `971|50-6555800`, or
    bare `0556103693` to E.164.

    DLD's format is usually `<country>|<local>`, but some entries have no
    separator. When the country side is empty we default to UAE (971).

    Returns `+<digits>` or None if no usable digits remain.
    """
    s = clean_str(s)
    if s is None:
        return None

    # already E.164-ish — just strip non-digits after the leading +
    if s.startswith("+"):
        digits = re.sub(r"\D", "", s)
        return f"+{digits}" if digits else None

    if "|" in s:
        country, _, local = s.partition("|")
    else:
        country, local = "", s

    country_digits = re.sub(r"\D", "", country)
    local_digits = re.sub(r"\D", "", local)

    if not country_digits:
        country_digits = DEFAULT_COUNTRY_CODE

    if local_digits.startswith("0"):
        local_digits = local_digits.lstrip("0")

    if not local_digits:
        return None

    return f"+{country_digits}{local_digits}"


def normalize_email(s: str | None) -> str | None:
    """Lowercase + strip; return None if input doesn't look like an email."""
    s = clean_str(s)
    if s is None:
        return None
    s = s.lower()
    return s if _EMAIL_RE.match(s) else None


def slugify_name(s: str | None) -> str:
    """Lowercase + collapse non-alphanumerics to underscore. Empty -> 'unknown'."""
    s = clean_str(s)
    if s is None:
        return "unknown"
    slug = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return slug or "unknown"


def to_int(v) -> int | None:
    """Coerce to int; return None on failure / empty."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
