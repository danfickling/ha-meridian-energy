"""Rate parsing for Meridian Energy / Powershop (v2 — Kraken API).

Parses rates and TOU schedules from the GraphQL API response,
mapping API bucket names (N9, PK5, OPK10, etc.) to canonical
period keys that match v1 entity IDs for statistics continuity.
"""

from __future__ import annotations

import logging
import re

from .const import (
    PERIOD_CONTROLLED,
    PERIOD_OFFPEAK,
    TOU_BUCKET_PREFIX_MAP,
    PERIOD_DISPLAY_NAMES,
)

_LOGGER = logging.getLogger(__name__)


def classify_bucket(bucket_name: str) -> str | None:
    """Map a touBucketName (e.g. 'N9', 'PK5', 'OPK10') to a canonical period key.

    Uses prefix matching against TOU_BUCKET_PREFIX_MAP.
    Falls back to deriving a key from the bucket name itself (lowercase,
    trailing digits stripped) so new plan types work automatically.
    Returns None only for empty buckets.
    """
    if not bucket_name:
        return None
    upper = bucket_name.upper()
    # Try longest prefix first to avoid "N" matching before "NO" etc.
    for prefix in sorted(TOU_BUCKET_PREFIX_MAP, key=len, reverse=True):
        if upper.startswith(prefix):
            return TOU_BUCKET_PREFIX_MAP[prefix]
    # Fallback: derive a key from the bucket name
    key = re.sub(r"\d+$", "", bucket_name).strip().lower().replace("-", "_").replace(" ", "_")
    if key:
        _LOGGER.info("Unknown TOU bucket '%s' mapped to period '%s'", bucket_name, key)
        return key
    _LOGGER.warning("Unrecognised TOU bucket name: '%s'", bucket_name)
    return None


def parse_rates(api_rates: list[dict]) -> dict:
    """Parse rates from the API response into a structured dict.

    Input: list of rate dicts from ``agreement.rates[]``, each with keys
    ``touBucketName``, ``rateIncludingTax``, ``unitType``, ``bandCategory``.

    Returns::

        {
            "tou_rates": {"night": 0.2362, "peak": 0.4077, ...},  # NZD/kWh
            "daily_charge": 4.14,  # NZD/day
        }

    Rates from the API are in **cents** (string like ``"23.62000"``).
    They are converted to **NZD** by dividing by 100.
    """
    tou_rates: dict[str, float] = {}
    daily_charge: float | None = None

    for rate in api_rates:
        unit_type = (rate.get("unitType") or "").strip()
        band = (rate.get("bandCategory") or "").strip().upper()
        bucket = (rate.get("touBucketName") or "").strip()

        try:
            value_cents = float(rate.get("rateIncludingTax", 0))
        except (ValueError, TypeError):
            continue

        value_dollars = round(value_cents / 100, 6)

        # Daily / standing charge
        if band == "STANDING_CHARGE" or "days" in unit_type.lower():
            daily_charge = value_dollars
            continue

        # TOU rate
        period = classify_bucket(bucket)
        if period and period not in tou_rates:
            tou_rates[period] = value_dollars
            continue

        # Non-TOU consumption → controlled (e.g. hot water cylinder)
        if not bucket and value_dollars > 0 and PERIOD_CONTROLLED not in tou_rates:
            tou_rates[PERIOD_CONTROLLED] = value_dollars

    return {
        "tou_rates": tou_rates,
        "daily_charge": daily_charge or 0.0,
    }


def period_display_name(period_key: str) -> str:
    """Return a human-readable display name for a period key."""
    return PERIOD_DISPLAY_NAMES.get(period_key, period_key.replace("_", " ").title())
