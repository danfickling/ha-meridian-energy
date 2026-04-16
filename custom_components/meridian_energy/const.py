"""Constants for Meridian Energy / Powershop integration (v2 — Kraken API)."""

from __future__ import annotations

from datetime import timedelta

from homeassistant.const import Platform

DOMAIN = "meridian_energy"
PLATFORMS = [Platform.SENSOR]

# ---------------------------------------------------------------------------
# Brand / supplier configuration
# ---------------------------------------------------------------------------
CONF_BRAND = "brand"
BRAND_POWERSHOP = "powershop"
BRAND_MERIDIAN = "meridian"
DEFAULT_BRAND = BRAND_POWERSHOP
BRAND_OPTIONS = [BRAND_POWERSHOP, BRAND_MERIDIAN]

# Shared Firebase project used by both brands (meridian-retail-ciam)
FIREBASE_API_KEY = "AIzaSyCYCKXQhGmo7haJxAAyO_7mIPrV7jtxsK8"

BRAND_CONFIG: dict[str, dict[str, str]] = {
    BRAND_POWERSHOP: {
        "name": "Powershop",
        "api_url": "https://api.powershop.nz/v1/graphql/",
        "auth_domain": "auth.powershop.nz",
        "app_origin": "https://app.powershop.nz",
        "manufacturer": "Powershop",
    },
    BRAND_MERIDIAN: {
        "name": "Meridian Energy",
        "api_url": "https://api.meridianenergy.nz/v1/graphql/",
        "auth_domain": "auth.meridianenergy.nz",
        "app_origin": "https://app.meridianenergy.nz",
        "manufacturer": "Meridian Energy",
    },
}

# Config entry data keys
CONF_REFRESH_TOKEN = "refresh_token"
CONF_ACCOUNT_NUMBER = "account_number"
CONF_EMAIL = "email"

# ---------------------------------------------------------------------------
# Update intervals
# ---------------------------------------------------------------------------
# Usage / stats refresh — 30-min data is available near-real-time
USAGE_UPDATE_INTERVAL = timedelta(minutes=30)

# Rates / TOU schedule refresh (changes infrequently)
RATES_UPDATE_INTERVAL = timedelta(hours=24)

# ---------------------------------------------------------------------------
# TOU period key mapping
# ---------------------------------------------------------------------------
# Maps API touBucketName prefixes to canonical period keys that match
# the existing v1 entity IDs (for statistics continuity).
PERIOD_NIGHT = "night"
PERIOD_PEAK = "peak"
PERIOD_OFFPEAK = "offpeak"
PERIOD_CONTROLLED = "controlled"

TOU_BUCKET_PREFIX_MAP: dict[str, str] = {
    "N": PERIOD_NIGHT,
    "PK": PERIOD_PEAK,
    "OPK": PERIOD_OFFPEAK,
    "CON": PERIOD_CONTROLLED,
}

# Display names for period keys (fallback: titlecased key)
PERIOD_DISPLAY_NAMES: dict[str, str] = {
    PERIOD_NIGHT: "Night",
    PERIOD_PEAK: "Peak",
    PERIOD_OFFPEAK: "Off-Peak",
    PERIOD_CONTROLLED: "Controlled",
}

# Default lookback for initial history import (days).
# Only used on first setup; subsequent polls resume incrementally.
DEFAULT_LOOKBACK_DAYS = 3650

# How many days of recent data to treat as estimated.
# The Kraken API provides no explicit flag; the Powershop app treats
# today and yesterday as estimated (pending meter reconciliation).
# Data older than this many days is considered actual.
ESTIMATED_DAYS = 2

# How many extra days beyond ESTIMATED_DAYS to re-process on each poll.
# If estimated data slipped through before ESTIMATED_DAYS elapsed, re-
# processing this window lets upserts correct it with actual data.
# Total check window = ESTIMATED_DAYS + RECONCILIATION_DAYS (= 3 days).
RECONCILIATION_DAYS = 1
