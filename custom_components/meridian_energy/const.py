"""Constants for Meridian Energy / Powershop integration."""

from __future__ import annotations

from datetime import timedelta

from homeassistant.const import Platform

DOMAIN = "meridian_energy"
PLATFORMS = [Platform.SENSOR]

# Supplier constants
CONF_SUPPLIER = "supplier"
SUPPLIER_POWERSHOP = "powershop"
SUPPLIER_MERIDIAN = "meridian"
DEFAULT_SUPPLIER = SUPPLIER_POWERSHOP
SUPPLIER_OPTIONS = [SUPPLIER_POWERSHOP, SUPPLIER_MERIDIAN]

SUPPLIER_CONFIG = {
    SUPPLIER_POWERSHOP: {
        "name": "Powershop",
        "base_url": "https://secure.powershop.co.nz/",
        "login_path": "",  # form action="/" (the site root)
        "login_fail_text": "Powershop Login",
        "manufacturer": "Powershop",
    },
    SUPPLIER_MERIDIAN: {
        "name": "Meridian Energy",
        "base_url": "https://secure.meridianenergy.co.nz/",
        "login_path": "customer/login",
        "login_fail_text": "Log in",
        "manufacturer": "Meridian Energy",
    },
}

# Config / options keys
CONF_RATE_TYPE = "rate_type"
CONF_NETWORK = "network"
CONF_HISTORY_START = "history_start"
CONF_COOKIE = "cookie"
DEFAULT_RATE_TYPE = "special"
DEFAULT_NETWORK = "Vector"
DEFAULT_HISTORY_START = ""  # empty = rolling 365 days
RATE_TYPE_OPTIONS = ["special", "base"]

# Update interval for the coordinator (CSV + stats)
USAGE_UPDATE_INTERVAL = timedelta(hours=24)

# TOU period keys
PERIOD_NIGHT = "night"
PERIOD_PEAK = "peak"
PERIOD_OFFPEAK = "offpeak"
PERIOD_WEEKEND_OFFPEAK = "weekend_offpeak"
PERIOD_CONTROLLED = "controlled"
PERIODS = [PERIOD_NIGHT, PERIOD_PEAK, PERIOD_OFFPEAK, PERIOD_WEEKEND_OFFPEAK, PERIOD_CONTROLLED]

# HTTP timeout constants (seconds)
HTTP_TIMEOUT = 30
HTTP_TIMEOUT_COOKIE_CHECK = 15

# Default rolling window for usage history (days)
DEFAULT_LOOKBACK_DAYS = 365
