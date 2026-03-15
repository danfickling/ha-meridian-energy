"""Diagnostics support for Meridian Energy / Powershop integration."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD

from . import MeridianConfigEntry
from .const import CONF_COOKIE

TO_REDACT = {CONF_EMAIL, CONF_PASSWORD, CONF_COOKIE}


async def async_get_config_entry_diagnostics(
    hass: Any,
    entry: MeridianConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    data = entry.runtime_data
    coordinator = data.coordinator
    coordinator_data = coordinator.data

    diag: dict[str, Any] = {
        "config_entry": async_redact_data(
            {
                "data": dict(entry.data),
                "options": dict(entry.options),
            },
            TO_REDACT,
        ),
    }

    if coordinator_data:
        diag["coordinator_data"] = {
            "supplier": coordinator_data.supplier,
            "sensor_name": coordinator_data.sensor_name,
            "rate_type": coordinator_data.rate_type,
            "tou_period": coordinator_data.tou_period,
            "current_rate": coordinator_data.current_rate,
            "daily_charge": coordinator_data.daily_charge,
            "rates": coordinator_data.rates,
            "base_rates": coordinator_data.base_rates,
            "special_rates": coordinator_data.special_rates,
            "has_solar": coordinator_data.has_solar,
            "solar_export_kwh": coordinator_data.solar_export_kwh,
            "schedule_network": coordinator_data.schedule_network,
            "schedule_changed": coordinator_data.schedule_changed,
            "last_usage_update": str(coordinator_data.last_usage_update)
            if coordinator_data.last_usage_update
            else None,
            "last_rate_scrape": coordinator_data.last_rate_scrape,
            "cache_months_special": coordinator_data.cache_months_special,
            "cache_months_base": coordinator_data.cache_months_base,
            "stats_days": coordinator_data.stats_days,
            "stats_rows": coordinator_data.stats_rows,
            "detected_periods": coordinator_data.detected_periods,
        }

    return diag
