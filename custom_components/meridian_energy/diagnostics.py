"""Diagnostics support for Meridian Energy / Powershop integration."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.const import CONF_EMAIL

from . import MeridianConfigEntry
from .const import CONF_ACCOUNT_NUMBER, CONF_REFRESH_TOKEN, DOMAIN

TO_REDACT = {CONF_EMAIL, CONF_REFRESH_TOKEN, CONF_ACCOUNT_NUMBER}


async def async_get_config_entry_diagnostics(
    hass: Any,
    entry: MeridianConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data.coordinator
    coordinator_data = coordinator.data

    diag: dict[str, Any] = {
        "integration_version": entry.version,
        "domain": DOMAIN,
        "config_entry": async_redact_data(
            {
                "data": dict(entry.data),
                "options": dict(entry.options),
            },
            TO_REDACT,
        ),
    }

    diag["coordinator_state"] = {
        "last_update_success": coordinator.last_update_success,
        "last_exception": str(coordinator.last_exception)
        if coordinator.last_exception
        else None,
        "update_interval_seconds": coordinator.update_interval.total_seconds()
        if coordinator.update_interval
        else None,
    }

    if coordinator_data:
        diag["coordinator_data"] = {
            "brand": coordinator_data.brand,
            "sensor_name": coordinator_data.sensor_name,
            "product": coordinator_data.product,
            "tou_period": coordinator_data.tou_period,
            "current_rate": coordinator_data.current_rate,
            "daily_charge": coordinator_data.daily_charge,
            "rates": coordinator_data.rates,
            "has_solar": coordinator_data.has_solar,
            "solar_export_kwh": coordinator_data.solar_export_kwh,
            "schedule": coordinator_data.schedule,
            "last_usage_update": str(coordinator_data.last_usage_update)
            if coordinator_data.last_usage_update
            else None,
            "detected_periods": coordinator_data.detected_periods,
            "balance": coordinator_data.balance,
            "billing_period_start": coordinator_data.billing_period_start,
            "billing_period_end": coordinator_data.billing_period_end,
            "next_billing_date": coordinator_data.next_billing_date,
        }

    return diag
