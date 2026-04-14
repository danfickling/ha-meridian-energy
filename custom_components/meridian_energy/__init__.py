"""Meridian Energy / Powershop integration (v2 — Kraken GraphQL).

Sets up the DataUpdateCoordinator, registers services, and
forwards platform setup to sensor.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import TypeAlias

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers import issue_registry as ir
import voluptuous as vol

from .api import MeridianEnergyApi
from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_BRAND,
    CONF_REFRESH_TOKEN,
    CONF_ACCOUNT_NUMBER,
    DEFAULT_BRAND,
    BRAND_CONFIG,
)
from .coordinator import MeridianCoordinator

_LOGGER = logging.getLogger(__name__)


@dataclass
class MeridianRuntimeData:
    """Runtime data stored on the config entry."""

    coordinator: MeridianCoordinator
    api: MeridianEnergyApi


MeridianConfigEntry: TypeAlias = ConfigEntry[MeridianRuntimeData]

SERVICE_REFRESH_RATES = "refresh_rates"
SERVICE_BACKFILL = "backfill"

# Map old v1 supplier names to v2 brand keys
_V1_SUPPLIER_TO_BRAND = {"powershop": "powershop", "meridian": "meridian"}


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate config entry from v1 to v2.

    v1 used password/cookie auth against the old API.
    v2 uses Firebase OTP auth against the Kraken GraphQL API.
    The auth data is incompatible so the user must re-authenticate.
    """
    if entry.version == 1:
        _LOGGER.info(
            "Migrating config entry %s from v1 → v2 (re-authentication required)",
            entry.entry_id,
        )
        old_data = dict(entry.data)
        new_data = {
            CONF_EMAIL: old_data.get("email", ""),
            CONF_BRAND: _V1_SUPPLIER_TO_BRAND.get(
                old_data.get("supplier", ""), DEFAULT_BRAND
            ),
            CONF_REFRESH_TOKEN: "",
            CONF_ACCOUNT_NUMBER: "",
        }
        hass.config_entries.async_update_entry(
            entry, data=new_data, version=2, minor_version=1,
        )
        ir.async_create_issue(
            hass,
            DOMAIN,
            f"v1_migration_{entry.entry_id}",
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key="v1_migration_required",
        )
        _LOGGER.warning(
            "Migration complete — please remove and re-add the integration "
            "to authenticate with the new OTP flow"
        )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Set up Meridian Energy / Powershop from a config entry."""
    brand = entry.data.get(CONF_BRAND, DEFAULT_BRAND)
    refresh_token = entry.data.get(CONF_REFRESH_TOKEN, "")
    account_number = entry.data.get(CONF_ACCOUNT_NUMBER, "")

    if not refresh_token or not account_number:
        raise ConfigEntryAuthFailed(
            "Missing credentials — please remove and re-add the integration"
        )

    session = async_get_clientsession(hass)
    api = MeridianEnergyApi(brand, refresh_token, account_number, session)

    coordinator = MeridianCoordinator(hass, api, entry)
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = MeridianRuntimeData(coordinator=coordinator, api=api)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    _register_services(hass)

    brand_name = BRAND_CONFIG.get(brand, BRAND_CONFIG[DEFAULT_BRAND])["name"]
    _LOGGER.info(
        "%s integration loaded (account=%s)",
        brand_name,
        account_number,
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Unload a config entry."""
    if await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        remaining = [
            e for e in hass.config_entries.async_entries(DOMAIN)
            if e.entry_id != entry.entry_id
        ]
        if not remaining:
            hass.services.async_remove(DOMAIN, SERVICE_REFRESH_RATES)
            hass.services.async_remove(DOMAIN, SERVICE_BACKFILL)
        return True
    return False


def _register_services(hass: HomeAssistant) -> None:
    """Register integration services (idempotent)."""
    if hass.services.has_service(DOMAIN, SERVICE_REFRESH_RATES):
        return

    async def handle_refresh_rates(call: ServiceCall) -> None:
        """Force refresh of rates and TOU schedule."""
        for entry in hass.config_entries.async_entries(DOMAIN):
            if hasattr(entry, "runtime_data") and entry.runtime_data:
                coord = entry.runtime_data.coordinator
                if coord.last_update_success:
                    await coord.async_force_rate_refresh()
                else:
                    _LOGGER.warning(
                        "Skipping refresh for %s — last update failed",
                        coord.sensor_name,
                    )

    async def handle_backfill(call: ServiceCall) -> None:
        """Re-fetch and re-publish statistics for a date range."""
        start_date: date = call.data["start_date"]
        end_date: date | None = call.data.get("end_date")
        for entry in hass.config_entries.async_entries(DOMAIN):
            if hasattr(entry, "runtime_data") and entry.runtime_data:
                coord = entry.runtime_data.coordinator
                if coord.last_update_success:
                    await coord.async_backfill(start_date, end_date)
                else:
                    _LOGGER.warning(
                        "Skipping backfill for %s — last update failed",
                        coord.sensor_name,
                    )

    hass.services.async_register(
        DOMAIN, SERVICE_REFRESH_RATES, handle_refresh_rates,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_BACKFILL,
        handle_backfill,
        schema=vol.Schema({
            vol.Required("start_date"): date.fromisoformat,
            vol.Optional("end_date"): date.fromisoformat,
        }),
    )
    _LOGGER.debug("Registered %s services", DOMAIN)
