"""Meridian Energy / Powershop integration.

Sets up the DataUpdateCoordinator, registers services, and
forwards platform setup to sensor.py.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TypeAlias

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant, ServiceCall
import homeassistant.helpers.config_validation as cv

from .api import MeridianEnergyApi
from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_RATE_TYPE,
    CONF_NETWORK,
    CONF_SUPPLIER,
    CONF_HISTORY_START,
    CONF_COOKIE,
    DEFAULT_RATE_TYPE,
    DEFAULT_NETWORK,
    DEFAULT_SUPPLIER,
    DEFAULT_HISTORY_START,
    SUPPLIER_CONFIG,
    RATE_TYPE_OPTIONS,
    SUPPLIER_OPTIONS,
)
from .coordinator import MeridianCoordinator
from .schedule import NETWORKS

_LOGGER = logging.getLogger(__name__)


@dataclass
class MeridianRuntimeData:
    """Runtime data stored on the config entry."""

    coordinator: MeridianCoordinator
    api: MeridianEnergyApi


MeridianConfigEntry: TypeAlias = ConfigEntry[MeridianRuntimeData]

SERVICE_REFRESH_RATES = "refresh_rates"
SERVICE_REIMPORT_HISTORY = "reimport_history"
SERVICE_CHECK_SCHEDULE = "check_schedule"
SERVICE_UPDATE_SCHEDULE = "update_schedule"


async def async_setup_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Set up Meridian Energy / Powershop from a config entry."""
    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]
    supplier = entry.data.get(CONF_SUPPLIER, DEFAULT_SUPPLIER)
    rate_type = entry.options.get(CONF_RATE_TYPE, DEFAULT_RATE_TYPE)
    history_start = (
        entry.options.get(CONF_HISTORY_START)
        or entry.data.get(CONF_HISTORY_START, DEFAULT_HISTORY_START)
    )
    network = (
        entry.options.get(CONF_NETWORK)
        or entry.data.get(CONF_NETWORK, DEFAULT_NETWORK)
    )
    cookie = (
        entry.options.get(CONF_COOKIE)
        or entry.data.get(CONF_COOKIE, "")
    )

    api = MeridianEnergyApi(email, password, supplier=supplier, history_start=history_start, cookie=cookie)
    coordinator = MeridianCoordinator(hass, api, entry, rate_type, network, supplier=supplier)

    # Initial data fetch (rate cache available immediately; CSV may fail gracefully)
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = MeridianRuntimeData(coordinator=coordinator, api=api)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Register integration services (idempotent)
    _register_services(hass)

    # Listen for options changes (e.g. rate_type, network, supplier)
    entry.async_on_unload(entry.add_update_listener(_options_update_listener))

    supplier_name = SUPPLIER_CONFIG[supplier]["name"]
    _LOGGER.info(
        "%s integration loaded (rate_type=%s, network=%s)",
        supplier_name,
        rate_type,
        network,
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: MeridianConfigEntry) -> bool:
    """Unload a config entry."""
    if await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        # Unregister services when last entry is removed
        remaining = [
            e for e in hass.config_entries.async_entries(DOMAIN)
            if e.entry_id != entry.entry_id
        ]
        if not remaining:
            for svc in (
                SERVICE_REFRESH_RATES,
                SERVICE_REIMPORT_HISTORY,
                SERVICE_CHECK_SCHEDULE,
                SERVICE_UPDATE_SCHEDULE,
            ):
                hass.services.async_remove(DOMAIN, svc)
        return True
    return False


async def _options_update_listener(
    hass: HomeAssistant, entry: MeridianConfigEntry
) -> None:
    """Handle options update (e.g. rate_type, network, or supplier change)."""
    data = entry.runtime_data
    coordinator = data.coordinator
    api = data.api
    new_rate_type = entry.options.get(CONF_RATE_TYPE, DEFAULT_RATE_TYPE)
    new_network = (
        entry.options.get(CONF_NETWORK)
        or entry.data.get(CONF_NETWORK, DEFAULT_NETWORK)
    )
    new_supplier = (
        entry.options.get(CONF_SUPPLIER)
        or entry.data.get(CONF_SUPPLIER, DEFAULT_SUPPLIER)
    )
    new_history_start = (
        entry.options.get(CONF_HISTORY_START)
        or entry.data.get(CONF_HISTORY_START, DEFAULT_HISTORY_START)
    )
    new_cookie = (
        entry.options.get(CONF_COOKIE)
        or entry.data.get(CONF_COOKIE, "")
    )

    # Validate before applying
    if new_rate_type not in RATE_TYPE_OPTIONS:
        _LOGGER.error("Invalid rate type '%s' — ignoring options update", new_rate_type)
        return
    if new_supplier not in SUPPLIER_OPTIONS:
        _LOGGER.error("Invalid supplier '%s' — ignoring options update", new_supplier)
        return
    if new_network not in NETWORKS:
        _LOGGER.warning("Unknown network '%s' — using default schedule", new_network)

    changed = False
    if coordinator.rate_type != new_rate_type:
        _LOGGER.info("Rate type changed to '%s'", new_rate_type)
        coordinator.rate_type = new_rate_type
        changed = True

    if coordinator.network != new_network:
        _LOGGER.info("Network changed to '%s'", new_network)
        coordinator.network = new_network
        changed = True

    if coordinator.supplier != new_supplier:
        _LOGGER.info("Supplier changed to '%s'", new_supplier)
        api.supplier = new_supplier
        coordinator.supplier = new_supplier
        changed = True

    if api.history_start != new_history_start:
        _LOGGER.info("History start changed to '%s'", new_history_start or "rolling 365 days")
        api.history_start = new_history_start
        changed = True

    if api.cookie != new_cookie:
        _LOGGER.info("Cookie auth %s", "updated" if new_cookie else "cleared")
        api.cookie = new_cookie
        changed = True

    if changed:
        await coordinator.async_refresh()


def _register_services(hass: HomeAssistant) -> None:
    """Register integration services (idempotent — safe to call multiple times)."""
    if hass.services.has_service(DOMAIN, SERVICE_REFRESH_RATES):
        return

    def _get_coordinators() -> list[MeridianCoordinator]:
        """Return all active, healthy coordinators."""
        coordinators = []
        for entry in hass.config_entries.async_entries(DOMAIN):
            if hasattr(entry, "runtime_data") and entry.runtime_data:
                coord = entry.runtime_data.coordinator
                if coord.last_update_success:
                    coordinators.append(coord)
                else:
                    _LOGGER.warning(
                        "Skipping service call for %s — last update failed",
                        coord.sensor_name,
                    )
        return coordinators

    async def handle_refresh_rates(call: ServiceCall) -> None:
        """Force refresh of the rate cache."""
        for coordinator in _get_coordinators():
            await coordinator.async_force_rate_refresh()

    async def handle_reimport_history(call: ServiceCall) -> None:
        """Re-download and reprocess all usage history."""
        for coordinator in _get_coordinators():
            await coordinator.async_reimport_history()

    async def handle_check_schedule(call: ServiceCall) -> None:
        """Check the Get Shifty page for TOU schedule changes."""
        for coordinator in _get_coordinators():
            result = await coordinator.async_check_schedule()
            if result.get("changed"):
                from homeassistant.components.persistent_notification import (
                    async_create as pn_async_create,
                )
                pn_async_create(
                    hass,
                    f"The TOU schedule image for your network has changed!\n\n"
                    f"Old hash: {result.get('old_hash')}\n"
                    f"New hash: {result.get('new_hash')}\n"
                    f"New URL: {result.get('new_url')}\n\n"
                    f"Please verify your TOU boundaries are still correct "
                    f"and update them using the `{DOMAIN}.update_schedule` "
                    f"service if needed.",
                    title="TOU Schedule Changed",
                    notification_id=f"{DOMAIN}_schedule_changed",
                )
            elif result.get("error"):
                _LOGGER.warning(
                    "Schedule check error: %s", result["error"]
                )
            else:
                _LOGGER.info("TOU schedule unchanged")

    _TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

    def _validate_time(t: str) -> str:
        """Validate HH:MM format, raise vol.Invalid if bad."""
        if not _TIME_RE.match(t):
            raise vol.Invalid(f"Invalid time format '{t}' — expected HH:MM (24h)")
        return t

    def _validate_time_pairs(pairs: list) -> list:
        """Validate a list of [start, end] time pairs."""
        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise vol.Invalid(
                    f"Expected [start, end] pair, got: {pair}"
                )
            _validate_time(pair[0])
            _validate_time(pair[1])
        return pairs

    async def handle_update_schedule(call: ServiceCall) -> None:
        """Update TOU schedule boundaries."""
        night_start = call.data.get("night_start")
        night_end = call.data.get("night_end")
        peak_weekday = call.data.get(
            "peak_weekday", [["07:00", "09:30"], ["17:30", "20:00"]]
        )
        weekend_offpeak = call.data.get("weekend_offpeak", False)

        # Validate time values
        if night_start:
            _validate_time(night_start)
        if night_end:
            _validate_time(night_end)
        if bool(night_start) != bool(night_end):
            _LOGGER.error(
                "Both night_start and night_end must be provided together"
            )
            return
        _validate_time_pairs(peak_weekday)

        new_schedule = {
            "peak_weekday": peak_weekday,
        }
        if night_start and night_end:
            new_schedule["night_start"] = night_start
            new_schedule["night_end"] = night_end
        if weekend_offpeak:
            new_schedule["weekend_offpeak"] = True
        for coordinator in _get_coordinators():
            coordinator.schedule_cache.update_schedule(new_schedule)
            _LOGGER.info("TOU schedule updated: %s", new_schedule)
            await coordinator.async_refresh()

    hass.services.async_register(
        DOMAIN, SERVICE_REFRESH_RATES, handle_refresh_rates
    )
    hass.services.async_register(
        DOMAIN, SERVICE_REIMPORT_HISTORY, handle_reimport_history
    )
    hass.services.async_register(
        DOMAIN, SERVICE_CHECK_SCHEDULE, handle_check_schedule
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_UPDATE_SCHEDULE,
        handle_update_schedule,
        schema=vol.Schema(
            {
                vol.Optional("night_start"): vol.All(
                    cv.string, vol.Match(r"^([01]\d|2[0-3]):[0-5]\d$")
                ),
                vol.Optional("night_end"): vol.All(
                    cv.string, vol.Match(r"^([01]\d|2[0-3]):[0-5]\d$")
                ),
                vol.Optional("peak_weekday"): vol.All(
                    cv.ensure_list,
                    [vol.All(cv.ensure_list, [vol.All(cv.string, vol.Match(r"^([01]\d|2[0-3]):[0-5]\d$"))], vol.Length(min=2, max=2))],
                ),
                vol.Optional("weekend_offpeak", default=False): cv.boolean,
            }
        ),
    )
    _LOGGER.debug("Registered %s services", DOMAIN)
