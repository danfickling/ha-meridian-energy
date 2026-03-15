"""Sensor entities for Meridian Energy / Powershop integration.

Creates 12 sensor entities under a single device:
  - 5 per-period kWh rates (night, peak, offpeak, weekend, controlled)
  - 1 daily connection charge
  - 1 current rate (updates at TOU boundary times, includes diagnostics)
  - 1 TOU period name (updates at TOU boundary times)
  - 1 solar export (disabled by default; shows 0 if no solar data)
  - 1 account balance (NZD credit from portal)
  - 1 future packs value
  - 1 daily cost
"""

from __future__ import annotations

import logging

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    DOMAIN,
    SUPPLIER_CONFIG,
    SUPPLIER_POWERSHOP,
    DEFAULT_SUPPLIER,
    PERIOD_NIGHT,
    PERIOD_PEAK,
    PERIOD_OFFPEAK,
    PERIOD_WEEKEND_OFFPEAK,
    PERIOD_CONTROLLED,
)
from .coordinator import MeridianCoordinator
from . import MeridianConfigEntry

_LOGGER = logging.getLogger(__name__)

# (period_key, display_name, icon)
RATE_SENSOR_DEFS = [
    (PERIOD_NIGHT, "Night Rate", "mdi:weather-night"),
    (PERIOD_PEAK, "Peak Rate", "mdi:flash-alert"),
    (PERIOD_OFFPEAK, "Off-Peak Rate", "mdi:flash-outline"),
    (PERIOD_WEEKEND_OFFPEAK, "Weekend Off-Peak Rate", "mdi:calendar-weekend"),
    (PERIOD_CONTROLLED, "Controlled Rate", "mdi:water-boiler"),
]

PERIOD_DISPLAY_NAMES: dict[str, str] = {
    "night": "Night",
    "peak": "Peak",
    "offpeak": "Off-Peak",
    "weekend_offpeak": "Weekend Off-Peak",
    "controlled": "Controlled",
}



async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up sensor entities from a config entry."""
    coordinator = entry.runtime_data.coordinator

    entities: list[SensorEntity] = []

    # Live / boundary-reactive sensors first
    entities.append(MeridianCurrentRateSensor(coordinator, entry))
    entities.append(MeridianTOUPeriodSensor(coordinator, entry))

    # Per-period rate sensors
    for period, name, icon in RATE_SENSOR_DEFS:
        entities.append(
            MeridianRateSensor(coordinator, entry, period, name, icon)
        )

    # Daily charge
    entities.append(MeridianDailyChargeSensor(coordinator, entry))

    # Solar export (always created — shows 0 if no solar data)
    entities.append(MeridianSolarExportSensor(coordinator, entry))

    # Account balance, future packs, daily cost
    entities.append(MeridianBalanceSensor(coordinator, entry))
    entities.append(MeridianFuturePacksSensor(coordinator, entry))
    entities.append(MeridianDailyCostSensor(coordinator, entry))

    async_add_entities(entities)



class MeridianBaseSensor(
    CoordinatorEntity[MeridianCoordinator], SensorEntity
):
    """Base class for all sensor entities."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
        key: str,
        name: str,
        icon: str,
    ) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_name = name
        self._attr_icon = icon
        self._entry = entry

    @property
    def device_info(self) -> DeviceInfo:
        """Return device info linking all sensors together."""
        supplier = self.coordinator.supplier
        config = SUPPLIER_CONFIG.get(supplier, SUPPLIER_CONFIG[DEFAULT_SUPPLIER])
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            name=self.coordinator.sensor_name,
            manufacturer=config["manufacturer"],
            model="Energy Rate Tracker",
            entry_type=DeviceEntryType.SERVICE,
        )



class MeridianRateSensor(MeridianBaseSensor):
    """Per-period rate in NZD/kWh (e.g. Night Rate, Peak Rate)."""

    _attr_native_unit_of_measurement = "NZD/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 4

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
        period: str,
        name: str,
        icon: str,
    ) -> None:
        super().__init__(coordinator, entry, f"rate_{period}", name, icon)
        self._period = period
        # Disable entity by default when the rate table doesn't include this period
        detected = coordinator.data and coordinator.data.detected_periods
        if detected and period not in detected:
            self._attr_entity_registry_enabled_default = False

    @property
    def native_value(self) -> float | None:
        """Return the rate for this period."""
        if self.coordinator.data and self.coordinator.data.rates:
            return self.coordinator.data.rates.get(self._period)
        return None

    @property
    def extra_state_attributes(self) -> dict:
        """Include both base and special rates."""
        if not self.coordinator.data:
            return {}
        return {
            "base_rate": self.coordinator.data.base_rates.get(self._period),
            "special_rate": self.coordinator.data.special_rates.get(
                self._period
            ),
            "active_rate_type": self.coordinator.data.rate_type,
        }



class MeridianDailyChargeSensor(MeridianBaseSensor, RestoreEntity):
    """Daily connection charge (NZD/day).

    ``RestoreEntity`` keeps the last daily charge across restarts.
    """

    _attr_native_unit_of_measurement = "NZD/day"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 4

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "daily_charge", "Daily Charge", "mdi:currency-usd"
        )
        self._restored_value: float | None = None

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (last_state := await self.async_get_last_state()) is not None:
            try:
                self._restored_value = float(last_state.state)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        if self.coordinator.data:
            return self.coordinator.data.daily_charge
        return self._restored_value

    @property
    def extra_state_attributes(self) -> dict:
        if not self.coordinator.data:
            return {}
        return {
            "base_daily": self.coordinator.data.base_daily,
            "special_daily": self.coordinator.data.special_daily,
            "active_rate_type": self.coordinator.data.rate_type,
        }



class MeridianCurrentRateSensor(MeridianBaseSensor, RestoreEntity):
    """Current NZD/kWh rate based on the active TOU period.

    Updates from the coordinator AND at TOU boundary times
    so it reacts instantly to period transitions.

    Inherits ``RestoreEntity`` so the last known rate survives HA
    restarts and is available immediately (before the first poll
    completes login + CSV download).
    """

    _attr_native_unit_of_measurement = "NZD/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 4

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "current_rate", "Current Rate", "mdi:cash"
        )
        self._restored_value: float | None = None

    async def async_added_to_hass(self) -> None:
        """Restore last state and register TOU boundary listeners."""
        await super().async_added_to_hass()

        # Restore previous value so sensor isn't "unavailable" on startup
        if (last_state := await self.async_get_last_state()) is not None:
            try:
                self._restored_value = float(last_state.state)
            except (ValueError, TypeError):
                self._restored_value = None

        boundary_times = self.coordinator.schedule_cache.get_boundary_times()
        for hour, minute in boundary_times:
            self.async_on_remove(
                async_track_time_change(
                    self.hass,
                    self._boundary_changed,
                    hour=hour,
                    minute=minute,
                    second=0,
                )
            )

    @callback
    def _boundary_changed(self, now) -> None:
        """React to TOU boundary time."""
        self.async_write_ha_state()

    @property
    def native_value(self) -> float | None:
        if not self.coordinator.data or not self.coordinator.data.rates:
            return self._restored_value
        period = self.coordinator.get_current_tou_period()
        return self.coordinator.data.rates.get(period, 0.0)

    @property
    def extra_state_attributes(self) -> dict:
        period = self.coordinator.get_current_tou_period()
        d = self.coordinator.data
        attrs = {
            "tou_period": PERIOD_DISPLAY_NAMES.get(period, period),
            "rate_type": d.rate_type if d else None,
        }
        if d:
            # Schedule info
            if d.schedule_summary:
                attrs["schedule"] = d.schedule_summary
            # Diagnostics (previously on status sensor)
            attrs["last_usage_update"] = (
                d.last_usage_update.isoformat() if d.last_usage_update else None
            )
            attrs["last_rate_scrape"] = d.last_rate_scrape
            attrs["cache_months_special"] = d.cache_months_special
            attrs["cache_months_base"] = d.cache_months_base
            attrs["stats_days_processed"] = d.stats_days
            attrs["stats_rows_processed"] = d.stats_rows
            attrs["network"] = d.schedule_network
            attrs["schedule_changed"] = d.schedule_changed
        return attrs



class MeridianTOUPeriodSensor(MeridianBaseSensor, RestoreEntity):
    """Current TOU period name (Night / Peak / Off-Peak / Weekend Off-Peak).

    Updates at TOU boundary times for instant period transitions.
    ``RestoreEntity`` keeps the last period name across restarts.
    """

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "tou_period", "Current Period", "mdi:clock-outline"
        )
        self._restored_value: str | None = None

    async def async_added_to_hass(self) -> None:
        """Restore last state and register TOU boundary listeners."""
        await super().async_added_to_hass()

        if (last_state := await self.async_get_last_state()) is not None:
            if last_state.state not in (None, "unknown", "unavailable"):
                self._restored_value = last_state.state

        boundary_times = self.coordinator.schedule_cache.get_boundary_times()
        for hour, minute in boundary_times:
            self.async_on_remove(
                async_track_time_change(
                    self.hass,
                    self._boundary_changed,
                    hour=hour,
                    minute=minute,
                    second=0,
                )
            )

    @callback
    def _boundary_changed(self, now) -> None:
        self.async_write_ha_state()

    @property
    def native_value(self) -> str:
        # TOU period is always computable from current time + schedule,
        # so coordinator data is not required for the primary value.
        period = self.coordinator.get_current_tou_period()
        return PERIOD_DISPLAY_NAMES.get(period, self._restored_value or "Off-Peak")

    @property
    def extra_state_attributes(self) -> dict:
        attrs = {}
        if self.coordinator.data and self.coordinator.data.schedule_summary:
            attrs["schedule"] = self.coordinator.data.schedule_summary
        return attrs



class MeridianSolarExportSensor(MeridianBaseSensor):
    """Total solar energy exported (return to grid) from CSV data.

    Always created but disabled by default; users with solar can enable it
    from the entity registry.  Shows 0.0 if no solar/export rows in the CSV.
    """

    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 2
    _attr_entity_registry_enabled_default = False

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator,
            entry,
            "solar_export",
            "Solar Export",
            "mdi:solar-power",
        )

    @property
    def native_value(self) -> float:
        if self.coordinator.data:
            return self.coordinator.data.solar_export_kwh
        return 0.0

    @property
    def extra_state_attributes(self) -> dict:
        if not self.coordinator.data:
            return {}
        return {
            "has_solar": self.coordinator.data.has_solar,
        }



class MeridianBalanceSensor(MeridianBaseSensor, RestoreEntity):
    """Account credit balance in NZD.

    Shows "You're about $NNN ahead" from the balance page.
    ``RestoreEntity`` keeps the last balance across restarts.
    """

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_native_unit_of_measurement = "NZD"
    _attr_state_class = SensorStateClass.TOTAL
    _attr_suggested_display_precision = 0

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "balance", "Account Balance", "mdi:wallet"
        )
        self._restored_value: float | None = None
        if coordinator.supplier != SUPPLIER_POWERSHOP:
            self._attr_entity_registry_enabled_default = False

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (last_state := await self.async_get_last_state()) is not None:
            try:
                self._restored_value = float(last_state.state)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        bal = self.coordinator.data and self.coordinator.data.balance
        if bal and bal.get("ahead") is not None:
            return bal["ahead"]
        return self._restored_value


class MeridianFuturePacksSensor(MeridianBaseSensor, RestoreEntity):
    """Pre-purchased Future Packs value in NZD.

    Shows "You also have $N,NNN in pre-purchased Future Packs" from the
    balance page.
    """

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_native_unit_of_measurement = "NZD"
    _attr_state_class = SensorStateClass.TOTAL
    _attr_suggested_display_precision = 0

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "future_packs", "Future Packs", "mdi:package-variant-closed"
        )
        self._restored_value: float | None = None
        if coordinator.supplier != SUPPLIER_POWERSHOP:
            self._attr_entity_registry_enabled_default = False

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (last_state := await self.async_get_last_state()) is not None:
            try:
                self._restored_value = float(last_state.state)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        bal = self.coordinator.data and self.coordinator.data.balance
        if bal and bal.get("future_packs") is not None:
            return bal["future_packs"]
        return self._restored_value


class MeridianDailyCostSensor(MeridianBaseSensor, RestoreEntity):
    """Estimated daily electricity cost in NZD.

    Shows "You're currently using about $NN.NN per day" from the
    balance page.
    """

    _attr_native_unit_of_measurement = "NZD/day"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_suggested_display_precision = 2

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "daily_cost", "Daily Cost", "mdi:cash-clock"
        )
        self._restored_value: float | None = None
        if coordinator.supplier != SUPPLIER_POWERSHOP:
            self._attr_entity_registry_enabled_default = False

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (last_state := await self.async_get_last_state()) is not None:
            try:
                self._restored_value = float(last_state.state)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        bal = self.coordinator.data and self.coordinator.data.balance
        if bal and bal.get("daily_cost") is not None:
            return bal["daily_cost"]
        return self._restored_value
