"""Sensor entities for Meridian Energy / Powershop integration (v2).

Creates sensor entities under a single device:
  - Dynamic per-period kWh rate sensors (created from plan TOU periods)
  - 1 daily connection charge
  - 1 current rate (updates at TOU boundary times)
  - 1 TOU period name (updates at TOU boundary times)
  - 1 solar export (disabled by default; shows 0 if no solar data)
  - 1 account balance (NZD credit from ledger)
  - 1 future packs value
  - 1 daily cost
"""

from __future__ import annotations

import logging
from datetime import date

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
    RestoreSensor,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    DOMAIN,
    BRAND_CONFIG,
    DEFAULT_BRAND,
)
from .rates import period_display_name
from .coordinator import MeridianCoordinator
from . import MeridianConfigEntry

_LOGGER = logging.getLogger(__name__)

PARALLEL_UPDATES = 0

# Icon lookup for known periods; unknown periods get the default icon.
_PERIOD_ICONS: dict[str, str] = {
    "night": "mdi:weather-night",
    "peak": "mdi:flash-alert",
    "offpeak": "mdi:flash-outline",
    "controlled": "mdi:water-boiler",
}
_DEFAULT_PERIOD_ICON = "mdi:flash"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeridianConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up sensor entities from a config entry."""
    coordinator = entry.runtime_data.coordinator

    entities: list[SensorEntity] = []

    # Live / boundary-reactive sensors
    entities.append(MeridianCurrentRateSensor(coordinator, entry))
    entities.append(MeridianTOUPeriodSensor(coordinator, entry))

    # Per-period rate sensors — created dynamically from detected periods
    for period in (coordinator.data.detected_periods if coordinator.data else []):
        display = period_display_name(period)
        icon = _PERIOD_ICONS.get(period, _DEFAULT_PERIOD_ICON)
        entities.append(
            MeridianRateSensor(coordinator, entry, period, f"{display} Rate", icon)
        )

    # Daily charge
    entities.append(MeridianDailyChargeSensor(coordinator, entry))

    # Solar export (always created — shows 0 if no solar data)
    entities.append(MeridianSolarExportSensor(coordinator, entry))

    # Account balance, future packs
    entities.append(MeridianBalanceSensor(coordinator, entry))
    entities.append(MeridianFuturePacksSensor(coordinator, entry))

    # Billing cycle
    entities.append(MeridianBillingPeriodStartSensor(coordinator, entry))
    entities.append(MeridianBillingPeriodEndSensor(coordinator, entry))
    entities.append(MeridianNextBillingDateSensor(coordinator, entry))

    async_add_entities(entities)

    # Remove stale per-period rate sensors when detected periods change
    # (e.g. plan changes from having "controlled" to not having it).
    current_rate_uids = {
        f"{entry.entry_id}_rate_{p}"
        for p in (coordinator.data.detected_periods if coordinator.data else [])
    }
    ent_reg = er.async_get(hass)
    for ent_entry in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
        if (
            ent_entry.unique_id.startswith(f"{entry.entry_id}_rate_")
            and ent_entry.unique_id not in current_rate_uids
        ):
            _LOGGER.info(
                "Removing stale rate sensor: %s", ent_entry.entity_id,
            )
            ent_reg.async_remove(ent_entry.entity_id)


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
        brand = self.coordinator.brand
        config = BRAND_CONFIG.get(brand, BRAND_CONFIG[DEFAULT_BRAND])
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

    @property
    def native_value(self) -> float | None:
        """Return the rate for this period."""
        if self.coordinator.data and self.coordinator.data.rates:
            return self.coordinator.data.rates.get(self._period)
        return None



class MeridianDailyChargeSensor(MeridianBaseSensor, RestoreSensor):
    """Daily connection charge (NZD/day).

    ``RestoreSensor`` keeps the last daily charge across restarts.
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
        if (data := await self.async_get_last_sensor_data()) is not None:
            try:
                self._restored_value = float(data.native_value)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        if self.coordinator.data:
            return self.coordinator.data.daily_charge
        return self._restored_value



class MeridianCurrentRateSensor(MeridianBaseSensor, RestoreSensor):
    """Current NZD/kWh rate based on the active TOU period.

    Updates from the coordinator AND at TOU boundary times
    so it reacts instantly to period transitions.

    Inherits ``RestoreSensor`` so the last known rate survives HA
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
        if (data := await self.async_get_last_sensor_data()) is not None:
            try:
                self._restored_value = float(data.native_value)
            except (ValueError, TypeError):
                self._restored_value = None

        boundary_times = self.coordinator.get_boundary_times()
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
        attrs: dict = {
            "tou_period": period_display_name(period),
        }
        if d:
            attrs["product"] = d.product
            attrs["last_usage_update"] = (
                d.last_usage_update.isoformat() if d.last_usage_update else None
            )
        return attrs



class MeridianTOUPeriodSensor(MeridianBaseSensor, RestoreSensor):
    """Current TOU period name (Night / Peak / Off-Peak / Controlled).

    Updates at TOU boundary times for instant period transitions.
    ``RestoreSensor`` keeps the last period name across restarts.
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

        if (data := await self.async_get_last_sensor_data()) is not None:
            if data.native_value not in (None, "unknown", "unavailable"):
                self._restored_value = str(data.native_value)

        boundary_times = self.coordinator.get_boundary_times()
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
        period = self.coordinator.get_current_tou_period()
        return period_display_name(period) if period else (self._restored_value or "Off-Peak")



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



class MeridianBalanceSensor(MeridianBaseSensor, RestoreSensor):
    """Account credit balance in NZD.

    Shows "You're about $NNN ahead" from the balance page.
    ``RestoreSensor`` keeps the last balance across restarts.
    """

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_native_unit_of_measurement = "NZD"
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

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (data := await self.async_get_last_sensor_data()) is not None:
            try:
                self._restored_value = float(data.native_value)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        bal = self.coordinator.data and self.coordinator.data.balance
        if bal and bal.get("ahead") is not None:
            return bal["ahead"]
        return self._restored_value


class MeridianFuturePacksSensor(MeridianBaseSensor, RestoreSensor):
    """Pre-purchased Future Packs value in NZD.

    Shows "You also have $N,NNN in pre-purchased Future Packs" from the
    balance page.  Disabled by default for Meridian Energy customers
    (Future Packs are a Powershop-specific feature).
    """

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_native_unit_of_measurement = "NZD"
    _attr_suggested_display_precision = 0

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "future_packs", "Future Packs", "mdi:package-variant-closed"
        )
        if coordinator.brand == "meridian_energy":
            self._attr_entity_registry_enabled_default = False
        self._restored_value: float | None = None

    async def async_added_to_hass(self) -> None:
        """Restore last state on startup."""
        await super().async_added_to_hass()
        if (data := await self.async_get_last_sensor_data()) is not None:
            try:
                self._restored_value = float(data.native_value)
            except (ValueError, TypeError):
                self._restored_value = None

    @property
    def native_value(self) -> float | None:
        bal = self.coordinator.data and self.coordinator.data.balance
        if bal and bal.get("future_packs") is not None:
            return bal["future_packs"]
        return self._restored_value


class MeridianBillingPeriodStartSensor(MeridianBaseSensor):
    """Start date of the current billing period."""

    _attr_device_class = SensorDeviceClass.DATE

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "billing_period_start",
            "Billing Period Start", "mdi:calendar-start",
        )

    @property
    def native_value(self) -> date | None:
        raw = self.coordinator.data and self.coordinator.data.billing_period_start
        if raw:
            return date.fromisoformat(raw)
        return None


class MeridianBillingPeriodEndSensor(MeridianBaseSensor):
    """End date of the current billing period."""

    _attr_device_class = SensorDeviceClass.DATE

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "billing_period_end",
            "Billing Period End", "mdi:calendar-end",
        )

    @property
    def native_value(self) -> date | None:
        raw = self.coordinator.data and self.coordinator.data.billing_period_end
        if raw:
            return date.fromisoformat(raw)
        return None


class MeridianNextBillingDateSensor(MeridianBaseSensor):
    """Date the next bill will be issued."""

    _attr_device_class = SensorDeviceClass.DATE

    def __init__(
        self,
        coordinator: MeridianCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator, entry, "next_billing_date",
            "Next Billing Date", "mdi:calendar-alert",
        )

    @property
    def native_value(self) -> date | None:
        raw = self.coordinator.data and self.coordinator.data.next_billing_date
        if raw:
            return date.fromisoformat(raw)
        return None
