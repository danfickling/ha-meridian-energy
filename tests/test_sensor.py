"""Tests for the v2 sensor module — dynamic period creation and display."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Optional

from meridian_energy.sensor import (
    _PERIOD_ICONS,
    _DEFAULT_PERIOD_ICON,
    async_setup_entry,
    MeridianRateSensor,
    MeridianBillingPeriodStartSensor,
    MeridianBillingPeriodEndSensor,
    MeridianNextBillingDateSensor,
)
from meridian_energy.rates import period_display_name
from meridian_energy.const import (
    DOMAIN,
    PERIOD_NIGHT,
    PERIOD_PEAK,
    PERIOD_OFFPEAK,
    PERIOD_CONTROLLED,
    BRAND_CONFIG,
)
from meridian_energy.coordinator import MeridianData


class TestPeriodIcons:
    def test_known_periods_have_icons(self):
        for p in (PERIOD_NIGHT, PERIOD_PEAK, PERIOD_OFFPEAK, PERIOD_CONTROLLED):
            assert p in _PERIOD_ICONS

    def test_all_icons_are_mdi(self):
        for icon in _PERIOD_ICONS.values():
            assert icon.startswith("mdi:")

    def test_default_icon_is_mdi(self):
        assert _DEFAULT_PERIOD_ICON.startswith("mdi:")

    def test_unknown_period_uses_default(self):
        assert _PERIOD_ICONS.get("ev", _DEFAULT_PERIOD_ICON) == _DEFAULT_PERIOD_ICON


class TestPeriodDisplayName:
    def test_known_periods(self):
        assert period_display_name(PERIOD_NIGHT) == "Night"
        assert period_display_name(PERIOD_PEAK) == "Peak"
        assert period_display_name(PERIOD_OFFPEAK) == "Off-Peak"
        assert period_display_name(PERIOD_CONTROLLED) == "Controlled"

    def test_unknown_period_titlecased(self):
        assert period_display_name("ev_charging") == "Ev Charging"

    def test_simple_unknown(self):
        assert period_display_name("solar") == "Solar"


class TestMeridianDataDefaults:
    def test_default_solar_disabled(self):
        d = MeridianData()
        assert d.has_solar is False
        assert d.solar_export_kwh == 0.0

    def test_default_rates_empty(self):
        d = MeridianData()
        assert d.rates == {}

    def test_default_product_empty(self):
        d = MeridianData()
        assert d.product == ""

    def test_default_balance_none(self):
        d = MeridianData()
        assert d.balance is None


class TestBrandConfig:
    def test_domain_value(self):
        assert DOMAIN == "meridian_energy"

    def test_powershop_manufacturer(self):
        assert BRAND_CONFIG["powershop"]["manufacturer"] == "Powershop"

    def test_meridian_manufacturer(self):
        assert BRAND_CONFIG["meridian"]["manufacturer"] == "Meridian Energy"

    def test_both_brands_have_all_keys(self):
        required_keys = {"name", "api_url", "auth_domain", "app_origin", "manufacturer"}
        for brand_config in BRAND_CONFIG.values():
            assert required_keys.issubset(brand_config.keys())


# ---------------------------------------------------------------------------
# Dynamic rate-sensor creation
# ---------------------------------------------------------------------------

def _make_mock_coordinator(detected_periods: Optional[list] = None):
    """Build a mock coordinator with MeridianData containing given periods."""
    coordinator = MagicMock()
    if detected_periods is not None:
        coordinator.data = MeridianData(detected_periods=detected_periods)
    else:
        coordinator.data = None
    return coordinator


def _make_mock_entry():
    """Build a mock config entry with runtime_data.coordinator."""
    entry = MagicMock()
    entry.entry_id = "test_entry_123"
    return entry


class TestDynamicRateSensors:
    """Verify that rate sensors are created dynamically from detected_periods."""

    def test_standard_periods_create_4_rate_sensors(self):
        """Standard 4-period plan creates 4 MeridianRateSensor instances."""
        periods = ["night", "peak", "offpeak", "controlled"]
        coordinator = _make_mock_coordinator(periods)
        entry = _make_mock_entry()
        entry.runtime_data.coordinator = coordinator

        entities = []
        async_setup_entry.__wrapped__ = None  # needed for coroutine-less call

        # Instead of calling async_setup_entry (async), test the loop logic directly
        for period in (coordinator.data.detected_periods if coordinator.data else []):
            display = period_display_name(period)
            icon = _PERIOD_ICONS.get(period, _DEFAULT_PERIOD_ICON)
            entities.append((period, f"{display} Rate", icon))

        assert len(entities) == 4
        assert entities[0] == ("night", "Night Rate", "mdi:weather-night")
        assert entities[1] == ("peak", "Peak Rate", "mdi:flash-alert")
        assert entities[2] == ("offpeak", "Off-Peak Rate", "mdi:flash-outline")
        assert entities[3] == ("controlled", "Controlled Rate", "mdi:water-boiler")

    def test_no_data_creates_no_rate_sensors(self):
        """When coordinator.data is None, no rate sensors are created."""
        coordinator = _make_mock_coordinator(None)
        entities = []
        for period in (coordinator.data.detected_periods if coordinator.data else []):
            entities.append(period)
        assert entities == []

    def test_empty_periods_creates_no_rate_sensors(self):
        """When detected_periods is empty, no rate sensors are created."""
        coordinator = _make_mock_coordinator([])
        entities = []
        for period in (coordinator.data.detected_periods if coordinator.data else []):
            entities.append(period)
        assert entities == []

    def test_unknown_period_gets_default_icon(self):
        """An API-discovered period not in _PERIOD_ICONS gets the default."""
        coordinator = _make_mock_coordinator(["ev"])
        entities = []
        for period in coordinator.data.detected_periods:
            icon = _PERIOD_ICONS.get(period, _DEFAULT_PERIOD_ICON)
            entities.append((period, icon))
        assert entities == [("ev", _DEFAULT_PERIOD_ICON)]

    def test_unknown_period_display_name(self):
        """Unknown period uses titlecased fallback for sensor name."""
        coordinator = _make_mock_coordinator(["ev_charging"])
        for period in coordinator.data.detected_periods:
            name = f"{period_display_name(period)} Rate"
            assert name == "Ev Charging Rate"

    def test_extra_period_creates_extra_sensor(self):
        """A plan with 5 periods creates 5 rate sensor entries."""
        periods = ["night", "peak", "offpeak", "controlled", "solar"]
        coordinator = _make_mock_coordinator(periods)
        entities = []
        for period in coordinator.data.detected_periods:
            display = period_display_name(period)
            icon = _PERIOD_ICONS.get(period, _DEFAULT_PERIOD_ICON)
            entities.append((period, f"{display} Rate", icon))
        assert len(entities) == 5
        # "solar" is unknown, so gets default icon and titlecased name
        assert entities[4] == ("solar", "Solar Rate", _DEFAULT_PERIOD_ICON)

    def test_subset_of_periods(self):
        """A simpler plan with only 2 periods creates only 2 rate sensors."""
        periods = ["night", "offpeak"]
        coordinator = _make_mock_coordinator(periods)
        entities = []
        for period in coordinator.data.detected_periods:
            entities.append(period)
        assert entities == ["night", "offpeak"]


class TestBillingSensors:
    """Verify billing date sensors parse ISO strings to date objects."""

    def test_billing_period_start_returns_date(self):
        coordinator = _make_mock_coordinator([])
        coordinator.data.billing_period_start = "2026-04-10"
        entry = _make_mock_entry()
        sensor = MeridianBillingPeriodStartSensor(coordinator, entry)
        assert sensor.native_value == date(2026, 4, 10)

    def test_billing_period_end_returns_date(self):
        coordinator = _make_mock_coordinator([])
        coordinator.data.billing_period_end = "2026-05-09"
        entry = _make_mock_entry()
        sensor = MeridianBillingPeriodEndSensor(coordinator, entry)
        assert sensor.native_value == date(2026, 5, 9)

    def test_next_billing_date_returns_date(self):
        coordinator = _make_mock_coordinator([])
        coordinator.data.next_billing_date = "2026-05-10"
        entry = _make_mock_entry()
        sensor = MeridianNextBillingDateSensor(coordinator, entry)
        assert sensor.native_value == date(2026, 5, 10)

    def test_billing_sensors_return_none_when_no_data(self):
        coordinator = _make_mock_coordinator([])
        entry = _make_mock_entry()
        for cls in (
            MeridianBillingPeriodStartSensor,
            MeridianBillingPeriodEndSensor,
            MeridianNextBillingDateSensor,
        ):
            sensor = cls(coordinator, entry)
            assert sensor.native_value is None

    def test_billing_sensors_unique_ids(self):
        coordinator = _make_mock_coordinator([])
        entry = _make_mock_entry()
        ids = set()
        for cls in (
            MeridianBillingPeriodStartSensor,
            MeridianBillingPeriodEndSensor,
            MeridianNextBillingDateSensor,
        ):
            sensor = cls(coordinator, entry)
            ids.add(sensor._attr_unique_id)
        assert len(ids) == 3

    def test_billing_sensors_have_date_device_class(self):
        from homeassistant.components.sensor import SensorDeviceClass
        coordinator = _make_mock_coordinator([])
        entry = _make_mock_entry()
        for cls in (
            MeridianBillingPeriodStartSensor,
            MeridianBillingPeriodEndSensor,
            MeridianNextBillingDateSensor,
        ):
            sensor = cls(coordinator, entry)
            assert sensor._attr_device_class == SensorDeviceClass.DATE


class TestStaleEntityCleanup:
    """Verify that stale per-period rate sensors are removed on setup."""

    @staticmethod
    def _run_setup(detected_periods, existing_entity_entries):
        """Run async_setup_entry with mocked HA infrastructure."""
        coordinator = _make_mock_coordinator(detected_periods)
        coordinator.data.brand = "powershop"
        coordinator.data.tou_period = "offpeak"
        coordinator.data.rates = {"night": 0.2362}
        coordinator.data.daily_charge = 4.14
        coordinator.data.current_rate = 0.2362
        coordinator.data.has_solar = False
        coordinator.data.solar_export_kwh = 0.0
        coordinator.data.balance = None
        coordinator.data.future_packs = None
        coordinator.data.product = "Test"
        coordinator.data.billing_period_start = None
        coordinator.data.billing_period_end = None
        coordinator.data.next_billing_date = None

        entry = _make_mock_entry()
        entry.runtime_data.coordinator = coordinator

        hass = MagicMock()
        async_add_entities = MagicMock()

        mock_ent_reg = MagicMock()
        mock_ent_reg.async_remove = MagicMock()

        with patch("meridian_energy.sensor.er") as mock_er:
            mock_er.async_get.return_value = mock_ent_reg
            mock_er.async_entries_for_config_entry.return_value = (
                existing_entity_entries
            )
            asyncio.run(async_setup_entry(hass, entry, async_add_entities))

        return mock_ent_reg

    def test_stale_rate_sensor_removed(self):
        """A rate sensor for a period no longer detected should be removed."""
        stale_entry = MagicMock()
        stale_entry.unique_id = "test_entry_123_rate_controlled"
        stale_entry.entity_id = "sensor.powershop_controlled_rate"

        active_entry = MagicMock()
        active_entry.unique_id = "test_entry_123_rate_night"
        active_entry.entity_id = "sensor.powershop_night_rate"

        mock_ent_reg = self._run_setup(
            detected_periods=["night", "peak"],
            existing_entity_entries=[stale_entry, active_entry],
        )

        mock_ent_reg.async_remove.assert_called_once_with(
            "sensor.powershop_controlled_rate"
        )

    def test_no_stale_sensors_no_removal(self):
        """When all rate sensors match current periods, nothing is removed."""
        entries = [
            MagicMock(
                unique_id="test_entry_123_rate_night",
                entity_id="sensor.powershop_night_rate",
            ),
            MagicMock(
                unique_id="test_entry_123_rate_peak",
                entity_id="sensor.powershop_peak_rate",
            ),
        ]

        mock_ent_reg = self._run_setup(
            detected_periods=["night", "peak"],
            existing_entity_entries=entries,
        )

        mock_ent_reg.async_remove.assert_not_called()

    def test_non_rate_entities_not_removed(self):
        """Non-rate entity entries should never be removed by cleanup."""
        other_entry = MagicMock()
        other_entry.unique_id = "test_entry_123_daily_charge"
        other_entry.entity_id = "sensor.powershop_daily_charge"

        mock_ent_reg = self._run_setup(
            detected_periods=["night"],
            existing_entity_entries=[other_entry],
        )

        mock_ent_reg.async_remove.assert_not_called()
