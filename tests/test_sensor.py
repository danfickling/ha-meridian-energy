"""Tests for sensor.py — entity metadata, device info, definitions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from meridian_energy.sensor import (
    RATE_SENSOR_DEFS,
    PERIOD_DISPLAY_NAMES,
    MeridianRateSensor,
    MeridianBalanceSensor,
    MeridianFuturePacksSensor,
    MeridianDailyCostSensor,
)
from meridian_energy.const import (
    DOMAIN,
    PERIOD_NIGHT,
    PERIOD_PEAK,
    PERIOD_OFFPEAK,
    PERIOD_WEEKEND_OFFPEAK,
    PERIOD_CONTROLLED,
    SUPPLIER_CONFIG,
    SUPPLIER_POWERSHOP,
    DEFAULT_SUPPLIER,
)



class TestRateSensorDefs:
    def test_five_rate_sensors_defined(self):
        assert len(RATE_SENSOR_DEFS) == 5

    def test_all_period_keys_present(self):
        period_keys = {d[0] for d in RATE_SENSOR_DEFS}
        expected = {PERIOD_NIGHT, PERIOD_PEAK, PERIOD_OFFPEAK, PERIOD_WEEKEND_OFFPEAK, PERIOD_CONTROLLED}
        assert period_keys == expected

    def test_defs_have_three_elements(self):
        for defn in RATE_SENSOR_DEFS:
            assert len(defn) == 3, f"Expected (key, name, icon), got {defn}"

    def test_icons_start_with_mdi(self):
        for _, _, icon in RATE_SENSOR_DEFS:
            assert icon.startswith("mdi:"), f"Icon {icon} does not start with mdi:"



class TestPeriodDisplayNames:
    def test_all_display_periods_present(self):
        assert "night" in PERIOD_DISPLAY_NAMES
        assert "peak" in PERIOD_DISPLAY_NAMES
        assert "offpeak" in PERIOD_DISPLAY_NAMES
        assert "weekend_offpeak" in PERIOD_DISPLAY_NAMES
        assert "controlled" in PERIOD_DISPLAY_NAMES

    def test_display_values_are_title_case(self):
        for key, name in PERIOD_DISPLAY_NAMES.items():
            # display names should be human-readable (first letter uppercase)
            assert name[0].isupper(), f"Display name for {key} should be title case: {name}"



class TestMeridianDataDefaults:
    def test_default_values(self):
        from meridian_energy.coordinator import MeridianData
        data = MeridianData()
        assert data.supplier == "powershop"
        assert data.sensor_name == "Powershop"
        assert data.rates == {}
        assert data.daily_charge == 0.0
        assert data.rate_type == "special"
        assert data.tou_period == "offpeak"
        assert data.current_rate == 0.0
        assert data.solar_export_kwh == 0.0
        assert data.has_solar is False
        assert data.last_usage_update is None
        assert data.balance is None

    def test_custom_values(self):
        from meridian_energy.coordinator import MeridianData
        rates = {"night": 0.17, "peak": 0.34}
        data = MeridianData(
            supplier="meridian",
            sensor_name="Meridian Energy",
            rates=rates,
            daily_charge=3.50,
            tou_period="peak",
            current_rate=0.34,
            has_solar=True,
            solar_export_kwh=5.5,
        )
        assert data.supplier == "meridian"
        assert data.rates == rates
        assert data.has_solar is True
        assert data.solar_export_kwh == 5.5



class TestSafeSupplierAccess:
    def test_unknown_supplier_falls_back_to_default(self):
        """SUPPLIER_CONFIG.get() with unknown key returns default config."""
        fallback = SUPPLIER_CONFIG.get("nonexistent", SUPPLIER_CONFIG[DEFAULT_SUPPLIER])
        assert fallback == SUPPLIER_CONFIG[DEFAULT_SUPPLIER]
        assert "name" in fallback
        assert "manufacturer" in fallback

    def test_known_supplier_returns_correct_config(self):
        config = SUPPLIER_CONFIG.get("powershop", SUPPLIER_CONFIG[DEFAULT_SUPPLIER])
        assert config["name"] == "Powershop"


# ---------------------------------------------------------------------------
# Helpers for sensor instantiation tests
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field


@dataclass
class _FakeData:
    detected_periods: list[str] = field(default_factory=list)
    rates: dict = field(default_factory=dict)
    base_rates: dict = field(default_factory=dict)
    special_rates: dict = field(default_factory=dict)
    rate_type: str = "special"
    balance: dict | None = None


def _coord(supplier="powershop", detected=None):
    c = MagicMock()
    c.supplier = supplier
    c.sensor_name = "Powershop" if supplier == "powershop" else "Meridian Energy"
    c.data = _FakeData(detected_periods=detected) if detected is not None else None
    return c


def _entry():
    e = MagicMock()
    e.entry_id = "test_123"
    return e


# ---------------------------------------------------------------------------
# Rate sensor auto-disable by detected periods
# ---------------------------------------------------------------------------


class TestRateSensorDetectedPeriods:
    def test_enabled_when_in_detected(self):
        s = MeridianRateSensor(_coord(detected=["night", "peak"]), _entry(), "night", "Night", "mdi:weather-night")
        assert s._attr_entity_registry_enabled_default is True

    def test_disabled_when_not_in_detected(self):
        s = MeridianRateSensor(_coord(detected=["night", "peak"]), _entry(), "controlled", "Controlled", "mdi:water-boiler")
        assert s._attr_entity_registry_enabled_default is False

    def test_enabled_when_no_data(self):
        s = MeridianRateSensor(_coord(detected=None), _entry(), "controlled", "Controlled", "mdi:water-boiler")
        assert s._attr_entity_registry_enabled_default is True

    def test_enabled_when_detected_empty(self):
        s = MeridianRateSensor(_coord(detected=[]), _entry(), "night", "Night", "mdi:weather-night")
        assert s._attr_entity_registry_enabled_default is True


# ---------------------------------------------------------------------------
# Balance / FuturePacks / DailyCost auto-disable for non-Powershop
# ---------------------------------------------------------------------------


class TestBalanceSensorSupplierAware:
    def test_balance_enabled_powershop(self):
        s = MeridianBalanceSensor(_coord(supplier="powershop"), _entry())
        assert getattr(s, "_attr_entity_registry_enabled_default", True) is True

    def test_balance_disabled_meridian(self):
        s = MeridianBalanceSensor(_coord(supplier="meridian_energy"), _entry())
        assert s._attr_entity_registry_enabled_default is False

    def test_future_packs_enabled_powershop(self):
        s = MeridianFuturePacksSensor(_coord(supplier="powershop"), _entry())
        assert getattr(s, "_attr_entity_registry_enabled_default", True) is True

    def test_future_packs_disabled_meridian(self):
        s = MeridianFuturePacksSensor(_coord(supplier="meridian_energy"), _entry())
        assert s._attr_entity_registry_enabled_default is False

    def test_daily_cost_enabled_powershop(self):
        s = MeridianDailyCostSensor(_coord(supplier="powershop"), _entry())
        assert getattr(s, "_attr_entity_registry_enabled_default", True) is True

    def test_daily_cost_disabled_meridian(self):
        s = MeridianDailyCostSensor(_coord(supplier="meridian_energy"), _entry())
        assert s._attr_entity_registry_enabled_default is False
