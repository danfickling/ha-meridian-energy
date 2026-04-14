"""Shared fixtures for meridian_energy v2 tests."""

from __future__ import annotations

import sys
import typing
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Polyfill typing.TypeAlias for Python < 3.10
# ---------------------------------------------------------------------------
if not hasattr(typing, "TypeAlias"):
    typing.TypeAlias = type


# ---------------------------------------------------------------------------
# Mock homeassistant and its sub-modules so we can import the integration
# without the full HA runtime.
# ---------------------------------------------------------------------------

def _install_ha_mocks() -> None:
    if "homeassistant" in sys.modules:
        return

    # -- aiohttp mock (needed by api.py) ---
    aiohttp_mock = MagicMock()
    aiohttp_mock.ClientSession = MagicMock
    aiohttp_mock.ClientError = type("ClientError", (Exception,), {})
    aiohttp_mock.ContentTypeError = type("ContentTypeError", (Exception,), {})
    sys.modules["aiohttp"] = aiohttp_mock

    ha = MagicMock()
    ha.const.Platform.SENSOR = "sensor"
    ha.const.CONF_EMAIL = "email"
    ha.const.UnitOfEnergy.KILO_WATT_HOUR = "kWh"

    class _SensorEntity:
        _attr_entity_registry_enabled_default = True

    class _RestoreSensor(_SensorEntity):
        async def async_get_last_sensor_data(self):
            return None

    class _CoordinatorEntity:
        def __init__(self, coordinator=None):
            self.coordinator = coordinator
        def __class_getitem__(cls, _):
            return cls

    sensor_mod = MagicMock()
    sensor_mod.SensorEntity = _SensorEntity
    sensor_mod.SensorStateClass = MagicMock()
    sensor_mod.RestoreSensor = _RestoreSensor

    class _DataUpdateCoordinator:
        def __class_getitem__(cls, _):
            return cls

    coordinator_mod = MagicMock()
    coordinator_mod.DataUpdateCoordinator = _DataUpdateCoordinator
    coordinator_mod.CoordinatorEntity = _CoordinatorEntity

    restore_mod = MagicMock()
    restore_mod.RestoreEntity = type("RestoreEntity", (), {})

    device_reg_mod = MagicMock()
    device_reg_mod.DeviceEntryType = MagicMock()
    device_reg_mod.DeviceInfo = dict

    class _ConfigFlow:
        def __init_subclass__(cls, domain=None, **kwargs):
            super().__init_subclass__(**kwargs)

    class _OptionsFlow:
        pass

    class _ConfigEntry:
        def __class_getitem__(cls, _):
            return cls

    config_entries_mod = MagicMock()
    config_entries_mod.ConfigFlow = _ConfigFlow
    config_entries_mod.OptionsFlow = _OptionsFlow
    config_entries_mod.ConfigEntry = _ConfigEntry
    config_entries_mod.ConfigFlowResult = dict

    ha.config_entries = config_entries_mod

    vol_mock = MagicMock()

    class _VolInvalid(Exception):
        pass

    vol_mock.Invalid = _VolInvalid

    entity_reg_mod = MagicMock()
    entity_reg_mod.async_get = MagicMock()
    entity_reg_mod.async_entries_for_config_entry = MagicMock(return_value=[])

    issue_reg_mod = MagicMock()
    issue_reg_mod.async_create_issue = MagicMock()
    issue_reg_mod.async_delete_issue = MagicMock()
    issue_reg_mod.IssueSeverity = MagicMock()
    issue_reg_mod.IssueSeverity.WARNING = "warning"
    issue_reg_mod.IssueSeverity.ERROR = "error"

    recorder_models_mod = MagicMock()
    recorder_models_mod.StatisticData = dict
    recorder_models_mod.StatisticMetaData = dict

    modules = {
        "voluptuous": vol_mock,
        "homeassistant": ha,
        "homeassistant.const": ha.const,
        "homeassistant.config_entries": config_entries_mod,
        "homeassistant.core": MagicMock(),
        "homeassistant.data_entry_flow": MagicMock(),
        "homeassistant.helpers": MagicMock(),
        "homeassistant.helpers.config_validation": MagicMock(),
        "homeassistant.helpers.update_coordinator": coordinator_mod,
        "homeassistant.helpers.aiohttp_client": MagicMock(),
        "homeassistant.helpers.device_registry": device_reg_mod,
        "homeassistant.helpers.entity_registry": entity_reg_mod,
        "homeassistant.helpers.entity_platform": MagicMock(),
        "homeassistant.helpers.event": MagicMock(),
        "homeassistant.helpers.issue_registry": issue_reg_mod,
        "homeassistant.helpers.restore_state": restore_mod,
        "homeassistant.helpers.typing": MagicMock(),
        "homeassistant.components": MagicMock(),
        "homeassistant.components.diagnostics": MagicMock(),
        "homeassistant.components.recorder": MagicMock(),
        "homeassistant.components.recorder.models": recorder_models_mod,
        "homeassistant.components.recorder.statistics": MagicMock(),
        "homeassistant.components.sensor": sensor_mod,
        "homeassistant.exceptions": MagicMock(),
    }
    sys.modules.update(modules)


_install_ha_mocks()
COMPONENT_DIR = Path(__file__).resolve().parent.parent / "custom_components" / "meridian_energy"
if str(COMPONENT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(COMPONENT_DIR.parent))


# ---------------------------------------------------------------------------
# Fixtures for v2 API response data
# ---------------------------------------------------------------------------

@pytest.fixture
def api_rates_response():
    """Sample API response from async_get_rates_and_tou()."""
    return {
        "product": "Standard User - ST06 - Apr 26",
        "rates": [
            {"touBucketName": "N9", "bandCategory": "CONSUMPTION_CHARGE", "rateIncludingTax": "23.62000", "unitType": "Kilowatt-hours consumed"},
            {"touBucketName": "PK5", "bandCategory": "CONSUMPTION_CHARGE", "rateIncludingTax": "40.77000", "unitType": "Kilowatt-hours consumed"},
            {"touBucketName": "OPK10", "bandCategory": "CONSUMPTION_CHARGE", "rateIncludingTax": "27.92000", "unitType": "Kilowatt-hours consumed"},
            {"touBucketName": "", "bandCategory": "STANDING_CHARGE", "rateIncludingTax": "414.00000", "unitType": "Days on supply"},
            {"touBucketName": "", "bandCategory": "CONSUMPTION_CHARGE", "rateIncludingTax": "23.62000", "unitType": "Kilowatt-hours consumed"},
        ],
        "tou_schemes": [
            {
                "name": "ST06",
                "timeslots": [
                    {"timeslot": "N9", "activeFrom": "00:00:00", "activeTo": "07:00:00", "weekdays": False, "weekends": False},
                    {"timeslot": "N9", "activeFrom": "22:00:00", "activeTo": "00:00:00", "weekdays": False, "weekends": False},
                    {"timeslot": "PK5", "activeFrom": "07:00:00", "activeTo": "09:30:00", "weekdays": True, "weekends": False},
                    {"timeslot": "OPK10", "activeFrom": "09:30:00", "activeTo": "17:30:00", "weekdays": True, "weekends": False},
                    {"timeslot": "PK5", "activeFrom": "17:30:00", "activeTo": "20:00:00", "weekdays": True, "weekends": False},
                    {"timeslot": "OPK10", "activeFrom": "20:00:00", "activeTo": "22:00:00", "weekdays": True, "weekends": False},
                    {"timeslot": "OPK10", "activeFrom": "07:00:00", "activeTo": "22:00:00", "weekdays": False, "weekends": True},
                ],
            }
        ],
    }


@pytest.fixture
def daily_cost_nodes():
    """Sample daily cost measurement nodes (new hash-label format)."""
    return [
        {
            "startAt": "2026-01-05T00:00:00+13:00",  # Monday
            "endAt": "2026-01-06T00:00:00+13:00",
            "value": "27.26",
            "metaData": {
                "statistics": [
                    {"label": "STANDING_CHARGE_abc123", "value": None, "costInclTax": {"estimatedAmount": "414.00"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan1_nighthash", "value": "3.87", "costInclTax": {"estimatedAmount": "91.39"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan1_peakhash", "value": "5.20", "costInclTax": {"estimatedAmount": "212.00"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan1_opkhash", "value": "18.19", "costInclTax": {"estimatedAmount": "508.02"}},
                    {"label": "CONSUMPTION_CHARGE_controlhash", "value": "0.00", "costInclTax": {"estimatedAmount": "0"}},
                ],
            },
        },
        {
            "startAt": "2026-01-10T00:00:00+13:00",  # Saturday
            "endAt": "2026-01-11T00:00:00+13:00",
            "value": "22.50",
            "metaData": {
                "statistics": [
                    {"label": "STANDING_CHARGE_abc123", "value": None, "costInclTax": {"estimatedAmount": "414.00"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan1_nighthash", "value": "4.10", "costInclTax": {"estimatedAmount": "96.82"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan1_opkhash", "value": "18.40", "costInclTax": {"estimatedAmount": "513.89"}},
                    {"label": "CONSUMPTION_CHARGE_controlhash", "value": "0.00", "costInclTax": {"estimatedAmount": "0"}},
                ],
            },
        },
    ]


@pytest.fixture
def tou_schedule():
    """Parsed TOU schedule from parse_tou_scheme()."""
    return {
        "scheme_name": "ST06",
        "timeslots": [
            {"period": "night", "bucket": "N9", "start": "00:00", "end": "07:00", "weekdays": True, "weekends": True},
            {"period": "night", "bucket": "N9", "start": "22:00", "end": "00:00", "weekdays": True, "weekends": True},
            {"period": "peak", "bucket": "PK5", "start": "07:00", "end": "09:30", "weekdays": True, "weekends": False},
            {"period": "offpeak", "bucket": "OPK10", "start": "09:30", "end": "17:30", "weekdays": True, "weekends": False},
            {"period": "peak", "bucket": "PK5", "start": "17:30", "end": "20:00", "weekdays": True, "weekends": False},
            {"period": "offpeak", "bucket": "OPK10", "start": "20:00", "end": "22:00", "weekdays": True, "weekends": False},
            {"period": "offpeak", "bucket": "OPK10", "start": "07:00", "end": "22:00", "weekdays": False, "weekends": True},
        ],
    }
