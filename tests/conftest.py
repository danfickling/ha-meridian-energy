"""Shared fixtures for meridian_energy tests."""

from __future__ import annotations

import sys
import typing
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Polyfill typing.TypeAlias for Python < 3.10 (local dev may be older than CI)
# ---------------------------------------------------------------------------
if not hasattr(typing, "TypeAlias"):
    typing.TypeAlias = type  # noqa: good enough for annotation-only usage


# ---------------------------------------------------------------------------
# Mock homeassistant and its sub-modules so we can import coordinator/const
# without the full HA runtime.
# ---------------------------------------------------------------------------

def _install_ha_mocks() -> None:
    """Inject lightweight mocks for homeassistant.* and voluptuous into sys.modules."""
    if "homeassistant" in sys.modules:
        return  # already real or already mocked

    ha = MagicMock()
    # homeassistant.const.Platform needs to be subscriptable/iterable
    ha.const.Platform.SENSOR = "sensor"
    # homeassistant.const.CONF_EMAIL / CONF_PASSWORD used by __init__.py
    ha.const.CONF_EMAIL = "email"
    ha.const.CONF_PASSWORD = "password"
    ha.const.UnitOfEnergy.KILO_WATT_HOUR = "kWh"

    # Build real stub classes for HA bases so sensor.py inheritance works
    # without metaclass conflicts.
    class _SensorEntity:
        _attr_entity_registry_enabled_default = True

    class _CoordinatorEntity:
        def __init__(self, coordinator=None):
            pass

        def __class_getitem__(cls, _):
            return cls

    sensor_mod = MagicMock()
    sensor_mod.SensorEntity = _SensorEntity
    sensor_mod.SensorStateClass = MagicMock()

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
    device_reg_mod.DeviceInfo = dict  # just use dict as a stand-in

    # Build real stub classes for HA config_entries so ConfigFlow/OptionsFlow
    # class attributes (VERSION, _options) work properly.
    class _ConfigFlow:
        """Stub ConfigFlow that supports domain= keyword in class definition."""
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

    # Bind directly on ha so `from homeassistant import config_entries` works
    ha.config_entries = config_entries_mod

    # Build a voluptuous mock with a real Invalid exception so raise/except works
    vol_mock = MagicMock()

    class _VolInvalid(Exception):
        """Stand-in for voluptuous.Invalid."""

    vol_mock.Invalid = _VolInvalid

    modules = {
        # --- voluptuous (used by __init__.py and config_flow.py) ---
        "voluptuous": vol_mock,
        # --- homeassistant core ---
        "homeassistant": ha,
        "homeassistant.const": ha.const,
        "homeassistant.config_entries": config_entries_mod,
        "homeassistant.core": MagicMock(),
        "homeassistant.data_entry_flow": MagicMock(),
        # --- homeassistant.helpers ---
        "homeassistant.helpers": MagicMock(),
        "homeassistant.helpers.config_validation": MagicMock(),
        "homeassistant.helpers.update_coordinator": coordinator_mod,
        "homeassistant.helpers.aiohttp_client": MagicMock(),
        "homeassistant.helpers.device_registry": device_reg_mod,
        "homeassistant.helpers.entity_platform": MagicMock(),
        "homeassistant.helpers.event": MagicMock(),
        "homeassistant.helpers.restore_state": restore_mod,
        "homeassistant.helpers.typing": MagicMock(),
        # --- homeassistant.components ---
        "homeassistant.components": MagicMock(),
        "homeassistant.components.diagnostics": MagicMock(),
        "homeassistant.components.recorder": MagicMock(),
        "homeassistant.components.recorder.models": MagicMock(),
        "homeassistant.components.recorder.statistics": MagicMock(),
        "homeassistant.components.sensor": sensor_mod,
        # --- homeassistant.exceptions ---
        "homeassistant.exceptions": MagicMock(),
    }
    sys.modules.update(modules)


_install_ha_mocks()
# Now we can safely add the component to the path
COMPONENT_DIR = Path(__file__).resolve().parent.parent / "custom_components" / "meridian_energy"
if str(COMPONENT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(COMPONENT_DIR.parent))


# ---------------------------------------------------------------------------
# Default schedule fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def default_schedule() -> dict:
    """Return the default Northpower TOU schedule."""
    return {
        "effective_from": "2023-06-09T00:00:00",
        "night_start": "22:00",
        "night_end": "07:00",
        "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
        "weekend_offpeak": True,
    }


@pytest.fixture
def sample_rate_data() -> dict:
    """Return a minimal rate cache dataset for testing fallback logic."""
    return {
        "last_updated": "2026-03-01T00:00:00",
        "special": {
            "2026-03": {
                "night": 0.167,
                "peak": 0.3161,
                "offpeak": 0.2267,
                "weekend_offpeak": 0.2267,
                "controlled": 0.167,
                "daily": 3.7375,
            },
            "2025-03": {
                "night": 0.155,
                "peak": 0.300,
                "offpeak": 0.210,
                "weekend_offpeak": 0.210,
                "controlled": 0.155,
                "daily": 3.50,
            },
        },
        "base": {},
    }
