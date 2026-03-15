"""Tests for __init__.py — options update listener and service handlers."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from meridian_energy import _options_update_listener
from meridian_energy.const import (
    CONF_RATE_TYPE,
    CONF_NETWORK,
    CONF_SUPPLIER,
    CONF_HISTORY_START,
    CONF_COOKIE,
    DEFAULT_RATE_TYPE,
    DEFAULT_NETWORK,
    DEFAULT_SUPPLIER,
    DEFAULT_HISTORY_START,
    SUPPLIER_POWERSHOP,
    SUPPLIER_MERIDIAN,
    RATE_TYPE_OPTIONS,
    SUPPLIER_OPTIONS,
)
from meridian_energy.schedule import NETWORKS


def _make_entry(*, options=None, data=None):
    """Build a mock config entry with runtime_data."""
    entry = MagicMock()
    entry.options = options or {}
    entry.data = data or {
        CONF_SUPPLIER: SUPPLIER_POWERSHOP,
        CONF_NETWORK: DEFAULT_NETWORK,
        CONF_HISTORY_START: DEFAULT_HISTORY_START,
        CONF_COOKIE: "",
    }

    coordinator = MagicMock()
    coordinator.rate_type = DEFAULT_RATE_TYPE
    coordinator.network = DEFAULT_NETWORK
    coordinator.supplier = DEFAULT_SUPPLIER
    coordinator.async_refresh = AsyncMock()

    api = MagicMock()
    api.history_start = DEFAULT_HISTORY_START
    api.cookie = ""

    runtime_data = MagicMock()
    runtime_data.coordinator = coordinator
    runtime_data.api = api
    entry.runtime_data = runtime_data

    return entry, coordinator, api


class TestOptionsUpdateListener:
    """Tests for _options_update_listener."""

    def test_invalid_rate_type_rejected(self, caplog):
        """Invalid rate_type should log error and not refresh."""
        entry, coordinator, api = _make_entry(
            options={CONF_RATE_TYPE: "nonexistent_type"}
        )
        with caplog.at_level(logging.ERROR, logger="meridian_energy"):
            asyncio.get_event_loop().run_until_complete(
                _options_update_listener(MagicMock(), entry)
            )
        assert "Invalid rate type" in caplog.text
        coordinator.async_refresh.assert_not_awaited()

    def test_invalid_supplier_rejected(self, caplog):
        """Invalid supplier should log error and not refresh."""
        entry, coordinator, api = _make_entry(
            options={CONF_SUPPLIER: "fake_supplier"}
        )
        with caplog.at_level(logging.ERROR, logger="meridian_energy"):
            asyncio.get_event_loop().run_until_complete(
                _options_update_listener(MagicMock(), entry)
            )
        assert "Invalid supplier" in caplog.text
        coordinator.async_refresh.assert_not_awaited()

    def test_unknown_network_warns(self, caplog):
        """Unknown network should log warning but still apply."""
        entry, coordinator, api = _make_entry(
            options={CONF_NETWORK: "UnknownNet_99_"}
        )
        coordinator.network = DEFAULT_NETWORK  # different from unknown
        with caplog.at_level(logging.WARNING, logger="meridian_energy"):
            asyncio.get_event_loop().run_until_complete(
                _options_update_listener(MagicMock(), entry)
            )
        assert "Unknown network" in caplog.text
        # Should still apply the change and refresh
        assert coordinator.network == "UnknownNet_99_"
        coordinator.async_refresh.assert_awaited_once()

    def test_valid_rate_type_change_triggers_refresh(self):
        """Valid rate_type change should update coordinator and refresh."""
        entry, coordinator, api = _make_entry(
            options={CONF_RATE_TYPE: "base"}
        )
        coordinator.rate_type = "special"  # original
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert coordinator.rate_type == "base"
        coordinator.async_refresh.assert_awaited_once()

    def test_valid_network_change_triggers_refresh(self):
        """Valid network change should update coordinator and refresh."""
        new_network = "Orion"
        entry, coordinator, api = _make_entry(
            options={CONF_NETWORK: new_network}
        )
        coordinator.network = DEFAULT_NETWORK
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert coordinator.network == new_network
        coordinator.async_refresh.assert_awaited_once()

    def test_supplier_change_updates_api_and_coordinator(self):
        """Supplier change should update both api and coordinator."""
        entry, coordinator, api = _make_entry(
            options={CONF_SUPPLIER: SUPPLIER_MERIDIAN}
        )
        coordinator.supplier = SUPPLIER_POWERSHOP
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert coordinator.supplier == SUPPLIER_MERIDIAN
        assert api.supplier == SUPPLIER_MERIDIAN
        coordinator.async_refresh.assert_awaited_once()

    def test_history_start_change_triggers_refresh(self):
        """history_start change should update api and refresh."""
        entry, coordinator, api = _make_entry(
            options={CONF_HISTORY_START: "01/01/2024"}
        )
        api.history_start = ""
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert api.history_start == "01/01/2024"
        coordinator.async_refresh.assert_awaited_once()

    def test_cookie_change_triggers_refresh(self):
        """Cookie change should update api and refresh."""
        entry, coordinator, api = _make_entry(
            options={CONF_COOKIE: "session=abc123"}
        )
        api.cookie = ""
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert api.cookie == "session=abc123"
        coordinator.async_refresh.assert_awaited_once()

    def test_no_change_does_not_refresh(self):
        """When nothing changed, coordinator.async_refresh should not be called."""
        entry, coordinator, api = _make_entry()
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        coordinator.async_refresh.assert_not_awaited()

    def test_multiple_changes_single_refresh(self):
        """Multiple simultaneous changes should trigger only one refresh."""
        entry, coordinator, api = _make_entry(
            options={
                CONF_RATE_TYPE: "base",
                CONF_NETWORK: "Orion",
                CONF_COOKIE: "new_cookie",
            }
        )
        coordinator.rate_type = "special"
        coordinator.network = DEFAULT_NETWORK
        api.cookie = ""
        asyncio.get_event_loop().run_until_complete(
            _options_update_listener(MagicMock(), entry)
        )
        assert coordinator.rate_type == "base"
        assert coordinator.network == "Orion"
        assert api.cookie == "new_cookie"
        coordinator.async_refresh.assert_awaited_once()


class TestHandleUpdateSchedule:
    """Tests for the handle_update_schedule service handler logic."""

    def _register_and_get_handler(self):
        """Register services on a mock hass and return the update_schedule handler."""
        from meridian_energy import _register_services
        hass = MagicMock()
        hass.services.has_service.return_value = False

        _register_services(hass)

        # Find the handle_update_schedule handler from the register calls
        for call_args in hass.services.async_register.call_args_list:
            args = call_args[0]
            if len(args) >= 2 and args[1] == "update_schedule":
                return args[2], hass  # handler, hass

        raise RuntimeError("update_schedule handler not registered")

    def test_valid_full_schedule(self):
        """Full schedule with night + peak + weekend_offpeak should apply."""
        handler, hass = self._register_and_get_handler()

        # Set up a coordinator
        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "night_start": "22:00",
            "night_end": "07:00",
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
            "weekend_offpeak": True,
        }

        asyncio.get_event_loop().run_until_complete(handler(call))
        coordinator.schedule_cache.update_schedule.assert_called_once()
        schedule_arg = coordinator.schedule_cache.update_schedule.call_args[0][0]
        assert schedule_arg["night_start"] == "22:00"
        assert schedule_arg["night_end"] == "07:00"
        assert schedule_arg["weekend_offpeak"] is True
        assert schedule_arg["peak_weekday"] == [["07:00", "09:30"], ["17:30", "20:00"]]

    def test_schedule_without_night(self):
        """Schedule with no night fields should not include night keys."""
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
            "weekend_offpeak": False,
        }

        asyncio.get_event_loop().run_until_complete(handler(call))
        schedule_arg = coordinator.schedule_cache.update_schedule.call_args[0][0]
        assert "night_start" not in schedule_arg
        assert "night_end" not in schedule_arg
        assert "weekend_offpeak" not in schedule_arg  # False → not included

    def test_lone_night_start_rejected(self, caplog):
        """Providing night_start without night_end should be rejected."""
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "night_start": "22:00",
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
        }

        with caplog.at_level(logging.ERROR, logger="meridian_energy"):
            asyncio.get_event_loop().run_until_complete(handler(call))
        assert "Both night_start and night_end must be provided together" in caplog.text
        coordinator.schedule_cache.update_schedule.assert_not_called()

    def test_lone_night_end_rejected(self, caplog):
        """Providing night_end without night_start should be rejected."""
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "night_end": "07:00",
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
        }

        with caplog.at_level(logging.ERROR, logger="meridian_energy"):
            asyncio.get_event_loop().run_until_complete(handler(call))
        assert "Both night_start and night_end must be provided together" in caplog.text
        coordinator.schedule_cache.update_schedule.assert_not_called()

    def test_invalid_time_format_raises(self):
        """Invalid time format should raise vol.Invalid."""
        import voluptuous as vol
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "night_start": "25:00",  # invalid
            "night_end": "07:00",
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
        }

        with pytest.raises(vol.Invalid):
            asyncio.get_event_loop().run_until_complete(handler(call))
        coordinator.schedule_cache.update_schedule.assert_not_called()

    def test_default_peak_weekday_used(self):
        """When peak_weekday is omitted, the default should be used."""
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        call = MagicMock()
        call.data = {
            "night_start": "22:00",
            "night_end": "07:00",
        }

        asyncio.get_event_loop().run_until_complete(handler(call))
        schedule_arg = coordinator.schedule_cache.update_schedule.call_args[0][0]
        assert schedule_arg["peak_weekday"] == [["07:00", "09:30"], ["17:30", "20:00"]]

    def test_weekend_offpeak_boolean(self):
        """weekend_offpeak should be included when True, omitted when False."""
        handler, hass = self._register_and_get_handler()

        coordinator = MagicMock()
        coordinator.last_update_success = True
        coordinator.schedule_cache = MagicMock()
        coordinator.async_refresh = AsyncMock()

        entry = MagicMock()
        entry.runtime_data = MagicMock()
        entry.runtime_data.coordinator = coordinator
        hass.config_entries.async_entries.return_value = [entry]

        # True case
        call = MagicMock()
        call.data = {
            "peak_weekday": [["07:00", "09:30"]],
            "weekend_offpeak": True,
        }
        asyncio.get_event_loop().run_until_complete(handler(call))
        schedule_arg = coordinator.schedule_cache.update_schedule.call_args[0][0]
        assert schedule_arg["weekend_offpeak"] is True

        # False case
        coordinator.schedule_cache.reset_mock()
        call.data = {
            "peak_weekday": [["07:00", "09:30"]],
            "weekend_offpeak": False,
        }
        asyncio.get_event_loop().run_until_complete(handler(call))
        schedule_arg = coordinator.schedule_cache.update_schedule.call_args[0][0]
        assert "weekend_offpeak" not in schedule_arg
