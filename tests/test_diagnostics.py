"""Tests for diagnostics.py — redaction contract and output structure."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from meridian_energy.diagnostics import TO_REDACT, async_get_config_entry_diagnostics
from meridian_energy.const import CONF_COOKIE
from meridian_energy.coordinator import MeridianData



class TestRedactionSet:
    def test_email_redacted(self):
        """Email must be in the redaction set."""
        # CONF_EMAIL from homeassistant.const is "email" in the mock env
        assert "email" in TO_REDACT

    def test_password_redacted(self):
        """Password must be in the redaction set."""
        assert "password" in TO_REDACT

    def test_cookie_redacted(self):
        """Cookie must be in the redaction set."""
        assert CONF_COOKIE in TO_REDACT

    def test_no_extra_redactions(self):
        """Only three fields should be redacted — no over-redaction."""
        assert len(TO_REDACT) == 3


class TestDiagnosticsFields:
    """Verify diagnostics output includes key coordinator fields."""

    def _build_entry_and_hass(self, coordinator_data=None):
        """Build a mock entry with runtime_data for diagnostics testing."""
        coordinator = MagicMock()
        coordinator.data = coordinator_data

        runtime_data = MagicMock()
        runtime_data.coordinator = coordinator

        entry = MagicMock()
        entry.data = {"email": "test@example.com", "password": "secret", "cookie": "session=x"}
        entry.options = {}
        entry.runtime_data = runtime_data

        hass = MagicMock()
        return hass, entry

    def test_detected_periods_in_output(self):
        """The diagnostics output should contain detected_periods key."""
        data = MeridianData(detected_periods=["peak", "offpeak", "night"])
        hass, entry = self._build_entry_and_hass(coordinator_data=data)

        result = asyncio.get_event_loop().run_until_complete(
            async_get_config_entry_diagnostics(hass, entry)
        )

        assert "coordinator_data" in result
        assert "detected_periods" in result["coordinator_data"]
        assert result["coordinator_data"]["detected_periods"] == ["peak", "offpeak", "night"]

    def test_output_contains_core_fields(self):
        """Diagnostics output should contain essential coordinator fields."""
        data = MeridianData(
            supplier="powershop",
            sensor_name="Powershop",
            rate_type="special",
            tou_period="peak",
            current_rate=0.31,
            daily_charge=3.73,
        )
        hass, entry = self._build_entry_and_hass(coordinator_data=data)

        result = asyncio.get_event_loop().run_until_complete(
            async_get_config_entry_diagnostics(hass, entry)
        )

        coord = result["coordinator_data"]
        assert coord["supplier"] == "powershop"
        assert coord["sensor_name"] == "Powershop"
        assert coord["rate_type"] == "special"
        assert coord["tou_period"] == "peak"
        assert coord["current_rate"] == 0.31
        assert coord["daily_charge"] == 3.73

    def test_output_without_coordinator_data(self):
        """Diagnostics should work when coordinator has no data yet."""
        hass, entry = self._build_entry_and_hass(coordinator_data=None)

        result = asyncio.get_event_loop().run_until_complete(
            async_get_config_entry_diagnostics(hass, entry)
        )

        assert "config_entry" in result
        assert "coordinator_data" not in result
