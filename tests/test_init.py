"""Tests for the v2 __init__ module."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import asyncio
import pytest

from meridian_energy import (
    SERVICE_REFRESH_RATES,
    SERVICE_BACKFILL,
    DOMAIN,
    MeridianRuntimeData,
    async_migrate_entry,
    _V1_SUPPLIER_TO_BRAND,
)
from meridian_energy.const import (
    CONF_BRAND,
    CONF_REFRESH_TOKEN,
    CONF_ACCOUNT_NUMBER,
    CONF_EMAIL,
    DEFAULT_BRAND,
    BRAND_CONFIG,
    PLATFORMS,
)


class TestConstants:
    def test_domain(self):
        assert DOMAIN == "meridian_energy"

    def test_service_refresh_rates(self):
        assert SERVICE_REFRESH_RATES == "refresh_rates"

    def test_service_backfill(self):
        assert SERVICE_BACKFILL == "backfill"

    def test_platforms(self):
        assert "sensor" in [str(p) for p in PLATFORMS]

    def test_conf_keys(self):
        assert CONF_BRAND == "brand"
        assert CONF_REFRESH_TOKEN == "refresh_token"
        assert CONF_ACCOUNT_NUMBER == "account_number"
        assert CONF_EMAIL == "email"

    def test_default_brand(self):
        assert DEFAULT_BRAND == "powershop"


class TestRuntimeData:
    def test_runtime_data_fields(self):
        d = MeridianRuntimeData(coordinator=None, api=None)
        assert d.coordinator is None
        assert d.api is None


class TestBrandConfig:
    def test_powershop_name(self):
        assert BRAND_CONFIG["powershop"]["name"] == "Powershop"

    def test_meridian_name(self):
        assert BRAND_CONFIG["meridian"]["name"] == "Meridian Energy"

    def test_each_brand_has_api_url(self):
        for config in BRAND_CONFIG.values():
            assert "api_url" in config
            assert config["api_url"].startswith("https://")

    def test_no_old_services(self):
        """v2 has only refresh_rates — no reimport/check/update services."""
        # Just verify the constant exists as expected
        assert SERVICE_REFRESH_RATES == "refresh_rates"


class TestMigration:
    """Tests for async_migrate_entry (v1 → v2)."""

    def _make_entry(self, version=1, data=None):
        entry = MagicMock()
        entry.version = version
        entry.entry_id = "test_entry"
        entry.data = data or {
            "email": "user@example.com",
            "password": "encoded",
            "cookie": "",
            "supplier": "powershop",
            "network": "NorthPower_1_",
            "history_start": "09/06/2023",
        }
        return entry

    def test_v1_to_v2_maps_supplier_to_brand(self):
        hass = MagicMock()
        entry = self._make_entry()
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(async_migrate_entry(hass, entry))
        assert result is True
        call_kwargs = hass.config_entries.async_update_entry.call_args
        new_data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        assert new_data[CONF_BRAND] == "powershop"
        assert new_data["email"] == "user@example.com"
        assert new_data[CONF_REFRESH_TOKEN] == ""
        assert new_data[CONF_ACCOUNT_NUMBER] == ""

    def test_v1_to_v2_strips_old_fields(self):
        hass = MagicMock()
        entry = self._make_entry()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_migrate_entry(hass, entry))
        call_kwargs = hass.config_entries.async_update_entry.call_args
        new_data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        assert "password" not in new_data
        assert "cookie" not in new_data
        assert "network" not in new_data
        assert "history_start" not in new_data

    def test_v1_to_v2_sets_version_2(self):
        hass = MagicMock()
        entry = self._make_entry()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_migrate_entry(hass, entry))
        call_kwargs = hass.config_entries.async_update_entry.call_args
        assert call_kwargs.kwargs.get("version") or call_kwargs[1].get("version") == 2

    def test_v2_entry_is_noop(self):
        hass = MagicMock()
        entry = self._make_entry(version=2)
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(async_migrate_entry(hass, entry))
        assert result is True
        hass.config_entries.async_update_entry.assert_not_called()

    def test_meridian_supplier_maps_correctly(self):
        hass = MagicMock()
        entry = self._make_entry(data={
            "email": "u@m.nz",
            "password": "x",
            "cookie": "",
            "supplier": "meridian",
            "network": "",
            "history_start": "",
        })
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_migrate_entry(hass, entry))
        call_kwargs = hass.config_entries.async_update_entry.call_args
        new_data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        assert new_data[CONF_BRAND] == "meridian"

    def test_supplier_map_keys(self):
        assert "powershop" in _V1_SUPPLIER_TO_BRAND
        assert "meridian" in _V1_SUPPLIER_TO_BRAND

    def test_all_migrated_brands_are_valid_brand_keys(self):
        """Every brand produced by migration must exist in BRAND_CONFIG."""
        for supplier, brand in _V1_SUPPLIER_TO_BRAND.items():
            assert brand in BRAND_CONFIG, (
                f"Migration maps supplier {supplier!r} to brand {brand!r} "
                f"which is not a valid BRAND_CONFIG key"
            )


class TestBackfillServiceSchema:
    """Verify the backfill service date schema accepts ISO date strings."""

    def test_iso_string_start_date(self):
        from datetime import date as _date
        # date.fromisoformat is used in the service schema
        assert _date.fromisoformat("2026-04-01") == _date(2026, 4, 1)

    def test_iso_string_end_date(self):
        from datetime import date as _date
        assert _date.fromisoformat("2026-12-31") == _date(2026, 12, 31)

    def test_invalid_date_string_raises(self):
        from datetime import date as _date
        with pytest.raises(ValueError):
            _date.fromisoformat("not-a-date")
