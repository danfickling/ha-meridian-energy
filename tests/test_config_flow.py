"""Tests for config_flow.py — form schemas, error propagation, reauth."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, AsyncMock
import asyncio

import pytest

from meridian_energy.config_flow import MeridianConfigFlow, MeridianOptionsFlow
from meridian_energy.const import (
    DOMAIN,
    CONF_SUPPLIER,
    CONF_NETWORK,
    CONF_RATE_TYPE,
    CONF_HISTORY_START,
    CONF_COOKIE,
    DEFAULT_SUPPLIER,
    DEFAULT_NETWORK,
    DEFAULT_RATE_TYPE,
    DEFAULT_HISTORY_START,
    SUPPLIER_POWERSHOP,
    HTTP_TIMEOUT,
    HTTP_TIMEOUT_COOKIE_CHECK,
    RATE_TYPE_OPTIONS,
    SUPPLIER_OPTIONS,
)



class TestConfigFlowVersion:
    def test_version_is_1(self):
        assert MeridianConfigFlow.VERSION == 1



class TestConfigFlowDomain:
    def test_domain_matches(self):
        # The flow is registered with the correct DOMAIN
        assert DOMAIN == "meridian_energy"



class TestOptionsFlowInit:
    def test_options_flow_created(self):
        flow = MeridianOptionsFlow()
        assert flow._options == {}



class TestDiagnosticsRedaction:
    def test_to_redact_contains_sensitive_fields(self):
        from meridian_energy.diagnostics import TO_REDACT
        assert "email" in TO_REDACT or any("email" in str(f).lower() for f in TO_REDACT)
        assert "password" in TO_REDACT or any("password" in str(f).lower() for f in TO_REDACT)
        assert CONF_COOKIE in TO_REDACT



class TestServiceConstants:
    def test_service_names_defined(self):
        from meridian_energy import (
            SERVICE_REFRESH_RATES,
            SERVICE_REIMPORT_HISTORY,
            SERVICE_CHECK_SCHEDULE,
            SERVICE_UPDATE_SCHEDULE,
        )
        assert SERVICE_REFRESH_RATES == "refresh_rates"
        assert SERVICE_REIMPORT_HISTORY == "reimport_history"
        assert SERVICE_CHECK_SCHEDULE == "check_schedule"
        assert SERVICE_UPDATE_SCHEDULE == "update_schedule"



class TestRuntimeData:
    def test_runtime_data_fields(self):
        from meridian_energy import MeridianRuntimeData
        coordinator = MagicMock()
        api = MagicMock()
        rd = MeridianRuntimeData(coordinator=coordinator, api=api)
        assert rd.coordinator is coordinator
        assert rd.api is api



class TestTimeoutConstants:
    def test_http_timeout_is_positive(self):
        assert HTTP_TIMEOUT > 0

    def test_cookie_timeout_is_positive(self):
        assert HTTP_TIMEOUT_COOKIE_CHECK > 0

    def test_cookie_timeout_shorter_than_default(self):
        assert HTTP_TIMEOUT_COOKIE_CHECK <= HTTP_TIMEOUT



class TestOptionsValidation:
    def test_rate_type_options_contains_defaults(self):
        assert "special" in RATE_TYPE_OPTIONS
        assert "base" in RATE_TYPE_OPTIONS

    def test_supplier_options_contains_powershop(self):
        assert SUPPLIER_POWERSHOP in SUPPLIER_OPTIONS

    def test_default_rate_type_in_options(self):
        assert DEFAULT_RATE_TYPE in RATE_TYPE_OPTIONS

    def test_default_supplier_in_options(self):
        assert DEFAULT_SUPPLIER in SUPPLIER_OPTIONS



class TestConfigFlowImports:
    def test_request_exception_imported(self):
        from meridian_energy.config_flow import RequestException
        assert RequestException is not None


class TestHistoryStartValidation:
    """Verify _validate_history_start accepts/rejects dates correctly."""

    def test_valid_date(self):
        from meridian_energy.config_flow import _validate_history_start
        assert _validate_history_start("01/06/2023") == "01/06/2023"

    def test_empty_string_allowed(self):
        from meridian_energy.config_flow import _validate_history_start
        assert _validate_history_start("") == ""

    def test_whitespace_only_allowed(self):
        from meridian_energy.config_flow import _validate_history_start
        assert _validate_history_start("  ") == ""

    def test_invalid_format_raises(self):
        from meridian_energy.config_flow import _validate_history_start
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            _validate_history_start("2023-06-01")

    def test_yyyy_mm_dd_rejected(self):
        from meridian_energy.config_flow import _validate_history_start
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            _validate_history_start("2023/06/01")

    def test_garbage_rejected(self):
        from meridian_energy.config_flow import _validate_history_start
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            _validate_history_start("not-a-date")

    def test_partial_date_rejected(self):
        from meridian_energy.config_flow import _validate_history_start
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            _validate_history_start("01/2023")

    def test_valid_leap_day(self):
        from meridian_energy.config_flow import _validate_history_start
        assert _validate_history_start("29/02/2024") == "29/02/2024"

    def test_invalid_day_rejected(self):
        from meridian_energy.config_flow import _validate_history_start
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            _validate_history_start("31/02/2023")


class TestReauthFlow:
    """UX tests for reauth form behavior."""

    def test_reauth_prefills_existing_email(self):
        flow = MeridianConfigFlow()
        flow._get_reauth_entry = MagicMock(
            return_value=MagicMock(
                data={
                    "email": "user@example.com",
                    CONF_SUPPLIER: SUPPLIER_POWERSHOP,
                }
            )
        )

        show_form = MagicMock(return_value={"step_id": "reauth_confirm"})
        flow.async_show_form = show_form

        with patch("meridian_energy.config_flow.vol.Required") as required_mock:
            required_mock.side_effect = lambda key, default=None: key
            result = asyncio.get_event_loop().run_until_complete(
                flow.async_step_reauth_confirm(user_input=None)
            )

        assert result["step_id"] == "reauth_confirm"
        required_mock.assert_any_call("email", default="user@example.com")
