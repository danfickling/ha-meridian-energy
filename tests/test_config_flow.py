"""Tests for the v2 config flow module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from meridian_energy.config_flow import MeridianConfigFlow
from meridian_energy.const import (
    DOMAIN,
    CONF_BRAND,
    CONF_REFRESH_TOKEN,
    CONF_ACCOUNT_NUMBER,
    DEFAULT_BRAND,
)
from meridian_energy.api import AuthError, ApiError


def _make_flow():
    """Create a MeridianConfigFlow with mocked HA framework methods."""
    flow = MeridianConfigFlow()
    flow.hass = MagicMock()
    flow.async_set_unique_id = AsyncMock()
    flow._abort_if_unique_id_configured = MagicMock()
    flow.async_show_form = MagicMock(side_effect=lambda **kw: {"type": "form", **kw})
    flow.async_create_entry = MagicMock(
        side_effect=lambda **kw: {"type": "create_entry", **kw}
    )
    flow.async_abort = MagicMock(
        side_effect=lambda **kw: {"type": "abort", **kw}
    )
    return flow


class TestConfigFlowMetadata:
    def test_version(self):
        assert MeridianConfigFlow.VERSION == 2

    def test_domain(self):
        assert DOMAIN == "meridian_energy"

    def test_default_brand(self):
        flow = MeridianConfigFlow()
        assert flow._brand == DEFAULT_BRAND

    def test_default_email_empty(self):
        flow = MeridianConfigFlow()
        assert flow._email == ""

    def test_default_journey_id_empty(self):
        flow = MeridianConfigFlow()
        assert flow._journey_id == ""


class TestConfigFlowConstants:
    def test_conf_brand_key(self):
        assert CONF_BRAND == "brand"

    def test_domain_value(self):
        assert DOMAIN == "meridian_energy"


class TestAsyncStepUser:
    """Behavioral tests for the user step (email + brand → OTP)."""

    def test_show_form_when_no_input(self):
        """No user input should show the initial form."""
        flow = _make_flow()
        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_user(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "user"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_successful_otp_send_proceeds_to_otp_step(self, mock_send, mock_session):
        """Valid email should send OTP and proceed to the OTP step."""
        flow = _make_flow()
        mock_session.return_value = MagicMock()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_user({
                "email": " Test@Example.com ",
                "brand": "powershop",
            })
        )
        # Should have called async_send_otp_email
        mock_send.assert_called_once()
        assert flow._email == "test@example.com"
        # The result should be from async_step_otp (showing the OTP form)
        assert result["type"] == "form"
        assert result["step_id"] == "otp"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_email_not_found_error(self, mock_send, mock_session):
        """AuthError with email_not_found should show the error."""
        flow = _make_flow()
        mock_session.return_value = MagicMock()
        mock_send.side_effect = AuthError("email_not_found")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_user({
                "email": "bad@example.com",
                "brand": "powershop",
            })
        )
        assert result["type"] == "form"
        assert result["step_id"] == "user"
        assert result["errors"]["email"] == "email_not_found"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_connection_error(self, mock_send, mock_session):
        """Network errors should show cannot_connect."""
        flow = _make_flow()
        mock_session.return_value = MagicMock()
        mock_send.side_effect = TimeoutError()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_user({
                "email": "user@example.com",
                "brand": "powershop",
            })
        )
        assert result["type"] == "form"
        assert result["step_id"] == "user"
        assert result["errors"]["base"] == "cannot_connect"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_auth_error_non_email_shows_cannot_connect(self, mock_send, mock_session):
        """AuthError without email_not_found should show cannot_connect."""
        flow = _make_flow()
        mock_session.return_value = MagicMock()
        mock_send.side_effect = AuthError("generic_auth_error")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_user({
                "email": "user@example.com",
                "brand": "powershop",
            })
        )
        assert result["type"] == "form"
        assert result["errors"]["base"] == "cannot_connect"


class TestAsyncStepOtp:
    """Behavioral tests for the OTP validation step."""

    def test_show_form_when_no_input(self):
        """No user input should show the OTP form."""
        flow = _make_flow()
        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"

    @patch("meridian_energy.config_flow.MeridianEnergyApi.async_discover_accounts", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_valid_otp_single_account_creates_entry(self, mock_session, mock_validate, mock_discover):
        """Valid OTP with single account should create a config entry."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {
            "idToken": "id-tok",
            "refreshToken": "refresh-tok",
        }
        mock_discover.return_value = [{"number": "A-12345"}]

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": " 123456 "})
        )
        assert result["type"] == "create_entry"
        assert result["data"]["email"] == "user@example.com"
        assert result["data"][CONF_REFRESH_TOKEN] == "refresh-tok"
        assert result["data"][CONF_ACCOUNT_NUMBER] == "A-12345"
        # Unique ID should include account number for multi-account support
        flow.async_set_unique_id.assert_called_with(
            "powershop_user@example.com_A-12345"
        )

    @patch("meridian_energy.config_flow.MeridianEnergyApi.async_discover_accounts", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_valid_otp_multiple_accounts_shows_selection(self, mock_session, mock_validate, mock_discover):
        """Valid OTP with multiple accounts should show account selection."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {
            "idToken": "id-tok",
            "refreshToken": "refresh-tok",
        }
        mock_discover.return_value = [
            {"number": "A-111"},
            {"number": "A-222"},
        ]

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "select_account"
        assert len(flow._accounts) == 2

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_invalid_otp_shows_error(self, mock_session, mock_validate):
        """AuthError during validation should show invalid_otp."""
        flow = _make_flow()
        flow._email = "user@example.com"
        mock_session.return_value = MagicMock()
        mock_validate.side_effect = AuthError("invalid code")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "000000"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "invalid_otp"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_missing_refresh_token_shows_error(self, mock_session, mock_validate):
        """Tokens missing refreshToken should show invalid_otp error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {"idToken": "id-tok"}  # no refreshToken

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "invalid_otp"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_missing_id_token_shows_error(self, mock_session, mock_validate):
        """Tokens missing idToken should show invalid_otp error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {"refreshToken": "ref-tok"}  # no idToken

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "invalid_otp"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_missing_refresh_token_shows_error(self, mock_session, mock_validate):
        """Tokens missing refreshToken should show invalid_otp error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {"idToken": "id-tok"}  # no refreshToken

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "invalid_otp"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_missing_id_token_shows_error(self, mock_session, mock_validate):
        """Tokens missing idToken should show invalid_otp error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {"refreshToken": "ref-tok"}  # no idToken

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "invalid_otp"

    @patch("meridian_energy.config_flow.MeridianEnergyApi.async_discover_accounts", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_account_discovery_failure(self, mock_session, mock_validate, mock_discover):
        """Account discovery failure should show error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {
            "idToken": "id-tok",
            "refreshToken": "refresh-tok",
        }
        mock_discover.side_effect = ApiError("no accounts")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "otp"
        assert result["errors"]["base"] == "account_not_found"


class TestAsyncStepSelectAccount:
    """Behavioral tests for the account selection step."""

    def test_show_form_lists_accounts(self):
        """No input should show account selection form."""
        flow = _make_flow()
        flow._accounts = [{"number": "A-111"}, {"number": "A-222"}]

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_select_account(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "select_account"

    def test_selecting_account_creates_entry(self):
        """Selecting an account should create a config entry."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        flow._refresh_token = "tok"

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_select_account({"account": "A-111"})
        )
        assert result["type"] == "create_entry"
        assert result["data"][CONF_ACCOUNT_NUMBER] == "A-111"


class TestAsyncStepReauth:
    """Behavioral tests for the reauth flow."""

    def test_reauth_sets_email_and_brand(self):
        """Reauth should parse existing entry data and show confirm form."""
        flow = _make_flow()
        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reauth({
                "email": "user@example.com",
                "brand": "meridian",
            })
        )
        assert flow._email == "user@example.com"
        assert flow._brand == "meridian"
        # Should show the reauth_confirm form
        assert result["type"] == "form"
        assert result["step_id"] == "reauth_confirm"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reauth_confirm_sends_otp(self, mock_send, mock_session):
        """Confirming reauth should send OTP and show OTP form."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        # _get_reauth_entry is called when chaining to reauth_otp step
        flow._get_reauth_entry = MagicMock(return_value=MagicMock())

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reauth_confirm({})
        )
        mock_send.assert_called_once()
        assert result["type"] == "form"
        assert result["step_id"] == "reauth_otp"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reauth_confirm_connection_error(self, mock_send, mock_session):
        """Connection error during reauth should show error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_send.side_effect = TimeoutError()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reauth_confirm({})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reauth_confirm"
        assert result["errors"]["base"] == "cannot_connect"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_reauth_otp_success(self, mock_session, mock_validate):
        """Valid OTP during reauth should update and abort."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {"refreshToken": "new-refresh-tok"}

        reauth_entry = MagicMock()
        reauth_entry.data = {"email": "user@example.com", "brand": "powershop"}
        flow._get_reauth_entry = MagicMock(return_value=reauth_entry)
        flow.async_update_reload_and_abort = MagicMock(
            return_value={"type": "abort", "reason": "reauth_successful"}
        )

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reauth_otp({"otp": "123456"})
        )
        flow.async_update_reload_and_abort.assert_called_once()
        assert result["type"] == "abort"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_reauth_otp_invalid(self, mock_session, mock_validate):
        """Invalid OTP during reauth should show error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        mock_session.return_value = MagicMock()
        mock_validate.side_effect = AuthError("bad otp")

        reauth_entry = MagicMock()
        flow._get_reauth_entry = MagicMock(return_value=reauth_entry)

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reauth_otp({"otp": "000000"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reauth_otp"
        assert result["errors"]["base"] == "invalid_otp"


class TestAsyncStepReconfigure:
    """Behavioral tests for the reconfigure flow."""

    def test_show_form_prefilled(self):
        """No input should show form pre-filled with current entry data."""
        flow = _make_flow()
        entry = MagicMock()
        entry.data = {
            "email": "old@example.com",
            "brand": "powershop",
        }
        flow._get_reconfigure_entry = MagicMock(return_value=entry)

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reconfigure_sends_otp_and_proceeds(self, mock_send, mock_session):
        """Valid email should send OTP and proceed to reconfigure_otp."""
        flow = _make_flow()
        entry = MagicMock()
        entry.entry_id = "entry-1"
        entry.unique_id = "powershop_old@example.com_A-123"
        entry.data = {"email": "old@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        mock_session.return_value = MagicMock()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure({
                "email": "new@example.com",
                "brand": "meridian",
            })
        )
        mock_send.assert_called_once()
        assert flow._email == "new@example.com"
        assert flow._brand == "meridian"
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_otp"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reconfigure_email_not_found(self, mock_send, mock_session):
        """Email not found should show error on reconfigure form."""
        flow = _make_flow()
        entry = MagicMock()
        entry.entry_id = "entry-1"
        entry.unique_id = "powershop_old@example.com_A-123"
        entry.data = {"email": "old@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        mock_session.return_value = MagicMock()
        mock_send.side_effect = AuthError("email_not_found")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure({
                "email": "bad@example.com",
                "brand": "powershop",
            })
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure"
        assert result["errors"]["email"] == "email_not_found"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reconfigure_same_email_different_account_proceeds(
        self, mock_send, mock_session,
    ):
        """Reconfiguring with same email/brand should proceed (collision is per-account)."""
        flow = _make_flow()
        current_entry = MagicMock()
        current_entry.entry_id = "entry-1"
        current_entry.unique_id = "powershop_old@example.com_A-111"
        current_entry.data = {"email": "old@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=current_entry)
        mock_session.return_value = MagicMock()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure({
                "email": "old@example.com",
                "brand": "powershop",
            })
        )
        mock_send.assert_called_once()
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_otp"

    @patch("meridian_energy.config_flow.async_get_clientsession")
    @patch("meridian_energy.config_flow.async_send_otp_email", new_callable=AsyncMock)
    def test_reconfigure_same_identity_proceeds(self, mock_send, mock_session):
        """Reconfiguring with same email/brand should proceed (not abort)."""
        flow = _make_flow()
        entry = MagicMock()
        entry.entry_id = "entry-1"
        entry.unique_id = "powershop_user@example.com_A-123"
        entry.data = {"email": "user@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        mock_session.return_value = MagicMock()

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure({
                "email": "user@example.com",
                "brand": "powershop",
            })
        )
        mock_send.assert_called_once()
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_otp"


class TestAsyncStepReconfigureOtp:
    """Behavioral tests for the reconfigure OTP step."""

    def test_show_otp_form(self):
        """No input should show the OTP form."""
        flow = _make_flow()
        flow._email = "user@example.com"
        entry = MagicMock()
        flow._get_reconfigure_entry = MagicMock(return_value=entry)

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_otp(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_otp"

    @patch("meridian_energy.config_flow.MeridianEnergyApi.async_discover_accounts", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_valid_otp_single_account_updates_entry(self, mock_session, mock_validate, mock_discover):
        """Valid OTP with single account should update the config entry."""
        flow = _make_flow()
        flow._email = "new@example.com"
        flow._brand = "meridian"
        entry = MagicMock()
        entry.data = {"email": "old@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        flow.async_update_reload_and_abort = MagicMock(
            return_value={"type": "abort", "reason": "reconfigure_successful"}
        )
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {
            "idToken": "id-tok",
            "refreshToken": "new-refresh",
        }
        mock_discover.return_value = [{"number": "B-999"}]

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_otp({"otp": "123456"})
        )
        flow.async_update_reload_and_abort.assert_called_once()
        call_kwargs = flow.async_update_reload_and_abort.call_args
        assert call_kwargs[1]["data"][CONF_ACCOUNT_NUMBER] == "B-999"
        assert call_kwargs[1]["data"]["email"] == "new@example.com"
        assert call_kwargs[1]["unique_id"] == "meridian_new@example.com_B-999"
        assert result["type"] == "abort"

    @patch("meridian_energy.config_flow.MeridianEnergyApi.async_discover_accounts", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_valid_otp_multiple_accounts_shows_selection(self, mock_session, mock_validate, mock_discover):
        """Valid OTP with multiple accounts should show selection form."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        entry = MagicMock()
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        mock_session.return_value = MagicMock()
        mock_validate.return_value = {
            "idToken": "id-tok",
            "refreshToken": "tok",
        }
        mock_discover.return_value = [
            {"number": "A-111"},
            {"number": "A-222"},
        ]

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_otp({"otp": "123456"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_select_account"

    @patch("meridian_energy.config_flow.async_validate_otp", new_callable=AsyncMock)
    @patch("meridian_energy.config_flow.async_get_clientsession")
    def test_invalid_otp_shows_error(self, mock_session, mock_validate):
        """Invalid OTP should show error."""
        flow = _make_flow()
        flow._email = "user@example.com"
        entry = MagicMock()
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        mock_session.return_value = MagicMock()
        mock_validate.side_effect = AuthError("bad")

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_otp({"otp": "000000"})
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_otp"
        assert result["errors"]["base"] == "invalid_otp"


class TestAsyncStepReconfigureSelectAccount:
    """Behavioral tests for account selection during reconfigure."""

    def test_show_account_selection(self):
        """No input should show account selection form."""
        flow = _make_flow()
        flow._accounts = [{"number": "A-111"}, {"number": "A-222"}]
        entry = MagicMock()
        flow._get_reconfigure_entry = MagicMock(return_value=entry)

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_select_account(None)
        )
        assert result["type"] == "form"
        assert result["step_id"] == "reconfigure_select_account"

    def test_selecting_account_updates_entry(self):
        """Selecting an account should update the config entry."""
        flow = _make_flow()
        flow._email = "user@example.com"
        flow._brand = "powershop"
        flow._refresh_token = "tok"
        entry = MagicMock()
        entry.data = {"email": "old@example.com", "brand": "powershop"}
        flow._get_reconfigure_entry = MagicMock(return_value=entry)
        flow.async_update_reload_and_abort = MagicMock(
            return_value={"type": "abort", "reason": "reconfigure_successful"}
        )

        result = asyncio.get_event_loop().run_until_complete(
            flow.async_step_reconfigure_select_account({"account": "A-222"})
        )
        flow.async_update_reload_and_abort.assert_called_once()
        call_kwargs = flow.async_update_reload_and_abort.call_args
        assert call_kwargs[1]["data"][CONF_ACCOUNT_NUMBER] == "A-222"
        assert call_kwargs[1]["unique_id"] == "powershop_user@example.com_A-222"
