"""Config flow for Meridian Energy / Powershop integration (v2 — OTP auth)."""

from __future__ import annotations

import logging
from uuid import uuid4

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.const import CONF_EMAIL
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import (
    MeridianEnergyApi,
    AuthError,
    ApiError,
    async_send_otp_email,
    async_validate_otp,
)
from .const import (
    DOMAIN,
    CONF_BRAND,
    CONF_REFRESH_TOKEN,
    CONF_ACCOUNT_NUMBER,
    BRAND_CONFIG,
    DEFAULT_BRAND,
)

_LOGGER = logging.getLogger(__name__)


class MeridianConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle config flow for Meridian Energy / Powershop (v2)."""

    VERSION = 2

    def __init__(self) -> None:
        self._email: str = ""
        self._brand: str = DEFAULT_BRAND
        self._journey_id: str = ""
        self._refresh_token: str = ""
        self._accounts: list[dict] = []

    # -- Initial setup -------------------------------------------------------

    async def async_step_user(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Step 1: Email and brand selection — sends OTP email."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._email = user_input[CONF_EMAIL].strip().lower()
            self._brand = user_input.get(CONF_BRAND, DEFAULT_BRAND)

            # Send OTP email
            self._journey_id = str(uuid4())
            session = async_get_clientsession(self.hass)
            try:
                await async_send_otp_email(
                    session, self._email, self._brand,
                    journey_id=self._journey_id,
                )
            except AuthError as err:
                if "email_not_found" in str(err):
                    errors[CONF_EMAIL] = "email_not_found"
                else:
                    errors["base"] = "cannot_connect"
            except (aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

            if not errors:
                return await self.async_step_otp()

        brand_options = {k: v["name"] for k, v in BRAND_CONFIG.items()}

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_BRAND, default=DEFAULT_BRAND
                    ): vol.In(brand_options),
                    vol.Required(CONF_EMAIL): cv.string,
                }
            ),
            errors=errors,
        )

    async def async_step_otp(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Step 2: Validate OTP code, discover account, create entry."""
        errors: dict[str, str] = {}

        if user_input is not None:
            raw = user_input["otp"].strip()
            session = async_get_clientsession(self.hass)
            tokens = None

            try:
                tokens = await async_validate_otp(
                    session, self._email, raw, self._brand,
                    journey_id=self._journey_id,
                )
            except AuthError:
                errors["base"] = "invalid_otp"
            except (aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

            if tokens:
                id_token = tokens.get("idToken")
                refresh_token = tokens.get("refreshToken")
                if not id_token or not refresh_token:
                    errors["base"] = "invalid_otp"
                else:
                    self._refresh_token = refresh_token

                    # Discover accounts
                    try:
                        accounts = await MeridianEnergyApi.async_discover_accounts(
                            session, id_token, self._brand,
                        )
                    except (AuthError, ApiError) as err:
                        _LOGGER.error("Account discovery failed: %s", err)
                        errors["base"] = "account_not_found"
                        accounts = []

                    if accounts:
                        if len(accounts) == 1:
                            account_number = accounts[0].get("number", "")
                            return await self._async_create_account_entry(
                                account_number,
                            )
                        # Multiple accounts — let the user choose
                        self._accounts = accounts
                        return await self.async_step_select_account()

        return self.async_show_form(
            step_id="otp",
            description_placeholders={"email": self._email},
            data_schema=vol.Schema(
                {
                    vol.Required("otp"): cv.string,
                }
            ),
            errors=errors,
        )

    # -- Account selection ---------------------------------------------------

    async def async_step_select_account(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Step 3 (optional): Select account when multiple are found."""
        if user_input is not None:
            return await self._async_create_account_entry(
                user_input["account"],
            )

        account_options = {
            a.get("number", ""): a.get("number", "")
            for a in self._accounts
            if a.get("number")
        }
        return self.async_show_form(
            step_id="select_account",
            data_schema=vol.Schema(
                {
                    vol.Required("account"): vol.In(account_options),
                }
            ),
        )

    async def _async_create_account_entry(
        self, account_number: str,
    ) -> ConfigFlowResult:
        """Create a config entry for the selected account."""
        # One entry per brand + email + account
        unique_id = f"{self._brand}_{self._email}_{account_number}"
        await self.async_set_unique_id(unique_id)
        self._abort_if_unique_id_configured()

        brand_name = BRAND_CONFIG[self._brand]["name"]
        return self.async_create_entry(
            title=f"{brand_name} ({account_number})",
            data={
                CONF_EMAIL: self._email,
                CONF_BRAND: self._brand,
                CONF_REFRESH_TOKEN: self._refresh_token,
                CONF_ACCOUNT_NUMBER: account_number,
            },
        )

    # -- Reauth flow ---------------------------------------------------------

    async def async_step_reauth(
        self, entry_data: dict,
    ) -> ConfigFlowResult:
        """Handle reauth when the refresh token expires."""
        self._email = entry_data.get(CONF_EMAIL, "")
        self._brand = entry_data.get(CONF_BRAND, DEFAULT_BRAND)
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Send OTP for reauth (shows a confirm button)."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._journey_id = str(uuid4())
            session = async_get_clientsession(self.hass)
            try:
                await async_send_otp_email(
                    session, self._email, self._brand,
                    journey_id=self._journey_id,
                )
            except (AuthError, aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

            if not errors:
                return await self.async_step_reauth_otp()

        return self.async_show_form(
            step_id="reauth_confirm",
            description_placeholders={
                "email": self._email,
                "brand": BRAND_CONFIG[self._brand]["name"],
            },
            data_schema=vol.Schema({}),
            errors=errors,
        )

    async def async_step_reauth_otp(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Validate reauth OTP code and update tokens."""
        errors: dict[str, str] = {}
        reauth_entry = self._get_reauth_entry()

        if user_input is not None:
            raw = user_input["otp"].strip()
            session = async_get_clientsession(self.hass)

            try:
                tokens = await async_validate_otp(
                    session, self._email, raw, self._brand,
                    journey_id=self._journey_id,
                )
                refresh_token = tokens.get("refreshToken") if tokens else None
                if refresh_token:
                    return self.async_update_reload_and_abort(
                        reauth_entry,
                        data={
                            **reauth_entry.data,
                            CONF_REFRESH_TOKEN: refresh_token,
                        },
                    )
                errors["base"] = "invalid_otp"
            except AuthError:
                errors["base"] = "invalid_otp"
            except (aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="reauth_otp",
            description_placeholders={"email": self._email},
            data_schema=vol.Schema(
                {
                    vol.Required("otp"): cv.string,
                }
            ),
            errors=errors,
        )

    # -- Reconfigure flow ----------------------------------------------------

    async def async_step_reconfigure(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Allow the user to change email, brand, or account."""
        errors: dict[str, str] = {}
        reconfigure_entry = self._get_reconfigure_entry()

        if user_input is not None:
            self._email = user_input[CONF_EMAIL].strip().lower()
            self._brand = user_input.get(CONF_BRAND, DEFAULT_BRAND)

            self._journey_id = str(uuid4())
            session = async_get_clientsession(self.hass)
            try:
                await async_send_otp_email(
                    session, self._email, self._brand,
                    journey_id=self._journey_id,
                )
            except AuthError as err:
                if "email_not_found" in str(err):
                    errors[CONF_EMAIL] = "email_not_found"
                else:
                    errors["base"] = "cannot_connect"
            except (aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

            if not errors:
                return await self.async_step_reconfigure_otp()

        current_email = reconfigure_entry.data.get(CONF_EMAIL, "")
        current_brand = reconfigure_entry.data.get(CONF_BRAND, DEFAULT_BRAND)
        brand_options = {k: v["name"] for k, v in BRAND_CONFIG.items()}

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_BRAND, default=current_brand,
                    ): vol.In(brand_options),
                    vol.Required(
                        CONF_EMAIL, default=current_email,
                    ): cv.string,
                }
            ),
            errors=errors,
        )

    async def async_step_reconfigure_otp(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Validate OTP and discover accounts for reconfigure."""
        errors: dict[str, str] = {}
        reconfigure_entry = self._get_reconfigure_entry()

        if user_input is not None:
            raw = user_input["otp"].strip()
            session = async_get_clientsession(self.hass)
            tokens = None

            try:
                tokens = await async_validate_otp(
                    session, self._email, raw, self._brand,
                    journey_id=self._journey_id,
                )
            except AuthError:
                errors["base"] = "invalid_otp"
            except (aiohttp.ClientError, TimeoutError):
                errors["base"] = "cannot_connect"

            if tokens:
                id_token = tokens.get("idToken")
                refresh_token = tokens.get("refreshToken")
                if not id_token or not refresh_token:
                    errors["base"] = "invalid_otp"
                else:
                    self._refresh_token = refresh_token

                    try:
                        accounts = await MeridianEnergyApi.async_discover_accounts(
                            session, id_token, self._brand,
                        )
                    except (AuthError, ApiError) as err:
                        _LOGGER.error("Account discovery failed: %s", err)
                        errors["base"] = "account_not_found"
                        accounts = []

                    if accounts:
                        if len(accounts) == 1:
                            account_number = accounts[0].get("number", "")
                            return self._update_account_entry(
                                reconfigure_entry, account_number,
                            )
                        self._accounts = accounts
                        return await self.async_step_reconfigure_select_account()

        return self.async_show_form(
            step_id="reconfigure_otp",
            description_placeholders={"email": self._email},
            data_schema=vol.Schema(
                {
                    vol.Required("otp"): cv.string,
                }
            ),
            errors=errors,
        )

    async def async_step_reconfigure_select_account(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Select account during reconfigure when multiple are found."""
        reconfigure_entry = self._get_reconfigure_entry()

        if user_input is not None:
            return self._update_account_entry(
                reconfigure_entry, user_input["account"],
            )

        account_options = {
            a.get("number", ""): a.get("number", "")
            for a in self._accounts
            if a.get("number")
        }
        return self.async_show_form(
            step_id="reconfigure_select_account",
            data_schema=vol.Schema(
                {
                    vol.Required("account"): vol.In(account_options),
                }
            ),
        )

    def _update_account_entry(
        self, entry: config_entries.ConfigEntry, account_number: str,
    ) -> ConfigFlowResult:
        """Update the existing config entry with new credentials."""
        brand_name = BRAND_CONFIG[self._brand]["name"]
        new_uid = f"{self._brand}_{self._email}_{account_number}"
        return self.async_update_reload_and_abort(
            entry,
            unique_id=new_uid,
            title=f"{brand_name} ({account_number})",
            data={
                CONF_EMAIL: self._email,
                CONF_BRAND: self._brand,
                CONF_REFRESH_TOKEN: self._refresh_token,
                CONF_ACCOUNT_NUMBER: account_number,
            },
        )