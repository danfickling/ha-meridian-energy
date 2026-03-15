"""Config flow for Meridian Energy / Powershop integration."""

from __future__ import annotations

import logging
from datetime import datetime

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
import homeassistant.helpers.config_validation as cv
from homeassistant.data_entry_flow import FlowResult

from .api import MeridianEnergyApi
from requests.exceptions import RequestException
from .const import (
    DOMAIN,
    CONF_RATE_TYPE,
    CONF_NETWORK,
    CONF_SUPPLIER,
    CONF_HISTORY_START,
    CONF_COOKIE,
    DEFAULT_RATE_TYPE,
    DEFAULT_NETWORK,
    DEFAULT_SUPPLIER,
    DEFAULT_HISTORY_START,
    SUPPLIER_CONFIG,
)
from .schedule import NETWORKS

_LOGGER = logging.getLogger(__name__)

_DATE_RE_PATTERN = r"^\d{2}/\d{2}/\d{4}$"


def _validate_history_start(value: str) -> str:
    """Return *value* unchanged if empty or valid DD/MM/YYYY, else raise."""
    value = value.strip()
    if not value:
        return value
    try:
        datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        raise vol.Invalid(
            f"Invalid date '{value}' — expected DD/MM/YYYY format (e.g. 01/06/2023)"
        )
    return value


class MeridianConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle config flow for Meridian Energy / Powershop."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Handle the initial setup step — supplier, credentials, network, history."""
        errors: dict[str, str] = {}

        if user_input is not None:
            supplier = user_input.get(CONF_SUPPLIER, DEFAULT_SUPPLIER)
            email = user_input[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]
            cookie = user_input.get(CONF_COOKIE, "")
            supplier_name = SUPPLIER_CONFIG[supplier]["name"]

            # Validate history_start format
            history_start = user_input.get(CONF_HISTORY_START, DEFAULT_HISTORY_START)
            try:
                history_start = _validate_history_start(history_start)
            except vol.Invalid:
                errors[CONF_HISTORY_START] = "invalid_date"
                history_start = None

            if not errors:
                # Unique-ID: one entry per email+supplier combination
                unique_id = f"{supplier}_{email.lower().strip()}"
                await self.async_set_unique_id(unique_id)
                self._abort_if_unique_id_configured()

                # Validate credentials before creating the entry
                api = MeridianEnergyApi(
                    email, password, supplier=supplier, cookie=cookie,
                )
                try:
                    valid = await self.hass.async_add_executor_job(
                        api.validate_credentials
                    )
                except RequestException:
                    errors["base"] = "cannot_connect"
                    valid = False

                if valid:
                    return self.async_create_entry(
                        title=supplier_name,
                        data={
                            CONF_SUPPLIER: supplier,
                            CONF_EMAIL: email,
                            CONF_PASSWORD: password,
                            CONF_NETWORK: user_input.get(CONF_NETWORK, DEFAULT_NETWORK),
                            CONF_HISTORY_START: history_start or DEFAULT_HISTORY_START,
                            CONF_COOKIE: cookie,
                        },
                    )
                if not errors:
                    errors["base"] = "invalid_auth"

        # Build dropdowns
        supplier_options = {k: v["name"] for k, v in SUPPLIER_CONFIG.items()}
        network_options = {k: v for k, v in NETWORKS.items()}

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_SUPPLIER, default=DEFAULT_SUPPLIER): vol.In(
                        supplier_options
                    ),
                    vol.Required(CONF_NETWORK, default=DEFAULT_NETWORK): vol.In(
                        network_options
                    ),
                    vol.Required(CONF_EMAIL): cv.string,
                    vol.Required(CONF_PASSWORD): cv.string,
                    vol.Optional(
                        CONF_HISTORY_START, default=DEFAULT_HISTORY_START
                    ): cv.string,
                    vol.Optional(CONF_COOKIE, default=""): cv.string,
                }
            ),
            errors=errors,
        )

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow handler."""
        return MeridianOptionsFlow()

    # -- Reauth flow -------------------------------------------------------

    async def async_step_reauth(
        self, entry_data: dict
    ) -> FlowResult:
        """Handle reauth when credentials become invalid."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict | None = None
    ) -> FlowResult:
        """Prompt user to re-enter credentials."""
        errors: dict[str, str] = {}
        reauth_entry = self._get_reauth_entry()
        current_email = reauth_entry.data.get(CONF_EMAIL, "")
        supplier = reauth_entry.data.get(CONF_SUPPLIER, DEFAULT_SUPPLIER)
        supplier_name = SUPPLIER_CONFIG[supplier]["name"]

        if user_input is not None:
            email = user_input[CONF_EMAIL]
            password = user_input[CONF_PASSWORD]
            cookie = user_input.get(CONF_COOKIE, "")

            # Validate new credentials before updating
            api = MeridianEnergyApi(
                email, password, supplier=supplier, cookie=cookie,
            )
            try:
                valid = await self.hass.async_add_executor_job(
                    api.validate_credentials
                )
            except RequestException:
                errors["base"] = "cannot_connect"
                valid = False

            if valid:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data={
                        **reauth_entry.data,
                        CONF_EMAIL: email,
                        CONF_PASSWORD: password,
                        CONF_COOKIE: cookie,
                    },
                )
            errors["base"] = "invalid_auth"

        return self.async_show_form(
            step_id="reauth_confirm",
            description_placeholders={"supplier": supplier_name},
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_EMAIL, default=current_email): cv.string,
                    vol.Required(CONF_PASSWORD): cv.string,
                    vol.Optional(CONF_COOKIE, default=""): cv.string,
                }
            ),
            errors=errors,
        )


class MeridianOptionsFlow(config_entries.OptionsFlow):
    """Handle options — two steps: supplier/network, then rate type + history."""

    def __init__(self) -> None:
        """Initialise options flow state."""
        self._options: dict = {}

    async def async_step_init(self, user_input=None):
        """Step 1: Supplier and network selection."""
        if user_input is not None:
            self._options.update(user_input)
            return await self.async_step_rates()

        current_supplier = (
            self.config_entry.options.get(CONF_SUPPLIER)
            or self.config_entry.data.get(CONF_SUPPLIER, DEFAULT_SUPPLIER)
        )
        current_network = (
            self.config_entry.options.get(CONF_NETWORK)
            or self.config_entry.data.get(CONF_NETWORK, DEFAULT_NETWORK)
        )

        supplier_options = {k: v["name"] for k, v in SUPPLIER_CONFIG.items()}
        network_options = {k: v for k, v in NETWORKS.items()}

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SUPPLIER, default=current_supplier
                    ): vol.In(supplier_options),
                    vol.Required(
                        CONF_NETWORK, default=current_network
                    ): vol.In(network_options),
                }
            ),
        )

    async def async_step_rates(self, user_input=None):
        """Step 2: Rate type, history start, and cookie auth."""
        errors: dict[str, str] = {}

        current_rate_type = self.config_entry.options.get(
            CONF_RATE_TYPE, DEFAULT_RATE_TYPE
        )
        current_history_start = (
            self.config_entry.options.get(CONF_HISTORY_START)
            or self.config_entry.data.get(CONF_HISTORY_START, DEFAULT_HISTORY_START)
        )
        current_cookie = (
            self.config_entry.options.get(CONF_COOKIE)
            or self.config_entry.data.get(CONF_COOKIE, "")
        )

        if user_input is not None:
            # Validate history_start format
            raw_date = user_input.get(CONF_HISTORY_START, "")
            try:
                _validate_history_start(raw_date)
            except vol.Invalid:
                errors[CONF_HISTORY_START] = "invalid_date"

            if not errors:
                self._options.update(user_input)
                return self.async_create_entry(data=self._options)

            # Preserve user's input as defaults when re-showing with errors
            current_rate_type = user_input.get(CONF_RATE_TYPE, current_rate_type)
            current_history_start = user_input.get(CONF_HISTORY_START, current_history_start)
            current_cookie = user_input.get(CONF_COOKIE, current_cookie)

        fields: dict = {}
        fields[vol.Required(CONF_RATE_TYPE, default=current_rate_type)] = vol.In(
            {
                "special": "Special (discount rates)",
                "base": "Base (standard rates)",
            }
        )
        fields[vol.Optional(CONF_HISTORY_START, default=current_history_start)] = (
            cv.string
        )
        fields[vol.Optional(CONF_COOKIE, default=current_cookie)] = cv.string

        return self.async_show_form(
            step_id="rates",
            data_schema=vol.Schema(fields),
            errors=errors,
        )
