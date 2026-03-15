"""Meridian Energy / Powershop API client."""

from __future__ import annotations

import logging
import re
import requests
from requests.exceptions import RequestException

from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from .const import (
    SUPPLIER_CONFIG,
    SUPPLIER_POWERSHOP,
    SUPPLIER_MERIDIAN,
    DEFAULT_SUPPLIER,
    CONF_COOKIE,
    HTTP_TIMEOUT,
    HTTP_TIMEOUT_COOKIE_CHECK,
    DEFAULT_LOOKBACK_DAYS,
)

_LOGGER = logging.getLogger(__name__)


class MeridianEnergyApi:
    """API client for Meridian Energy and Powershop portals."""

    def __init__(self, email, password, supplier=DEFAULT_SUPPLIER, history_start="", cookie=""):
        self._email = email
        self._password = password
        self._supplier = supplier
        self._config = SUPPLIER_CONFIG[supplier]
        self._history_start = history_start  # DD/MM/YYYY or empty for rolling
        self._cookie = cookie  # Optional browser cookie for auth fallback
        self._url_base = self._config["base_url"]
        self._token = None
        self._data = None
        self._session = requests.Session()
        self._logged_in = False

    @property
    def session(self):
        return self._session

    @property
    def logged_in(self) -> bool:
        return self._logged_in

    @property
    def supplier(self) -> str:
        return self._supplier

    @supplier.setter
    def supplier(self, value: str) -> None:
        self._supplier = value
        self._config = SUPPLIER_CONFIG[value]
        self._url_base = self._config["base_url"]
        self._logged_in = False

    @property
    def history_start(self) -> str:
        """Return the configured history start date (DD/MM/YYYY or empty)."""
        return self._history_start

    @history_start.setter
    def history_start(self, value: str) -> None:
        """Update the history start date."""
        self._history_start = value

    @property
    def supplier_name(self) -> str:
        """Return the display name for the configured supplier."""
        return self._config["name"]

    @property
    def cookie(self) -> str:
        """Return the configured cookie string."""
        return self._cookie

    @cookie.setter
    def cookie(self, value: str) -> None:
        """Update the cookie string."""
        self._cookie = value

    def token(self):
        """Get CSRF token from the login page.

        If a browser cookie is configured, try cookie-based auth first.
        Falls back to email/password login if cookie auth fails.
        """
        # Create a fresh session for each update cycle to avoid stale cookies
        if self._session:
            self._session.close()
        self._session = requests.Session()
        self._logged_in = False

        # Try cookie auth first (bypasses captcha/2FA)
        if self._cookie:
            if self._try_cookie_auth():
                return

            _LOGGER.warning(
                "Cookie auth failed for %s — falling back to email/password",
                self._config["name"],
            )

        response = self._session.get(self._url_base, timeout=HTTP_TIMEOUT)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            token_el = soup.find("input", {"name": "authenticity_token"})
            if token_el:
                self._token = token_el["value"]
                self.login()
            else:
                _LOGGER.error("CSRF token not found on %s login page", self._config["name"])
        else:
            _LOGGER.error(
                "Failed to retrieve the %s token page (status %s)",
                self._config["name"],
                response.status_code,
            )

    def login(self):
        """Login to the supplier portal.

        Powershop login form action is ``/`` (the site root).
        Meridian login form posts to ``/customer/login``.
        """
        result = False
        form_data = {
            "authenticity_token": self._token,
            "email": self._email,
            "password": self._password,
            "commit": "Login",
        }

        login_url = self._url_base + self._config["login_path"]

        login_result = self._session.post(
            login_url,
            data=form_data,
            allow_redirects=True,
            timeout=HTTP_TIMEOUT,
        )

        if login_result.status_code == 200:
            # Check for error messages (account locked, bad password, etc.)
            fail_text = self._config["login_fail_text"]
            if fail_text in login_result.text[:1000]:
                soup = BeautifulSoup(login_result.text, "html.parser")
                msg_el = soup.find(class_="message")
                msg = msg_el.get_text(strip=True) if msg_el else "unknown error"
                _LOGGER.error("%s login failed: %s", self._config["name"], msg)
            else:
                _LOGGER.debug("Logged in to %s successfully", self._config["name"])
                self._logged_in = True
                result = True
        else:
            _LOGGER.error(
                "%s login POST returned status %s",
                self._config["name"],
                login_result.status_code,
            )

        return result

    def _try_cookie_auth(self) -> bool:
        """Authenticate using a browser cookie header.

        The cookie string should be the value of the ``Cookie`` header
        copied from a browser session (e.g. from DevTools > Network tab).
        This bypasses captcha/2FA challenges that may block normal login.

        Returns ``True`` if the cookie session is valid.
        """
        try:
            # Pass cookie in the request (not on session) to avoid leaking
            # it into subsequent requests if auth fails mid-flight.
            cookie_header = {"Cookie": self._cookie.strip()}

            # Verify session is valid by requesting the dashboard/home page
            resp = self._session.get(
                self._url_base, headers=cookie_header,
                allow_redirects=False, timeout=HTTP_TIMEOUT_COOKIE_CHECK,
            )

            # If we get redirected to login, the cookie is invalid
            if resp.status_code in (301, 302):
                location = resp.headers.get("Location", "")
                if "login" in location.lower():
                    _LOGGER.debug("Cookie auth: redirected to login — cookie expired")
                    self._session.close()
                    self._session = requests.Session()
                    return False

            if resp.status_code == 200:
                # Check if the page contains authenticated content
                # (not a login form)
                fail_text = self._config["login_fail_text"]
                if fail_text in resp.text[:1000]:
                    _LOGGER.debug("Cookie auth: got login page — cookie expired")
                    self._session.close()
                    self._session = requests.Session()
                    return False

                # Success — persist cookie on session for subsequent requests
                self._session.headers["Cookie"] = self._cookie.strip()
                _LOGGER.info("Cookie auth successful for %s", self._config["name"])
                self._logged_in = True
                return True

            _LOGGER.debug("Cookie auth: unexpected status %s", resp.status_code)
            self._session.close()
            self._session = requests.Session()
            return False

        except RequestException as exc:
            _LOGGER.debug("Cookie auth network error: %s", exc)
            self._session.close()
            self._session = requests.Session()
            return False
        except (ValueError, KeyError, AttributeError) as exc:
            _LOGGER.debug("Cookie auth parse error: %s", exc)
            self._session.close()
            self._session = requests.Session()
            return False

    def get_data(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
    ):
        """Get consumption data from the API.

        Downloads the EIEP 13A CSV for the given date range.
        The CSV endpoint is the same for both Meridian and Powershop.
        Defaults: ``date_from=history_start or rolling 365 days``,
        ``date_to=today``.
        Date format: ``DD/MM/YYYY``.

        Returns the CSV text on success, or ``None`` on failure.
        """
        if not self._logged_in:
            _LOGGER.warning("Not logged in to %s — skipping data fetch", self._config["name"])
            return None

        if date_from is None:
            if self._history_start:
                date_from = self._history_start
            else:
                date_from = (datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%d/%m/%Y")
        if date_to is None:
            date_to = datetime.now().strftime("%d/%m/%Y")

        url = (
            self._url_base
            + "reports/consumption_data/detailed_export?date_from="
            + date_from
            + "&date_to="
            + date_to
            + "&all_icps=&download=true"
        )

        response = self._session.get(url, timeout=HTTP_TIMEOUT)

        if response.status_code != 200:
            _LOGGER.error(
                "Could not fetch consumption data (status %s)", response.status_code
            )
            return None

        data = response.text
        if not data:
            _LOGGER.warning("Fetched consumption successfully but response was empty")
            return None

        # Sanity-check: the first line of a valid CSV starts with "HDR"
        if not data.lstrip().startswith("HDR"):
            _LOGGER.error(
                "Consumption response is not CSV (starts with %r) — "
                "session may have expired",
                data[:80],
            )
            return None

        return data

    def validate_credentials(self) -> bool:
        """Attempt login and return True if credentials are valid.

        Creates a fresh session, attempts authentication, and returns
        whether the login succeeded.  Used by the config flow to validate
        credentials before creating/updating config entries.
        """
        try:
            self.token()
            return self._logged_in
        except RequestException as exc:
            _LOGGER.debug("Credential validation network error: %s", exc)
            return False
        except (ValueError, KeyError, AttributeError) as exc:
            _LOGGER.debug("Credential validation parse error: %s", exc)
            return False

    def get_balance(self) -> dict[str, float | None] | None:
        """Fetch account balance info from the portal.

        Both Powershop and Meridian redirect to a balance/dashboard page
        after login.  Powershop shows:
        - "You're about $NNN (NN days) ahead" — the account credit
        - "You also have $N,NNN (NN weeks) in pre-purchased Future Packs."
        - "You're currently using about $NN.NN per day"

        Returns a dict with keys ``ahead``, ``future_packs``,
        ``daily_cost`` (all float or None), or ``None`` on failure.
        """
        if not self._logged_in:
            return None

        try:
            resp = self._session.get(self._url_base, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                _LOGGER.debug("Balance page returned status %s", resp.status_code)
                return None

            # Check if we're actually logged in
            fail_text = self._config["login_fail_text"]
            if fail_text in resp.text[:1000]:
                _LOGGER.debug("Balance page: session expired (got login page)")
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            body_text = soup.get_text(separator=" ", strip=True) if soup.body else ""

            result: dict[str, float | None] = {
                "ahead": None,
                "future_packs": None,
                "daily_cost": None,
            }

            # Pattern for dollar amounts (with or without decimals/commas)
            dollar_re = re.compile(r"\$\s*([\d,]+(?:\.\d{1,2})?)")

            # "You're about $498 (48 days) ahead"
            ahead_re = re.compile(
                r"\$\s*([\d,]+(?:\.\d{1,2})?)\s*\([^)]*\)\s*ahead",
                re.IGNORECASE,
            )
            m = ahead_re.search(body_text)
            if m:
                result["ahead"] = float(m.group(1).replace(",", ""))
                _LOGGER.debug("Balance ahead: $%.2f", result["ahead"])

            # "You also have $1,049 (10 weeks) in pre-purchased Future Packs"
            packs_re = re.compile(
                r"\$\s*([\d,]+(?:\.\d{1,2})?)\s*\([^)]*\)\s*in\s+pre-purchased",
                re.IGNORECASE,
            )
            m = packs_re.search(body_text)
            if m:
                result["future_packs"] = float(m.group(1).replace(",", ""))
                _LOGGER.debug("Future packs: $%.2f", result["future_packs"])

            # "You're currently using about $10.30 per day"
            daily_re = re.compile(
                r"\$\s*([\d,]+\.\d{2})\s*per\s+day",
                re.IGNORECASE,
            )
            m = daily_re.search(body_text)
            if m:
                result["daily_cost"] = float(m.group(1).replace(",", ""))
                _LOGGER.debug("Daily cost: $%.2f", result["daily_cost"])

            if any(v is not None for v in result.values()):
                return result

            _LOGGER.warning(
                "Could not find balance info on page (text preview: %.200s)",
                body_text[:200],
            )
            return None

        except RequestException as exc:
            _LOGGER.debug("Balance fetch network error: %s", exc)
            return None
        except (ValueError, KeyError, AttributeError) as exc:
            _LOGGER.debug("Balance parse error: %s", exc)
            return None
