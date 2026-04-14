"""Kraken GraphQL API client for Meridian Energy / Powershop (v2).

Authenticates via Firebase (email OTP / magic-link), then queries
the per-brand Kraken GraphQL endpoint for account data, consumption
measurements, rates, TOU schedules, and ledger balances.
"""

from __future__ import annotations

import logging
from datetime import datetime

import aiohttp

from .const import (
    BRAND_CONFIG,
    DEFAULT_BRAND,
    FIREBASE_API_KEY,
)

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Firebase REST helpers
# ---------------------------------------------------------------------------
_FIREBASE_TOKEN_URL = (
    "https://securetoken.googleapis.com/v1/token?key={key}"
)
_FIREBASE_SIGNIN_CUSTOM_TOKEN_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={key}"
)

# Cloudflare blocks non-browser User-Agents (error 1010).
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)


class AuthError(Exception):
    """Raised when authentication fails (invalid credentials / token)."""


class ApiError(Exception):
    """Raised on non-auth GraphQL or HTTP errors."""


# ---------------------------------------------------------------------------
# Auth helpers (static — used by config flow before an Api instance exists)
# ---------------------------------------------------------------------------

async def async_send_otp_email(
    session: aiohttp.ClientSession,
    email: str,
    brand: str = DEFAULT_BRAND,
    *,
    journey_id: str | None = None,
) -> None:
    """Trigger an OTP / magic-link email via the brand's auth endpoint.

    Raises ``AuthError`` on failure (e.g. user not found).
    """
    cfg = BRAND_CONFIG[brand]
    url = f"https://{cfg['auth_domain']}/cf/email-connector"
    payload: dict[str, object] = {
        "brand": brand,
        "email": email,
        "redirectUrl": f"{cfg['app_origin']}/login",
        "otpEnabled": True,
    }
    if journey_id:
        payload["journeyId"] = journey_id
    headers = {
        "Content-Type": "application/json",
        "X-Client-Platform": "web",
    }
    async with session.post(url, json=payload, headers=headers) as resp:
        if resp.status == 404:
            raise AuthError("email_not_found")
        if resp.status != 200:
            body = await resp.text()
            raise AuthError(f"send_otp_failed ({resp.status}): {body[:200]}")


async def async_validate_otp(
    session: aiohttp.ClientSession,
    email: str,
    otp: str,
    brand: str = DEFAULT_BRAND,
    *,
    journey_id: str | None = None,
) -> dict:
    """Validate an OTP code and return Firebase tokens.

    Returns ``{"idToken": ..., "refreshToken": ..., "expiresIn": ...}``.
    Raises ``AuthError`` on invalid OTP.
    """
    cfg = BRAND_CONFIG[brand]
    url = f"https://{cfg['auth_domain']}/cf/email-otp-authenticator"
    payload: dict[str, str] = {"email": email, "otp": otp, "brand": brand}
    if journey_id:
        payload["journeyId"] = journey_id
    headers = {
        "Content-Type": "application/json",
        "X-Client-Platform": "web",
    }

    async with session.post(url, json=payload, headers=headers) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise AuthError(f"otp_invalid ({resp.status}): {body[:200]}")
        try:
            data = await resp.json()
        except (ValueError, aiohttp.ContentTypeError) as exc:
            body = await resp.text()
            raise AuthError(
                f"Invalid JSON in OTP response: {body[:200]}"
            ) from exc
        custom_token = data.get("customToken")
        if not custom_token:
            raise AuthError("otp_response_missing_token")

    # Exchange custom token for Firebase ID + refresh tokens
    url = _FIREBASE_SIGNIN_CUSTOM_TOKEN_URL.format(key=FIREBASE_API_KEY)
    async with session.post(url, json={"token": custom_token, "returnSecureToken": True}) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise AuthError(f"firebase_custom_token_failed ({resp.status}): {body[:200]}")
        try:
            return await resp.json()
        except (ValueError, aiohttp.ContentTypeError) as exc:
            body = await resp.text()
            raise AuthError(
                f"Invalid JSON in Firebase response: {body[:200]}"
            ) from exc


async def async_refresh_token(
    session: aiohttp.ClientSession,
    refresh_token: str,
) -> dict:
    """Refresh a Firebase ID token.

    Returns ``{"id_token": ..., "refresh_token": ..., "expires_in": ...}``.
    Raises ``AuthError`` if the refresh token is revoked or invalid.
    """
    url = _FIREBASE_TOKEN_URL.format(key=FIREBASE_API_KEY)
    async with session.post(
        url, json={"grant_type": "refresh_token", "refresh_token": refresh_token}
    ) as resp:
        if resp.status != 200:
            body = await resp.text()
            raise AuthError(f"token_refresh_failed ({resp.status}): {body[:200]}")
        try:
            return await resp.json()
        except (ValueError, aiohttp.ContentTypeError) as exc:
            body = await resp.text()
            raise AuthError(
                f"Invalid JSON in token refresh response: {body[:200]}"
            ) from exc


# ---------------------------------------------------------------------------
# GraphQL API client
# ---------------------------------------------------------------------------

class MeridianEnergyApi:
    """Async GraphQL client for the Kraken API (v2)."""

    def __init__(
        self,
        brand: str,
        refresh_token: str,
        account_number: str,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._brand = brand
        self._config = BRAND_CONFIG[brand]
        self._refresh_token = refresh_token
        self._account_number = account_number
        self._id_token: str | None = None
        self._owns_session = session is None
        self._session = session or aiohttp.ClientSession()

    @property
    def brand(self) -> str:
        return self._brand

    @property
    def account_number(self) -> str:
        return self._account_number

    @property
    def refresh_token(self) -> str:
        return self._refresh_token

    async def async_close(self) -> None:
        """Close the HTTP session if we own it."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    # -- Token management ---------------------------------------------------

    async def async_ensure_token(self) -> None:
        """Ensure we have a valid Firebase ID token (refresh if needed)."""
        if self._id_token:
            return  # Assume valid; caller retries on 401
        await self._async_refresh()

    async def _async_refresh(self) -> None:
        """Refresh the Firebase ID token."""
        data = await async_refresh_token(self._session, self._refresh_token)
        self._id_token = data["id_token"]
        new_refresh = data.get("refresh_token")
        if new_refresh:
            self._refresh_token = new_refresh

    def invalidate_token(self) -> None:
        """Mark the current ID token as expired so the next call refreshes."""
        self._id_token = None

    def _headers(self) -> dict[str, str]:
        """Return headers for a GraphQL request."""
        return {
            "Content-Type": "application/json",
            "User-Agent": _BROWSER_UA,
            "Authorization": self._id_token or "",
            "Origin": self._config["app_origin"],
            "Referer": f"{self._config['app_origin']}/",
        }

    # -- GraphQL transport --------------------------------------------------

    async def _async_graphql(
        self, query: str, variables: dict | None = None, *, retry_auth: bool = True,
    ) -> dict:
        """Execute a GraphQL query, refreshing the token on 401.

        Returns the ``data`` dict from the response.
        Raises ``AuthError`` on auth failures, ``ApiError`` on other errors.
        """
        await self.async_ensure_token()

        url = self._config["api_url"]
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables

        async with self._session.post(
            url, json=payload, headers=self._headers()
        ) as resp:
            if resp.status in (401, 403):
                if retry_auth:
                    self._id_token = None
                    await self._async_refresh()
                    return await self._async_graphql(
                        query, variables, retry_auth=False,
                    )
                raise AuthError("auth_expired")

            if resp.status != 200:
                body = await resp.text()
                raise ApiError(f"HTTP {resp.status}: {body[:300]}")

            try:
                result = await resp.json()
            except (ValueError, aiohttp.ContentTypeError) as exc:
                body = await resp.text()
                raise ApiError(
                    f"Invalid JSON response: {body[:300]}"
                ) from exc

        errors = result.get("errors")
        if errors:
            codes = [e.get("extensions", {}).get("errorCode", "") for e in errors]
            msgs = [e.get("message", "") for e in errors]
            is_auth = (
                any("KT-CT-1139" in c or "KT-CT-1111" in c for c in codes)
                or any("jwt" in m.lower() and "expired" in m.lower() for m in msgs)
            )
            if is_auth:
                if retry_auth:
                    self._id_token = None
                    await self._async_refresh()
                    return await self._async_graphql(
                        query, variables, retry_auth=False,
                    )
                raise AuthError(f"auth_error: {msgs}")
            raise ApiError(f"GraphQL errors: {msgs}")

        return result.get("data", {})

    # -- High-level queries -------------------------------------------------

    async def async_get_account(self) -> dict:
        """Fetch account details, properties, meter points, and ledgers."""
        data = await self._async_graphql(
            _Q_ACCOUNT, {"acct": self._account_number},
        )
        return data.get("account", {})

    async def async_get_rates_and_tou(self) -> dict:
        """Fetch rates and TOU schedule from the active agreement.

        Returns ``{"rates": [...], "tou_schemes": [...], "product": "..."}``.
        """
        data = await self._async_graphql(
            _Q_RATES_TOU, {"acct": self._account_number},
        )
        account = data.get("account", {})
        result: dict = {"rates": [], "tou_schemes": [], "product": ""}
        for prop in account.get("properties") or []:
            for mp in prop.get("meterPoints") or []:
                agreement = mp.get("activeAgreement") or {}
                result["rates"] = agreement.get("rates") or []
                result["tou_schemes"] = agreement.get("timeOfUseSchemes") or []
                product = agreement.get("product") or {}
                result["product"] = (
                    product.get("fullName") or product.get("code") or ""
                    if isinstance(product, dict) else str(product)
                )
                if result["rates"]:
                    return result
        return result

    async def async_get_measurements(
        self,
        start: datetime,
        end: datetime,
        frequency: str = "DAY_INTERVAL",
        direction: str = "CONSUMPTION",
        first: int = 500,
    ) -> list[dict]:
        """Fetch consumption/generation measurements.

        ``frequency``: DAY_INTERVAL, THIRTY_MIN_INTERVAL, HOUR_INTERVAL, etc.
        ``direction``: CONSUMPTION or GENERATION.

        Returns a flat list of IntervalMeasurementType dicts.
        """
        all_nodes: list[dict] = []
        cursor: str | None = None

        _MAX_PAGES = 100

        while True:
            variables: dict = {
                "acct": self._account_number,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "first": first,
            }
            query = _Q_MEASUREMENTS.format(
                frequency=frequency, direction=direction,
            )
            if cursor:
                variables["after"] = cursor

            data = await self._async_graphql(query, variables)
            account = data.get("account", {})
            has_next = False
            for prop in account.get("properties") or []:
                measurements = prop.get("measurements") or {}
                for edge in measurements.get("edges") or []:
                    node = edge.get("node") or {}
                    if node.get("value") is not None:
                        all_nodes.append(node)
                page_info = measurements.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    end_cursor = page_info.get("endCursor")
                    if not end_cursor:
                        _LOGGER.warning(
                            "API returned hasNextPage=true but no endCursor; "
                            "stopping pagination after %d nodes",
                            len(all_nodes),
                        )
                        break
                    cursor = end_cursor
                    has_next = True

            _MAX_PAGES -= 1
            if not has_next or _MAX_PAGES <= 0:
                break

        return all_nodes

    async def async_get_daily_cost_measurements(
        self,
        start: datetime,
        end: datetime,
        first: int = 100,
    ) -> list[dict]:
        """Fetch daily measurements with cost statistics."""
        all_nodes: list[dict] = []
        cursor: str | None = None
        _MAX_PAGES = 100

        while True:
            variables: dict = {
                "acct": self._account_number,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "first": first,
            }
            if cursor:
                variables["after"] = cursor

            data = await self._async_graphql(_Q_DAILY_COSTS, variables)
            account = data.get("account", {})
            has_next = False
            for prop in account.get("properties") or []:
                measurements = prop.get("measurements") or {}
                for edge in measurements.get("edges") or []:
                    node = edge.get("node") or {}
                    if node:
                        all_nodes.append(node)
                page_info = measurements.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    end_cursor = page_info.get("endCursor")
                    if not end_cursor:
                        _LOGGER.warning(
                            "API returned hasNextPage=true but no endCursor; "
                            "stopping pagination after %d nodes",
                            len(all_nodes),
                        )
                        break
                    cursor = end_cursor
                    has_next = True

            _MAX_PAGES -= 1
            if not has_next or _MAX_PAGES <= 0:
                break

        return all_nodes

    async def async_get_ledger_balances(self) -> dict:
        """Fetch ledger balances.

        Returns ``{"electricity": <cents>, "powerpacks": <cents>}``.
        """
        data = await self._async_graphql(
            _Q_LEDGERS, {"acct": self._account_number},
        )
        account = data.get("account", {})
        result = {"electricity": 0, "powerpacks": 0}
        for ledger in account.get("ledgers") or []:
            ltype = (ledger.get("ledgerType") or "").upper()
            balance = ledger.get("balance", 0)
            if "ELECTRICITY" in ltype:
                result["electricity"] = balance
            elif "POWERPACK" in ltype:
                result["powerpacks"] = balance
        return result

    async def async_get_generation_total(
        self,
        start: datetime,
        end: datetime,
    ) -> float:
        """Fetch total solar export in the date range (kWh)."""
        nodes = await self.async_get_measurements(
            start, end, frequency="DAY_INTERVAL", direction="GENERATION",
        )
        return sum(float(n.get("value", 0)) for n in nodes)

    async def async_get_billing_info(self) -> dict:
        """Fetch billing period dates.

        Returns ``{"period_start": "YYYY-MM-DD", "period_end": "YYYY-MM-DD",
                    "next_billing_date": "YYYY-MM-DD"}``
        with ``None`` values for any missing fields.
        """
        data = await self._async_graphql(
            _Q_BILLING, {"acct": self._account_number},
        )
        opts = data.get("account", {}).get("billingOptions") or {}
        return {
            "period_start": opts.get("currentBillingPeriodStartDate"),
            "period_end": opts.get("currentBillingPeriodEndDate"),
            "next_billing_date": opts.get("nextBillingDate"),
        }

    # -- Static helpers for config flow / account discovery -----------------

    @staticmethod
    async def async_discover_accounts(
        session: aiohttp.ClientSession,
        id_token: str,
        brand: str = DEFAULT_BRAND,
    ) -> list[dict]:
        """Query the viewer endpoint to discover all accounts.

        Returns a list of AccountType dicts (number, brand, status, properties).
        Raises ``AuthError`` or ``ApiError`` on failure.
        """
        cfg = BRAND_CONFIG[brand]
        headers = {
            "Content-Type": "application/json",
            "User-Agent": _BROWSER_UA,
            "Authorization": id_token,
            "Origin": cfg["app_origin"],
            "Referer": f"{cfg['app_origin']}/",
        }
        async with session.post(
            cfg["api_url"],
            json={"query": _Q_DISCOVER_ACCOUNT},
            headers=headers,
        ) as resp:
            if resp.status in (401, 403):
                raise AuthError("auth_invalid")
            if resp.status != 200:
                body = await resp.text()
                raise ApiError(f"HTTP {resp.status}: {body[:300]}")
            try:
                result = await resp.json()
            except (ValueError, aiohttp.ContentTypeError) as exc:
                body = await resp.text()
                raise ApiError(
                    f"Invalid JSON response: {body[:300]}"
                ) from exc

        errors = result.get("errors")
        if errors:
            raise ApiError(f"GraphQL errors: {[e.get('message') for e in errors]}")

        accounts = (
            result.get("data", {}).get("viewer", {}).get("accounts") or []
        )
        if not accounts:
            raise ApiError("no_accounts_found")
        return accounts


# ---------------------------------------------------------------------------
# GraphQL query strings
# ---------------------------------------------------------------------------

_Q_DISCOVER_ACCOUNT = """
{
  viewer {
    accounts {
      ... on AccountType {
        number brand status
        properties {
          id address
          meterPoints { id marketIdentifier }
        }
      }
    }
  }
}
"""

_Q_ACCOUNT = """
query($acct: String!) {
  account(accountNumber: $acct) {
    ... on AccountType {
      number brand status balance
      properties {
        id address
        meterPoints {
          id marketIdentifier
          activeAgreement {
            id validFrom validTo
            product { code fullName }
            rates {
              touBucketName label unitType bandCategory
              rateIncludingTax rateExcludingTax
            }
            timeOfUseSchemes {
              name
              timeslots {
                timeslot activeFrom activeTo
                weekdays weekends
              }
            }
          }
          registers {
            identifier meterSerial isFeedIn
          }
        }
      }
      ledgers { name ledgerType balance }
    }
  }
}
"""

_Q_RATES_TOU = """
query($acct: String!) {
  account(accountNumber: $acct) {
    ... on AccountType {
      properties {
        meterPoints {
          activeAgreement {
            product { code fullName }
            rates {
              touBucketName label unitType bandCategory
              rateIncludingTax rateExcludingTax
            }
            timeOfUseSchemes {
              name
              timeslots {
                timeslot activeFrom activeTo
                weekdays weekends
              }
            }
          }
        }
      }
    }
  }
}
"""

_Q_MEASUREMENTS = """
query($acct: String!, $start: DateTime!, $end: DateTime!, $first: Int!, $after: String) {{
  account(accountNumber: $acct) {{
    ... on AccountType {{
      properties {{
        measurements(
          startAt: $start
          endAt: $end
          timezone: "Pacific/Auckland"
          utilityFilters: [{{ electricityFilters: {{
            readingDirection: {direction}
            readingFrequencyType: {frequency}
          }}}}]
          first: $first
          after: $after
        ) {{
          totalCount
          pageInfo {{ hasNextPage endCursor }}
          edges {{ node {{ ... on IntervalMeasurementType {{
            value unit startAt endAt
            metaData {{ statistics {{ label value costInclTax {{ estimatedAmount }} }} }}
          }}}}}}
        }}
      }}
    }}
  }}
}}
"""

_Q_DAILY_COSTS = """
query($acct: String!, $start: DateTime!, $end: DateTime!, $first: Int!, $after: String) {
  account(accountNumber: $acct) {
    ... on AccountType {
      properties {
        measurements(
          startAt: $start
          endAt: $end
          timezone: "Pacific/Auckland"
          utilityFilters: [{ electricityFilters: {
            readingDirection: CONSUMPTION
            readingFrequencyType: DAY_INTERVAL
          }}]
          first: $first
          after: $after
        ) {
          totalCount
          pageInfo { hasNextPage endCursor }
          edges { node { ... on IntervalMeasurementType {
            value unit startAt endAt
            metaData {
              statistics {
                label
                value
                costInclTax { estimatedAmount }
              }
            }
          }}}
        }
      }
    }
  }
}
"""

_Q_LEDGERS = """
query($acct: String!) {
  account(accountNumber: $acct) {
    ... on AccountType {
      ledgers { name ledgerType balance }
    }
  }
}
"""

_Q_BILLING = """
query($acct: String!) {
  account(accountNumber: $acct) {
    ... on AccountType {
      billingOptions {
        currentBillingPeriodStartDate
        currentBillingPeriodEndDate
        nextBillingDate
      }
    }
  }
}
"""
