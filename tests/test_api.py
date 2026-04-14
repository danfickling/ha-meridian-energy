"""Tests for the v2 API module (Firebase auth + GraphQL client)."""

import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch

from meridian_energy.api import (
    MeridianEnergyApi,
    AuthError,
    ApiError,
    async_send_otp_email,
    async_validate_otp,
)
from meridian_energy.const import BRAND_CONFIG, FIREBASE_API_KEY


class TestApiExceptions:
    def test_auth_error_is_exception(self):
        assert issubclass(AuthError, Exception)

    def test_api_error_is_exception(self):
        assert issubclass(ApiError, Exception)

    def test_auth_error_message(self):
        e = AuthError("token_expired")
        assert str(e) == "token_expired"

    def test_api_error_message(self):
        e = ApiError("HTTP 500: Internal error")
        assert "500" in str(e)


class TestBrandConfig:
    def test_powershop_config_exists(self):
        assert "powershop" in BRAND_CONFIG

    def test_meridian_config_exists(self):
        assert "meridian" in BRAND_CONFIG

    def test_powershop_api_url(self):
        assert "api.powershop.nz" in BRAND_CONFIG["powershop"]["api_url"]

    def test_meridian_api_url(self):
        assert "api.meridianenergy.nz" in BRAND_CONFIG["meridian"]["api_url"]

    def test_powershop_auth_domain(self):
        assert "auth.powershop.nz" in BRAND_CONFIG["powershop"]["auth_domain"]

    def test_meridian_auth_domain(self):
        assert "auth.meridianenergy.nz" in BRAND_CONFIG["meridian"]["auth_domain"]

    def test_firebase_api_key_present(self):
        assert FIREBASE_API_KEY.startswith("AIza")


class TestApiConstruction:
    def test_brand_stored(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        assert api.brand == "powershop"

    def test_account_number_stored(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        assert api.account_number == "A-123"

    def test_refresh_token_stored(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        assert api.refresh_token == "tok"

    def test_meridian_brand(self):
        session = MagicMock()
        api = MeridianEnergyApi("meridian", "tok", "A-123", session)
        assert api.brand == "meridian"

    def test_config_uses_brand(self):
        session = MagicMock()
        api = MeridianEnergyApi("meridian", "tok", "A-123", session)
        assert api._config == BRAND_CONFIG["meridian"]


class TestApiHeaders:
    def test_headers_include_user_agent(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert "User-Agent" in headers

    def test_headers_include_authorization(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert headers["Authorization"] == "test_token"

    def test_headers_include_origin(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert "Origin" in headers
        assert "powershop" in headers["Origin"]

    def test_headers_include_referer(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert "Referer" in headers

    def test_headers_content_type(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert headers["Content-Type"] == "application/json"

    def test_headers_meridian_origin(self):
        session = MagicMock()
        api = MeridianEnergyApi("meridian", "tok", "A-123", session)
        api._id_token = "test_token"
        headers = api._headers()
        assert "meridianenergy" in headers["Origin"]

    def test_headers_empty_token_uses_empty_string(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        headers = api._headers()
        assert headers["Authorization"] == ""


class TestTokenManagement:
    def test_invalidate_token(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "old_token"
        api.invalidate_token()
        assert api._id_token is None

    def test_no_token_initially(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        assert api._id_token is None

    def test_jwt_expired_message_triggers_refresh(self):
        """GraphQL 'Signature of the JWT has expired' should retry with new token."""
        call_count = 0

        def fake_post(url, *, json=None, headers=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: JWT expired error from Kraken
                return _FakeResponse(200, json_data={
                    "errors": [{"message": "Signature of the JWT has expired."}],
                })
            if call_count == 2:
                # Refresh token call
                return _FakeResponse(200, json_data={
                    "id_token": "new_token",
                    "refresh_token": "new_refresh",
                    "expires_in": "3600",
                })
            # Retry: success
            return _FakeResponse(200, json_data={
                "data": {"result": "ok"},
            })

        session = MagicMock()
        session.post = fake_post

        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "expired_token"

        result = asyncio.get_event_loop().run_until_complete(
            api._async_graphql("query { test }")
        )
        assert result == {"result": "ok"}
        assert api._id_token == "new_token"
        assert call_count == 3


class TestPaginationLogic:
    """Verify pagination uses has_next flag and max-pages guard."""

    def test_measurements_single_page(self):
        """Single page with hasNextPage=False returns nodes."""
        import asyncio
        from datetime import datetime

        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        graphql_response = {
            "account": {
                "properties": [{
                    "measurements": {
                        "edges": [
                            {"node": {"value": "1.5", "startAt": "2026-01-01T00:00:00+13:00"}},
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }]
            }
        }

        async def mock_graphql(*a, **kw):
            return graphql_response

        api._async_graphql = mock_graphql
        nodes = asyncio.get_event_loop().run_until_complete(
            api.async_get_measurements(datetime(2026, 1, 1), datetime(2026, 1, 2))
        )
        assert len(nodes) == 1
        assert nodes[0]["value"] == "1.5"

    def test_daily_costs_multi_page(self):
        """Multi-page response fetches all pages."""
        import asyncio
        from datetime import datetime

        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        call_count = 0

        async def mock_graphql(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "account": {"properties": [{
                        "measurements": {
                            "edges": [{"node": {"startAt": "2026-01-01", "value": "10"}}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cur1"},
                        }
                    }]}
                }
            return {
                "account": {"properties": [{
                    "measurements": {
                        "edges": [{"node": {"startAt": "2026-01-02", "value": "11"}}],
                        "pageInfo": {"hasNextPage": False},
                    }
                }]}
            }

        api._async_graphql = mock_graphql
        nodes = asyncio.get_event_loop().run_until_complete(
            api.async_get_daily_cost_measurements(datetime(2026, 1, 1), datetime(2026, 1, 3))
        )
        assert len(nodes) == 2
        assert call_count == 2

    def test_daily_costs_max_pages_guard(self):
        """Pagination stops after max pages even if hasNextPage stays True."""
        import asyncio
        from datetime import datetime

        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        call_count = 0

        async def mock_graphql(*a, **kw):
            nonlocal call_count
            call_count += 1
            return {
                "account": {"properties": [{
                    "measurements": {
                        "edges": [{"node": {"startAt": f"2026-01-{call_count:02d}", "value": "1"}}],
                        "pageInfo": {"hasNextPage": True, "endCursor": f"cur{call_count}"},
                    }
                }]}
            }

        api._async_graphql = mock_graphql
        nodes = asyncio.get_event_loop().run_until_complete(
            api.async_get_daily_cost_measurements(datetime(2026, 1, 1), datetime(2026, 12, 31))
        )
        # Should stop at max pages (100) and not loop forever
        assert call_count == 100
        assert len(nodes) == 100


class _FakeResponse:
    """Minimal mock for aiohttp ClientResponse used in auth tests."""

    def __init__(self, status, body=None, json_data=None):
        self.status = status
        self._body = body or ""
        self._json = json_data

    async def text(self):
        return self._body

    async def json(self):
        return self._json

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestSendOtpEmailPayload:
    """Verify async_send_otp_email sends otpEnabled, journeyId, and headers."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_payload_includes_otp_enabled(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return _FakeResponse(200, body="OK")

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(session, "user@test.com", "powershop"))
        assert captured["json"]["otpEnabled"] is True

    def test_payload_includes_journey_id(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["json"] = json
            return _FakeResponse(200)

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(
            session, "user@test.com", "powershop",
            journey_id="test-uuid-123",
        ))
        assert captured["json"]["journeyId"] == "test-uuid-123"

    def test_payload_omits_journey_id_when_none(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["json"] = json
            return _FakeResponse(200)

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(session, "user@test.com", "powershop"))
        assert "journeyId" not in captured["json"]

    def test_headers_include_platform(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["headers"] = headers
            return _FakeResponse(200)

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(session, "user@test.com", "powershop"))
        assert captured["headers"]["X-Client-Platform"] == "web"

    def test_404_raises_email_not_found(self):
        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(404)

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(async_send_otp_email(session, "bad@test.com", "powershop"))
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "email_not_found" in str(e)

    def test_url_uses_brand_auth_domain(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["url"] = url
            return _FakeResponse(200)

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(session, "u@t.com", "meridian"))
        assert "auth.meridianenergy.nz" in captured["url"]

    def test_redirect_url_uses_brand_app_origin(self):
        captured = {}

        def fake_post(url, *, json=None, headers=None):
            captured["json"] = json
            return _FakeResponse(200)

        session = MagicMock()
        session.post = fake_post

        self._run(async_send_otp_email(session, "u@t.com", "meridian"))
        assert "app.meridianenergy.nz" in captured["json"]["redirectUrl"]


class TestValidateOtpPayload:
    """Verify async_validate_otp sends journeyId and headers."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_payload_includes_journey_id(self):
        captured = {}
        call_count = [0]

        def fake_post(url, *, json=None, headers=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # OTP validation call
                captured["json"] = json
                captured["headers"] = headers
                return _FakeResponse(200, json_data={"customToken": "ct"})
            # Firebase exchange call
            return _FakeResponse(200, json_data={
                "idToken": "idt", "refreshToken": "rt", "expiresIn": "3600",
            })

        session = MagicMock()
        session.post = fake_post

        result = self._run(async_validate_otp(
            session, "user@test.com", "123456", "powershop",
            journey_id="j-uuid",
        ))
        assert captured["json"]["journeyId"] == "j-uuid"
        assert result["idToken"] == "idt"

    def test_payload_omits_journey_id_when_none(self):
        captured = {}
        call_count = [0]

        def fake_post(url, *, json=None, headers=None):
            call_count[0] += 1
            if call_count[0] == 1:
                captured["json"] = json
                return _FakeResponse(200, json_data={"customToken": "ct"})
            return _FakeResponse(200, json_data={
                "idToken": "idt", "refreshToken": "rt", "expiresIn": "3600",
            })

        session = MagicMock()
        session.post = fake_post

        self._run(async_validate_otp(session, "user@test.com", "123456", "powershop"))
        assert "journeyId" not in captured["json"]

    def test_headers_include_platform(self):
        captured = {}
        call_count = [0]

        def fake_post(url, *, json=None, headers=None):
            call_count[0] += 1
            if call_count[0] == 1:
                captured["headers"] = headers
                return _FakeResponse(200, json_data={"customToken": "ct"})
            return _FakeResponse(200, json_data={
                "idToken": "idt", "refreshToken": "rt", "expiresIn": "3600",
            })

        session = MagicMock()
        session.post = fake_post

        self._run(async_validate_otp(session, "u@t.com", "123456", "powershop"))
        assert captured["headers"]["X-Client-Platform"] == "web"

    def test_invalid_otp_raises_auth_error(self):
        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(401, body='{"error":"Invalid OTP"}')

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(async_validate_otp(session, "u@t.com", "000000", "powershop"))
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "otp_invalid" in str(e)


class TestBillingInfo:
    """Verify async_get_billing_info parses the GraphQL response."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_billing_info_returns_dates(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        async def mock_graphql(*a, **kw):
            return {
                "account": {
                    "billingOptions": {
                        "currentBillingPeriodStartDate": "2026-04-10",
                        "currentBillingPeriodEndDate": "2026-05-09",
                        "nextBillingDate": "2026-05-10",
                    }
                }
            }

        api._async_graphql = mock_graphql
        result = self._run(api.async_get_billing_info())
        assert result["period_start"] == "2026-04-10"
        assert result["period_end"] == "2026-05-09"
        assert result["next_billing_date"] == "2026-05-10"

    def test_billing_info_missing_options(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        async def mock_graphql(*a, **kw):
            return {"account": {}}

        api._async_graphql = mock_graphql
        result = self._run(api.async_get_billing_info())
        assert result["period_start"] is None
        assert result["period_end"] is None
        assert result["next_billing_date"] is None

    def test_billing_info_empty_account(self):
        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        async def mock_graphql(*a, **kw):
            return {}

        api._async_graphql = mock_graphql
        result = self._run(api.async_get_billing_info())
        assert result["period_start"] is None


class TestValidateOtpJsonErrors:
    """Auth helpers raise AuthError on malformed JSON responses."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_otp_invalid_json_raises_auth_error(self):
        """OTP endpoint returns 200 with non-JSON body."""

        class _BadJsonResponse:
            status = 200
            async def text(self):
                return "<html>Bad Gateway</html>"
            async def json(self):
                raise ValueError("No JSON")
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass

        def fake_post(url, *, json=None, headers=None):
            return _BadJsonResponse()

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(async_validate_otp(session, "u@t.com", "123456", "powershop"))
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "Invalid JSON" in str(e)

    def test_firebase_exchange_invalid_json_raises_auth_error(self):
        """Firebase custom token exchange returns non-JSON."""
        call_count = [0]

        class _BadJsonResponse:
            status = 200
            async def text(self):
                return "not json"
            async def json(self):
                raise ValueError("No JSON")
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass

        def fake_post(url, *, json=None, headers=None):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FakeResponse(200, json_data={"customToken": "ct"})
            return _BadJsonResponse()

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(async_validate_otp(session, "u@t.com", "123456", "powershop"))
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "Invalid JSON" in str(e)


class TestRefreshTokenJsonErrors:
    """async_refresh_token raises AuthError on malformed JSON."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_refresh_invalid_json_raises_auth_error(self):
        from meridian_energy.api import async_refresh_token

        class _BadJsonResponse:
            status = 200
            async def text(self):
                return "<!DOCTYPE html>"
            async def json(self):
                raise ValueError("No JSON")
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                pass

        def fake_post(url, *, json=None, headers=None):
            return _BadJsonResponse()

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(async_refresh_token(session, "old_refresh_tok"))
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "Invalid JSON" in str(e)


class TestPaginationCursorValidation:
    """Pagination stops when hasNextPage is true but endCursor is missing."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_measurements_stops_on_missing_cursor(self):
        from datetime import datetime

        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        async def mock_graphql(*a, **kw):
            return {
                "account": {"properties": [{
                    "measurements": {
                        "edges": [{"node": {"value": "5", "startAt": "2026-01-01"}}],
                        "pageInfo": {"hasNextPage": True, "endCursor": None},
                    }
                }]}
            }

        api._async_graphql = mock_graphql
        nodes = self._run(
            api.async_get_measurements(datetime(2026, 1, 1), datetime(2026, 1, 2))
        )
        assert len(nodes) == 1  # stops after first page

    def test_daily_costs_stops_on_missing_cursor(self):
        from datetime import datetime

        session = MagicMock()
        api = MeridianEnergyApi("powershop", "tok", "A-123", session)
        api._id_token = "tok"

        async def mock_graphql(*a, **kw):
            return {
                "account": {"properties": [{
                    "measurements": {
                        "edges": [{"node": {"startAt": "2026-01-01", "value": "10"}}],
                        "pageInfo": {"hasNextPage": True},  # endCursor absent
                    }
                }]}
            }

        api._async_graphql = mock_graphql
        nodes = self._run(
            api.async_get_daily_cost_measurements(datetime(2026, 1, 1), datetime(2026, 1, 2))
        )
        assert len(nodes) == 1  # stops after first page


class TestDiscoverAccounts:
    """Tests for async_discover_accounts (plural)."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_returns_all_accounts(self):
        accounts_data = [
            {"number": "A-111", "brand": "powershop", "status": "ACTIVE"},
            {"number": "A-222", "brand": "powershop", "status": "ACTIVE"},
        ]

        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(200, json_data={
                "data": {"viewer": {"accounts": accounts_data}},
            })

        session = MagicMock()
        session.post = fake_post

        result = self._run(
            MeridianEnergyApi.async_discover_accounts(session, "tok", "powershop")
        )
        assert len(result) == 2
        assert result[0]["number"] == "A-111"
        assert result[1]["number"] == "A-222"

    def test_single_account_returns_list(self):
        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(200, json_data={
                "data": {"viewer": {"accounts": [{"number": "A-111"}]}},
            })

        session = MagicMock()
        session.post = fake_post

        result = self._run(
            MeridianEnergyApi.async_discover_accounts(session, "tok", "powershop")
        )
        assert len(result) == 1

    def test_no_accounts_raises_api_error(self):
        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(200, json_data={
                "data": {"viewer": {"accounts": []}},
            })

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(
                MeridianEnergyApi.async_discover_accounts(session, "tok", "powershop")
            )
            assert False, "Should have raised ApiError"
        except ApiError as e:
            assert "no_accounts_found" in str(e)

    def test_auth_failure_raises_auth_error(self):
        def fake_post(url, *, json=None, headers=None):
            return _FakeResponse(401)

        session = MagicMock()
        session.post = fake_post

        try:
            self._run(
                MeridianEnergyApi.async_discover_accounts(session, "tok", "powershop")
            )
            assert False, "Should have raised AuthError"
        except AuthError as e:
            assert "auth_invalid" in str(e)
