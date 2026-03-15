"""Tests for api.py — balance parsing, CSV validation, credential validation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import re

import pytest
import requests

from meridian_energy.api import MeridianEnergyApi, HTTP_TIMEOUT



class TestHTTPTimeout:
    def test_timeout_is_positive(self):
        assert HTTP_TIMEOUT > 0

    def test_timeout_reasonable(self):
        assert 10 <= HTTP_TIMEOUT <= 60



class TestApiConstruction:
    def test_default_supplier(self):
        api = MeridianEnergyApi("test@example.com", "password123")
        assert "powershop" in api._url_base.lower()

    def test_meridian_supplier(self):
        api = MeridianEnergyApi("test@example.com", "password123", supplier="meridian")
        assert "meridian" in api._url_base.lower()

    def test_history_start_setter(self):
        api = MeridianEnergyApi("test@example.com", "password123")
        api.history_start = "01/06/2023"
        assert api._history_start == "01/06/2023"

    def test_supplier_name(self):
        api = MeridianEnergyApi("test@example.com", "password123")
        assert api.supplier_name == "Powershop"

    def test_supplier_name_meridian(self):
        api = MeridianEnergyApi("test@example.com", "password123", supplier="meridian")
        assert api.supplier_name == "Meridian Energy"

    def test_cookie_setter(self):
        api = MeridianEnergyApi("test@example.com", "password123")
        api.cookie = "session_id=abc123"
        assert api.cookie == "session_id=abc123"



class TestGetBalance:
    """Test balance page parsing with mocked HTTP responses.

    get_balance uses BeautifulSoup to extract body text, then applies
    regex patterns.  Mock HTML must include a <body> tag.
    """

    def _make_api(self) -> MeridianEnergyApi:
        api = MeridianEnergyApi("test@example.com", "password123")
        api._logged_in = True
        return api

    def _mock_response(self, html: str) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.text = html
        return resp

    def test_parses_ahead_amount(self):
        api = self._make_api()
        html = (
            "<html><body>"
            "<div>You're about $498 (48 days) ahead</div>"
            "<div>You have $1,049 (10 weeks) in pre-purchased Future Packs</div>"
            "<div>You're currently using about $10.30 per day</div>"
            "</body></html>"
        )
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is not None
        assert result["ahead"] == 498.0

    def test_parses_future_packs(self):
        api = self._make_api()
        html = (
            "<html><body>"
            "<div>You're about $200 (20 days) ahead</div>"
            "<div>You have $1,049 (10 weeks) in pre-purchased Future Packs</div>"
            "</body></html>"
        )
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is not None
        assert result["future_packs"] == 1049.0

    def test_parses_daily_cost(self):
        api = self._make_api()
        html = (
            "<html><body>"
            "<div>You're about $100 (5 days) ahead</div>"
            "<div>You're currently using about $10.30 per day</div>"
            "</body></html>"
        )
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is not None
        assert result["daily_cost"] == 10.30

    def test_all_fields(self):
        api = self._make_api()
        html = (
            "<html><body>"
            "<div>You're about $488 (42 days) ahead</div>"
            "<div>You also have $2,100 (20 weeks) in pre-purchased Future Packs</div>"
            "<div>You're currently using about $11.62 per day</div>"
            "</body></html>"
        )
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is not None
        assert result["ahead"] == 488.0
        assert result["future_packs"] == 2100.0
        assert result["daily_cost"] == 11.62

    def test_no_balance_info_returns_none(self):
        api = self._make_api()
        html = "<html><body><div>Some random content</div></body></html>"
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is None

    def test_not_logged_in_returns_none(self):
        api = self._make_api()
        api._logged_in = False
        assert api.get_balance() is None

    def test_expired_session_returns_none(self):
        """If balance page shows login form, treat as expired."""
        api = self._make_api()
        html = (
            "<html><body>"
            "<h1>Powershop Login</h1>"
            "<form>...</form>"
            "</body></html>"
        )
        api._session = MagicMock()
        api._session.get.return_value = self._mock_response(html)

        result = api.get_balance()
        assert result is None

    def test_http_error_returns_none(self):
        api = self._make_api()
        resp = MagicMock()
        resp.status_code = 500
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_balance()
        assert result is None



class TestGetData:
    def _make_api(self) -> MeridianEnergyApi:
        api = MeridianEnergyApi("test@example.com", "password123")
        api._logged_in = True
        return api

    def test_valid_csv(self):
        api = self._make_api()
        csv_text = "HDR,ICPID,Stream,Channel\nDET,123,UN,1,RD,kWh,..."
        resp = MagicMock()
        resp.status_code = 200
        resp.text = csv_text
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_data()
        assert result is not None
        assert result.startswith("HDR")

    def test_invalid_response_html(self):
        api = self._make_api()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "<html><head><title>Login</title>"
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_data()
        assert result is None

    def test_empty_response(self):
        api = self._make_api()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = ""
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_data()
        assert result is None

    def test_http_error(self):
        api = self._make_api()
        resp = MagicMock()
        resp.status_code = 500
        resp.text = "Internal Server Error"
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_data()
        assert result is None

    def test_not_logged_in(self):
        api = self._make_api()
        api._logged_in = False
        result = api.get_data()
        assert result is None

    def test_custom_date_range(self):
        api = self._make_api()
        csv_text = "HDR,ICPID\nDET,123"
        resp = MagicMock()
        resp.status_code = 200
        resp.text = csv_text
        api._session = MagicMock()
        api._session.get.return_value = resp

        result = api.get_data(date_from="01/01/2025", date_to="28/02/2025")
        assert result is not None
        # Verify the URL includes our date range
        call_url = api._session.get.call_args[0][0]
        assert "01/01/2025" in call_url
        assert "28/02/2025" in call_url



class TestValidateCredentials:
    """validate_credentials calls token() which creates a fresh requests.Session.

    We need to patch requests.Session at the module level to inject our mock.
    """

    @patch("meridian_energy.api.requests.Session")
    def test_successful_login(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value = session

        # token() GETs the base URL, parses for authenticity_token
        token_resp = MagicMock()
        token_resp.status_code = 200
        token_resp.text = '<input name="authenticity_token" value="abc123" />'

        # login() POSTs and checks for fail text
        login_resp = MagicMock()
        login_resp.status_code = 200
        login_resp.text = "<div>Welcome back</div>"

        session.get.return_value = token_resp
        session.post.return_value = login_resp

        api = MeridianEnergyApi("test@example.com", "password123")
        result = api.validate_credentials()
        assert result is True

    @patch("meridian_energy.api.requests.Session")
    def test_failed_login(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value = session

        token_resp = MagicMock()
        token_resp.status_code = 200
        token_resp.text = '<input name="authenticity_token" value="abc123" />'

        # Login page contains fail text "Powershop Login"
        login_resp = MagicMock()
        login_resp.status_code = 200
        login_resp.text = (
            '<div class="message">Invalid email or password</div>'
            "Powershop Login"
        )

        session.get.return_value = token_resp
        session.post.return_value = login_resp

        api = MeridianEnergyApi("test@example.com", "wrongpass")
        result = api.validate_credentials()
        assert result is False

    @patch("meridian_energy.api.requests.Session")
    def test_network_error(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value = session
        session.get.side_effect = requests.exceptions.ConnectionError("unreachable")

        api = MeridianEnergyApi("test@example.com", "password123")
        result = api.validate_credentials()
        assert result is False

    @patch("meridian_energy.api.requests.Session")
    def test_no_csrf_token(self, mock_session_cls):
        """If the login page doesn't have authenticty_token, login fails."""
        session = MagicMock()
        mock_session_cls.return_value = session

        token_resp = MagicMock()
        token_resp.status_code = 200
        token_resp.text = "<html><body>No token here</body></html>"

        session.get.return_value = token_resp

        api = MeridianEnergyApi("test@example.com", "password123")
        result = api.validate_credentials()
        assert result is False
