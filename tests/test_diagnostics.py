"""Tests for the v2 diagnostics module."""

from meridian_energy.diagnostics import TO_REDACT
from meridian_energy.const import CONF_REFRESH_TOKEN


class TestRedactionSet:
    def test_email_redacted(self):
        assert "email" in TO_REDACT

    def test_refresh_token_redacted(self):
        assert CONF_REFRESH_TOKEN in TO_REDACT

    def test_no_password_in_v2(self):
        assert "password" not in TO_REDACT

    def test_no_cookie_in_v2(self):
        assert "cookie" not in TO_REDACT

    def test_account_number_redacted(self):
        from meridian_energy.const import CONF_ACCOUNT_NUMBER
        assert CONF_ACCOUNT_NUMBER in TO_REDACT

    def test_redact_set_size(self):
        assert len(TO_REDACT) == 3
