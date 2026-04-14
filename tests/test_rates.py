"""Tests for the v2 rates module (API response parsing)."""

import pytest

from meridian_energy.rates import classify_bucket, parse_rates, period_display_name


class TestClassifyBucket:
    """Tests for classify_bucket() — maps TOU bucket names to period keys."""

    def test_night_bucket(self):
        assert classify_bucket("N9") == "night"

    def test_peak_bucket(self):
        assert classify_bucket("PK5") == "peak"

    def test_offpeak_bucket(self):
        assert classify_bucket("OPK10") == "offpeak"

    def test_controlled_bucket(self):
        assert classify_bucket("CON1") == "controlled"

    def test_night_longer_name(self):
        assert classify_bucket("N12_extra") == "night"

    def test_peak_numeric_suffix(self):
        assert classify_bucket("PK99") == "peak"

    def test_offpeak_simple(self):
        assert classify_bucket("OPK") == "offpeak"

    def test_controlled_no_suffix(self):
        assert classify_bucket("CON") == "controlled"

    def test_empty_string_returns_none(self):
        assert classify_bucket("") is None

    def test_unknown_bucket_uses_fallback(self):
        assert classify_bucket("UNKNOWN") == "unknown"

    def test_standing_charge_uses_fallback(self):
        assert classify_bucket("STANDING") == "standing"

    def test_unknown_strips_trailing_digits(self):
        assert classify_bucket("EV1") == "ev"

    def test_fallback_lowercases(self):
        assert classify_bucket("SOLAR5") == "solar"

    def test_longest_prefix_match(self):
        assert classify_bucket("OPK10") == "offpeak"

    def test_case_insensitive(self):
        assert classify_bucket("n9") == "night"

    def test_pk_prefix_matches(self):
        assert classify_bucket("PK") == "peak"


class TestParseRates:
    """Tests for parse_rates() — parses API rate list into structured dict."""

    def test_basic_parsing(self, api_rates_response):
        result = parse_rates(api_rates_response["rates"])
        assert "tou_rates" in result
        assert "daily_charge" in result

    def test_night_rate_value(self, api_rates_response):
        result = parse_rates(api_rates_response["rates"])
        assert abs(result["tou_rates"]["night"] - 0.2362) < 0.0001

    def test_peak_rate_value(self, api_rates_response):
        result = parse_rates(api_rates_response["rates"])
        assert abs(result["tou_rates"]["peak"] - 0.4077) < 0.0001

    def test_offpeak_rate_value(self, api_rates_response):
        result = parse_rates(api_rates_response["rates"])
        assert abs(result["tou_rates"]["offpeak"] - 0.2792) < 0.0001

    def test_daily_charge_converted_from_cents(self, api_rates_response):
        result = parse_rates(api_rates_response["rates"])
        assert abs(result["daily_charge"] - 4.14) < 0.01

    def test_no_weekend_offpeak_fabricated(self, api_rates_response):
        """Periods come from the API — no fabricated weekend_offpeak."""
        result = parse_rates(api_rates_response["rates"])
        assert "weekend_offpeak" not in result["tou_rates"]

    def test_empty_rates(self):
        result = parse_rates([])
        assert result["tou_rates"] == {}
        assert result["daily_charge"] == 0.0

    def test_standing_charge_only(self):
        rates = [
            {"touBucketName": "", "bandCategory": "STANDING_CHARGE",
             "rateIncludingTax": "300.00000", "unitType": "Days on supply"},
        ]
        result = parse_rates(rates)
        assert result["daily_charge"] == 3.0
        assert result["tou_rates"] == {}

    def test_no_standing_charge(self):
        rates = [
            {"touBucketName": "N9", "bandCategory": "CONSUMPTION",
             "rateIncludingTax": "20.00000", "unitType": "kWh"},
        ]
        result = parse_rates(rates)
        assert result["daily_charge"] == 0.0
        assert "night" in result["tou_rates"]

    def test_controlled_rate_included(self):
        rates = [
            {"touBucketName": "CON1", "bandCategory": "CONSUMPTION",
             "rateIncludingTax": "15.00000", "unitType": "kWh"},
        ]
        result = parse_rates(rates)
        assert "controlled" in result["tou_rates"]
        assert abs(result["tou_rates"]["controlled"] - 0.15) < 0.001

    def test_controlled_rate_from_empty_bucket(self):
        """New API: controlled rate has no touBucketName."""
        rates = [
            {"touBucketName": "N9", "bandCategory": "CONSUMPTION_CHARGE",
             "rateIncludingTax": "23.62000", "unitType": "Kilowatt-hours consumed"},
            {"touBucketName": "", "bandCategory": "CONSUMPTION_CHARGE",
             "rateIncludingTax": "23.62000", "unitType": "Kilowatt-hours consumed"},
        ]
        result = parse_rates(rates)
        assert "controlled" in result["tou_rates"]
        assert abs(result["tou_rates"]["controlled"] - 0.2362) < 0.001

    def test_controlled_from_fixture(self, api_rates_response):
        """The main fixture includes a controlled entry with empty bucket."""
        result = parse_rates(api_rates_response["rates"])
        assert "controlled" in result["tou_rates"]

    def test_other_band_category_still_parsed_if_bucket_recognised(self):
        """parse_rates uses bucket prefix, not bandCategory, for TOU rates."""
        rates = [
            {"touBucketName": "N9", "bandCategory": "OTHER",
             "rateIncludingTax": "99.99", "unitType": "kWh"},
        ]
        result = parse_rates(rates)
        assert "night" in result["tou_rates"]


class TestPeriodDisplayName:

    def test_night(self):
        assert period_display_name("night") == "Night"

    def test_peak(self):
        assert period_display_name("peak") == "Peak"

    def test_offpeak(self):
        assert period_display_name("offpeak") == "Off-Peak"

    def test_unknown_fallback_with_underscores(self):
        assert period_display_name("weekend_offpeak") == "Weekend Offpeak"

    def test_controlled(self):
        assert period_display_name("controlled") == "Controlled"

    def test_unknown_returns_titlecased(self):
        assert period_display_name("mystery") == "Mystery"
