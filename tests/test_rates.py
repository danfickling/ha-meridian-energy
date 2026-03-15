"""Tests for rates.py — period extraction, rate lookups, fallback chain."""

from __future__ import annotations

from datetime import datetime

import pytest

from meridian_energy.rates import (
    _extract_period,
    _keyword_classify,
    RateCache,
)



class TestExtractPeriod:
    """Test period classification from rate table labels.

    SVG icons are stripped by _parse_rate_rows before _extract_period
    is called, so these tests use the clean visible text.
    """

    # --- Primary keyword matching ---
    def test_night_label(self):
        assert _extract_period("Night") == "night"

    def test_peak_label(self):
        assert _extract_period("Weekday Peak") == "peak"

    def test_offpeak_label(self):
        assert _extract_period("Off Peak") == "offpeak"

    def test_offpeak_hyphen(self):
        assert _extract_period("Off-Peak Rate") == "offpeak"

    def test_weekend_offpeak_label(self):
        assert _extract_period("Weekend Day (Off Peak)") == "weekend_offpeak"

    def test_controlled_label(self):
        assert _extract_period("Controlled") == "controlled"

    def test_daily_charge(self):
        assert _extract_period("Daily Charge (connection fee)") == "daily"

    def test_daily_connection(self):
        assert _extract_period("Daily Connection fee") == "daily"

    # --- Keyword ordering: weekend before off-peak ---
    def test_weekend_before_offpeak(self):
        """'Weekend Day (Off Peak)' should match 'weekend' before 'off peak'."""
        assert _extract_period("Weekend Day (Off Peak)") == "weekend_offpeak"

    # --- Additional labels ---
    def test_shoulder_label(self):
        assert _extract_period("Shoulder rate") == "offpeak"

    def test_overnight_label(self):
        assert _extract_period("Overnight Period") == "night"

    def test_uncontrolled_label(self):
        assert _extract_period("Uncontrolled") == "offpeak"

    def test_anytime_label(self):
        assert _extract_period("Anytime") == "offpeak"

    def test_day_label(self):
        assert _extract_period("Day") == "offpeak"

    # --- Edge cases ---
    def test_empty_string(self):
        assert _extract_period("") is None

    def test_none(self):
        assert _extract_period(None) is None

    def test_whitespace_only(self):
        assert _extract_period("   ") is None

    def test_unrecognized_label(self):
        assert _extract_period("Unknown Rate Type XYZ") is None

    def test_case_insensitive(self):
        assert _extract_period("NIGHT RATE") == "night"



class TestRateCacheNeedsRefresh:
    def _make_cache(self, last_updated: str | None) -> RateCache:
        cache = RateCache(cache_dir=None)
        cache._data = {"last_updated": last_updated, "special": {}, "base": {}}
        cache._loaded = True
        return cache

    def test_no_timestamp(self):
        assert self._make_cache(None).needs_refresh() is True

    def test_stale(self):
        old = datetime(2020, 1, 1).isoformat()
        assert self._make_cache(old).needs_refresh() is True

    def test_fresh(self):
        recent = datetime.now().isoformat()
        assert self._make_cache(recent).needs_refresh() is False

    def test_invalid_timestamp(self):
        assert self._make_cache("garbage").needs_refresh() is True



class TestRateCacheGetRates:
    def _make_cache(self, data: dict) -> RateCache:
        cache = RateCache(cache_dir=None)
        cache._data = data
        cache._loaded = True
        return cache

    def test_exact_match(self, sample_rate_data):
        cache = self._make_cache(sample_rate_data)
        rates = cache.get_rates(2026, 3, "special")
        assert rates["night"] == 0.167
        assert rates["peak"] == 0.3161
        assert "daily" not in rates  # daily is stripped

    def test_seasonal_fallback(self, sample_rate_data):
        """When exact month missing, find same month from earlier year."""
        cache = self._make_cache(sample_rate_data)
        rates = cache.get_rates(2027, 3, "special")  # 2027-03 not in cache
        # Should fall back to 2026-03
        assert rates["night"] == 0.167

    def test_seasonal_fallback_earlier_year(self, sample_rate_data):
        """When checking seasonal, prefers earlier years first."""
        cache = self._make_cache(sample_rate_data)
        rates = cache.get_rates(2024, 3, "special")  # 2024 not in cache
        # Should try 2023 (not found), then go to later years: 2025-03 exists
        assert rates["night"] == 0.155

    def test_oldest_fallback(self, sample_rate_data):
        """When no seasonal match, use the oldest month in cache."""
        cache = self._make_cache(sample_rate_data)
        rates = cache.get_rates(2026, 7, "special")  # July never cached
        # Oldest key is "2025-03"
        assert rates["night"] == 0.155

    def test_emergency_defaults(self):
        """Empty cache returns hardcoded emergency values."""
        cache = self._make_cache({"special": {}, "base": {}})
        rates = cache.get_rates(2026, 3, "special")
        assert rates["night"] == 0.17
        assert rates["peak"] == 0.34
        assert rates["offpeak"] == 0.22
        assert rates["weekend_offpeak"] == 0.22
        assert rates["controlled"] == 0.17

    def test_base_rate_type(self):
        """Test that rate_type='base' looks in the base bucket."""
        data = {
            "special": {},
            "base": {
                "2026-03": {
                    "night": 0.20,
                    "peak": 0.40,
                    "offpeak": 0.30,
                    "weekend_offpeak": 0.30,
                    "controlled": 0.20,
                }
            },
        }
        cache = self._make_cache(data)
        rates = cache.get_rates(2026, 3, "base")
        assert rates["night"] == 0.20



class TestRateCacheGetDailyCharge:
    def _make_cache(self, data: dict) -> RateCache:
        cache = RateCache(cache_dir=None)
        cache._data = data
        cache._loaded = True
        return cache

    def test_exact_match(self, sample_rate_data):
        cache = self._make_cache(sample_rate_data)
        assert cache.get_daily_charge(2026, 3, "special") == 3.7375

    def test_seasonal_fallback(self, sample_rate_data):
        cache = self._make_cache(sample_rate_data)
        assert cache.get_daily_charge(2027, 3, "special") == 3.7375  # from 2026-03

    def test_oldest_fallback(self, sample_rate_data):
        cache = self._make_cache(sample_rate_data)
        charge = cache.get_daily_charge(2026, 7, "special")
        assert charge == 3.50  # from 2025-03 (oldest)

    def test_emergency_default(self):
        cache = self._make_cache({"special": {}, "base": {}})
        assert cache.get_daily_charge(2026, 3, "special") == 4.14

    def test_missing_daily_key(self):
        """If exact month exists but has no 'daily' key, fall through."""
        data = {
            "special": {
                "2026-03": {"night": 0.17, "peak": 0.34},
            },
            "base": {},
        }
        cache = self._make_cache(data)
        assert cache.get_daily_charge(2026, 3, "special") == 4.14  # emergency



class TestKeywordClassify:
    """Tests for the _keyword_classify helper."""

    def test_daily_charge(self):
        assert _keyword_classify("daily charge (connection fee)") == "daily"

    def test_daily_connection(self):
        assert _keyword_classify("daily connection fee") == "daily"

    def test_controlled(self):
        assert _keyword_classify("controlled") == "controlled"

    def test_uncontrolled_before_controlled(self):
        """'uncontrolled' must NOT match 'controlled'."""
        assert _keyword_classify("uncontrolled") == "offpeak"

    def test_weekend(self):
        assert _keyword_classify("weekend day (off peak)") == "weekend_offpeak"

    def test_offpeak(self):
        assert _keyword_classify("off peak") == "offpeak"

    def test_offpeak_hyphen(self):
        assert _keyword_classify("off-peak") == "offpeak"

    def test_shoulder(self):
        assert _keyword_classify("shoulder") == "offpeak"

    def test_peak(self):
        assert _keyword_classify("weekday peak") == "peak"

    def test_night(self):
        assert _keyword_classify("night") == "night"

    def test_overnight(self):
        assert _keyword_classify("overnight") == "night"

    def test_evernight(self):
        assert _keyword_classify("evernight") == "night"

    def test_anytime(self):
        assert _keyword_classify("anytime") == "offpeak"

    def test_all_time(self):
        assert _keyword_classify("all time") == "offpeak"

    def test_day_not_daily(self):
        assert _keyword_classify("day") == "offpeak"

    def test_unknown_returns_none(self):
        assert _keyword_classify("some weird label") is None


class TestDetectedPeriods:
    """Tests for the detected_periods property on RateCache."""

    def _make_cache(self, data: dict) -> RateCache:
        cache = RateCache(cache_dir=None)
        cache._data = data
        cache._loaded = True
        return cache

    def test_detected_from_special(self):
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {
                "2026-03": {
                    "night": 0.17,
                    "peak": 0.34,
                    "daily": 3.74,
                },
            },
            "base": {},
            "detected_periods": ["night", "peak"],
        }
        cache = self._make_cache(data)
        assert cache.detected_periods == ["night", "peak"]

    def test_empty_when_missing(self):
        data = {"last_updated": None, "special": {}, "base": {}}
        cache = self._make_cache(data)
        assert cache.detected_periods == []

    def test_derives_from_cache_when_field_empty(self):
        """When detected_periods field is empty, derive from cached months."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {
                "2026-03": {"night": 0.17, "peak": 0.34, "offpeak": 0.22, "daily": 3.74},
            },
            "base": {},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        assert cache.detected_periods == ["night", "offpeak", "peak"]

    def test_derives_from_base_when_special_empty(self):
        """When special is empty, derive detected_periods from base."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {},
            "base": {
                "2026-03": {"night": 0.19, "peak": 0.34, "controlled": 0.19, "daily": 3.74},
            },
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        assert "night" in cache.detected_periods
        assert "controlled" in cache.detected_periods
        assert "daily" not in cache.detected_periods

    def test_daily_excluded(self):
        """'daily' should not appear in detected_periods."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {},
            "base": {},
            "detected_periods": ["night", "peak"],
        }
        cache = self._make_cache(data)
        assert "daily" not in cache.detected_periods


class TestBaseUrlGuard:
    """scrape_and_update should fail cleanly when base_url is empty."""

    def test_empty_url_returns_false(self):
        cache = RateCache(cache_dir=None)
        cache._data = {"last_updated": None, "special": {}, "base": {}, "detected_periods": []}
        cache._loaded = True
        assert cache.scrape_and_update(session=None, base_url="") is False


class TestEffectiveBucketFallback:
    """Auto-fallback when the requested rate bucket is empty."""

    def _make_cache(self, data: dict) -> RateCache:
        cache = RateCache(cache_dir=None)
        cache._data = data
        cache._loaded = True
        return cache

    def test_special_empty_falls_back_to_base(self):
        """When special is empty, get_rates('special') uses base data."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {},
            "base": {"2026-03": {"peak": 0.34, "night": 0.19, "daily": 3.74}},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        rates = cache.get_rates(2026, 3, "special")
        assert rates["peak"] == 0.34
        assert "daily" not in rates

    def test_base_empty_falls_back_to_special(self):
        """When base is empty, get_rates('base') uses special data."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {"2026-03": {"peak": 0.30, "night": 0.15, "daily": 3.50}},
            "base": {},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        rates = cache.get_rates(2026, 3, "base")
        assert rates["peak"] == 0.30

    def test_both_present_no_fallback(self):
        """When both buckets have data, use the requested one."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {"2026-03": {"peak": 0.30}},
            "base": {"2026-03": {"peak": 0.34}},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        assert cache.get_rates(2026, 3, "special")["peak"] == 0.30
        assert cache.get_rates(2026, 3, "base")["peak"] == 0.34

    def test_daily_charge_fallback(self):
        """get_daily_charge also uses auto-fallback."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {},
            "base": {"2026-03": {"peak": 0.34, "daily": 3.74}},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        assert cache.get_daily_charge(2026, 3, "special") == 3.74

    def test_both_empty_returns_emergency(self):
        """When both buckets empty, emergency defaults are returned."""
        data = {
            "last_updated": "2026-03-01T00:00:00",
            "special": {},
            "base": {},
            "detected_periods": [],
        }
        cache = self._make_cache(data)
        rates = cache.get_rates(2026, 3, "special")
        assert rates["peak"] == 0.34  # emergency default
