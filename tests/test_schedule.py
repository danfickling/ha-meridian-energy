"""Tests for the v2 schedule module (API-driven TOU parsing)."""

from datetime import datetime, time

import pytest

from meridian_energy.schedule import (
    parse_tou_scheme,
    classify_period,
    get_boundary_times,
    _parse_time,
    _time_to_minutes,
    _in_range,
)


class TestParseTime:
    def test_morning(self):
        assert _parse_time("07:00") == time(7, 0)

    def test_night(self):
        assert _parse_time("22:00") == time(22, 0)

    def test_half_hour(self):
        assert _parse_time("09:30") == time(9, 30)

    def test_midnight(self):
        assert _parse_time("00:00") == time(0, 0)

    def test_with_seconds(self):
        assert _parse_time("07:00:00") == time(7, 0)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Invalid TOU time format"):
            _parse_time("")

    def test_no_colon_raises(self):
        with pytest.raises(ValueError, match="Invalid TOU time format"):
            _parse_time("0700")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError, match="Invalid TOU time format"):
            _parse_time("ab:cd")


class TestTimeToMinutes:
    def test_midnight(self):
        assert _time_to_minutes(time(0, 0)) == 0

    def test_seven_am(self):
        assert _time_to_minutes(time(7, 0)) == 420

    def test_ten_thirty_pm(self):
        assert _time_to_minutes(time(22, 30)) == 1350

    def test_end_of_day(self):
        assert _time_to_minutes(time(23, 59)) == 1439


class TestInRange:
    """_in_range(minutes, start_str, end_str) — inclusive start, exclusive end."""

    def test_within_normal_range(self):
        assert _in_range(480, "07:00", "09:30") is True  # 8:00

    def test_at_start_boundary(self):
        assert _in_range(420, "07:00", "09:30") is True  # 7:00

    def test_at_end_boundary_exclusive(self):
        assert _in_range(570, "07:00", "09:30") is False  # 9:30

    def test_outside_normal_range(self):
        assert _in_range(600, "07:00", "09:30") is False  # 10:00

    def test_midnight_wrap_inside_late(self):
        assert _in_range(1380, "22:00", "07:00") is True  # 23:00

    def test_midnight_wrap_inside_early(self):
        assert _in_range(180, "22:00", "07:00") is True  # 3:00

    def test_midnight_wrap_outside(self):
        assert _in_range(720, "22:00", "07:00") is False  # 12:00

    def test_midnight_wrap_at_start(self):
        assert _in_range(1320, "22:00", "07:00") is True  # 22:00

    def test_midnight_wrap_at_end_exclusive(self):
        assert _in_range(420, "22:00", "07:00") is False  # 7:00


class TestParseTouScheme:
    def test_parses_scheme_name(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        assert result["scheme_name"] == "ST06"

    def test_parses_timeslots(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        assert len(result["timeslots"]) == 7

    def test_has_no_weekend_offpeak_key(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        assert "has_weekend_offpeak" not in result

    def test_timeslot_structure(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        slot = result["timeslots"][0]
        assert "period" in slot
        assert "bucket" in slot
        assert "start" in slot
        assert "end" in slot
        assert "weekdays" in slot
        assert "weekends" in slot

    def test_timeslot_period_mapped(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        periods = {s["period"] for s in result["timeslots"]}
        assert "night" in periods
        assert "peak" in periods
        assert "offpeak" in periods

    def test_empty_schemes(self):
        result = parse_tou_scheme([])
        assert result["scheme_name"] == ""
        assert result["timeslots"] == []

    def test_all_everyday_scheme(self):
        """New format: weekdays=False, weekends=False means all days."""
        schemes = [{
            "name": "FLAT",
            "timeslots": [
                {"timeslot": "PK1", "activeFrom": "07:00:00", "activeTo": "21:00:00",
                 "weekdays": False, "weekends": False},
                {"timeslot": "OPK1", "activeFrom": "21:00:00", "activeTo": "07:00:00",
                 "weekdays": False, "weekends": False},
            ],
        }]
        result = parse_tou_scheme(schemes)
        for slot in result["timeslots"]:
            assert slot["weekdays"] is True
            assert slot["weekends"] is True

    def test_peak_weekdays_only_detected(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        peak_slots = [s for s in result["timeslots"] if s["period"] == "peak"]
        for slot in peak_slots:
            assert slot["weekdays"] is True
            assert slot["weekends"] is False

    def test_start_end_are_strings(self, api_rates_response):
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        for slot in result["timeslots"]:
            assert isinstance(slot["start"], str)
            assert isinstance(slot["end"], str)

    def test_night_applies_all_days(self, api_rates_response):
        """Night slots (weekdays=False, weekends=False) should apply to all days."""
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        night_slots = [s for s in result["timeslots"] if s["period"] == "night"]
        for slot in night_slots:
            assert slot["weekdays"] is True
            assert slot["weekends"] is True

    def test_legacy_activation_rules_format(self):
        """Legacy format with activationRules still works."""
        schemes = [{
            "name": "LEGACY",
            "timeslots": [
                {"name": "N9", "startTime": "22:00", "endTime": "07:00",
                 "activationRules": {"weekdays": True, "weekends": True}},
                {"name": "PK5", "startTime": "07:00", "endTime": "22:00",
                 "activationRules": {"weekdays": True, "weekends": False}},
            ],
        }]
        result = parse_tou_scheme(schemes)
        assert len(result["timeslots"]) == 2
        assert result["timeslots"][0]["bucket"] == "N9"
        assert result["timeslots"][0]["weekdays"] is True
        assert result["timeslots"][0]["weekends"] is True

    def test_weekend_only_slot(self, api_rates_response):
        """Weekend-only OPK slot (weekdays=False, weekends=True) applies on weekends."""
        result = parse_tou_scheme(api_rates_response["tou_schemes"])
        we_slots = [s for s in result["timeslots"]
                     if s.get("weekends") and not s.get("weekdays")]
        assert len(we_slots) == 1
        assert we_slots[0]["bucket"] == "OPK10"


class TestClassifyPeriod:
    """Test TOU period classification with ST06 schedule."""

    def test_weekday_peak_morning(self, tou_schedule):
        dt = datetime(2026, 1, 5, 8, 0)  # Monday 8:00
        assert classify_period(dt, tou_schedule) == "peak"

    def test_weekday_peak_evening(self, tou_schedule):
        dt = datetime(2026, 1, 5, 18, 0)  # Monday 18:00
        assert classify_period(dt, tou_schedule) == "peak"

    def test_weekday_offpeak_midday(self, tou_schedule):
        dt = datetime(2026, 1, 5, 12, 0)  # Monday 12:00
        assert classify_period(dt, tou_schedule) == "offpeak"

    def test_weekday_offpeak_after_peak(self, tou_schedule):
        dt = datetime(2026, 1, 5, 20, 30)  # Monday 20:30
        assert classify_period(dt, tou_schedule) == "offpeak"

    def test_weekday_night_late(self, tou_schedule):
        dt = datetime(2026, 1, 5, 23, 0)  # Monday 23:00
        assert classify_period(dt, tou_schedule) == "night"

    def test_weekday_night_early_morning(self, tou_schedule):
        dt = datetime(2026, 1, 5, 3, 0)  # Monday 3:00
        assert classify_period(dt, tou_schedule) == "night"

    def test_saturday_morning_offpeak(self, tou_schedule):
        dt = datetime(2026, 1, 10, 8, 0)  # Saturday 8:00
        assert classify_period(dt, tou_schedule) == "offpeak"

    def test_sunday_afternoon_offpeak(self, tou_schedule):
        """Sunday 14:00 falls in OPK10 (07:00-22:00, weekends) — offpeak."""
        dt = datetime(2026, 1, 11, 14, 0)  # Sunday 14:00
        assert classify_period(dt, tou_schedule) == "offpeak"

    def test_saturday_night(self, tou_schedule):
        dt = datetime(2026, 1, 10, 23, 0)  # Saturday 23:00
        assert classify_period(dt, tou_schedule) == "night"

    def test_peak_boundary_start(self, tou_schedule):
        dt = datetime(2026, 1, 5, 7, 0)  # Monday 7:00 exactly
        assert classify_period(dt, tou_schedule) == "peak"

    def test_peak_boundary_end_transitions(self, tou_schedule):
        dt = datetime(2026, 1, 5, 9, 30)  # Monday 9:30 exactly
        assert classify_period(dt, tou_schedule) == "offpeak"

    def test_night_boundary_start(self, tou_schedule):
        dt = datetime(2026, 1, 5, 22, 0)  # Monday 22:00
        assert classify_period(dt, tou_schedule) == "night"

    def test_night_boundary_end_transitions(self, tou_schedule):
        dt = datetime(2026, 1, 5, 7, 0)  # Monday 7:00 — night ends, peak starts
        assert classify_period(dt, tou_schedule) == "peak"

    def test_empty_schedule_returns_offpeak(self):
        dt = datetime(2026, 1, 5, 12, 0)
        result = classify_period(dt, {})
        assert result == "offpeak"

    def test_weekend_evening_offpeak_not_peak(self, tou_schedule):
        dt = datetime(2026, 1, 10, 18, 0)  # Saturday 18:00 (would be peak on weekday)
        result = classify_period(dt, tou_schedule)
        assert result == "offpeak"


class TestGetBoundaryTimes:
    def test_returns_unique_times(self, tou_schedule):
        times = get_boundary_times(tou_schedule)
        assert len(times) == len(set(times))

    def test_contains_key_boundaries(self, tou_schedule):
        times = get_boundary_times(tou_schedule)
        assert (7, 0) in times
        assert (22, 0) in times
        assert (9, 30) in times
        assert (17, 30) in times
        assert (20, 0) in times

    def test_empty_schedule(self):
        times = get_boundary_times({})
        assert times == []

    def test_sorted_output(self, tou_schedule):
        times = get_boundary_times(tou_schedule)
        assert times == sorted(times)
