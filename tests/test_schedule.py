"""Tests for schedule.py — TOU classification, boundary times, schedule lookup."""

from __future__ import annotations

from datetime import datetime, time

import pytest

from meridian_energy.schedule import (
    _parse_time,
    _time_to_minutes,
    _in_range,
    classify_period,
    get_boundary_times,
    ScheduleCache,
    _DEFAULT_SCHEDULE,
    _schedules_equal,
    NETWORK_SCHEDULES,
)



class TestParseTime:
    def test_morning(self):
        assert _parse_time("07:00") == time(7, 0)

    def test_night(self):
        assert _parse_time("22:30") == time(22, 30)

    def test_midnight(self):
        assert _parse_time("00:00") == time(0, 0)

    def test_end_of_day(self):
        assert _parse_time("23:59") == time(23, 59)



class TestTimeToMinutes:
    def test_midnight(self):
        assert _time_to_minutes(time(0, 0)) == 0

    def test_seven_am(self):
        assert _time_to_minutes(time(7, 0)) == 420

    def test_ten_pm_thirty(self):
        assert _time_to_minutes(time(22, 30)) == 1350

    def test_end_of_day(self):
        assert _time_to_minutes(time(23, 59)) == 1439



class TestInRange:
    def test_within_normal_range(self):
        """8:00 is within 07:00-09:30."""
        assert _in_range(480, "07:00", "09:30") is True

    def test_at_start_boundary(self):
        """07:00 (420 min) is within [07:00, 09:30)."""
        assert _in_range(420, "07:00", "09:30") is True

    def test_at_end_boundary(self):
        """09:30 (570 min) is NOT within [07:00, 09:30) — exclusive end."""
        assert _in_range(570, "07:00", "09:30") is False

    def test_outside_normal_range(self):
        """10:00 is outside 07:00-09:30."""
        assert _in_range(600, "07:00", "09:30") is False

    def test_midnight_wrap_inside_late(self):
        """23:00 is within 22:00-07:00 (midnight wrap)."""
        assert _in_range(1380, "22:00", "07:00") is True

    def test_midnight_wrap_inside_early(self):
        """03:00 (180 min) is within 22:00-07:00 (midnight wrap)."""
        assert _in_range(180, "22:00", "07:00") is True

    def test_midnight_wrap_outside(self):
        """12:00 is NOT within 22:00-07:00."""
        assert _in_range(720, "22:00", "07:00") is False

    def test_midnight_wrap_at_start(self):
        """22:00 (1320 min) is within [22:00, 07:00)."""
        assert _in_range(1320, "22:00", "07:00") is True

    def test_midnight_wrap_at_end(self):
        """07:00 (420 min) is NOT within [22:00, 07:00) — exclusive end."""
        assert _in_range(420, "22:00", "07:00") is False



class TestClassifyPeriod:
    """Test TOU period classification using the default Vector schedule.

    Default schedule (Vector / _SCHED_D):
      No night rate.
      Peak (weekday):   07:00-11:00, 17:00-21:00
      Off-Peak:         everything else on weekdays
      Weekend Off-Peak: all day on Sat/Sun
    """

    # Peak period tests (weekday only)
    def test_peak_morning(self):
        dt = datetime(2026, 3, 10, 8, 0)  # Monday 08:00
        assert classify_period(dt) == "peak"

    def test_peak_morning_start(self):
        dt = datetime(2026, 3, 10, 7, 0)  # Monday 07:00 (boundary)
        assert classify_period(dt) == "peak"

    def test_peak_morning_end_exclusive(self):
        dt = datetime(2026, 3, 10, 11, 0)  # Monday 11:00 (end, exclusive)
        assert classify_period(dt) == "offpeak"

    def test_peak_evening(self):
        dt = datetime(2026, 3, 12, 18, 0)  # Wednesday 18:00
        assert classify_period(dt) == "peak"

    def test_peak_evening_start(self):
        dt = datetime(2026, 3, 12, 17, 0)  # Wednesday 17:00 (boundary)
        assert classify_period(dt) == "peak"

    def test_peak_evening_end_exclusive(self):
        dt = datetime(2026, 3, 12, 21, 0)  # Wednesday 21:00 (end, exclusive)
        assert classify_period(dt) == "offpeak"

    # Off-Peak period tests (weekday)
    def test_offpeak_midday(self):
        dt = datetime(2026, 3, 10, 12, 0)  # Monday 12:00
        assert classify_period(dt) == "offpeak"

    def test_offpeak_after_morning_peak(self):
        dt = datetime(2026, 3, 10, 11, 0)  # Monday 11:00 (peak ends)
        assert classify_period(dt) == "offpeak"

    def test_offpeak_evening(self):
        dt = datetime(2026, 3, 10, 21, 0)  # Monday 21:00
        assert classify_period(dt) == "offpeak"

    def test_offpeak_late_night(self):
        dt = datetime(2026, 3, 10, 23, 0)  # Monday 23:00 (no night rate)
        assert classify_period(dt) == "offpeak"

    def test_offpeak_early_morning(self):
        dt = datetime(2026, 3, 10, 3, 0)  # Monday 03:00 (no night rate)
        assert classify_period(dt) == "offpeak"

    # Weekend Off-Peak
    def test_weekend_offpeak_saturday(self):
        dt = datetime(2026, 3, 14, 12, 0)  # Saturday 12:00
        assert classify_period(dt) == "weekend_offpeak"

    def test_weekend_offpeak_sunday(self):
        dt = datetime(2026, 3, 15, 21, 30)  # Sunday 21:30
        assert classify_period(dt) == "weekend_offpeak"

    def test_weekend_offpeak_boundary(self):
        dt = datetime(2026, 3, 14, 7, 0)  # Saturday 07:00
        assert classify_period(dt) == "weekend_offpeak"

    def test_weekend_offpeak_early_morning(self):
        dt = datetime(2026, 3, 14, 3, 0)  # Saturday 03:00 (no night)
        assert classify_period(dt) == "weekend_offpeak"

    # Custom schedule with night
    def test_custom_schedule_with_night(self):
        custom = {
            "night_start": "23:00",
            "night_end": "06:00",
            "peak_weekday": [["08:00", "10:00"]],
            "weekend_offpeak": [["06:00", "23:00"]],
        }
        dt = datetime(2026, 3, 10, 22, 30)  # Monday 22:30
        assert classify_period(dt, custom) == "offpeak"  # not night until 23:00

    def test_none_schedule_uses_default(self):
        dt = datetime(2026, 3, 10, 8, 0)  # Monday 08:00
        assert classify_period(dt, None) == "peak"

    def test_explicit_false_weekend_offpeak(self):
        """weekend_offpeak=False should NOT classify weekends as weekend_offpeak."""
        sched = {
            "effective_from": "2023-06-09T00:00:00",
            "night_start": "22:00",
            "night_end": "07:00",
            "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
            "weekend_offpeak": False,
        }
        dt = datetime(2026, 3, 14, 12, 0)  # Saturday 12:00
        # Should fall through to weekday peak logic (everyday schedule)
        assert classify_period(dt, sched) == "offpeak"


class TestClassifyPeriodNightSchedule:
    """Test classification for schedules with a night rate (e.g. Top Energy)."""

    SCHED = NETWORK_SCHEDULES["TopEnergy_1_"]

    def test_night_weekday_late(self):
        dt = datetime(2026, 3, 11, 23, 0)  # Wednesday 23:00
        assert classify_period(dt, self.SCHED) == "night"

    def test_night_weekday_early(self):
        dt = datetime(2026, 3, 11, 3, 0)  # Wednesday 03:00
        assert classify_period(dt, self.SCHED) == "night"

    def test_night_weekend_late(self):
        dt = datetime(2026, 3, 14, 22, 30)  # Saturday 22:30
        assert classify_period(dt, self.SCHED) == "night"

    def test_night_weekend_early(self):
        dt = datetime(2026, 3, 15, 6, 30)  # Sunday 06:30
        assert classify_period(dt, self.SCHED) == "night"

    def test_peak_morning(self):
        dt = datetime(2026, 3, 10, 8, 0)  # Monday 08:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_peak_evening(self):
        dt = datetime(2026, 3, 12, 18, 0)  # Wednesday 18:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_offpeak_midday(self):
        dt = datetime(2026, 3, 10, 12, 0)  # Monday 12:00
        assert classify_period(dt, self.SCHED) == "offpeak"

    def test_weekend_offpeak(self):
        dt = datetime(2026, 3, 14, 12, 0)  # Saturday 12:00
        assert classify_period(dt, self.SCHED) == "weekend_offpeak"


class TestClassifyPeriodEverydaySchedule:
    """Test for 'everyday' schedules (no weekend_offpeak)."""

    SCHED = NETWORK_SCHEDULES["Unison_1_"]  # Night 23-07, Peak ED 07-11+17-21

    def test_night_late(self):
        dt = datetime(2026, 3, 11, 23, 30)  # Wednesday 23:30
        assert classify_period(dt, self.SCHED) == "night"

    def test_night_early(self):
        dt = datetime(2026, 3, 11, 5, 0)  # Wednesday 05:00
        assert classify_period(dt, self.SCHED) == "night"

    def test_peak_weekday(self):
        dt = datetime(2026, 3, 10, 8, 0)  # Monday 08:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_peak_weekend(self):
        """Everyday schedule: peak applies on weekends too."""
        dt = datetime(2026, 3, 14, 8, 0)  # Saturday 08:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_offpeak_weekday(self):
        dt = datetime(2026, 3, 10, 12, 0)  # Monday 12:00
        assert classify_period(dt, self.SCHED) == "offpeak"

    def test_offpeak_weekend(self):
        """Everyday schedule: offpeak between peaks on weekend."""
        dt = datetime(2026, 3, 14, 12, 0)  # Saturday 12:00
        assert classify_period(dt, self.SCHED) == "offpeak"

    def test_night_weekend(self):
        dt = datetime(2026, 3, 15, 0, 30)  # Sunday 00:30
        assert classify_period(dt, self.SCHED) == "night"


class TestClassifyPeriodDayNightOnly:
    """Test day/night-only schedules (e.g. Scanpower)."""

    SCHED = NETWORK_SCHEDULES["ScanPower"]  # Night 23-07, Peak 07-23

    def test_night(self):
        dt = datetime(2026, 3, 11, 0, 0)  # Wednesday midnight
        assert classify_period(dt, self.SCHED) == "night"

    def test_day_weekday(self):
        """Peak covers all of daytime."""
        dt = datetime(2026, 3, 10, 12, 0)  # Monday 12:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_day_weekend(self):
        """Everyday: peak on weekends too."""
        dt = datetime(2026, 3, 14, 12, 0)  # Saturday 12:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_boundary_night_to_day(self):
        dt = datetime(2026, 3, 10, 7, 0)  # Monday 07:00
        assert classify_period(dt, self.SCHED) == "peak"

    def test_boundary_day_to_night(self):
        dt = datetime(2026, 3, 10, 23, 0)  # Monday 23:00
        assert classify_period(dt, self.SCHED) == "night"


class TestGetBoundaryTimes:
    def test_default_schedule_boundaries(self):
        """Default (Vector) schedule has no night; 4 peak boundaries + weekend."""
        times = get_boundary_times()
        assert (7, 0) in times
        assert (11, 0) in times
        assert (17, 0) in times
        assert (21, 0) in times

    def test_night_schedule_boundaries(self):
        """Schedule with night rate should include night boundaries."""
        sched = NETWORK_SCHEDULES["TopEnergy_1_"]
        times = get_boundary_times(sched)
        assert (7, 0) in times
        assert (9, 30) in times
        assert (17, 30) in times
        assert (20, 0) in times
        assert (22, 0) in times

    def test_sorted(self):
        times = get_boundary_times()
        assert times == sorted(times)

    def test_custom_schedule(self):
        custom = {
            "night_start": "23:00",
            "night_end": "06:00",
            "peak_weekday": [["08:00", "10:00"]],
            "weekend_offpeak": [["06:00", "23:00"]],
        }
        times = get_boundary_times(custom)
        assert (6, 0) in times
        assert (8, 0) in times
        assert (10, 0) in times
        assert (23, 0) in times

    def test_none_uses_default(self):
        assert get_boundary_times(None) == get_boundary_times(_DEFAULT_SCHEDULE)



class TestScheduleCacheGetScheduleFor:
    def _make_cache(self, schedules: list[dict]) -> ScheduleCache:
        cache = ScheduleCache(cache_dir=None)
        cache._data = {"schedules": schedules}
        cache._loaded = True
        return cache

    def test_none_returns_latest(self):
        s1 = {"effective_from": "2023-06-01T00:00:00", "night_start": "22:00", "night_end": "07:00"}
        s2 = {"effective_from": "2025-01-01T00:00:00", "night_start": "23:00", "night_end": "06:00"}
        cache = self._make_cache([s1, s2])
        assert cache.get_schedule_for(None) == s2

    def test_dt_between_schedules(self):
        s1 = {"effective_from": "2023-06-01T00:00:00", "night_start": "22:00", "night_end": "07:00"}
        s2 = {"effective_from": "2025-01-01T00:00:00", "night_start": "23:00", "night_end": "06:00"}
        cache = self._make_cache([s1, s2])
        dt = datetime(2024, 6, 15, 12, 0)
        result = cache.get_schedule_for(dt)
        assert result == s1  # before s2's effective_from

    def test_dt_after_all_schedules(self):
        s1 = {"effective_from": "2023-06-01T00:00:00", "night_start": "22:00", "night_end": "07:00"}
        s2 = {"effective_from": "2025-01-01T00:00:00", "night_start": "23:00", "night_end": "06:00"}
        cache = self._make_cache([s1, s2])
        dt = datetime(2026, 3, 15, 12, 0)
        result = cache.get_schedule_for(dt)
        assert result == s2

    def test_dt_before_all_schedules(self):
        s1 = {"effective_from": "2023-06-01T00:00:00", "night_start": "22:00", "night_end": "07:00"}
        cache = self._make_cache([s1])
        dt = datetime(2020, 1, 1, 12, 0)
        result = cache.get_schedule_for(dt)
        assert result == s1  # falls back to oldest

    def test_empty_schedules_returns_default(self):
        cache = self._make_cache([])
        result = cache.get_schedule_for(datetime.now())
        assert result == _DEFAULT_SCHEDULE



class TestScheduleCacheNeedsCheck:
    def _make_cache(self, last_checked: str | None) -> ScheduleCache:
        cache = ScheduleCache(cache_dir=None)
        cache._data = {"last_checked": last_checked}
        cache._loaded = True
        return cache

    def test_none_needs_check(self):
        assert self._make_cache(None).needs_check() is True

    def test_stale_needs_check(self):
        old = datetime(2020, 1, 1).isoformat()
        assert self._make_cache(old).needs_check() is True

    def test_recent_no_check(self):
        recent = datetime.now().isoformat()
        assert self._make_cache(recent).needs_check() is False

    def test_invalid_timestamp(self):
        assert self._make_cache("not-a-date").needs_check() is True

    def test_month_boundary_triggers_check(self):
        """Check triggers when a new calendar month has started."""
        now = datetime.now()
        if now.month == 1:
            prev_month = datetime(now.year - 1, 12, 15)
        else:
            prev_month = datetime(now.year, now.month - 1, 15)
        assert self._make_cache(prev_month.isoformat()).needs_check() is True

    def test_same_month_no_check(self):
        """No check if still in the same month and cache is young."""
        recent = datetime(datetime.now().year, datetime.now().month, 1, 0, 1).isoformat()
        assert self._make_cache(recent).needs_check() is False


class TestScheduleSummary:
    """Verify schedule_summary returns clean boolean for weekend_offpeak."""

    def _make_cache(self, schedule: dict, network_name: str = "Test") -> ScheduleCache:
        cache = ScheduleCache(cache_dir=None)
        cache._data = {
            "network_name": network_name,
            "schedules": [schedule],
            "scheme_hash": "abc123",
            "last_checked": "2024-01-01T00:00:00",
        }
        cache._loaded = True
        return cache

    def test_weekend_offpeak_true(self):
        sched = {
            "effective_from": "2023-06-09T00:00:00",
            "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
            "weekend_offpeak": True,
        }
        summary = self._make_cache(sched, "Vector").schedule_summary
        assert summary["weekend_offpeak"] is True

    def test_weekend_offpeak_false_when_absent(self):
        sched = {
            "effective_from": "2023-06-09T00:00:00",
            "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
        }
        summary = self._make_cache(sched, "Centralines").schedule_summary
        assert summary["weekend_offpeak"] is False

    def test_summary_is_bool_not_list(self):
        sched = {
            "effective_from": "2023-06-09T00:00:00",
            "peak_weekday": [["07:00", "21:00"]],
            "weekend_offpeak": True,
        }
        summary = self._make_cache(sched).schedule_summary
        assert isinstance(summary["weekend_offpeak"], bool)


class TestNetworkChangeSeedSchedule:
    """Verify that changing the network appends only when schedule differs."""

    def _make_cache(self, network: str = "Vector") -> ScheduleCache:
        cache = ScheduleCache(cache_dir=None)
        cache._data = {
            "network": network,
            "network_name": "Vector, United Networks (Auckland)",
            "schedules": [
                {
                    "effective_from": "2023-01-01T00:00:00",
                    "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
                    "weekend_offpeak": True,
                }
            ],
        }
        cache._loaded = True
        return cache

    def test_network_change_appends_schedule(self):
        cache = self._make_cache("Vector")
        assert len(cache._data["schedules"]) == 1
        cache.network = "Orion"
        assert len(cache._data["schedules"]) == 2
        assert cache._data["network"] == "Orion"

    def test_same_network_no_change(self):
        cache = self._make_cache("Vector")
        cache.network = "Vector"
        assert len(cache._data["schedules"]) == 1

    def test_new_schedule_matches_network(self):
        from meridian_energy.schedule import NETWORK_SCHEDULES
        cache = self._make_cache("Vector")
        cache.network = "TopEnergy_1_"
        new_sched = cache._data["schedules"][-1]
        expected = NETWORK_SCHEDULES["TopEnergy_1_"]
        assert new_sched["peak_weekday"] == expected["peak_weekday"]

    def test_unknown_network_uses_default(self):
        from meridian_energy.schedule import _DEFAULT_SCHEDULE
        cache = self._make_cache("Vector")
        cache.network = "UnknownNetwork_1_"
        new_sched = cache._data["schedules"][-1]
        assert new_sched["peak_weekday"] == _DEFAULT_SCHEDULE["peak_weekday"]

    def test_identical_schedule_skips_append(self):
        """Switching to a network with the same TOU boundaries doesn't duplicate."""
        from meridian_energy.schedule import NETWORK_SCHEDULES
        # CountiesPower uses the same schedule (SCHED_D) as Vector
        cache = self._make_cache("Vector")
        assert len(cache._data["schedules"]) == 1
        cache.network = "CountiesPower"
        assert len(cache._data["schedules"]) == 1
        assert cache._data["network"] == "CountiesPower"

    def test_different_schedule_does_append(self):
        """Switching to a network with different boundaries appends."""
        cache = self._make_cache("Vector")
        cache.network = "TopEnergy_1_"  # SCHED_A, different from SCHED_D
        assert len(cache._data["schedules"]) == 2


class TestUpdateScheduleSemantics:
    """Verify update_schedule handles boolean weekend_offpeak correctly."""

    def _make_cache(self) -> ScheduleCache:
        cache = ScheduleCache(cache_dir=None)
        cache._data = {
            "network": "Vector",
            "network_name": "Vector",
            "schedules": [],
        }
        cache._loaded = True
        return cache

    def test_boolean_weekend_offpeak_stored(self):
        cache = self._make_cache()
        cache.update_schedule({
            "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
            "weekend_offpeak": True,
        })
        sched = cache._data["schedules"][-1]
        assert sched["weekend_offpeak"] is True

    def test_no_weekend_offpeak_when_false(self):
        cache = self._make_cache()
        cache.update_schedule({
            "night_start": "23:00",
            "night_end": "07:00",
            "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
        })
        sched = cache._data["schedules"][-1]
        assert "weekend_offpeak" not in sched

    def test_classification_uses_new_schedule(self):
        cache = self._make_cache()
        cache.update_schedule({
            "peak_weekday": [["07:00", "21:00"]],
            "weekend_offpeak": True,
        })
        sched = cache._data["schedules"][-1]
        # Weekend should classify as weekend_offpeak
        dt_sat = datetime(2025, 3, 15, 14, 0)  # Saturday
        assert classify_period(dt_sat, sched) == "weekend_offpeak"

    def test_effective_from_auto_set(self):
        cache = self._make_cache()
        cache.update_schedule({
            "peak_weekday": [["07:00", "11:00"]],
        })
        sched = cache._data["schedules"][-1]
        assert "effective_from" in sched
