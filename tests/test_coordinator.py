"""Tests for the v2 coordinator module."""

import asyncio
from collections import defaultdict
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from meridian_energy.coordinator import MeridianCoordinator, MeridianData, _energy_stat_id, _cost_stat_id
from meridian_energy.const import DEFAULT_BRAND, DOMAIN, PERIOD_OFFPEAK, PERIOD_CONTROLLED, OVERLAP_DAYS

NZ_TZ = ZoneInfo("Pacific/Auckland")


def _make_coordinator(**kwargs):
    """Build a MeridianCoordinator with mocked HA dependencies."""
    api = MagicMock()
    api.brand = kwargs.pop("brand", "powershop")
    hass = MagicMock()
    entry = MagicMock()
    with patch.object(MeridianCoordinator, "__init__", lambda self, *a, **kw: None):
        coord = MeridianCoordinator.__new__(MeridianCoordinator)
    coord._api = api
    coord._brand = api.brand
    coord._sensor_name = "Powershop"
    coord._rates = kwargs.pop("rates", {})
    coord._daily_charge = kwargs.pop("daily_charge", 4.14)
    coord._detected_periods = kwargs.pop("detected_periods", [])
    coord._rate_to_period = kwargs.pop("rate_to_period", {})
    coord._label_to_period = kwargs.pop("label_to_period", {})
    coord._energy_sums = defaultdict(float)
    coord._cost_sums = defaultdict(float)
    coord._daily_charge_sum = 0.0
    coord._solar_sum = 0.0
    coord._schedule = kwargs.pop("schedule", {})
    return coord


class TestMeridianData:
    def test_default_brand(self):
        d = MeridianData()
        assert d.brand == DEFAULT_BRAND

    def test_default_tou_period(self):
        d = MeridianData()
        assert d.tou_period == PERIOD_OFFPEAK

    def test_default_rates_empty(self):
        d = MeridianData()
        assert d.rates == {}

    def test_default_daily_charge(self):
        d = MeridianData()
        assert d.daily_charge == 0.0

    def test_default_solar(self):
        d = MeridianData()
        assert d.solar_export_kwh == 0.0
        assert d.has_solar is False

    def test_default_balance(self):
        d = MeridianData()
        assert d.balance is None

    def test_default_billing_fields(self):
        d = MeridianData()
        assert d.billing_period_start is None
        assert d.billing_period_end is None
        assert d.next_billing_date is None

    def test_billing_fields_set(self):
        d = MeridianData(
            billing_period_start="2026-04-10",
            billing_period_end="2026-05-09",
            next_billing_date="2026-05-10",
        )
        assert d.billing_period_start == "2026-04-10"
        assert d.billing_period_end == "2026-05-09"
        assert d.next_billing_date == "2026-05-10"

    def test_default_schedule(self):
        d = MeridianData()
        assert d.schedule == {}

    def test_default_detected_periods(self):
        d = MeridianData()
        assert d.detected_periods == []

    def test_custom_values(self):
        d = MeridianData(
            brand="meridian",
            sensor_name="Meridian Energy",
            rates={"night": 0.2362, "peak": 0.4077},
            daily_charge=4.14,
            product="ST06",
            tou_period="night",
            current_rate=0.2362,
        )
        assert d.brand == "meridian"
        assert d.tou_period == "night"
        assert d.current_rate == 0.2362
        assert d.daily_charge == 4.14


class TestStatisticIds:
    def test_energy_stat_id_night(self):
        assert _energy_stat_id("night") == "meridian_energy:consumption_night"

    def test_energy_stat_id_peak(self):
        assert _energy_stat_id("peak") == "meridian_energy:consumption_peak"

    def test_energy_stat_id_offpeak(self):
        assert _energy_stat_id("offpeak") == "meridian_energy:consumption_offpeak"

    def test_energy_stat_id_weekend_offpeak(self):
        assert _energy_stat_id("weekend_offpeak") == "meridian_energy:consumption_weekend_offpeak"

    def test_energy_stat_id_controlled(self):
        assert _energy_stat_id("controlled") == "meridian_energy:consumption_controlled"

    def test_cost_stat_id_night(self):
        assert _cost_stat_id("night") == "meridian_energy:cost_night"

    def test_cost_stat_id_peak(self):
        assert _cost_stat_id("peak") == "meridian_energy:cost_peak"

    def test_cost_stat_id_offpeak(self):
        assert _cost_stat_id("offpeak") == "meridian_energy:cost_offpeak"

    def test_cost_stat_id_weekend_offpeak(self):
        assert _cost_stat_id("weekend_offpeak") == "meridian_energy:cost_weekend_offpeak"

    def test_cost_stat_id_controlled(self):
        assert _cost_stat_id("controlled") == "meridian_energy:cost_controlled"


class TestDynamicStatIds:
    """Stat IDs work for any period key — not just hardcoded ones."""

    def test_energy_stat_id_unknown_period(self):
        assert _energy_stat_id("ev") == "meridian_energy:consumption_ev"

    def test_cost_stat_id_unknown_period(self):
        assert _cost_stat_id("solar") == "meridian_energy:cost_solar"

    def test_stat_ids_for_arbitrary_strings(self):
        assert _energy_stat_id("ev_charging") == "meridian_energy:consumption_ev_charging"
        assert _cost_stat_id("ev_charging") == "meridian_energy:cost_ev_charging"


class TestDetectedPeriods:
    """MeridianData.detected_periods stores dynamically discovered periods."""

    def test_default_empty(self):
        d = MeridianData()
        assert d.detected_periods == []

    def test_standard_four_periods(self):
        d = MeridianData(detected_periods=["night", "peak", "offpeak", "controlled"])
        assert len(d.detected_periods) == 4

    def test_custom_periods_preserved(self):
        d = MeridianData(detected_periods=["night", "offpeak", "ev"])
        assert "ev" in d.detected_periods
        assert len(d.detected_periods) == 3

    def test_periods_order_preserved(self):
        periods = ["peak", "night", "offpeak"]
        d = MeridianData(detected_periods=periods)
        assert d.detected_periods == periods


class TestIdentifyStatPeriod:
    """Tests for MeridianCoordinator._identify_stat_period."""

    def test_standing_charge_returns_none(self):
        coord = _make_coordinator()
        stat = {"label": "STANDING_CHARGE_abc123", "value": None}
        assert coord._identify_stat_period(stat) is None

    def test_empty_label_returns_none(self):
        coord = _make_coordinator()
        assert coord._identify_stat_period({"label": ""}) is None
        assert coord._identify_stat_period({}) is None

    def test_consumption_charge_non_tou_is_controlled(self):
        coord = _make_coordinator()
        stat = {"label": "CONSUMPTION_CHARGE_controlhash", "value": "1.0"}
        assert coord._identify_stat_period(stat) == PERIOD_CONTROLLED

    def test_legacy_bucket_n9(self):
        coord = _make_coordinator()
        assert coord._identify_stat_period({"label": "N9"}) == "night"

    def test_legacy_bucket_pk5(self):
        coord = _make_coordinator()
        assert coord._identify_stat_period({"label": "PK5"}) == "peak"

    def test_legacy_bucket_opk10(self):
        coord = _make_coordinator()
        assert coord._identify_stat_period({"label": "OPK10"}) == "offpeak"

    def test_tou_hash_label_rate_match(self):
        coord = _make_coordinator(
            rate_to_period={23.62: "night", 40.77: "peak", 27.92: "offpeak"},
        )
        stat = {
            "label": "CONSUMPTION_CHARGE_TOU_plan1_nighthash",
            "value": "3.87",
            "costInclTax": {"estimatedAmount": "91.39"},
        }
        result = coord._identify_stat_period(stat)
        assert result == "night"

    def test_tou_hash_label_cached_after_first_lookup(self):
        coord = _make_coordinator(
            rate_to_period={23.62: "night"},
        )
        stat = {
            "label": "CONSUMPTION_CHARGE_TOU_plan1_nighthash",
            "value": "3.87",
            "costInclTax": {"estimatedAmount": "91.39"},
        }
        coord._identify_stat_period(stat)
        assert "CONSUMPTION_CHARGE_TOU_plan1_nighthash" in coord._label_to_period

    def test_tou_hash_no_rate_map_returns_none(self):
        coord = _make_coordinator(rate_to_period={})
        stat = {
            "label": "CONSUMPTION_CHARGE_TOU_plan1_hash",
            "value": "3.87",
            "costInclTax": {"estimatedAmount": "91.39"},
        }
        assert coord._identify_stat_period(stat) is None


class TestPublishConsumptionStats:
    """Regression tests for _publish_consumption_stats."""

    def test_daily_charge_accumulates_per_node(self, daily_cost_nodes):
        """Daily charge sum should increment once per day node."""
        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night", 40.77: "peak", 27.92: "offpeak"},
        )
        coord.hass = MagicMock()

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats(daily_cost_nodes)

        assert coord._daily_charge_sum == 4.14 * 2

    def test_incremental_daily_charge_sums_continue(self, daily_cost_nodes):
        """Incremental daily charge fetches must not reset cumulative sums."""
        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night", 40.77: "peak", 27.92: "offpeak"},
        )
        coord.hass = MagicMock()

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats(daily_cost_nodes)
            dc_after_first = coord._daily_charge_sum

            # Second call with same nodes
            coord._publish_daily_charge_stats(daily_cost_nodes)

        assert coord._daily_charge_sum == dc_after_first * 2


class TestExtractPeriodEntries:
    """Tests for _extract_period_entries (half-hourly metadata splitting)."""

    def test_tou_and_controlled_split(self):
        """Nodes with TOU + controlled labels produce separate entries."""
        coord = _make_coordinator(
            rate_to_period={23.62: "night", 40.77: "peak", 27.92: "offpeak"},
        )
        stats = [
            {"label": "STANDING_CHARGE_abc", "value": None,
             "costInclTax": {"estimatedAmount": "414"}},
            {"label": "CONSUMPTION_CHARGE_TOU_plan_nighthash",
             "value": "0.14", "costInclTax": {"estimatedAmount": "3.3068"}},
            {"label": "CONSUMPTION_CHARGE_ctrlhash",
             "value": "0.374", "costInclTax": {"estimatedAmount": "8.83388"}},
        ]
        entries = coord._extract_period_entries(stats)
        assert len(entries) == 2
        periods = {e[0] for e in entries}
        assert "night" in periods
        assert "controlled" in periods
        # Verify kWh values
        by_period = {e[0]: e for e in entries}
        assert by_period["night"][1] == 0.14
        assert by_period["controlled"][1] == 0.374

    def test_empty_stats_returns_empty(self):
        coord = _make_coordinator()
        assert coord._extract_period_entries([]) == []

    def test_standing_charge_only_returns_empty(self):
        coord = _make_coordinator()
        stats = [
            {"label": "STANDING_CHARGE_abc", "value": None,
             "costInclTax": {"estimatedAmount": "414"}},
        ]
        assert coord._extract_period_entries(stats) == []

    def test_cost_converted_from_cents_to_dollars(self):
        coord = _make_coordinator(
            rate_to_period={23.62: "night"},
        )
        stats = [
            {"label": "CONSUMPTION_CHARGE_TOU_plan_nighthash",
             "value": "1.0", "costInclTax": {"estimatedAmount": "23.62"}},
        ]
        entries = coord._extract_period_entries(stats)
        assert len(entries) == 1
        _, kwh, cost_nzd = entries[0]
        assert kwh == 1.0
        assert abs(cost_nzd - 0.2362) < 0.0001


class TestHourlyMetadataClassification:
    """Tests that hourly stats use metaData.statistics for per-period split."""

    def test_controlled_separated_from_tou(self):
        """Half-hourly nodes with metadata should split controlled from TOU."""
        coord = _make_coordinator(
            detected_periods=["night", "controlled"],
            rates={"night": 0.2362, "controlled": 0.2362},
            rate_to_period={23.62: "night"},
        )
        coord.hass = MagicMock()

        nodes = [
            {
                "startAt": "2026-04-08T00:00:00+12:00",
                "value": "0.514",
                "metaData": {"statistics": [
                    {"label": "STANDING_CHARGE_abc", "value": None,
                     "costInclTax": {"estimatedAmount": "414"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan_nighthash",
                     "value": "0.14", "costInclTax": {"estimatedAmount": "3.3068"}},
                    {"label": "CONSUMPTION_CHARGE_ctrlhash",
                     "value": "0.374", "costInclTax": {"estimatedAmount": "8.83388"}},
                ]},
            },
            {
                "startAt": "2026-04-08T00:30:00+12:00",
                "value": "0.144",
                "metaData": {"statistics": [
                    {"label": "STANDING_CHARGE_abc", "value": None,
                     "costInclTax": {"estimatedAmount": "414"}},
                    {"label": "CONSUMPTION_CHARGE_TOU_plan_nighthash",
                     "value": "0.144", "costInclTax": {"estimatedAmount": "3.40"}},
                    {"label": "CONSUMPTION_CHARGE_ctrlhash",
                     "value": "0", "costInclTax": {"estimatedAmount": "0"}},
                ]},
            },
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats(nodes)

        # Night should have 0.14 + 0.144 = 0.284
        assert abs(coord._energy_sums["night"] - 0.284) < 0.001
        # Controlled should have 0.374 + 0.0 = 0.374
        assert abs(coord._energy_sums["controlled"] - 0.374) < 0.001

    def test_fallback_to_schedule_without_metadata(self):
        """Nodes without metaData fall back to classify_period()."""
        schedule = {
            "scheme_name": "test",
            "timeslots": [
                {"period": "night", "bucket": "N9", "start": "00:00",
                 "end": "07:00", "weekdays": True, "weekends": True},
            ],
        }
        coord = _make_coordinator(
            detected_periods=["night"],
            rates={"night": 0.2362},
            schedule=schedule,
        )
        coord.hass = MagicMock()

        nodes = [
            {
                "startAt": "2026-04-08T01:00:00+12:00",
                "value": "0.5",
            },
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats(nodes)

        assert abs(coord._energy_sums["night"] - 0.5) < 0.001


class TestSeedFromLatest:
    """Tests for _async_seed_sums_from_db."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def _mock_get_last_statistics(self, stat_db):
        """Return a mock that simulates get_last_statistics.

        stat_db: dict mapping statistic_id -> list of
            {"sum": float, "start": float} sorted most-recent-first.
            ``start`` is a Unix timestamp (float), matching the real API.
            For convenience, if the value is a plain float, it is wrapped
            with a start timestamp well before any typical api_start.
        """
        old_ts = datetime(2020, 1, 1, tzinfo=NZ_TZ).timestamp()
        def _get_last(hass, nr, stat_id, convert, types):
            if stat_id in stat_db:
                raw = stat_db[stat_id]
                if isinstance(raw, (int, float)):
                    return {stat_id: [{"sum": raw, "start": old_ts}]}
                return {stat_id: raw}
            return {}
        return _get_last

    def test_seeds_energy_and_cost_sums(self):
        coord = _make_coordinator(detected_periods=["night", "peak", "offpeak", "controlled"])
        coord.hass = MagicMock()
        recorder_mock = MagicMock()
        recorder_mock.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *a, **kw: fn(*a, **kw)
        )
        db = {
            f"{DOMAIN}:consumption_night": 6368.16,
            f"{DOMAIN}:cost_night": 1155.63,
            f"{DOMAIN}:consumption_peak": 4724.07,
            f"{DOMAIN}:cost_peak": 1654.22,
            f"{DOMAIN}:cost_daily_charge": 4200.83,
            f"{DOMAIN}:return_to_grid": 123.45,
        }
        stat_ids = list(db.keys())
        with patch("meridian_energy.coordinator.get_instance", return_value=recorder_mock), \
             patch("meridian_energy.coordinator.get_last_statistics",
                   side_effect=self._mock_get_last_statistics(db)):
            result, last_states = self._run(coord._async_seed_from_latest(stat_ids))

        assert coord._energy_sums["night"] == 6368.16
        assert coord._cost_sums["night"] == 1155.63
        assert coord._energy_sums["peak"] == 4724.07
        assert coord._cost_sums["peak"] == 1654.22
        assert coord._daily_charge_sum == 4200.83
        assert coord._solar_sum == 123.45
        # Returns latest timestamps for each stat ID
        assert len(result) == len(db)

    def test_missing_stats_default_to_zero(self):
        coord = _make_coordinator(detected_periods=["night"])
        coord.hass = MagicMock()
        recorder_mock = MagicMock()
        recorder_mock.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *a, **kw: fn(*a, **kw)
        )
        stat_ids = [f"{DOMAIN}:consumption_night", f"{DOMAIN}:cost_night"]
        with patch("meridian_energy.coordinator.get_instance", return_value=recorder_mock), \
             patch("meridian_energy.coordinator.get_last_statistics", return_value={}):
            result, last_states = self._run(coord._async_seed_from_latest(stat_ids))

        assert coord._energy_sums["night"] == 0.0
        assert coord._cost_sums["night"] == 0.0
        # Empty DB → no skip timestamps
        assert result == {}
        assert last_states == {}

    def test_seeds_from_latest_entry(self):
        """Seeding uses the latest DB entry (not a specific api_start)."""
        coord = _make_coordinator(detected_periods=["night"])
        coord.hass = MagicMock()
        recorder_mock = MagicMock()
        recorder_mock.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *a, **kw: fn(*a, **kw)
        )
        # DB has entries at Apr 9 (most recent) and older ones
        db = {
            f"{DOMAIN}:consumption_night": [
                {"sum": 6378.82, "start": datetime(2026, 4, 9, tzinfo=NZ_TZ).timestamp()},
                {"sum": 6371.97, "start": datetime(2026, 4, 7, tzinfo=NZ_TZ).timestamp()},
                {"sum": 6368.16, "start": datetime(2026, 4, 5, tzinfo=NZ_TZ).timestamp()},
            ],
        }
        stat_ids = [f"{DOMAIN}:consumption_night"]
        with patch("meridian_energy.coordinator.get_instance", return_value=recorder_mock), \
             patch("meridian_energy.coordinator.get_last_statistics",
                   side_effect=self._mock_get_last_statistics(db)):
            result, last_states = self._run(coord._async_seed_from_latest(stat_ids))

        # Should seed from Apr 9 (latest entry)
        assert coord._energy_sums["night"] == 6378.82
        # Should return the latest timestamp
        assert f"{DOMAIN}:consumption_night" in result
        assert result[f"{DOMAIN}:consumption_night"] == datetime(2026, 4, 9, tzinfo=NZ_TZ)

    def test_daily_charge_separate_from_consumption(self):
        """Daily charge uses its own sum, independent of consumption."""
        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night"},
        )
        coord.hass = MagicMock()

        node = {
            "startAt": "2026-04-07T00:00:00+12:00",
            "metaData": {"statistics": [
                {"label": "CONSUMPTION_CHARGE_TOU_plan1_nighthash",
                 "value": "3.8", "costInclTax": {"estimatedAmount": "89.76"}},
            ]},
        }

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats([node])

        assert coord._daily_charge_sum == 4.14


class TestHourlyConsumptionStats:
    """Tests for _publish_hourly_consumption_stats."""

    def _make_schedule(self):
        return {
            "scheme_name": "ST06",
            "timeslots": [
                {"period": "night", "bucket": "N9", "start": "00:00", "end": "07:00",
                 "weekdays": True, "weekends": True},
                {"period": "night", "bucket": "N9", "start": "22:00", "end": "00:00",
                 "weekdays": True, "weekends": True},
                {"period": "peak", "bucket": "PK5", "start": "07:00", "end": "09:30",
                 "weekdays": True, "weekends": False},
                {"period": "offpeak", "bucket": "OPK10", "start": "09:30", "end": "17:30",
                 "weekdays": True, "weekends": False},
                {"period": "peak", "bucket": "PK5", "start": "17:30", "end": "20:00",
                 "weekdays": True, "weekends": False},
                {"period": "offpeak", "bucket": "OPK10", "start": "20:00", "end": "22:00",
                 "weekdays": True, "weekends": False},
                {"period": "offpeak", "bucket": "OPK10", "start": "07:00", "end": "22:00",
                 "weekdays": False, "weekends": True},
            ],
        }

    def test_classifies_and_aggregates_hourly(self):
        """Half-hourly data should produce hourly stats per TOU period."""
        coord = _make_coordinator(
            rates={"night": 0.2362, "peak": 0.4077, "offpeak": 0.2792},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()

        # Two half-hours in the same night hour (00:00-00:30, 00:30-01:00)
        # on a weekday => night period
        nodes = [
            {"startAt": "2026-01-05T00:00:00+13:00", "value": "0.4"},
            {"startAt": "2026-01-05T00:30:00+13:00", "value": "0.3"},
            # Peak hour (07:00-07:30, 07:30-08:00) on weekday
            {"startAt": "2026-01-05T07:00:00+13:00", "value": "0.8"},
            {"startAt": "2026-01-05T07:30:00+13:00", "value": "0.6"},
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_hourly_consumption_stats(nodes)

        # Night: 0.4 + 0.3 = 0.7 kWh, cost = 0.7 * 0.2362
        assert abs(coord._energy_sums["night"] - 0.7) < 1e-9
        assert abs(coord._cost_sums["night"] - 0.7 * 0.2362) < 1e-9
        # Peak: 0.8 + 0.6 = 1.4 kWh, cost = 1.4 * 0.4077
        assert abs(coord._energy_sums["peak"] - 1.4) < 1e-9
        assert abs(coord._cost_sums["peak"] - 1.4 * 0.4077) < 1e-9
        # Should have published 4 statistics (2 periods × energy + cost)
        assert len(calls) == 4

    def test_skip_before_prevents_recount(self):
        """The boundary hour is re-aggregated (< not <=); entries
        strictly before skip_before are skipped.  Caller must adjust
        the seed sum by subtracting the last state first.
        """
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()

        node_a = {"startAt": "2026-01-05T00:00:00+13:00", "value": "0.5"}
        node_b = {"startAt": "2026-01-05T01:00:00+13:00", "value": "0.3"}

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats([node_a])
            assert abs(coord._energy_sums["night"] - 0.5) < 1e-9

            # Simulate adjusted seed: sum - state (as the caller does)
            coord._energy_sums["night"] = 0.5 - 0.5  # 0.0
            coord._cost_sums["night"] = 0.0
            skip = datetime(2026, 1, 5, 0, 0, tzinfo=NZ_TZ)
            coord._publish_hourly_consumption_stats(
                [node_a, node_b], skip_before=skip,
            )
            # node_a re-aggregated (0.5) + node_b (0.3) = 0.8
            assert abs(coord._energy_sums["night"] - 0.8) < 1e-9

    def test_seeded_sums_continue(self):
        """Pre-seeded sums should be continued, not overwritten."""
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()
        coord._energy_sums["night"] = 6368.16
        coord._cost_sums["night"] = 1155.63

        node = {"startAt": "2026-01-05T01:00:00+13:00", "value": "0.5"}

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats([node])

        assert abs(coord._energy_sums["night"] - (6368.16 + 0.5)) < 1e-9
        assert abs(coord._cost_sums["night"] - (1155.63 + 0.5 * 0.2362)) < 1e-9

    def test_weekend_classified_correctly(self):
        """Weekend hours should be classified as offpeak, not peak."""
        coord = _make_coordinator(
            rates={"night": 0.2362, "peak": 0.4077, "offpeak": 0.2792},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()

        # 2026-01-10 is a Saturday; 08:00 on weekend = offpeak
        nodes = [
            {"startAt": "2026-01-10T08:00:00+13:00", "value": "1.0"},
            {"startAt": "2026-01-10T08:30:00+13:00", "value": "0.5"},
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats(nodes)

        # Should be offpeak, not peak
        assert coord._energy_sums["offpeak"] == 1.5
        assert coord._energy_sums.get("peak", 0.0) == 0.0


class TestFutureTimestampFiltering:
    """Verify that publish methods skip entries with future timestamps."""

    def test_hourly_skips_future_hours(self):
        """Half-hourly data for a future hour should not be published."""
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule={
                "scheme_name": "ST06",
                "timeslots": [
                    {"period": "night", "bucket": "N9", "start": "00:00",
                     "end": "00:00", "weekdays": True, "weekends": True},
                ],
            },
        )
        coord.hass = MagicMock()

        # now = 03:00, so hour_start=04:00 is in the future
        now = datetime(2026, 4, 11, 3, 0, tzinfo=NZ_TZ)
        nodes = [
            {"startAt": "2026-04-11T02:00:00+12:00", "value": "0.5"},
            {"startAt": "2026-04-11T02:30:00+12:00", "value": "0.3"},
            # Future:
            {"startAt": "2026-04-11T04:00:00+12:00", "value": "0.9"},
            {"startAt": "2026-04-11T04:30:00+12:00", "value": "0.7"},
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_hourly_consumption_stats(
                nodes, now=now,
            )

        # Only past data (0.5 + 0.3 = 0.8) counted; future (0.9 + 0.7) skipped
        assert abs(coord._energy_sums["night"] - 0.8) < 1e-9

    def test_daily_charge_skips_future_days(self):
        """Daily charge for a future date should not be published."""
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()

        now = datetime(2026, 4, 10, 15, 0, tzinfo=NZ_TZ)
        # Node for Apr 10 = today (should publish), Apr 12 = future (skip)
        nodes = [
            {
                "startAt": "2026-04-10T00:00:00+12:00",
                "metaData": {"statistics": [{
                    "label": "STANDING_CHARGE_abc",
                    "costInclTax": {"estimatedAmount": "414"},
                }]},
            },
            {
                "startAt": "2026-04-12T00:00:00+12:00",
                "metaData": {"statistics": [{
                    "label": "STANDING_CHARGE_abc",
                    "costInclTax": {"estimatedAmount": "414"},
                }]},
            },
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_daily_charge_stats(
                nodes, now=now,
            )

        # Only 1 day published (Apr 10), not 2
        assert abs(coord._daily_charge_sum - 4.14) < 0.01

    def test_hourly_no_filter_when_now_none(self):
        """When now is None, all entries should be published."""
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule={
                "scheme_name": "ST06",
                "timeslots": [
                    {"period": "night", "bucket": "N9", "start": "00:00",
                     "end": "00:00", "weekdays": True, "weekends": True},
                ],
            },
        )
        coord.hass = MagicMock()

        nodes = [
            {"startAt": "2099-12-31T23:00:00+13:00", "value": "1.0"},
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats(
                nodes, now=None,
            )

        # Should still publish (no filters active)
        assert abs(coord._energy_sums["night"] - 1.0) < 1e-9


class TestDailyChargeTodayFallback:
    """Verify that the current day uses the full-day rate, not prorated."""

    def test_today_uses_full_daily_charge(self):
        """For today's node, code should use self._daily_charge not API."""
        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night"},
        )
        coord.hass = MagicMock()
        today_str = datetime.now(NZ_TZ).strftime("%Y-%m-%dT00:00:00+12:00")
        node = {
            "startAt": today_str,
            "metaData": {"statistics": [
                # API returns prorated standing charge (e.g., 67% of 414)
                {"label": "STANDING_CHARGE_abc", "value": None,
                 "costInclTax": {"estimatedAmount": "277"}},
            ]},
        }

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats([node])

        # Should use full day rate ($4.14), not prorated $2.77
        assert coord._daily_charge_sum == 4.14

    def test_past_day_uses_api_standing_charge(self):
        """Completed days should use the API's standing charge."""
        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night"},
        )
        coord.hass = MagicMock()
        # Use a past date
        node = {
            "startAt": "2026-03-15T00:00:00+13:00",
            "metaData": {"statistics": [
                {"label": "STANDING_CHARGE_abc", "value": None,
                 "costInclTax": {"estimatedAmount": "373.75"}},
            ]},
        }

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats([node])

        # Should use API's rate ($3.7375), not current ($4.14)
        assert abs(coord._daily_charge_sum - 3.7375) < 0.01


class TestDailyChargeMonotonicity:
    """Verify monotonicity check logs errors for non-increasing sums."""

    def test_monotonic_sums_no_error(self):
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()
        nodes = [
            {"startAt": "2026-04-06T00:00:00+12:00"},
            {"startAt": "2026-04-07T00:00:00+12:00"},
        ]
        with patch("meridian_energy.coordinator.async_add_external_statistics"), \
             patch("meridian_energy.coordinator._LOGGER") as mock_log:
            coord._publish_daily_charge_stats(nodes)
        mock_log.error.assert_not_called()


class TestBackfill:
    """Tests for the async_backfill method."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_backfill_seeds_and_publishes(self):
        """Backfill seeds from just before start_date, then publishes."""
        from datetime import date, timedelta

        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night"},
            detected_periods=["night"],
        )
        coord.hass = MagicMock()

        # Mock _async_refresh_rates
        coord._async_refresh_rates = AsyncMock()
        coord._last_rates_refresh = None

        # Mock recorder for statistics_during_period seeding
        recorder_mock = MagicMock()
        recorder_mock.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *a, **kw: fn(*a, **kw)
        )

        # Seed DB: latest entry before Apr 6 has consumption_night=100, cost_night=20
        def mock_stats_during_period(
            hass, start_time, end_time, stat_ids, period, units, types
        ):
            seed_ts = datetime(2026, 4, 5, 0, 0, tzinfo=NZ_TZ).timestamp()
            result = {}
            for sid in stat_ids:
                if sid == f"{DOMAIN}:consumption_night":
                    result[sid] = [{"sum": 100.0, "start": seed_ts}]
                elif sid == f"{DOMAIN}:cost_night":
                    result[sid] = [{"sum": 20.0, "start": seed_ts}]
                elif sid == f"{DOMAIN}:cost_daily_charge":
                    result[sid] = [{"sum": 4200.0, "start": seed_ts}]
            return result

        # Mock API responses
        hh_node = {
            "startAt": "2026-04-06T08:00:00+12:00",
            "endAt": "2026-04-06T08:30:00+12:00",
            "value": "1.5",
        }
        daily_node = {
            "startAt": "2026-04-06T00:00:00+12:00",
            "metaData": {"statistics": [
                {"label": "STANDING_CHARGE_abc", "value": None,
                 "costInclTax": {"estimatedAmount": "414"}},
            ]},
        }
        coord._api.async_get_daily_cost_measurements = AsyncMock(
            return_value=[daily_node],
        )
        coord._api.async_get_measurements = AsyncMock(
            return_value=[hh_node],
        )

        with patch("meridian_energy.coordinator.get_instance", return_value=recorder_mock), \
             patch("meridian_energy.coordinator.statistics_during_period",
                   side_effect=mock_stats_during_period), \
             patch("meridian_energy.coordinator.async_add_external_statistics"), \
             patch.object(coord, "_async_delete_statistics_range",
                          new_callable=AsyncMock, return_value=0):
            self._run(coord.async_backfill(date(2026, 4, 6)))

        # Daily charge should have been seeded from 4200 + 4.14
        assert coord._daily_charge_sum == 4200.0 + 4.14
        # Energy sums should have been seeded from 100
        assert coord._energy_sums["night"] == 100.0

    def test_backfill_calls_delete_statistics_range(self):
        """Backfill should delete existing stats before re-publishing."""
        from datetime import date

        coord = _make_coordinator(
            daily_charge=4.14,
            rate_to_period={23.62: "night"},
            detected_periods=["night"],
        )
        coord.hass = MagicMock()
        coord._async_refresh_rates = AsyncMock()
        coord._last_rates_refresh = None

        recorder_mock = MagicMock()
        recorder_mock.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *a, **kw: fn(*a, **kw)
        )

        def mock_stats_during_period(
            hass, start_time, end_time, stat_ids, period, units, types
        ):
            return {}

        coord._api.async_get_daily_cost_measurements = AsyncMock(return_value=[])
        coord._api.async_get_measurements = AsyncMock(return_value=[])

        delete_mock = AsyncMock(return_value=0)
        with patch("meridian_energy.coordinator.get_instance", return_value=recorder_mock), \
             patch("meridian_energy.coordinator.statistics_during_period",
                   side_effect=mock_stats_during_period), \
             patch("meridian_energy.coordinator.async_add_external_statistics"), \
             patch.object(coord, "_async_delete_statistics_range",
                          new=delete_mock):
            self._run(coord.async_backfill(date(2026, 4, 6)))

        # Verify _async_delete_statistics_range was called with correct range
        delete_mock.assert_called_once()
        call_args = delete_mock.call_args
        start_arg = call_args[0][0]
        end_arg = call_args[0][1]
        assert start_arg.date() == date(2026, 4, 6)
        # end extends to now or beyond the end_date
        assert end_arg > start_arg


class TestDailyChargeSanityCheck:
    """Verify anomalous standing charges are rejected."""

    def test_normal_standing_charge_used(self):
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()
        node = {
            "startAt": "2026-04-08T00:00:00+12:00",
            "metaData": {"statistics": [{
                "label": "STANDING_CHARGE_abc",
                "value": 0.0,
                "costInclTax": {"estimatedAmount": 414},
            }]},
        }
        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats([node])
        assert abs(coord._daily_charge_sum - 4.14) < 0.01

    def test_anomalous_standing_charge_rejected(self):
        """Standing charge >2x daily rate should fall back to known rate."""
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()
        node = {
            "startAt": "2026-04-08T00:00:00+12:00",
            "metaData": {"statistics": [{
                "label": "STANDING_CHARGE_abc",
                "value": 0.0,
                "costInclTax": {"estimatedAmount": 1317},
            }]},
        }
        with patch("meridian_energy.coordinator.async_add_external_statistics"), \
             patch("meridian_energy.coordinator._LOGGER") as mock_log:
            coord._publish_daily_charge_stats([node])
        # Should use known rate, not the anomalous API value
        assert abs(coord._daily_charge_sum - 4.14) < 0.01
        mock_log.warning.assert_called()

    def test_multiple_days_sanity_check_runs(self):
        """Sanity check iterates over multiple nodes without errors."""
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()
        nodes = [
            {
                "startAt": "2026-04-08T00:00:00+12:00",
                "metaData": {"statistics": [{
                    "label": "STANDING_CHARGE_abc",
                    "value": 0.0,
                    "costInclTax": {"estimatedAmount": 414},
                }]},
            },
            {
                "startAt": "2026-04-09T00:00:00+12:00",
                "metaData": {"statistics": [{
                    "label": "STANDING_CHARGE_abc",
                    "value": 0.0,
                    "costInclTax": {"estimatedAmount": 414},
                }]},
            },
        ]
        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_daily_charge_stats(nodes)
        assert abs(coord._daily_charge_sum - 8.28) < 0.01


class TestStatisticDataStateField:
    """Verify that published StatisticData includes both state and sum fields."""

    @staticmethod
    def _make_schedule():
        return {
            "scheme_name": "ST06",
            "timeslots": [
                {"period": "night", "bucket": "N9", "start": "00:00", "end": "07:00",
                 "weekdays": True, "weekends": True},
                {"period": "night", "bucket": "N9", "start": "21:00", "end": "00:00",
                 "weekdays": True, "weekends": True},
                {"period": "peak", "bucket": "PK5", "start": "07:00", "end": "21:00",
                 "weekdays": True, "weekends": False},
                {"period": "offpeak", "bucket": "OPK10", "start": "07:00", "end": "21:00",
                 "weekdays": False, "weekends": True},
            ],
        }

    def test_hourly_stats_include_state(self):
        """Hourly consumption StatisticData should include per-interval state."""
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()

        nodes = [
            {"startAt": "2026-01-05T00:00:00+13:00", "value": "0.4"},
            {"startAt": "2026-01-05T00:30:00+13:00", "value": "0.3"},
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_hourly_consumption_stats(nodes)

        # calls are (hass, metadata, stats_list) tuples
        found = False
        for _hass, _meta, stat_list in calls:
            if stat_list and "state" in stat_list[0]:
                entry = stat_list[0]
                assert "state" in entry
                assert "sum" in entry
                assert entry["state"] >= 0
                found = True
                break
        assert found, "No StatisticData with 'state' field found"

    def test_daily_charge_stats_include_state(self):
        """Daily charge StatisticData should include per-day state."""
        coord = _make_coordinator(daily_charge=4.14)
        coord.hass = MagicMock()

        node = {
            "startAt": "2026-04-08T00:00:00+12:00",
            "metaData": {"statistics": [{
                "label": "STANDING_CHARGE_abc",
                "value": 0.0,
                "costInclTax": {"estimatedAmount": 414},
            }]},
        }

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_daily_charge_stats([node])

        # calls are (hass, metadata, stats_list) tuples
        for _hass, meta, stat_list in calls:
            sid = meta.get("statistic_id", "") if isinstance(meta, dict) else getattr(meta, "statistic_id", "")
            if sid.endswith("cost_daily_charge") and stat_list:
                assert "state" in stat_list[0]
                assert abs(stat_list[0]["state"] - 4.14) < 0.01
                break
        else:
            assert False, "cost_daily_charge stat not found"

    def test_solar_stats_include_state(self):
        """Solar export StatisticData should include per-day state."""
        coord = _make_coordinator()
        coord.hass = MagicMock()

        nodes = [
            {"startAt": "2026-04-08T00:00:00+12:00", "value": "5.3"},
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_solar_stats(nodes)

        assert len(calls) == 1
        _hass, _meta, stat_list = calls[0]
        assert stat_list[0]["state"] == 5.3
        assert stat_list[0]["sum"] == 5.3

    def test_solar_skip_boundary_preserved(self):
        """Solar skip_before from DB seed should not be overwritten."""
        coord = _make_coordinator()
        coord.hass = MagicMock()

        # Node before skip boundary should be skipped, node after published
        skip = datetime(2026, 4, 8, 0, 0, tzinfo=NZ_TZ)
        nodes = [
            {"startAt": "2026-04-08T00:00:00+12:00", "value": "3.0"},
            {"startAt": "2026-04-09T00:00:00+12:00", "value": "5.0"},
        ]

        calls = []
        with patch("meridian_energy.coordinator.async_add_external_statistics",
                   side_effect=lambda *a: calls.append(a)):
            coord._publish_solar_stats(nodes, skip_before=skip)

        assert len(calls) == 1
        _hass, _meta, stat_list = calls[0]
        # Only Apr 9 should be published (Apr 8 at-or-before skip)
        assert len(stat_list) == 1
        assert stat_list[0]["state"] == 5.0


class TestNegativeValueClamping:
    """Verify that negative consumption/cost values are clamped to 0."""

    @staticmethod
    def _make_schedule():
        return {
            "scheme_name": "ALLNIGHT",
            "timeslots": [
                {"period": "night", "bucket": "N1", "start": "00:00", "end": "00:00",
                 "weekdays": True, "weekends": True},
            ],
        }

    def test_negative_half_hourly_value_clamped(self):
        """Negative hourly consumption should be clamped to 0."""
        coord = _make_coordinator(
            rates={"night": 0.2362},
            schedule=self._make_schedule(),
        )
        coord.hass = MagicMock()

        # Two separate hours: first negative, second positive
        nodes = [
            {"startAt": "2026-01-05T00:00:00+13:00", "value": "-0.5"},
            {"startAt": "2026-01-05T01:00:00+13:00", "value": "0.3"},
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_hourly_consumption_stats(nodes)

        # -0.5 clamped to 0, so only 0.3 kWh counted
        assert abs(coord._energy_sums["night"] - 0.3) < 1e-9

    def test_negative_solar_clamped(self):
        """Negative solar export should be clamped to 0."""
        coord = _make_coordinator()
        coord.hass = MagicMock()

        nodes = [
            {"startAt": "2026-04-08T00:00:00+12:00", "value": "-3.0"},
        ]

        with patch("meridian_energy.coordinator.async_add_external_statistics"):
            coord._publish_solar_stats(nodes)

        assert coord._solar_sum == 0.0


class TestOverlapConstant:
    """Verify the overlap window constant."""

    def test_overlap_days_constant(self):
        """OVERLAP_DAYS should be a positive integer."""
        assert OVERLAP_DAYS >= 1
        assert isinstance(OVERLAP_DAYS, int)
