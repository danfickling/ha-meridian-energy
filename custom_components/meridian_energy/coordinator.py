"""DataUpdateCoordinator for Meridian Energy / Powershop integration (v2).

Fetches rates, TOU schedules, consumption measurements, daily costs,
and ledger balances via the Kraken GraphQL API.  Publishes external
statistics to the HA recorder for the energy dashboard.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import aiohttp
from zoneinfo import ZoneInfo

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
    StatisticMeanType,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.exceptions import ConfigEntryAuthFailed

from .api import MeridianEnergyApi, AuthError, ApiError
from .rates import parse_rates, classify_bucket, period_display_name
from .schedule import parse_tou_scheme, classify_period, get_boundary_times
from .const import (
    DOMAIN,
    USAGE_UPDATE_INTERVAL,
    RATES_UPDATE_INTERVAL,
    BRAND_CONFIG,
    DEFAULT_BRAND,
    DEFAULT_LOOKBACK_DAYS,
    ESTIMATE_PRECISION_THRESHOLD,
    PERIOD_CONTROLLED,
    PERIOD_OFFPEAK,
)

_LOGGER = logging.getLogger(__name__)

NZ_TZ = ZoneInfo("Pacific/Auckland")

# Data freshness threshold — if the most recent API measurement is older
# than this, raise a repair issue so the user knows data is stale.
STALE_DATA_THRESHOLD = timedelta(hours=48)


def _is_estimated_value(value_str: str) -> bool:
    """Return True if a measurement value appears to be estimated.

    Actual meter reads have ≤ ``ESTIMATE_PRECISION_THRESHOLD`` significant
    decimal digits (e.g. ``"24.854000000000000000"`` → 3).  Estimated /
    interpolated values from the API have many more (e.g. 26–28 digits
    like ``"21.28208437045405498041797199"``).
    """
    if not isinstance(value_str, str) or "." not in value_str:
        return False
    decimal_part = value_str.split(".")[1].rstrip("0")
    return len(decimal_part) > ESTIMATE_PRECISION_THRESHOLD


def _energy_stat_id(period: str) -> str:
    return f"{DOMAIN}:consumption_{period}"


def _cost_stat_id(period: str) -> str:
    return f"{DOMAIN}:cost_{period}"


@dataclass
class MeridianData:
    """Data returned by the coordinator for sensor entities."""

    brand: str = DEFAULT_BRAND
    sensor_name: str = "Powershop"

    # Rates
    rates: dict[str, float] = field(default_factory=dict)
    daily_charge: float = 0.0
    product: str = ""

    # TOU
    tou_period: str = PERIOD_OFFPEAK
    current_rate: float = 0.0
    schedule: dict = field(default_factory=dict)

    # Solar
    solar_export_kwh: float = 0.0
    has_solar: bool = False

    # Balance (dict with ahead, future_packs)
    balance: dict[str, float | None] | None = None

    # Billing cycle
    billing_period_start: str | None = None
    billing_period_end: str | None = None
    next_billing_date: str | None = None

    # Status
    last_usage_update: datetime | None = None
    detected_periods: list[str] = field(default_factory=list)


class MeridianCoordinator(DataUpdateCoordinator[MeridianData]):
    """Coordinate energy data: rates, usage, statistics, balance."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: MeridianEnergyApi,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            config_entry=entry,
            update_interval=USAGE_UPDATE_INTERVAL,
            always_update=False,
        )
        self._api = api
        self._brand = api.brand
        self._sensor_name = BRAND_CONFIG[self._brand]["name"]

        # Cached rates + schedule (persists across poll cycles)
        self._schedule: dict = {}
        self._rates: dict[str, float] = {}
        self._daily_charge: float = 0.0
        self._detected_periods: list[str] = []
        self._product: str = ""
        self._last_rates_refresh: datetime | None = None
        # Map from TOU rate (cents/kWh) to canonical period key
        self._rate_to_period: dict[float, str] = {}
        # Cache of stat label hash → period (populated on first encounter)
        self._label_to_period: dict[str, str] = {}
        # Cumulative sums for external statistics (re-seeded from DB each poll)
        self._energy_sums: dict[str, float] = defaultdict(float)
        self._cost_sums: dict[str, float] = defaultdict(float)
        self._daily_charge_sum: float = 0.0
        self._solar_sum: float = 0.0

    # -- Public interface for sensors -----------------------------------------

    @property
    def sensor_name(self) -> str:
        """Display name derived from brand."""
        return self._sensor_name

    @property
    def brand(self) -> str:
        return self._brand

    def get_current_tou_period(self) -> str:
        """Return canonical period key for the current local time."""
        return classify_period(datetime.now(NZ_TZ), self._schedule)

    def get_current_rate(self) -> float:
        """Return the NZD/kWh rate for the current TOU period."""
        if not self._rates:
            return 0.0
        return self._rates.get(self.get_current_tou_period(), 0.0)

    def get_boundary_times(self) -> list[tuple[int, int]]:
        """TOU boundary (hour, minute) pairs for time-change listeners."""
        return get_boundary_times(self._schedule)

    async def async_force_rate_refresh(self) -> None:
        """Force an immediate rate + TOU refresh, then update sensors."""
        self._last_rates_refresh = None
        await self.async_refresh()

    async def async_backfill(
        self, start_date: date, end_date: date | None = None,
    ) -> None:
        """Re-fetch and re-publish statistics from *start_date*.

        Seeds cumulative sums from the DB entry just before
        *start_date*, fetches API data from *start_date* to now,
        and re-publishes all statistics — overwriting any existing
        entries in that range so that cumulative sums are correct.
        """
        now = datetime.now(NZ_TZ)
        end_date = end_date or now.date()
        start_dt = datetime.combine(
            start_date, datetime.min.time(), tzinfo=NZ_TZ,
        )
        # Always extend to now so cumulative sums stay consistent
        # for any entries published after end_date in previous runs.
        fetch_end = max(
            datetime.combine(
                end_date + timedelta(days=1),
                datetime.min.time(),
                tzinfo=NZ_TZ,
            ),
            now,
        )

        # Refresh rates first so _daily_charge and _rate_to_period
        # are populated for the publish methods.
        self._last_rates_refresh = None
        await self._async_refresh_rates()

        # -- Seed cumulative sums from just before start_date ----------
        all_stat_ids: list[str] = []
        for p in self._detected_periods:
            all_stat_ids.append(_energy_stat_id(p))
            all_stat_ids.append(_cost_stat_id(p))
        all_stat_ids.append(f"{DOMAIN}:return_to_grid")
        all_stat_ids.append(f"{DOMAIN}:cost_daily_charge")
        all_stat_ids.append(f"{DOMAIN}:consumption_daily_charge")

        recorder = get_instance(self.hass)
        seed_result: dict[str, list] = await recorder.async_add_executor_job(
            statistics_during_period,
            self.hass,
            None,
            start_dt,
            set(all_stat_ids),
            "hour",
            None,
            {"sum"},
        )

        self._energy_sums.clear()
        self._cost_sums.clear()
        self._daily_charge_sum = 0.0
        self._solar_sum = 0.0

        seed_skip: datetime | None = None
        for stat_id, entries in seed_result.items():
            if not entries:
                continue
            last = entries[-1]
            seed_sum = last.get("sum") or 0.0
            entry_ts = last.get("start", 0.0)
            ts = datetime.fromtimestamp(entry_ts, tz=NZ_TZ)
            if seed_skip is None or ts > seed_skip:
                seed_skip = ts

            if stat_id == f"{DOMAIN}:return_to_grid":
                self._solar_sum = seed_sum
            elif stat_id == f"{DOMAIN}:cost_daily_charge":
                self._daily_charge_sum = seed_sum
            elif stat_id == f"{DOMAIN}:consumption_daily_charge":
                pass
            elif stat_id.startswith(f"{DOMAIN}:consumption_"):
                period = stat_id.removeprefix(f"{DOMAIN}:consumption_")
                self._energy_sums[period] = seed_sum
            elif stat_id.startswith(f"{DOMAIN}:cost_"):
                period = stat_id.removeprefix(f"{DOMAIN}:cost_")
                self._cost_sums[period] = seed_sum

        _LOGGER.info(
            "Backfill: seeded from %s, energy=%s, cost=%s, dc=%.2f",
            seed_skip,
            {k: f"{v:.1f}" for k, v in self._energy_sums.items()},
            {k: f"{v:.2f}" for k, v in self._cost_sums.items()},
            self._daily_charge_sum,
        )

        # -- Fetch API data --------------------------------------------
        hh_start = max(start_dt, fetch_end - timedelta(days=30))
        daily_nodes: list[dict] = []
        half_hourly_nodes: list[dict] = []
        try:
            daily_nodes = await self._api.async_get_daily_cost_measurements(
                start_dt, fetch_end,
            )
        except (AuthError, ApiError, aiohttp.ClientError) as err:
            _LOGGER.warning("Backfill: daily cost fetch failed: %s", err)
        try:
            half_hourly_nodes = await self._api.async_get_measurements(
                hh_start, fetch_end,
                frequency="THIRTY_MIN_INTERVAL",
                direction="CONSUMPTION",
            )
        except (AuthError, ApiError, aiohttp.ClientError) as err:
            _LOGGER.debug("Backfill: half-hourly fetch failed: %s", err)

        # Use seed_skip so entries at-or-before the seed point are NOT
        # overwritten (preserves historical data before the backfill).
        skip = seed_skip

        # -- Publish ---------------------------------------------------
        # Skip estimated values even in backfills — we only want actual
        # meter reads in long-term statistics.
        now = datetime.now(NZ_TZ)
        if half_hourly_nodes:
            self._publish_hourly_consumption_stats(
                half_hourly_nodes,
                skip_before=skip,
                now=now,
            )
        elif daily_nodes:
            self._publish_daily_consumption_stats(
                daily_nodes,
                skip_before=skip,
                now=now,
            )

        if daily_nodes:
            dc_skip = None
            for stat_id, entries in seed_result.items():
                if stat_id == f"{DOMAIN}:cost_daily_charge" and entries:
                    dc_skip = datetime.fromtimestamp(
                        entries[-1].get("start", 0.0), tz=NZ_TZ,
                    )
            self._publish_daily_charge_stats(
                daily_nodes,
                skip_before=dc_skip,
                now=now,
            )

        _LOGGER.info(
            "Backfill complete: %s to %s (%d daily, %d half-hourly nodes)",
            start_date, end_date,
            len(daily_nodes), len(half_hourly_nodes),
        )

    # -- Core update loop ----------------------------------------------------

    async def _async_update_data(self) -> MeridianData:
        """Fetch data from the Kraken API and publish statistics."""
        try:
            await self._async_refresh_rates()
            stats_info = await self._async_fetch_and_publish_stats()
            balance = await self._async_fetch_balance()
            billing = await self._async_fetch_billing()
        except AuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except (ApiError, aiohttp.ClientError) as err:
            raise UpdateFailed(str(err)) from err

        tou = self.get_current_tou_period()

        # Check data freshness and manage repair issue
        self._check_data_freshness(stats_info)

        return MeridianData(
            brand=self._brand,
            sensor_name=self._sensor_name,
            rates=self._rates.copy(),
            daily_charge=self._daily_charge,
            product=self._product,
            tou_period=tou,
            current_rate=self._rates.get(tou, 0.0),
            solar_export_kwh=stats_info.get("solar_kwh", 0.0),
            has_solar=stats_info.get("has_solar", False),
            balance=balance,
            billing_period_start=billing.get("period_start"),
            billing_period_end=billing.get("period_end"),
            next_billing_date=billing.get("next_billing_date"),
            schedule=self._schedule,
            last_usage_update=datetime.now(NZ_TZ),
            detected_periods=self._detected_periods.copy(),
        )

    def _check_data_freshness(self, stats_info: dict) -> None:
        """Create or clear a repair issue based on data freshness."""
        latest_data_ts: datetime | None = stats_info.get("latest_data_ts")
        issue_id = f"stale_usage_data_{self.config_entry.entry_id}"
        now = datetime.now(NZ_TZ)

        if latest_data_ts is not None and (now - latest_data_ts) <= STALE_DATA_THRESHOLD:
            # Data is fresh — clear any existing issue
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)
        elif latest_data_ts is not None:
            # Data is stale — create a repair issue
            age_hours = int((now - latest_data_ts).total_seconds() / 3600)
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                issue_id,
                is_fixable=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key="stale_usage_data",
                translation_placeholders={
                    "sensor_name": self._sensor_name,
                    "hours_ago": str(age_hours),
                },
            )

    # -- Rates + TOU ---------------------------------------------------------

    async def _async_refresh_rates(self) -> None:
        """Refresh rates and TOU schedule from the API (daily)."""
        now = datetime.now(NZ_TZ)
        if (
            self._last_rates_refresh
            and (now - self._last_rates_refresh) < RATES_UPDATE_INTERVAL
            and self._rates
        ):
            return

        try:
            data = await self._api.async_get_rates_and_tou()
        except AuthError:
            raise
        except (ApiError, aiohttp.ClientError) as err:
            if self._rates:
                _LOGGER.warning("Rate refresh failed, using cached: %s", err)
                return
            raise

        parsed = parse_rates(data.get("rates", []))
        self._rates = parsed["tou_rates"]
        self._daily_charge = parsed["daily_charge"]
        self._detected_periods = list(self._rates.keys())
        self._product = data.get("product", "")
        self._schedule = parse_tou_scheme(data.get("tou_schemes", []))
        self._last_rates_refresh = now

        # Build rate → period map for stat label identification
        self._rate_to_period = {}
        for rate_entry in data.get("rates", []):
            bucket = (rate_entry.get("touBucketName") or "").strip()
            band = (rate_entry.get("bandCategory") or "").upper()
            if not bucket or "STANDING" in band:
                continue
            period = classify_bucket(bucket)
            rate_val = float(rate_entry.get("rateIncludingTax") or 0)
            if period and rate_val > 0:
                self._rate_to_period[rate_val] = period
        # Clear label cache when rates change (hashes may differ)
        self._label_to_period = {}

        _LOGGER.info(
            "Rates refreshed: %s, daily=$%.4f, scheme=%s",
            {k: f"${v:.4f}" for k, v in self._rates.items()},
            self._daily_charge,
            self._schedule.get("scheme_name", "?"),
        )

    # -- Stat identification ---------------------------------------------------

    def _identify_stat_period(self, stat: dict) -> str | None:
        """Identify the canonical TOU period from a statistic entry.

        Handles both new-format hash labels (CONSUMPTION_CHARGE_TOU_<hash>)
        and legacy bucket-name labels (N9, PK5, OPK10, etc.).
        """
        label = (stat.get("label") or "").strip()
        if not label:
            return None

        # Check label cache first
        if label in self._label_to_period:
            return self._label_to_period[label]

        # New format: hash-based labels
        if label.startswith("STANDING_CHARGE_"):
            return None  # Daily charge, not energy

        if label.startswith("CONSUMPTION_CHARGE_TOU_"):
            # TOU energy — match by effective rate
            period = self._match_stat_rate_to_period(stat)
            if period:
                self._label_to_period[label] = period
            return period

        if label.startswith("CONSUMPTION_CHARGE_"):
            self._label_to_period[label] = PERIOD_CONTROLLED
            return PERIOD_CONTROLLED

        # Legacy format: direct bucket names (N9, PK5, etc.)
        period = classify_bucket(label)
        if period:
            self._label_to_period[label] = period
        return period

    def _match_stat_rate_to_period(self, stat: dict) -> str | None:
        """Match a TOU stat to a period by comparing its effective rate."""
        if not self._rate_to_period:
            return None

        value = float(stat.get("value") or 0)
        cost_raw = float(
            (stat.get("costInclTax") or {}).get("estimatedAmount") or 0
        )
        if value <= 0 or cost_raw <= 0:
            return None

        eff_rate = cost_raw / value  # cents per kWh

        best_period: str | None = None
        best_diff = float("inf")
        for known_rate, period in self._rate_to_period.items():
            diff = abs(eff_rate - known_rate)
            if diff < best_diff:
                best_diff = diff
                best_period = period

        # Allow up to 5% tolerance for rounding
        if best_period and best_diff / eff_rate < 0.05:
            return best_period
        return None

    def _extract_period_entries(
        self, stats: list[dict],
    ) -> list[tuple[str, float, float]]:
        """Extract per-period (period, kwh, cost_nzd) from stat labels.

        Returns a list of ``(period, kwh, cost_nzd)`` tuples for each
        consumption stat in *stats*.  Standing-charge entries are
        skipped.  Returns an empty list if no consumption entries were
        identified (caller should fall back to schedule classification).
        """
        entries: list[tuple[str, float, float]] = []
        for stat in stats:
            period = self._identify_stat_period(stat)
            if not period:
                continue
            kwh = float(stat.get("value") or 0)
            cost_incl = stat.get("costInclTax") or {}
            cost_nzd = float(
                cost_incl.get("estimatedAmount") or 0,
            ) / 100.0
            entries.append((period, kwh, cost_nzd))
        return entries

    # -- Statistics -----------------------------------------------------------

    async def _async_seed_from_latest(
        self,
        stat_ids: list[str],
    ) -> dict[str, datetime]:
        """Seed cumulative sums from the latest DB entry per stat ID.

        Returns a dict mapping each stat ID that has existing data to
        its latest entry's NZ-aware timestamp.  Callers use these
        timestamps as *skip_before* boundaries so that existing DB
        entries are never overwritten — only genuinely new data (after
        the latest entry) is published.

        Fresh installs return an empty dict (no skip).
        """
        recorder = get_instance(self.hass)
        latest_map: dict[str, datetime] = {}

        for stat_id in stat_ids:
            last = await recorder.async_add_executor_job(
                get_last_statistics, self.hass, 1, stat_id, False,
                {"sum"},
            )
            if stat_id not in last or not last[stat_id]:
                continue

            entry = last[stat_id][0]
            seed_sum = entry.get("sum") or 0.0
            entry_ts = entry.get("start", 0.0)

            if stat_id == f"{DOMAIN}:return_to_grid":
                self._solar_sum = seed_sum
            elif stat_id == f"{DOMAIN}:cost_daily_charge":
                self._daily_charge_sum = seed_sum
            elif stat_id == f"{DOMAIN}:consumption_daily_charge":
                pass  # Always 0
            elif stat_id.startswith(f"{DOMAIN}:consumption_"):
                period = stat_id.removeprefix(f"{DOMAIN}:consumption_")
                self._energy_sums[period] = seed_sum
            elif stat_id.startswith(f"{DOMAIN}:cost_"):
                period = stat_id.removeprefix(f"{DOMAIN}:cost_")
                self._cost_sums[period] = seed_sum

            if entry_ts:
                latest_map[stat_id] = datetime.fromtimestamp(
                    entry_ts, tz=NZ_TZ,
                )

        _LOGGER.info(
            "Seeded from latest DB entries: energy=%s, cost=%s, "
            "dc=%.2f, solar=%.1f",
            {k: f"{v:.1f}" for k, v in self._energy_sums.items()},
            {k: f"{v:.2f}" for k, v in self._cost_sums.items()},
            self._daily_charge_sum,
            self._solar_sum,
        )
        return latest_map

    async def _async_fetch_and_publish_stats(self) -> dict:
        """Fetch measurements and publish external statistics.

        Fetches half-hourly consumption data for hourly-resolution
        per-period statistics (energy dashboard), daily cost data for
        the daily charge stat and cost sensor, and solar generation data.

        Every poll seeds cumulative sums from the database, fetches
        API data with overlap, skips entries at-or-before the last
        known timestamp, and publishes only new entries.  The
        ``async_add_external_statistics`` API performs upserts, so
        re-publishing the same timestamp is safe.
        """
        now = datetime.now(NZ_TZ)

        # Seed cumulative sums from the latest DB entry per stat ID.
        all_stat_ids: list[str] = []
        for p in self._detected_periods:
            all_stat_ids.append(_energy_stat_id(p))
            all_stat_ids.append(_cost_stat_id(p))
        all_stat_ids.append(f"{DOMAIN}:return_to_grid")
        all_stat_ids.append(f"{DOMAIN}:cost_daily_charge")
        all_stat_ids.append(f"{DOMAIN}:consumption_daily_charge")
        latest_map = await self._async_seed_from_latest(all_stat_ids)

        # Derive per-category skip boundaries
        consumption_ts = [
            t for sid, t in latest_map.items()
            if sid.startswith(f"{DOMAIN}:consumption_")
            and sid != f"{DOMAIN}:consumption_daily_charge"
        ]
        cost_ts = [
            t for sid, t in latest_map.items()
            if sid.startswith(f"{DOMAIN}:cost_")
            and sid != f"{DOMAIN}:cost_daily_charge"
        ]
        energy_skip = max(consumption_ts + cost_ts) if (consumption_ts or cost_ts) else None
        dc_skip = latest_map.get(f"{DOMAIN}:cost_daily_charge")
        solar_skip = latest_map.get(f"{DOMAIN}:return_to_grid")

        # Determine fetch window from the latest known timestamp.
        # Use a 3-day overlap because actual (non-estimated) data can
        # take up to ~2 days to appear after the measurement period.
        all_ts = [t for t in latest_map.values() if t is not None]
        latest_ts = max(all_ts) if all_ts else None
        if latest_ts is not None:
            start = latest_ts - timedelta(days=3)
        else:
            start = now - timedelta(days=DEFAULT_LOOKBACK_DAYS)

        # Half-hourly lookback is capped — the API rarely serves
        # more than ~30 days of half-hourly data.
        hh_start = max(start, now - timedelta(days=30))

        # Daily cost measurements (for daily charge + cost sensor)
        daily_nodes: list[dict] = []
        try:
            daily_nodes = await self._api.async_get_daily_cost_measurements(
                start, now,
            )
        except (AuthError, ApiError) as err:
            _LOGGER.warning("Cost measurements fetch failed: %s", err)
            return {"solar_kwh": 0.0, "has_solar": False, "latest_data_ts": None}

        # Half-hourly consumption data (for hourly per-period statistics)
        half_hourly_nodes: list[dict] = []
        try:
            half_hourly_nodes = await self._api.async_get_measurements(
                hh_start, now,
                frequency="THIRTY_MIN_INTERVAL",
                direction="CONSUMPTION",
            )
        except (AuthError, ApiError) as err:
            _LOGGER.debug("Half-hourly measurements unavailable: %s", err)

        # Estimated data filtering: each publish method checks individual
        # node values via _is_estimated_value() to skip estimated data
        # (high decimal precision) and only publish actual meter reads.

        if daily_nodes or half_hourly_nodes:
            # Per-period energy/cost: hourly resolution from half-hourly
            # data (preferred), or daily fallback.  We do NOT reprocess
            # old daily per-period data alongside hourly data because
            # the two sources use different TOU classification methods,
            # producing sum discontinuities.  Historical per-period
            # entries (before the HH window) are retained as-is from
            # prior runs or backup imports.
            #
            # EXCEPTION: if the latest DB entry is older than the
            # half-hourly window (>30 day gap), fill the gap with
            # daily-resolution data so the energy dashboard doesn't
            # have a hole.
            if half_hourly_nodes:
                if daily_nodes and energy_skip is not None:
                    hh_min_str = min(
                        (n.get("startAt", "") for n in half_hourly_nodes),
                        default="",
                    )
                    if hh_min_str:
                        hh_earliest = datetime.fromisoformat(hh_min_str)
                        if energy_skip < hh_earliest - timedelta(hours=1):
                            gap_nodes = [
                                n for n in daily_nodes
                                if n.get("startAt", "") < hh_min_str
                            ]
                            if gap_nodes:
                                _LOGGER.info(
                                    "Filling %d-day gap with daily data"
                                    " (DB latest=%s, HH start=%s)",
                                    (hh_earliest - energy_skip).days,
                                    energy_skip.date(),
                                    hh_earliest.date(),
                                )
                                self._publish_daily_consumption_stats(
                                    gap_nodes,
                                    skip_before=energy_skip,
                                    now=now,
                                )

                self._publish_hourly_consumption_stats(
                    half_hourly_nodes,
                    skip_before=energy_skip,
                    now=now,
                )
            elif daily_nodes:
                self._publish_daily_consumption_stats(
                    daily_nodes,
                    skip_before=energy_skip,
                    now=now,
                )

            # Daily charge + cost sensor (always from daily data)
            if daily_nodes:
                self._publish_daily_charge_stats(
                    daily_nodes,
                    skip_before=dc_skip,
                    now=now,
                )


        # Solar export
        solar_kwh = 0.0
        has_solar = False
        try:
            solar_nodes = await self._api.async_get_measurements(
                start, now,
                frequency="DAY_INTERVAL",
                direction="GENERATION",
            )
            if solar_nodes:
                solar_kwh = sum(float(n.get("value", 0)) for n in solar_nodes)
                has_solar = solar_kwh > 0
                if has_solar:
                    self._publish_solar_stats(
                        solar_nodes,
                        skip_before=solar_skip,
                        now=now,
                    )
        except (AuthError, ApiError):
            _LOGGER.debug("Solar data unavailable (may not have solar)")

        # Determine the most recent data timestamp from API results
        latest_data_ts: datetime | None = None
        for nodes_list in (daily_nodes, half_hourly_nodes):
            for node in nodes_list:
                end_str = node.get("endAt") or node.get("startAt")
                if end_str:
                    try:
                        ts = datetime.fromisoformat(end_str)
                        if latest_data_ts is None or ts > latest_data_ts:
                            latest_data_ts = ts
                    except (ValueError, TypeError):
                        pass

        return {"solar_kwh": solar_kwh, "has_solar": has_solar, "latest_data_ts": latest_data_ts}

    def _publish_hourly_consumption_stats(
        self, nodes: list[dict], *,
        skip_before: datetime | None = None,
        skip_estimated: bool = True,
        now: datetime | None = None,
    ) -> None:
        """Publish per-period energy/cost stats at hourly resolution.

        Takes half-hourly consumption measurements, splits each into
        per-period kWh and cost using the node's ``metaData.statistics``
        labels (which distinguish TOU periods and controlled load), and
        aggregates into hourly buckets for the energy dashboard.

        Falls back to schedule-based ``classify_period()`` only when
        a node has no metadata statistics.

        When *skip_before* is set, entries at or before that timestamp
        are skipped (already in DB).

        When *skip_estimated* is True (default), nodes whose ``value``
        has too many significant decimal digits are skipped because
        they are API estimates rather than actual meter reads.
        """

        sorted_nodes = sorted(nodes, key=lambda n: n.get("startAt", ""))

        # Step 1: Aggregate half-hourly data into hourly buckets per period
        # hourly_agg[period][hour_start] = [total_kwh, total_cost]
        hourly_agg: dict[str, dict[datetime, list[float]]] = defaultdict(
            lambda: defaultdict(lambda: [0.0, 0.0])
        )

        for node in sorted_nodes:
            start_str = node.get("startAt")
            if not start_str:
                continue

            ts = datetime.fromisoformat(start_str)
            ts_nz = ts.astimezone(NZ_TZ)
            hour_start = ts_nz.replace(minute=0, second=0, microsecond=0)

            # Skip data for hours that haven't started yet.
            if now is not None and hour_start > now:
                continue

            # Skip estimated data (high decimal precision = not a real meter read).
            if skip_estimated and _is_estimated_value(str(node.get("value", ""))):
                continue

            # Try to extract per-period breakdown from metadata stats.
            # Each half-hourly node may contain labels like:
            #   STANDING_CHARGE_<hash>        → skip (daily charge)
            #   CONSUMPTION_CHARGE_TOU_<hash> → TOU period (rate-matched)
            #   CONSUMPTION_CHARGE_<hash>     → controlled load
            stats = (
                (node.get("metaData") or {}).get("statistics") or []
            )
            period_entries = self._extract_period_entries(stats)

            if not period_entries:
                # Fallback: no metadata — use schedule classification
                # with the node's total value.
                kwh = float(node.get("value", 0))
                period = classify_period(ts_nz, self._schedule)
                rate = self._rates.get(period, 0.0)
                period_entries = [(period, kwh, kwh * rate)]

            for period, kwh, cost in period_entries:
                # Skip data that already exists in DB.
                if skip_before is not None and hour_start <= skip_before:
                    continue

                hourly_agg[period][hour_start][0] += kwh
                hourly_agg[period][hour_start][1] += cost

        # Step 2: Build cumulative statistics per period, sorted by time
        energy_stats: dict[str, list[StatisticData]] = defaultdict(list)
        cost_stats: dict[str, list[StatisticData]] = defaultdict(list)

        all_hours: set[datetime] = set()
        for period_data in hourly_agg.values():
            all_hours.update(period_data.keys())

        all_timestamps = sorted(all_hours)

        for ts in all_timestamps:
            for period in hourly_agg:
                if ts in hourly_agg[period]:
                    kwh, cost = hourly_agg[period][ts]
                    kwh = max(0.0, kwh)
                    cost = max(0.0, cost)
                    self._energy_sums[period] += kwh
                    self._cost_sums[period] += cost
                    energy_stats[period].append(
                        StatisticData(
                            start=ts,
                            state=kwh,
                            sum=self._energy_sums[period],
                        )
                    )
                    cost_stats[period].append(
                        StatisticData(
                            start=ts,
                            state=cost,
                            sum=self._cost_sums[period],
                        )
                    )

        # Step 3: Publish
        for period in energy_stats:
            if energy_stats[period]:
                async_add_external_statistics(
                    self.hass,
                    StatisticMetaData(
                        has_mean=False,
                        has_sum=True,
                        name=(
                            f"{self._sensor_name}"
                            f" ({period_display_name(period)})"
                        ),
                        source=DOMAIN,
                        statistic_id=_energy_stat_id(period),
                        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                        mean_type=StatisticMeanType.NONE,
                        unit_class="energy",
                    ),
                    energy_stats[period],
                )
            if cost_stats[period]:
                async_add_external_statistics(
                    self.hass,
                    StatisticMetaData(
                        has_mean=False,
                        has_sum=True,
                        name=(
                            f"{self._sensor_name} Cost"
                            f" ({period_display_name(period)})"
                        ),
                        source=DOMAIN,
                        statistic_id=_cost_stat_id(period),
                        unit_of_measurement="NZD",
                        mean_type=StatisticMeanType.NONE,
                        unit_class=None,
                    ),
                    cost_stats[period],
                )

        _LOGGER.info(
            "Hourly statistics published: %d hours, %s",
            len(all_hours),
            {
                p: f"{self._energy_sums[p]:.1f}kWh/${self._cost_sums[p]:.2f}"
                for p in energy_stats
                if self._energy_sums[p] > 0
            },
        )

    def _publish_daily_consumption_stats(
        self, nodes: list[dict], *,
        skip_before: datetime | None = None,
        skip_estimated: bool = True,
        now: datetime | None = None,
    ) -> None:
        """Fallback: publish per-period stats at daily resolution.

        Used when half-hourly data is unavailable.  Extracts per-period
        energy and cost from the daily cost node's ``metaData.statistics``
        TOU breakdown.

        When *skip_before* is set, entries at or before that timestamp
        are skipped (already in DB).

        When *skip_estimated* is True (default), nodes whose ``value``
        has too many significant decimal digits are skipped because
        they are API estimates rather than actual meter reads.
        """

        sorted_nodes = sorted(nodes, key=lambda n: n.get("startAt", ""))

        energy_data: dict[str, list[StatisticData]] = defaultdict(list)
        cost_data: dict[str, list[StatisticData]] = defaultdict(list)

        for node in sorted_nodes:
            start_str = node.get("startAt")
            if not start_str:
                continue

            ts = datetime.fromisoformat(start_str)
            date_only = ts.date()
            ts = datetime.combine(
                date_only, datetime.min.time(), tzinfo=NZ_TZ,
            )

            # Skip future days.
            if now is not None and ts > now:
                continue

            # Skip estimated data (high decimal precision = not a real meter read).
            if skip_estimated and _is_estimated_value(str(node.get("value", ""))):
                continue

            for stat in (
                (node.get("metaData") or {}).get("statistics") or []
            ):
                period = self._identify_stat_period(stat)
                if not period:
                    continue

                # Skip data that already exists in DB.
                if skip_before is not None and ts <= skip_before:
                    continue

                kwh = max(0.0, float(stat.get("value") or 0))
                cost_incl = stat.get("costInclTax") or {}
                cost_nzd = max(
                    0.0,
                    float(cost_incl.get("estimatedAmount") or 0) / 100.0,
                )

                self._energy_sums[period] += kwh
                self._cost_sums[period] += cost_nzd
                energy_data[period].append(
                    StatisticData(
                        start=ts, state=kwh,
                        sum=self._energy_sums[period],
                    )
                )
                cost_data[period].append(
                    StatisticData(
                        start=ts, state=cost_nzd,
                        sum=self._cost_sums[period],
                    )
                )

        for period in energy_data:
            if energy_data[period]:
                async_add_external_statistics(
                    self.hass,
                    StatisticMetaData(
                        has_mean=False,
                        has_sum=True,
                        name=(
                            f"{self._sensor_name}"
                            f" ({period_display_name(period)})"
                        ),
                        source=DOMAIN,
                        statistic_id=_energy_stat_id(period),
                        unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                        mean_type=StatisticMeanType.NONE,
                        unit_class="energy",
                    ),
                    energy_data[period],
                )
            if cost_data[period]:
                async_add_external_statistics(
                    self.hass,
                    StatisticMetaData(
                        has_mean=False,
                        has_sum=True,
                        name=(
                            f"{self._sensor_name} Cost"
                            f" ({period_display_name(period)})"
                        ),
                        source=DOMAIN,
                        statistic_id=_cost_stat_id(period),
                        unit_of_measurement="NZD",
                        mean_type=StatisticMeanType.NONE,
                        unit_class=None,
                    ),
                    cost_data[period],
                )

        _LOGGER.info(
            "Daily statistics published: %d days, %s",
            len(sorted_nodes),
            {
                p: f"{self._energy_sums[p]:.1f}kWh/${self._cost_sums[p]:.2f}"
                for p in energy_data
                if self._energy_sums[p] > 0
            },
        )

    def _publish_daily_charge_stats(
        self, nodes: list[dict], *,
        skip_before: datetime | None = None,
        skip_estimated: bool = True,
        now: datetime | None = None,
    ) -> None:
        """Publish daily charge statistics and cache latest daily cost.

        When *skip_before* is set, skip API nodes whose date falls
        at or before that timestamp (already in DB).

        When *skip_estimated* is True (default), nodes whose ``value``
        has too many significant decimal digits are skipped because
        they are API estimates rather than actual meter reads.
        The latest daily cost for the balance sensor is still cached
        from all nodes regardless.
        """

        sorted_nodes = sorted(nodes, key=lambda n: n.get("startAt", ""))
        today = datetime.now(NZ_TZ).date()

        dc_energy: list[StatisticData] = []
        dc_cost: list[StatisticData] = []

        for node in sorted_nodes:
            start_str = node.get("startAt")
            if not start_str:
                continue

            ts = datetime.fromisoformat(start_str)
            date_only = ts.date()
            ts = datetime.combine(
                date_only, datetime.min.time(), tzinfo=NZ_TZ,
            )

            # Skip future days.
            if now is not None and ts > now:
                continue

            # Skip estimated data (high decimal precision = not a real meter read).
            if skip_estimated and _is_estimated_value(str(node.get("value", ""))):
                continue

            # Skip dates that already have entries in the DB
            # (preserves v1 data with historical rates).
            if skip_before is not None and ts <= skip_before:
                continue

            # Extract actual standing charge from the API data so
            # historical entries use the rate that was active at the
            # time, not the current rate.
            day_charge = 0.0
            for stat in (
                (node.get("metaData") or {}).get("statistics") or []
            ):
                label = (stat.get("label") or "").strip()
                if label.startswith("STANDING_CHARGE"):
                    ci = stat.get("costInclTax") or {}
                    day_charge += float(
                        ci.get("estimatedAmount") or 0,
                    ) / 100.0
            # For the current (incomplete) day the API prorates the
            # standing charge; use the full-day rate instead so the
            # cumulative sum stays correct after restarts.
            if day_charge == 0.0 or date_only >= today:
                day_charge = self._daily_charge
            # Sanity check: reject anomalous standing charges (e.g.
            # API returning cumulative totals, wrong components).
            elif self._daily_charge > 0 and day_charge > self._daily_charge * 2:
                _LOGGER.warning(
                    "Standing charge for %s is $%.2f (expected ~$%.2f); "
                    "using known rate to prevent stat corruption",
                    date_only, day_charge, self._daily_charge,
                )
                day_charge = self._daily_charge

            self._daily_charge_sum += day_charge
            dc_energy.append(StatisticData(start=ts, state=0.0, sum=0.0))
            dc_cost.append(
                StatisticData(
                    start=ts, state=day_charge,
                    sum=self._daily_charge_sum,
                )
            )

        # Sanity check: cumulative sums must be monotonically increasing
        for i in range(1, len(dc_cost)):
            prev_s = dc_cost[i - 1]["sum"]
            curr_s = dc_cost[i]["sum"]
            if (
                isinstance(prev_s, (int, float))
                and isinstance(curr_s, (int, float))
                and curr_s < prev_s
            ):
                _LOGGER.error(
                    "Daily charge sum decreased: %s (%.4f) -> %s (%.4f)",
                    dc_cost[i - 1]["start"], prev_s,
                    dc_cost[i]["start"], curr_s,
                )

        if dc_cost:
            async_add_external_statistics(
                self.hass,
                StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    name=f"{self._sensor_name} (Daily Charge)",
                    source=DOMAIN,
                    statistic_id=f"{DOMAIN}:consumption_daily_charge",
                    unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                    mean_type=StatisticMeanType.NONE,
                    unit_class="energy",
                ),
                dc_energy,
            )
            async_add_external_statistics(
                self.hass,
                StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    name=f"{self._sensor_name} Cost (Daily Charge)",
                    source=DOMAIN,
                    statistic_id=f"{DOMAIN}:cost_daily_charge",
                    unit_of_measurement="NZD",
                    mean_type=StatisticMeanType.NONE,
                    unit_class=None,
                ),
                dc_cost,
            )

    def _publish_solar_stats(
        self, solar_nodes: list[dict], *,
        skip_before: datetime | None = None,
        skip_estimated: bool = True,
        now: datetime | None = None,
    ) -> None:
        """Publish solar export statistics from daily generation data."""

        sorted_nodes = sorted(
            solar_nodes, key=lambda n: n.get("startAt", "")
        )
        data: list[StatisticData] = []

        for node in sorted_nodes:
            start_str = node.get("startAt")
            if not start_str:
                continue

            ts = datetime.fromisoformat(start_str)
            date_only = ts.date()
            ts = datetime.combine(
                date_only, datetime.min.time(), tzinfo=NZ_TZ,
            )

            # Skip future days.
            if now is not None and ts > now:
                continue

            # Skip estimated data (high decimal precision = not a real meter read).
            if skip_estimated and _is_estimated_value(str(node.get("value", ""))):
                continue

            if skip_before is not None and ts <= skip_before:
                continue

            solar_kwh = max(0.0, float(node.get("value", 0)))
            self._solar_sum += solar_kwh
            data.append(
                StatisticData(start=ts, state=solar_kwh, sum=self._solar_sum)
            )

        if data:
            async_add_external_statistics(
                self.hass,
                StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    name=f"{self._sensor_name} (Solar Export)",
                    source=DOMAIN,
                    statistic_id=f"{DOMAIN}:return_to_grid",
                    unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                    mean_type=StatisticMeanType.NONE,
                    unit_class="energy",
                ),
                data,
            )

    # -- Balance --------------------------------------------------------------

    async def _async_fetch_balance(self) -> dict[str, float | None] | None:
        """Fetch ledger balances."""
        try:
            ledgers = await self._api.async_get_ledger_balances()
            electricity = ledgers.get("electricity", 0) / 100.0
            powerpacks = ledgers.get("powerpacks", 0) / 100.0
            return {
                "ahead": round(electricity, 2),
                "future_packs": round(powerpacks, 2),
            }
        except (AuthError, ApiError) as err:
            _LOGGER.warning("Balance fetch failed: %s", err)
            return self.data.balance if self.data else None

    # -- Billing ---------------------------------------------------------------

    async def _async_fetch_billing(self) -> dict:
        """Fetch billing period dates."""
        try:
            return await self._api.async_get_billing_info()
        except (AuthError, ApiError) as err:
            _LOGGER.warning("Billing info fetch failed: %s", err)
            if self.data:
                return {
                    "period_start": self.data.billing_period_start,
                    "period_end": self.data.billing_period_end,
                    "next_billing_date": self.data.next_billing_date,
                }
            return {}
