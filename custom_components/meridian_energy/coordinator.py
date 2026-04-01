"""DataUpdateCoordinator for Meridian Energy / Powershop integration.

Handles:
- Login + CSV download (blocking, run in executor)
- Rate cache refresh (scraping, blocking, run in executor)
- EIEP 13A CSV parsing and TOU classification
- Solar export tracking (return to grid)
- External statistics publishing (14 statistic IDs)
- Current-month rate lookups for sensor entities
- TOU schedule management (via ScheduleCache)
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from io import StringIO
from pathlib import Path

from zoneinfo import ZoneInfo

from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
    StatisticMeanType,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.exceptions import ConfigEntryAuthFailed

from .api import MeridianEnergyApi
from .rates import RateCache
from .schedule import ScheduleCache, classify_period
from .const import (
    DOMAIN,
    USAGE_UPDATE_INTERVAL,
    SUPPLIER_CONFIG,
    DEFAULT_SUPPLIER,
)

from requests.exceptions import RequestException

_LOGGER = logging.getLogger(__name__)

NZ_TZ = ZoneInfo("Pacific/Auckland")



def _build_period_meta(sensor_name: str) -> dict:
    """Build period metadata dict using the given sensor display name."""
    return {
        "night": {
            "name": f"{sensor_name} (Night)",
            "stat_id": f"{DOMAIN}:consumption_night",
            "cost_name": f"{sensor_name} Cost (Night)",
            "cost_stat_id": f"{DOMAIN}:cost_night",
        },
        "peak": {
            "name": f"{sensor_name} (Peak)",
            "stat_id": f"{DOMAIN}:consumption_peak",
            "cost_name": f"{sensor_name} Cost (Peak)",
            "cost_stat_id": f"{DOMAIN}:cost_peak",
        },
        "offpeak": {
            "name": f"{sensor_name} (Off-Peak)",
            "stat_id": f"{DOMAIN}:consumption_offpeak",
            "cost_name": f"{sensor_name} Cost (Off-Peak)",
            "cost_stat_id": f"{DOMAIN}:cost_offpeak",
        },
        "weekend_offpeak": {
            "name": f"{sensor_name} (Weekend Off-Peak)",
            "stat_id": f"{DOMAIN}:consumption_weekend_offpeak",
            "cost_name": f"{sensor_name} Cost (Weekend Off-Peak)",
            "cost_stat_id": f"{DOMAIN}:cost_weekend_offpeak",
        },
        "controlled": {
            "name": f"{sensor_name} (Controlled)",
            "stat_id": f"{DOMAIN}:consumption_controlled",
            "cost_name": f"{sensor_name} Cost (Controlled)",
            "cost_stat_id": f"{DOMAIN}:cost_controlled",
        },
    }



@dataclass
class MeridianData:
    """Data returned by the coordinator for sensor entities."""

    # Supplier info
    supplier: str = "powershop"
    sensor_name: str = "Powershop"

    # Current-month rates (active rate type)
    rates: dict[str, float] = field(default_factory=dict)
    daily_charge: float = 0.0
    rate_type: str = "special"

    # Both rate types (for sensor attributes)
    base_rates: dict[str, float] = field(default_factory=dict)
    special_rates: dict[str, float] = field(default_factory=dict)
    base_daily: float = 0.0
    special_daily: float = 0.0

    # TOU context (updated at boundary times too)
    tou_period: str = "offpeak"
    current_rate: float = 0.0

    # Solar export
    solar_export_kwh: float = 0.0
    has_solar: bool = False

    # Schedule info
    schedule_network: str = ""
    schedule_summary: dict = field(default_factory=dict)
    schedule_changed: bool = False

    # Status / diagnostics
    last_usage_update: datetime | None = None
    last_rate_scrape: str | None = None
    cache_months_special: int = 0
    cache_months_base: int = 0
    stats_days: int = 0
    stats_rows: int = 0

    # Account balance (dict with ahead, future_packs, daily_cost)
    balance: dict[str, float | None] | None = None

    # Detected TOU period keys from rate table
    detected_periods: list[str] = field(default_factory=list)



class MeridianCoordinator(DataUpdateCoordinator[MeridianData]):
    """Coordinate all energy data: rates, usage, statistics."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: MeridianEnergyApi,
        entry: ConfigEntry,
        rate_type: str = "special",
        network: str = "Vector",
        supplier: str = DEFAULT_SUPPLIER,
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
        self._rate_type = rate_type
        cache_dir = Path(hass.config.path())
        self._rate_cache = RateCache(cache_dir)
        self._schedule_cache = ScheduleCache(cache_dir)
        self._network = network
        self._supplier = supplier
        self._sensor_name = SUPPLIER_CONFIG[supplier]["name"]
        self._period_meta = _build_period_meta(self._sensor_name)

    @property
    def rate_type(self) -> str:
        """Return the active rate type."""
        return self._rate_type

    @rate_type.setter
    def rate_type(self, value: str) -> None:
        self._rate_type = value

    @property
    def network(self) -> str:
        """Return the configured network ID."""
        return self._network

    @network.setter
    def network(self, value: str) -> None:
        self._network = value
        self._schedule_cache.network = value

    @property
    def supplier(self) -> str:
        """Return the configured supplier."""
        return self._supplier

    @supplier.setter
    def supplier(self, value: str) -> None:
        self._supplier = value
        self._sensor_name = SUPPLIER_CONFIG[value]["name"]
        self._period_meta = _build_period_meta(self._sensor_name)

    @property
    def sensor_name(self) -> str:
        """Return the display name for the supplier."""
        return self._sensor_name

    @property
    def schedule_cache(self) -> ScheduleCache:
        """Expose the schedule cache for sensor boundary lookups."""
        return self._schedule_cache

    def get_current_tou_period(self) -> str:
        """Determine the current TOU period from the schedule cache."""
        now = datetime.now(NZ_TZ)
        schedule = self._schedule_cache.get_schedule_for(now)
        return classify_period(now, schedule)

    def get_current_rate(self) -> float:
        """Return the $/kWh rate for the current TOU period."""
        if not self.data or not self.data.rates:
            return 0.0
        return self.data.rates.get(self.get_current_tou_period(), 0.0)

    async def _async_setup(self) -> None:
        """One-time setup: load caches from disk (blocking, in executor)."""
        await self.hass.async_add_executor_job(self._sync_load_caches)

    def _sync_load_caches(self) -> None:
        """Load rate + schedule caches (blocking I/O)."""
        self._rate_cache.load()
        self._schedule_cache.load()
        if not self._schedule_cache.has_schedules:
            _LOGGER.warning(
                "No valid schedule cache — using default TOU boundaries for %s. "
                "Use the %s.update_schedule service to set correct boundaries.",
                self._network, DOMAIN,
            )
            self._schedule_cache.initialise(self._network)
        elif self._schedule_cache.network != self._network:
            self._schedule_cache.network = self._network

    async def async_force_rate_refresh(self) -> None:
        """Force a rate-cache refresh, then update sensors."""
        await self.hass.async_add_executor_job(self._sync_refresh_rates, True)
        await self.async_refresh()

    async def async_reimport_history(self) -> None:
        """Re-download and reprocess all CSV history."""
        await self.async_refresh()

    async def async_check_schedule(self) -> dict:
        """Check the Get Shifty page for schedule changes.

        Returns a dict with change detection results.
        """
        result = await self.hass.async_add_executor_job(
            self._schedule_cache.check_for_changes, self._api.session
        )
        if result.get("changed"):
            _LOGGER.warning(
                "TOU schedule image has changed! Please verify your "
                "TOU boundaries are still correct."
            )
        return result

    async def _async_update_data(self) -> MeridianData:
        """Fetch, parse, publish statistics, and return sensor data."""
        try:
            result = await self.hass.async_add_executor_job(
                self._sync_fetch_and_process
            )
        except ConfigEntryAuthFailed:
            raise  # Let HA trigger reauth flow
        except (RequestException, OSError, ValueError, KeyError, TypeError, csv.Error) as err:
            raise UpdateFailed(f"Update failed: {err}") from err

        # Publish external statistics when CSV was processed successfully
        if result.get("stats") and result.get("daily"):
            try:
                await self._publish_statistics(
                    result["stats"], result["daily"], result.get("solar"),
                )
            except (KeyError, ValueError, TypeError) as exc:
                _LOGGER.error("Failed to publish statistics: %s", exc)

        tou = self.get_current_tou_period()

        return MeridianData(
            supplier=self._supplier,
            sensor_name=self._sensor_name,
            rates=result["rates"],
            daily_charge=result["daily_charge"],
            rate_type=self._rate_type,
            base_rates=result["base_rates"],
            special_rates=result["special_rates"],
            base_daily=result["base_daily"],
            special_daily=result["special_daily"],
            tou_period=tou,
            current_rate=result["rates"].get(tou, 0.0),
            solar_export_kwh=result.get("solar_kwh", 0.0),
            has_solar=result.get("has_solar", False),
            schedule_network=self._schedule_cache.network,
            schedule_summary=self._schedule_cache.schedule_summary,
            schedule_changed=result.get("schedule_changed", False),
            last_usage_update=datetime.now() if result.get("stats") else (
                self.data.last_usage_update if self.data else None
            ),
            last_rate_scrape=result["last_rate_scrape"],
            cache_months_special=result["cache_months_special"],
            cache_months_base=result["cache_months_base"],
            stats_days=result.get("stats_days", 0),
            stats_rows=result.get("stats_rows", 0),
            balance=result.get("balance"),
            detected_periods=result.get("detected_periods", []),
        )

    def _sync_fetch_and_process(self) -> dict:
        """Login, refresh rates, download CSV, parse.

        Always returns a dict with rate data (from cache).
        ``stats`` / ``daily`` keys are ``None`` if CSV was unavailable.
        """
        now = datetime.now()
        stats = None
        daily = None
        solar = None
        solar_kwh = 0.0
        has_solar = False
        stats_rows = 0
        stats_days = 0
        schedule_changed = False
        balance = None

        try:
            self._api.token()

            if self._api.logged_in:
                # Refresh rate cache using the authenticated session
                self._sync_refresh_rates(force=False)

                # Fetch account balance
                balance = self._api.get_balance()

                # Check TOU schedule monthly (uses public page, no auth needed)
                if self._schedule_cache.needs_check():
                    result = self._schedule_cache.check_for_changes()
                    schedule_changed = result.get("changed", False)
                    if schedule_changed:
                        _LOGGER.warning(
                            "TOU schedule image has changed for %s! "
                            "Please check your TOU boundaries.",
                            self._schedule_cache.network,
                        )

                # Download and process CSV (always full history for correct
                # cumulative statistics — fetched once per 24 h cycle)
                csv_text = self._api.get_data()
                if csv_text:
                    csv_result = self._process_csv(csv_text)
                    if csv_result is not None:
                        stats, daily, solar, solar_kwh, has_solar, stats_rows, stats_days = csv_result
            else:
                raise ConfigEntryAuthFailed(
                    f"{self._sensor_name} login failed — credentials may be invalid"
                )
        except ConfigEntryAuthFailed:
            raise  # Let HA handle reauth
        except (OSError, ConnectionError) as exc:
            _LOGGER.error("%s network error: %s", self._sensor_name, exc)
        except (ValueError, KeyError, csv.Error) as exc:
            _LOGGER.error("%s data processing error: %s", self._sensor_name, exc)

        # Rate lookups always succeed (cache + fallback)
        rates = self._rate_cache.get_rates(now.year, now.month, self._rate_type)
        daily_charge = self._rate_cache.get_daily_charge(
            now.year, now.month, self._rate_type
        )
        base_rates = self._rate_cache.get_rates(now.year, now.month, "base")
        special_rates = self._rate_cache.get_rates(now.year, now.month, "special")
        base_daily = self._rate_cache.get_daily_charge(now.year, now.month, "base")
        special_daily = self._rate_cache.get_daily_charge(
            now.year, now.month, "special"
        )

        return {
            "stats": stats,
            "daily": daily,
            "solar": solar,
            "solar_kwh": solar_kwh,
            "has_solar": has_solar,
            "stats_rows": stats_rows,
            "stats_days": stats_days,
            "schedule_changed": schedule_changed,
            "balance": balance,
            "rates": rates,
            "daily_charge": daily_charge,
            "base_rates": base_rates,
            "special_rates": special_rates,
            "base_daily": base_daily,
            "special_daily": special_daily,
            "last_rate_scrape": self._rate_cache.last_updated,
            "cache_months_special": self._rate_cache.cache_months_special,
            "cache_months_base": self._rate_cache.cache_months_base,
            "detected_periods": self._rate_cache.detected_periods,
        }

    def _sync_refresh_rates(self, force: bool = False) -> None:
        """Refresh the rate cache if stale (or forced).

        Both Powershop and Meridian use the Flux Federation platform,
        so the same /rates page is available on both portals.
        """
        if force or self._rate_cache.needs_refresh():
            base_url = SUPPLIER_CONFIG[self._supplier]["base_url"]
            _LOGGER.info("Refreshing rate cache (force=%s, supplier=%s)", force, self._supplier)
            if self._rate_cache.scrape_and_update(self._api.session, base_url):
                _LOGGER.info("Rate cache refreshed successfully")
            else:
                _LOGGER.warning("Rate scrape failed — using cached rates")

    def _process_csv(
        self, csv_text: str
    ) -> tuple[dict, dict, dict | None, float, bool, int, int] | None:
        """Parse EIEP 13A CSV and compute external-statistics data.

        Returns ``(stats, daily_data, solar_data, solar_kwh, has_solar,
        row_count, day_count)`` or ``None`` if the CSV contained no valid
        rows.
        """
        parsed_rows, solar_rows = self._parse_csv_rows(csv_text)

        if not parsed_rows and not solar_rows:
            _LOGGER.warning("CSV parsed but contained no valid rows")
            return None

        _LOGGER.debug(
            "Pass 1 complete: %d consumption rows + %d solar rows across %d days",
            len(parsed_rows),
            len(solar_rows),
            len({r[0].date() for r in parsed_rows}),
        )

        stats, daily_data = self._accumulate_stats(parsed_rows)
        solar_data, solar_total_kwh, has_solar = self._accumulate_solar(solar_rows)

        days_seen = len({r[0].date() for r in parsed_rows})

        _LOGGER.info(
            "TOU statistics — night: %.2f kWh ($%.2f), "
            "peak: %.2f kWh ($%.2f), offpeak: %.2f kWh ($%.2f), "
            "weekend_offpeak: %.2f kWh ($%.2f), "
            "controlled: %.2f kWh ($%.2f), daily_charge: $%.2f (%d days)%s",
            stats["night"]["sum"],
            stats["night"]["cost"],
            stats["peak"]["sum"],
            stats["peak"]["cost"],
            stats["offpeak"]["sum"],
            stats["offpeak"]["cost"],
            stats["weekend_offpeak"]["sum"],
            stats["weekend_offpeak"]["cost"],
            stats["controlled"]["sum"],
            stats["controlled"]["cost"],
            daily_data["charge_sum"],
            days_seen,
            f", solar: {solar_total_kwh:.2f} kWh" if has_solar else "",
        )

        daily = {
            "energy_data": daily_data["energy_data"],
            "cost_data": daily_data["cost_data"],
        }

        return stats, daily, solar_data, solar_total_kwh, has_solar, len(parsed_rows), days_seen

    def _parse_csv_rows(
        self, csv_text: str
    ) -> tuple[
        list[tuple[datetime, str, float, tuple[int, int]]],
        list[tuple[datetime, float]],
    ]:
        """Parse EIEP 13A CSV into classified consumption and solar rows."""
        parsed_rows: list[tuple[datetime, str, float, tuple[int, int]]] = []
        solar_rows: list[tuple[datetime, float]] = []
        csv_file = csv.reader(StringIO(csv_text))

        for row in csv_file:
            if len(row) < 13 or row[0] != "DET":
                continue

            flow_direction = row[6].strip()
            channel_type = row[7].strip()

            if not flow_direction or not row[9].strip():
                _LOGGER.debug("Skipping row with empty critical field: %s", row[:5])
                continue

            if row[11] != "RD":
                _LOGGER.debug("Skipping non-RD row (read_type=%s): %s", row[11], row[:5])
                continue

            try:
                start_date = datetime.strptime(row[9], "%d/%m/%Y %H:%M:%S")
            except (ValueError, IndexError):
                continue

            # Skip daily summary rows (span > 1 hour)
            read_end = row[10]
            if read_end:
                try:
                    end_date = datetime.strptime(read_end, "%d/%m/%Y %H:%M:%S")
                    if (end_date - start_date).total_seconds() > 3600:
                        continue
                except (ValueError, IndexError):
                    pass

            # Skip HH:59 daily aggregate summaries
            if start_date.minute == 59:
                continue

            start_date = start_date.replace(tzinfo=NZ_TZ)
            rounded_date = start_date.replace(minute=0, second=0, microsecond=0)

            try:
                energy = float(row[12])
            except (ValueError, IndexError):
                continue

            if flow_direction == "I":
                solar_rows.append((rounded_date, energy))
                continue

            period = (
                "controlled" if channel_type == "CN"
                else classify_period(
                    start_date,
                    self._schedule_cache.get_schedule_for(start_date),
                )
            )
            parsed_rows.append((rounded_date, period, energy, (start_date.year, start_date.month)))

        return parsed_rows, solar_rows

    def _accumulate_stats(
        self, parsed_rows: list[tuple[datetime, str, float, tuple[int, int]]]
    ) -> tuple[dict, dict]:
        """Accumulate TOU statistics and daily charges from parsed rows.

        Returns ``(stats, daily_data)`` where ``daily_data`` includes
        ``energy_data``, ``cost_data``, and ``charge_sum``.
        """
        stats: dict[str, dict] = {
            p: {"sum": 0.0, "cost": 0.0, "data": [], "cost_data": []}
            for p in ("night", "peak", "offpeak", "weekend_offpeak", "controlled")
        }

        daily_charge_sum = 0.0
        daily_energy_data: list[StatisticData] = []
        daily_cost_data: list[StatisticData] = []
        days_seen: set[date] = set()
        rates_cache: dict[tuple[int, int], dict[str, float]] = {}
        daily_cache: dict[tuple[int, int], float] = {}

        for rounded_date, period, energy, month_key in parsed_rows:
            stats[period]["sum"] += energy
            stats[period]["data"].append(
                StatisticData(start=rounded_date, sum=stats[period]["sum"])
            )

            if month_key not in rates_cache:
                rates_cache[month_key] = self._rate_cache.get_rates(
                    *month_key, self._rate_type
                )
            kwh_cost = energy * rates_cache[month_key].get(period, 0)
            stats[period]["cost"] += kwh_cost
            stats[period]["cost_data"].append(
                StatisticData(start=rounded_date, sum=stats[period]["cost"])
            )

            day_key = rounded_date.date()
            if day_key not in days_seen:
                days_seen.add(day_key)
                if month_key not in daily_cache:
                    daily_cache[month_key] = self._rate_cache.get_daily_charge(
                        *month_key, self._rate_type
                    )
                daily_charge_sum += daily_cache[month_key]
                daily_energy_data.append(
                    StatisticData(start=rounded_date, sum=0.0)
                )
                daily_cost_data.append(
                    StatisticData(start=rounded_date, sum=daily_charge_sum)
                )

        return stats, {
            "energy_data": daily_energy_data,
            "cost_data": daily_cost_data,
            "charge_sum": daily_charge_sum,
        }

    @staticmethod
    def _accumulate_solar(
        solar_rows: list[tuple[datetime, float]],
    ) -> tuple[dict | None, float, bool]:
        """Accumulate solar export statistics.

        Returns ``(solar_data, total_kwh, has_solar)``.
        """
        if not solar_rows:
            return None, 0.0, False

        solar_total_kwh = 0.0
        solar_stat_data: list[StatisticData] = []
        for rounded_date, energy in solar_rows:
            solar_total_kwh += energy
            solar_stat_data.append(
                StatisticData(start=rounded_date, sum=solar_total_kwh)
            )
        _LOGGER.info(
            "Solar export: %.2f kWh total (%d rows)",
            solar_total_kwh,
            len(solar_rows),
        )
        return {"data": solar_stat_data}, solar_total_kwh, True

    async def _publish_statistics(
        self, stats: dict, daily: dict, solar: dict | None = None,
    ) -> None:
        """Publish all external statistics to the recorder.

        Base: 12 stats (5 TOU × energy+cost + daily charge × energy+cost).
        Plus 1 solar export stat if solar data is present (total: 13).
        """
        for period_key, meta in self._period_meta.items():
            energy_metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                name=meta["name"],
                source=DOMAIN,
                statistic_id=meta["stat_id"],
                unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                mean_type=StatisticMeanType.NONE,
                unit_class="energy",
            )
            async_add_external_statistics(
                self.hass, energy_metadata, stats[period_key]["data"]
            )

            cost_metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                name=meta["cost_name"],
                source=DOMAIN,
                statistic_id=meta["cost_stat_id"],
                unit_of_measurement="NZD",
                mean_type=StatisticMeanType.NONE,
                unit_class=None,
            )
            async_add_external_statistics(
                self.hass, cost_metadata, stats[period_key]["cost_data"]
            )

        # Daily connection charge
        daily_energy_meta = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{self._sensor_name} (Daily Charge)",
            source=DOMAIN,
            statistic_id=f"{DOMAIN}:consumption_daily_charge",
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            mean_type=StatisticMeanType.NONE,
            unit_class="energy",
        )
        async_add_external_statistics(
            self.hass, daily_energy_meta, daily["energy_data"]
        )

        daily_cost_meta = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{self._sensor_name} Cost (Daily Charge)",
            source=DOMAIN,
            statistic_id=f"{DOMAIN}:cost_daily_charge",
            unit_of_measurement="NZD",
            mean_type=StatisticMeanType.NONE,
            unit_class=None,
        )
        async_add_external_statistics(
            self.hass, daily_cost_meta, daily["cost_data"]
        )

        # Solar export
        if solar and solar.get("data"):
            solar_meta = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                name=f"{self._sensor_name} (Solar Export)",
                source=DOMAIN,
                statistic_id=f"{DOMAIN}:return_to_grid",
                unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                mean_type=StatisticMeanType.NONE,
                unit_class="energy",
            )
            async_add_external_statistics(
                self.hass, solar_meta, solar["data"]
            )
