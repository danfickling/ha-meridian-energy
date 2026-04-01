"""TOU schedule management for Meridian Energy integration.

Manages the Time-of-Use schedule:
- Stores TOU boundary definitions in a persistent JSON cache
- Scrapes the "Get Shifty" page to detect schedule changes
  (image URL + hash per network)
- Provides `classify_period(dt)` for both live and historical classification
- Supports schedule history — old boundaries are kept so that historical
  CSV rows are always classified with the schedule that was active at their time

Schedule cache format (schedule_cache.json):
```json
{
  "network": "Vector",
  "network_name": "Vector, United Networks (Auckland)",
  "schedules": [
    {
      "effective_from": "2023-06-09T00:00:00",
      "night_start": "22:00",
      "night_end": "07:00",
      "peak_weekday": [["07:00","09:30"], ["17:30","20:00"]],
      "weekend_offpeak": true
    }
  ],
  "scheme_url": "https://www.powershop.co.nz/public/Get-Shifty-schemes/ST06-26__...",
  "scheme_hash": "90a509c8397cec16ad3651c83291557c",
  "last_checked": "2026-03-12T00:00:00"
}
```
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from .const import HTTP_TIMEOUT

_LOGGER = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path(__file__).parent
CACHE_FILENAME = "meridian_schedule_cache.json"

# How often to check the Get Shifty page for changes (days).
CHECK_INTERVAL_DAYS = 30

GET_SHIFTY_URL = "https://www.powershop.co.nz/get-shifty/"

# ---------------------------------------------------------------------------
# Per-network TOU schedule definitions
#
# Extracted from the Get Shifty page (CSS-encoded bar charts).
# Schedule keys:
#   night_start / night_end  - Night period wrapping midnight.  Omit for
#                              networks that have no night rate.
#   peak_weekday             - [[start, end], ...] peak windows on weekdays.
#   weekend_offpeak          - When truthy the entire weekend is classified
#                              as "weekend_offpeak" (outside of night).
#                              When falsy / absent, weekday peak windows
#                              also apply on weekends ("everyday" schedules).
# ---------------------------------------------------------------------------

# Schedule A: Night 22-07, Peak WD 07:00-09:30 + 17:30-20:00, WE offpeak
_SCHED_A = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "22:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
    "weekend_offpeak": True,
}

# Schedule B: Night 22-07, Peak ED 07:00-09:30 + 17:30-20:00 (everyday)
_SCHED_B = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "22:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
}

# Schedule C: Night 23-07, Peak ED 07:00-09:30 + 17:30-20:00 (everyday)
_SCHED_C = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "23:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "09:30"], ["17:30", "20:00"]],
}

# Schedule D: No night, Peak WD 07:00-11:00 + 17:00-21:00, WE offpeak
_SCHED_D = {
    "effective_from": "2023-06-09T00:00:00",
    "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
    "weekend_offpeak": True,
}

# Schedule E: Night 23-07, Peak ED 07:00-11:00 + 17:00-21:00 (everyday)
_SCHED_E = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "23:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
}

# Schedule F: Night 22-07, Peak ED 07:00-11:00 + 17:00-21:00 (everyday)
_SCHED_F = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "22:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
}

# Schedule G: No night, Peak ED 07:00-11:00 + 17:00-21:00 (everyday)
_SCHED_G = {
    "effective_from": "2023-06-09T00:00:00",
    "peak_weekday": [["07:00", "11:00"], ["17:00", "21:00"]],
}

# Schedule H: Night 23-07, Day 07:00-23:00 (day/night only, everyday)
_SCHED_H = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "23:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "23:00"]],
}

# Schedule I: Night 23-07, Peak WD 07:00-23:00, WE offpeak
_SCHED_I = {
    "effective_from": "2023-06-09T00:00:00",
    "night_start": "23:00",
    "night_end": "07:00",
    "peak_weekday": [["07:00", "23:00"]],
    "weekend_offpeak": True,
}

# Schedule J: No night, Peak WD 07:00-23:00, WE offpeak
_SCHED_J = {
    "effective_from": "2023-06-09T00:00:00",
    "peak_weekday": [["07:00", "23:00"]],
    "weekend_offpeak": True,
}

# Schedule K: No night, Peak WD 07:00-21:00, WE offpeak
_SCHED_K = {
    "effective_from": "2023-06-09T00:00:00",
    "peak_weekday": [["07:00", "21:00"]],
    "weekend_offpeak": True,
}

# Schedule L: No night, Peak ED 07:00-12:00 + 17:00-22:00 (everyday)
_SCHED_L = {
    "effective_from": "2023-06-09T00:00:00",
    "peak_weekday": [["07:00", "12:00"], ["17:00", "22:00"]],
}

# Map each network ID to its schedule definition
NETWORK_SCHEDULES: dict[str, dict] = {
    # Schedule A: Night 22-07, Peak WD 07:00-09:30+17:30-20:00, WE offpeak
    "TopEnergy_1_": _SCHED_A,
    "NorthPower_1_": _SCHED_A,
    "WelNetworks": _SCHED_A,
    # Schedule B: Night 22-07, Peak ED 07:00-09:30+17:30-20:00
    "WaipaNetworks": _SCHED_B,
    # Schedule C: Night 23-07, Peak ED 07:00-09:30+17:30-20:00
    "LinesCompany": _SCHED_C,
    # Schedule D: No night, Peak WD 07:00-11:00+17:00-21:00, WE offpeak
    "Vector": _SCHED_D,
    "CountiesPower": _SCHED_D,
    "PowercoEast": _SCHED_D,
    "PowercoWest": _SCHED_D,
    "WellingtonElectricity": _SCHED_D,
    # Schedule E: Night 23-07, Peak ED 07:00-11:00+17:00-21:00
    "Unison_1_": _SCHED_E,
    "Electra": _SCHED_E,
    "OtagonetJointVenture": _SCHED_E,
    "PowernetLtd": _SCHED_E,
    # Schedule F: Night 22-07, Peak ED 07:00-11:00+17:00-21:00
    "Horizon_1_": _SCHED_F,
    # Schedule G: No night, Peak ED 07:00-11:00+17:00-21:00
    "CentralLines": _SCHED_G,
    # Schedule H: Night 23-07, Day/Night only (everyday)
    "ScanPower": _SCHED_H,
    "Buller": _SCHED_H,
    "WestPower": _SCHED_H,
    "AlpineEnergy": _SCHED_H,
    "NetworkWaitaki": _SCHED_H,
    # Schedule I: Night 23-07, Peak WD 07:00-23:00, WE offpeak
    "NetworkTasman": _SCHED_I,
    # Schedule J: No night, Peak WD 07:00-23:00, WE offpeak
    "Nelson_1_": _SCHED_J,
    # Schedule K: No night, Peak WD 07:00-21:00, WE offpeak
    "Orion": _SCHED_K,
    "eaNetworks_1_": _SCHED_K,
    # Schedule L: No night, Peak ED 07:00-12:00+17:00-22:00
    "Aurora_1_": _SCHED_L,
    # Schedule M/N: Same as D (Eastland, Marlborough, Mainpower)
    "Eastland_1_": _SCHED_D,
    "Marlborough_1_": _SCHED_D,
    "Mainpower_1_": _SCHED_D,
}

# Default schedule: Vector (Auckland)
_DEFAULT_SCHEDULE = _SCHED_D


def _parse_time(s: str) -> time:
    """Parse 'HH:MM' to a time object."""
    parts = s.split(":")
    return time(int(parts[0]), int(parts[1]))


def _time_to_minutes(t: time) -> int:
    """Convert time to minutes since midnight."""
    return t.hour * 60 + t.minute


def _in_range(minutes: int, start: str, end: str) -> bool:
    """Check if minutes-since-midnight falls within [start, end)."""
    s = _time_to_minutes(_parse_time(start))
    e = _time_to_minutes(_parse_time(end))
    if s < e:
        return s <= minutes < e
    # Wraps midnight (e.g. 22:00-07:00)
    return minutes >= s or minutes < e


def classify_period(dt: datetime, schedule: dict | None = None) -> str:
    """Return the TOU period name for a given local datetime.

    Uses the provided schedule dict (one entry from the schedules list).
    Returns one of: 'night', 'peak', 'offpeak', 'weekend_offpeak'.

    Schedule fields:
    - ``night_start`` / ``night_end``: optional.  When present the night
      period wraps midnight.  Omit for networks without a night rate.
    - ``peak_weekday``: list of ``[start, end]`` peak windows.
    - ``weekend_offpeak``: optional.  When present weekends are classified
      as "weekend_offpeak" (outside of night).  When absent the weekday
      peak windows also apply on weekends ("everyday" schedules).
    """
    if schedule is None:
        schedule = _DEFAULT_SCHEDULE

    minutes = dt.hour * 60 + dt.minute
    is_weekend = dt.weekday() >= 5

    # Night: wraps midnight (only if the schedule defines a night period)
    night_start = schedule.get("night_start")
    night_end = schedule.get("night_end")
    if night_start and night_end:
        if _in_range(minutes, night_start, night_end):
            return "night"

    # Weekend handling
    if is_weekend:
        if schedule.get("weekend_offpeak"):
            # Explicit weekend definition — all non-night time is offpeak
            return "weekend_offpeak"
        # No weekend definition — weekday peaks also apply on weekends
        for start, end in schedule.get("peak_weekday", []):
            if _in_range(minutes, start, end):
                return "peak"
        return "offpeak"

    # Weekday peak periods
    for start, end in schedule.get("peak_weekday", []):
        if _in_range(minutes, start, end):
            return "peak"

    # Everything else on weekdays is off-peak
    return "offpeak"


def get_boundary_times(schedule: dict | None = None) -> list[tuple[int, int]]:
    """Extract unique boundary (hour, minute) pairs from a schedule.

    These are the times when the TOU period might change, used by sensors
    to register `async_track_time_change` listeners.
    """
    if schedule is None:
        schedule = _DEFAULT_SCHEDULE

    boundaries: set[tuple[int, int]] = set()

    # Night boundaries (optional)
    for key in ("night_start", "night_end"):
        val = schedule.get(key)
        if val:
            t = _parse_time(val)
            boundaries.add((t.hour, t.minute))

    # Peak boundaries
    for start, end in schedule.get("peak_weekday", []):
        t = _parse_time(start)
        boundaries.add((t.hour, t.minute))
        t = _parse_time(end)
        boundaries.add((t.hour, t.minute))

    # Weekend off-peak is a boolean flag; no extra boundaries needed.
    # When True, the entire weekend (outside night) is off-peak and
    # transitions are already captured by night and peak boundaries.

    return sorted(boundaries)


# Network list (scraped from Get Shifty page)
# accordion-ID -> display name.  Used for the config flow selector.
NETWORKS: dict[str, str] = {
    "TopEnergy_1_": "Top Energy (Kaitaia, Kerikeri, Pahia)",
    "NorthPower_1_": "Northpower (Whangarei)",
    "Vector": "Vector, United Networks (Auckland)",
    "CountiesPower": "Counties Power (Counties)",
    "PowercoEast": "Powerco East (Tauranga, Coromandel, Thames Valley)",
    "WelNetworks": "WEL Networks (Hamilton)",
    "WaipaNetworks": "Waipa Networks (Waipa District)",
    "LinesCompany": "The Lines Company (King Country)",
    "Unison_1_": "Unison (Taupo, Rotorua, Hawke's Bay)",
    "PowercoWest": "Powerco West (Taranaki, Whanganui, Wairarapa)",
    "Horizon_1_": "Horizon (Eastern Bay of Plenty)",
    "Eastland_1_": "Firstlight Network (Gisborne)",
    "Electra": "Electra (Kapiti & Horowhenua)",
    "CentralLines": "Centralines (Central Hawke's Bay)",
    "ScanPower": "Scanpower (Southern Hawke's Bay)",
    "WellingtonElectricity": "Wellington Electricity (Wellington)",
    "Nelson_1_": "Nelson Electricity (Nelson City)",
    "NetworkTasman": "Network Tasman (Tasman)",
    "Buller": "Buller (West Coast)",
    "WestPower": "Westpower (Westland)",
    "Marlborough_1_": "Marlborough Lines (Marlborough)",
    "Mainpower_1_": "Mainpower (North Canterbury)",
    "Orion": "Orion (Christchurch)",
    "eaNetworks_1_": "Electricity Ashburton (Mid Canterbury)",
    "AlpineEnergy": "Alpine Energy (South Canterbury)",
    "NetworkWaitaki": "Network Waitaki (Waitaki)",
    "OtagonetJointVenture": "OtagoNet JV (East and South Otago)",
    "Aurora_1_": "Aurora (Central Otago, Dunedin, Queenstown)",
    "PowernetLtd": "The Power Company (Southland, Invercargill)",
}


def _schedules_equal(a: dict, b: dict) -> bool:
    """Return True if two schedules define the same TOU boundaries.

    Compares only the boundary-defining keys, ignoring ``effective_from``
    which differs between entries by design.
    """
    _KEYS = ("night_start", "night_end", "peak_weekday", "weekend_offpeak")
    return all(a.get(k) == b.get(k) for k in _KEYS)


class ScheduleCache:
    """Persistent TOU schedule cache with change detection."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir
        self._cache_path = (cache_dir or _DEFAULT_CACHE_DIR) / CACHE_FILENAME
        self._data: dict = {}
        self._loaded = False

    def load(self) -> None:
        """Load the schedule cache from disk."""
        if self._loaded:
            return
        if self._cache_path.exists():
            try:
                with open(self._cache_path) as fh:
                    self._data = json.load(fh)
                _LOGGER.info(
                    "Schedule cache loaded: network=%s, %d schedule(s)",
                    self._data.get("network_name", "?"),
                    len(self._data.get("schedules", [])),
                )
            except (json.JSONDecodeError, OSError) as exc:
                _LOGGER.warning("Failed to load schedule cache: %s", exc)
                self._data = {}
        else:
            _LOGGER.info("No schedule cache — will use defaults")
        self._loaded = True

    def _save(self) -> None:
        """Atomically write the schedule cache to disk."""
        if self._cache_dir is None:
            return  # No explicit cache dir — skip writes (test/dev safety)
        tmp = self._cache_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as fh:
                json.dump(self._data, fh, default=str)
            tmp.replace(self._cache_path)
        except Exception as exc:
            _LOGGER.error("Failed to save schedule cache: %s", exc)
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def initialise(self, network: str) -> None:
        """Seed the cache with default schedule for the given network.

        Called when no cache exists yet (first install).
        Uses the network-specific schedule from NETWORK_SCHEDULES,
        falling back to _DEFAULT_SCHEDULE for unknown networks.
        """
        sched = NETWORK_SCHEDULES.get(network, _DEFAULT_SCHEDULE)
        self._data = {
            "network": network,
            "network_name": NETWORKS.get(network, network),
            "schedules": [sched.copy()],
            "scheme_url": None,
            "scheme_hash": None,
            "last_checked": None,
        }
        self._save()
        _LOGGER.info("Schedule cache initialised for %s", network)

    @property
    def has_schedules(self) -> bool:
        """Return True if at least one schedule is loaded."""
        return bool(self._data.get("schedules"))

    @property
    def network(self) -> str:
        """Return the configured network ID."""
        return self._data.get("network", "Vector")

    @network.setter
    def network(self, value: str) -> None:
        if self._data.get("network") != value:
            self._data["network"] = value
            self._data["network_name"] = NETWORKS.get(value, value)
            # Append the new network's default schedule so TOU
            # classification switches immediately — but only if it
            # differs from the current latest schedule (de-duplicate).
            new_sched = NETWORK_SCHEDULES.get(value, _DEFAULT_SCHEDULE).copy()
            schedules = self._data.setdefault("schedules", [])
            if not schedules or not _schedules_equal(schedules[-1], new_sched):
                schedules.append(new_sched)
                _LOGGER.info(
                    "Network changed to %s — new schedule appended", value
                )
            else:
                _LOGGER.info(
                    "Network changed to %s — schedule unchanged, skipped append",
                    value,
                )
            self._save()

    def get_schedule_for(self, dt: datetime | None = None) -> dict:
        """Return the schedule that was active at the given datetime.

        Walks the schedule list (sorted by effective_from descending) and
        returns the first one where effective_from <= dt.  Falls back to
        the default schedule if none match.
        """
        schedules = self._data.get("schedules", [])
        if not schedules:
            return _DEFAULT_SCHEDULE

        if dt is None:
            # Current schedule = most recent
            return schedules[-1]

        # Schedules are stored chronologically; walk backwards
        # Strip tzinfo for comparison — effective_from is stored as naive (local)
        dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
        for sched in reversed(schedules):
            eff = datetime.fromisoformat(sched["effective_from"])
            if dt_naive >= eff:
                return sched

        # Before all known schedules — use the oldest
        return schedules[0]

    def get_boundary_times(self) -> list[tuple[int, int]]:
        """Return boundary times for the current (latest) schedule."""
        return get_boundary_times(self.get_schedule_for())

    def needs_check(self) -> bool:
        """Return True when we should re-scrape the Get Shifty page.

        Triggers when the cache is older than CHECK_INTERVAL_DAYS
        **or** when a new calendar month has started since the last check
        (to detect schedule changes at month boundaries promptly).
        """
        ts = self._data.get("last_checked")
        if not ts:
            return True
        try:
            last = datetime.fromisoformat(ts)
            now = datetime.now()
            age = (now - last).total_seconds() / 86400
            if age >= CHECK_INTERVAL_DAYS:
                return True
            # Also check at the start of a new month
            if (now.year, now.month) != (last.year, last.month):
                return True
            return False
        except (ValueError, TypeError):
            return True

    def check_for_changes(self, session: requests.Session | None = None) -> dict:
        """Scrape the Get Shifty page and check for schedule image changes.

        Returns a dict with:
          - "changed": bool — True if the scheme image has changed
          - "old_hash": str | None
          - "new_hash": str | None
          - "new_url": str | None
          - "error": str | None — set if scraping failed
        """
        network = self._data.get("network", "Vector")
        old_url = self._data.get("scheme_url")
        old_hash = self._data.get("scheme_hash")

        result = {
            "changed": False,
            "old_hash": old_hash,
            "new_hash": None,
            "new_url": None,
            "error": None,
        }

        try:
            s = session or requests.Session()
            resp = s.get(
                GET_SHIFTY_URL,
                headers={"User-Agent": "Mozilla/5.0 (HomeAssistant MeridianEnergy)"},
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code != 200:
                result["error"] = f"HTTP {resp.status_code}"
                return result

            soup = BeautifulSoup(resp.text, "html.parser")
            accordion_id = f"accordion-{network}"
            details = soup.find("details", id=accordion_id)

            if not details:
                # Fall back to matching by slug ID on wrapper div
                slug = network.lower().replace("_1_", "").replace("_", "-")
                wrapper = soup.find("div", id=slug)
                if wrapper:
                    details = wrapper.find("details")

            if not details:
                result["error"] = f"Network {network} not found on page"
                return result

            content = details.find(
                "div", class_="regionmap-accordion-item__content"
            )
            if not content:
                result["error"] = "No content div found"
                return result

            img = content.find("img")
            if not img:
                result["error"] = "No scheme image found"
                return result

            new_url = img.get("src", "")
            result["new_url"] = new_url

            # Download and hash the image
            img_resp = s.get(new_url, timeout=HTTP_TIMEOUT)
            if img_resp.status_code != 200:
                result["error"] = f"Image download failed: HTTP {img_resp.status_code}"
                return result

            new_hash = hashlib.md5(img_resp.content).hexdigest()
            result["new_hash"] = new_hash

            # Update cache
            self._data["scheme_url"] = new_url
            self._data["scheme_hash"] = new_hash
            self._data["last_checked"] = datetime.now().isoformat()
            self._save()

            if old_hash and new_hash != old_hash:
                result["changed"] = True
                _LOGGER.warning(
                    "TOU schedule image has changed for %s! "
                    "Old hash: %s, New hash: %s. "
                    "Please verify TOU boundaries are still correct.",
                    NETWORKS.get(network, network),
                    old_hash,
                    new_hash,
                )
            elif not old_hash:
                _LOGGER.info(
                    "TOU schedule image recorded for %s: %s",
                    NETWORKS.get(network, network),
                    new_hash,
                )
            else:
                _LOGGER.debug(
                    "TOU schedule image unchanged for %s",
                    NETWORKS.get(network, network),
                )

            return result

        except (OSError, ConnectionError) as exc:
            result["error"] = f"Network error: {exc}"
            _LOGGER.error("Schedule check network error: %s", exc)
            return result
        except (ValueError, KeyError, AttributeError) as exc:
            result["error"] = f"Parse error: {exc}"
            _LOGGER.error("Schedule check parse error: %s", exc)
            return result

    def update_schedule(self, new_schedule: dict) -> None:
        """Add a new schedule entry (effective from now).

        The existing schedule is preserved for historical classification.
        """
        new_schedule.setdefault(
            "effective_from", datetime.now().isoformat()
        )

        schedules = self._data.setdefault("schedules", [])
        schedules.append(new_schedule)

        # Sort by effective_from
        schedules.sort(key=lambda s: s["effective_from"])

        self._save()
        _LOGGER.info(
            "New TOU schedule added (effective from %s)",
            new_schedule["effective_from"],
        )

    @property
    def current_schedule(self) -> dict:
        """Return the currently active schedule."""
        return self.get_schedule_for()

    @property
    def schedule_summary(self) -> dict:
        """Return a summary dict for sensor attributes."""
        sched = self.get_schedule_for()
        return {
            "network": self._data.get("network_name", "Unknown"),
            "night": f"{sched.get('night_start', '?')}-{sched.get('night_end', '?')}",
            "peak_weekday": sched.get("peak_weekday", []),
            "weekend_offpeak": bool(sched.get("weekend_offpeak")),
            "effective_from": sched.get("effective_from", "?"),
            "scheme_hash": self._data.get("scheme_hash"),
            "last_checked": self._data.get("last_checked"),
        }
