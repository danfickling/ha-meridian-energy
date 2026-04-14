"""TOU schedule management for Meridian Energy integration (v2 — Kraken API).

Parses the time-of-use scheme from the GraphQL API and provides
`classify_period(dt)` for both live and historical classification.
"""

from __future__ import annotations

import logging
from datetime import datetime, time

from .rates import classify_bucket
from .const import PERIOD_OFFPEAK

_LOGGER = logging.getLogger(__name__)


def _parse_time(s: str) -> time:
    """Parse 'HH:MM' or 'HH:MM:SS' to a time object."""
    parts = s.split(":")
    if len(parts) < 2:
        raise ValueError(f"Invalid TOU time format: {s!r}")
    try:
        return time(int(parts[0]), int(parts[1]))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid TOU time format: {s!r}") from exc


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


def parse_tou_scheme(api_schemes: list[dict]) -> dict:
    """Parse timeOfUseSchemes from the API into a schedule dict.

    Input: list of scheme dicts from ``agreement.timeOfUseSchemes[]``,
    each with ``name`` and ``timeslots[]``.  Each timeslot has:
    ``timeslot`` (bucket name), ``activeFrom``, ``activeTo``,
    ``weekdays`` (bool), ``weekends`` (bool).

    Day semantics:
    - weekdays=False, weekends=False → applies to ALL days
    - weekdays=True,  weekends=False → weekdays only
    - weekdays=False, weekends=True  → weekends only

    Returns a schedule dict::

        {
            "scheme_name": "ST06",
            "timeslots": [
                {
                    "period": "night",        # canonical period key
                    "bucket": "N9",           # raw API bucket name
                    "start": "22:00",
                    "end": "07:00",
                    "weekdays": True,         # normalised: applies on weekdays?
                    "weekends": True,         # normalised: applies on weekends?
                },
                ...
            ],
        }
    """
    if not api_schemes:
        return {"scheme_name": "", "timeslots": []}

    scheme = api_schemes[0]
    scheme_name = scheme.get("name", "")
    parsed_slots: list[dict] = []

    for slot in scheme.get("timeslots") or []:
        # Support both new format (timeslot) and legacy (name)
        bucket = (slot.get("timeslot") or slot.get("name") or "").strip()
        period = classify_bucket(bucket)
        if not period:
            continue

        # New API format: activeFrom / activeTo with HH:MM:SS
        start = slot.get("activeFrom") or slot.get("startTime") or "00:00"
        end = slot.get("activeTo") or slot.get("endTime") or "00:00"

        # Normalise to HH:MM
        if len(start) > 5:
            start = start[:5]
        if len(end) > 5:
            end = end[:5]

        # Resolve day applicability.
        # New API: weekdays/weekends are top-level bools where
        #   false+false = all days, true+false = weekdays only, etc.
        # Legacy API: nested activationRules with true+true = all days.
        if "activationRules" in slot:
            # Legacy format
            rules = slot["activationRules"]
            applies_weekdays = rules.get("weekdays", True)
            applies_weekends = rules.get("weekends", True)
        else:
            # New format: both-false means "all days"
            wd = slot.get("weekdays", False)
            we = slot.get("weekends", False)
            if not wd and not we:
                applies_weekdays = True
                applies_weekends = True
            else:
                applies_weekdays = bool(wd)
                applies_weekends = bool(we)

        parsed_slots.append({
            "period": period,
            "bucket": bucket,
            "start": start,
            "end": end,
            "weekdays": applies_weekdays,
            "weekends": applies_weekends,
        })

    return {
        "scheme_name": scheme_name,
        "timeslots": parsed_slots,
    }


def classify_period(dt: datetime, schedule: dict) -> str:
    """Return the canonical TOU period key for a given local datetime.

    Uses the parsed schedule from ``parse_tou_scheme()``.
    Returns the period directly from the matching timeslot.
    """
    if not schedule or not schedule.get("timeslots"):
        return PERIOD_OFFPEAK

    minutes = dt.hour * 60 + dt.minute
    is_weekend = dt.weekday() >= 5

    for slot in schedule["timeslots"]:
        if is_weekend and not slot.get("weekends", True):
            continue
        if not is_weekend and not slot.get("weekdays", True):
            continue

        if _in_range(minutes, slot["start"], slot["end"]):
            return slot["period"]

    return PERIOD_OFFPEAK


def get_boundary_times(schedule: dict) -> list[tuple[int, int]]:
    """Extract unique boundary (hour, minute) pairs from a schedule.

    These are the times when the TOU period might change, used by sensors
    to register ``async_track_time_change`` listeners.
    """
    boundaries: set[tuple[int, int]] = set()
    for slot in schedule.get("timeslots") or []:
        for key in ("start", "end"):
            t = _parse_time(slot[key])
            boundaries.add((t.hour, t.minute))
    return sorted(boundaries)