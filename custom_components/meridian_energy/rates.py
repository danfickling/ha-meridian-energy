"""Dynamic rate management for Powershop / Meridian TOU rates.

Scrapes rates from the supplier's ``/rates`` page (both Powershop and
Meridian share the same Flux Federation platform), caches them to a
persistent JSON file, and provides lookup functions with seasonal
fallback for months not yet scraped.

The rates page shows a rolling 13-month window (1 past + current +
11 future).  This module accumulates rates over time so that
historical months are never lost from the cache.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from .const import HTTP_TIMEOUT

_LOGGER = logging.getLogger(__name__)

# Default cache file location — overridden at runtime by hass.config.path()
_DEFAULT_CACHE_DIR = Path(__file__).parent
CACHE_FILENAME = "meridian_rates_cache.json"

# Re-scrape interval (days).  Rates change monthly; monthly is enough.
REFRESH_INTERVAL_DAYS = 30

# Month abbreviation → number
_MONTH_ABBR: dict[str, int] = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

def _month_key(year: int, month: int) -> str:
    """Return 'YYYY-MM' key."""
    return f"{year:04d}-{month:02d}"


def _keyword_classify(name: str) -> str | None:
    """Map a period label to one of the canonical period keys.

    Returns one of: ``daily``, ``night``, ``peak``, ``offpeak``,
    ``weekend_offpeak``, ``controlled``.  Returns ``None`` when no
    keyword matches.

    Order matters: more-specific keywords must come first so they are
    not shadowed by shorter substrings.
    """
    if "daily charge" in name or "daily connection" in name:
        return "daily"
    if "uncontrolled" in name:
        return "offpeak"
    if "control" in name:
        return "controlled"
    if "weekend" in name:
        return "weekend_offpeak"
    if "off peak" in name or "off-peak" in name or "offpeak" in name:
        return "offpeak"
    if "shoulder" in name:
        return "offpeak"
    if "peak" in name:
        return "peak"
    if "night" in name or "overnight" in name or "evernight" in name:
        return "night"
    if "anytime" in name or "all time" in name:
        return "offpeak"
    if "day" in name and "daily" not in name:
        return "offpeak"
    return None


def _extract_period(raw_text: str) -> str | None:
    """Classify a rate label into a canonical period key.

    Lowercases the visible label text and passes it through
    :func:`_keyword_classify`.  Returns ``None`` (with a warning)
    when no keyword matches — the caller should skip the row.
    """
    if not raw_text or not raw_text.strip():
        return None

    name = raw_text.lower().strip()
    result = _keyword_classify(name)

    if result is None:
        _LOGGER.warning(
            "Unrecognised rate label '%s' — skipping",
            raw_text.strip(),
        )

    return result


class RateCache:
    """Persistent TOU-rate cache with web-scraping and seasonal fallback."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self._cache_dir = cache_dir
        self._cache_path = (cache_dir or _DEFAULT_CACHE_DIR) / CACHE_FILENAME
        self._data: dict = {
            "last_updated": None,
            "special": {},
            "base": {},
            "detected_periods": [],
        }
        self._loaded = False

    @property
    def last_updated(self) -> str | None:
        """Return the ISO timestamp of the last scrape."""
        return self._data.get("last_updated")

    @property
    def cache_months_special(self) -> int:
        """Return the number of cached special-rate months."""
        return len(self._data.get("special", {}))

    @property
    def cache_months_base(self) -> int:
        """Return the number of cached base-rate months."""
        return len(self._data.get("base", {}))

    @property
    def detected_periods(self) -> list[str]:
        """Return TOU period keys found in the most recent scrape.

        This is the union of period keys across all months in the active
        rate type (special by default).  Period keys like ``'daily'`` are
        excluded — only TOU periods are included.
        """
        stored = self._data.get("detected_periods", [])
        if stored:
            return list(stored)
        # Derive from cached month data when the field is missing/empty
        active = self._data.get("special", {}) or self._data.get("base", {})
        periods: set[str] = set()
        for month_data in active.values():
            periods.update(k for k in month_data if k != "daily")
        return sorted(periods)

    def load(self) -> None:
        """Load the cache from disk.  Safe to call multiple times."""
        if self._loaded:
            return
        if self._cache_path.exists():
            try:
                with open(self._cache_path) as fh:
                    self._data = json.load(fh)
                _LOGGER.info(
                    "Rate cache loaded: %d special month(s), %d base month(s)",
                    len(self._data.get("special", {})),
                    len(self._data.get("base", {})),
                )
            except (json.JSONDecodeError, OSError) as exc:
                _LOGGER.warning(
                    "Failed to load rate cache, starting fresh: %s", exc
                )
                self._data = {"last_updated": None, "special": {}, "base": {}}
        else:
            _LOGGER.info("No rate cache found — will scrape on first update")
        self._loaded = True

    def _save(self) -> None:
        """Atomically write the cache to disk."""
        if self._cache_dir is None:
            return  # No explicit cache dir — skip writes (test/dev safety)
        tmp = self._cache_path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as fh:
                json.dump(self._data, fh, indent=2, sort_keys=True)
            tmp.replace(self._cache_path)  # atomic on POSIX
            _LOGGER.debug("Rate cache saved to %s", self._cache_path)
        except Exception as exc:
            _LOGGER.error("Failed to save rate cache: %s", exc)
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def needs_refresh(self) -> bool:
        """Return True when a new scrape is due."""
        ts = self._data.get("last_updated")
        if not ts:
            return True
        try:
            last = datetime.fromisoformat(ts)
            age_days = (datetime.now() - last).total_seconds() / 86400
            return age_days >= REFRESH_INTERVAL_DAYS
        except (ValueError, TypeError):
            return True

    def scrape_and_update(self, session, base_url: str = "") -> bool:
        """Fetch ``/rates``, parse both tables, and merge into cache.

        *session* must be an **authenticated** ``requests.Session``.
        *base_url* is the supplier's portal root (trailing slash).
        The coordinator always passes the supplier-specific URL; the
        empty default exists only to satisfy the function signature.
        Returns ``True`` on success.
        """
        if not base_url:
            _LOGGER.error("No base_url provided — cannot scrape rates")
            return False
        try:
            rates_url = base_url.rstrip("/") + "/rates"
            _LOGGER.debug("Fetching rates from %s", rates_url)
            resp = session.get(rates_url, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                _LOGGER.error(
                    "Failed to fetch rates page (status %s)", resp.status_code
                )
                return False

            if "rates-table-container" not in resp.text:
                # Check if we got a login page instead (session expired)
                if "authenticity_token" in resp.text or "login" in resp.text.lower()[:500]:
                    _LOGGER.error(
                        "Rates page returned login form — session has expired"
                    )
                else:
                    _LOGGER.error(
                        "Rates page missing expected tables — unexpected page content "
                        "(first 200 chars: %s)",
                        resp.text[:200].replace("\n", " "),
                    )
                return False

            soup = BeautifulSoup(resp.text, "html.parser")
            tables_found = 0
            months_scraped = 0

            for table_type in ("special", "base"):
                css = f"div.rates-table-container.{table_type}-rates-table"
                container = soup.select_one(css)
                if not container:
                    _LOGGER.warning("'%s' table not found on rates page", table_type)
                    continue

                table_el = container.select_one("table")
                if not table_el:
                    _LOGGER.warning("No <table> inside '%s' container", table_type)
                    continue

                tables_found += 1

                months = self._parse_month_columns(table_el)
                if not months:
                    _LOGGER.warning(
                        "Could not determine month columns for '%s'", table_type
                    )
                    continue

                month_rates = self._parse_rate_rows(table_el, months)

                # Validate scraped data — warn if expected periods are missing
                for mk, rates in month_rates.items():
                    period_keys = set(rates.keys())
                    expected = {"peak", "offpeak", "night", "daily"}
                    missing = expected - period_keys
                    if missing:
                        _LOGGER.warning(
                            "Month %s (%s) missing expected periods: %s (found: %s)",
                            mk,
                            table_type,
                            missing,
                            period_keys,
                        )

                # Merge — newer data overwrites; old months are kept.
                bucket = self._data.setdefault(table_type, {})
                for mk, rates in month_rates.items():
                    if rates:  # only overwrite if we actually parsed rates
                        bucket[mk] = rates

                months_scraped += len(month_rates)
                _LOGGER.debug(
                    "Scraped %d month(s) for '%s' table",
                    len(month_rates),
                    table_type,
                )

            if tables_found == 0:
                _LOGGER.error("No rate tables found on page despite container existing")
                return False

            # Compute detected periods from the active rate type (prefer special)
            active_bucket = self._data.get("special", {}) or self._data.get("base", {})
            all_periods: set[str] = set()
            for month_data in active_bucket.values():
                all_periods.update(k for k in month_data if k != "daily")
            self._data["detected_periods"] = sorted(all_periods)
            _LOGGER.info(
                "Detected TOU periods from rate table: %s",
                self._data["detected_periods"],
            )

            self._data["last_updated"] = datetime.now().isoformat()
            self._save()
            _LOGGER.info(
                "Rate cache updated: %d table(s), %d total month-sets scraped",
                tables_found,
                months_scraped,
            )
            return True

        except (OSError, ConnectionError) as exc:
            _LOGGER.error("Rate scrape network error: %s", exc)
            return False
        except (ValueError, KeyError, AttributeError) as exc:
            _LOGGER.error("Rate scrape parse error: %s", exc)
            return False

    def _parse_month_columns(self, table) -> list[str]:
        """Map each data column to a ``'YYYY-MM'`` string.

        Month names are 3-letter abbreviations without years.
        The ``current`` CSS class on ``<td>`` cells identifies today's
        billing month, which anchors the year assignment.
        """
        now = datetime.now()

        # Collect header month abbreviations
        header_months: list[int] = []
        header_row = table.select_one("thead tr")
        if header_row is None:
            rows = table.select("tr")
            header_row = rows[0] if rows else None
        if header_row is None:
            return []

        for cell in header_row.select("th, td"):
            text = cell.get_text(strip=True)
            if text in _MONTH_ABBR:
                header_months.append(_MONTH_ABBR[text])

        if not header_months:
            return []

        # Find the column with the 'current' CSS class
        current_idx: int | None = None
        data_rows = table.select("tbody tr")
        if not data_rows:
            data_rows = table.select("tr")[1:]
        for row in data_rows:
            tds = row.select("td.base-rates")
            for idx, td in enumerate(tds):
                if "current" in (td.get("class") or []):
                    current_idx = idx
                    break
            if current_idx is not None:
                break

        if current_idx is None:
            _LOGGER.debug(
                "No 'current' class found; assuming column 1 is current month"
            )
            current_idx = 1

        # Assign (year, month) from offset to 'now'
        result: list[str] = []
        for i, _month_num in enumerate(header_months):
            offset = i - current_idx
            total = now.year * 12 + (now.month - 1) + offset
            year = total // 12
            month = total % 12 + 1
            result.append(_month_key(year, month))

        return result

    @staticmethod
    def _parse_rate_rows(
        table, months: list[str]
    ) -> dict[str, dict[str, float]]:
        """Extract TOU rates from the table body.

        Returns ``{month_key: {period: rate_in_dollars, …}}``.
        All page values are in **NZ cents**; they are converted to
        **NZD/kWh** by dividing by 100 here.
        """
        result: dict[str, dict[str, float]] = {m: {} for m in months}
        seen: set[str] = set()
        unclassified_labels: list[str] = []

        data_rows = table.select("tbody tr")
        if not data_rows:
            data_rows = table.select("tr")[1:]

        if not data_rows:
            _LOGGER.warning("Rate table has no data rows")
            return result

        for row in data_rows:
            cells = row.select("td, th")
            if not cells:
                continue

            label_cell = cells[0]
            for svg in label_cell.find_all("svg"):
                svg.decompose()
            raw_label = label_cell.get_text(strip=True)
            period_key = _extract_period(raw_label)
            if period_key is None:
                if raw_label:  # track non-empty unclassified labels
                    unclassified_labels.append(raw_label[:60])
                continue

            # Skip duplicate channels (:2 is identical to :1)
            if period_key in seen:
                continue
            seen.add(period_key)

            # Walk rate cells
            rate_cells_parsed = 0
            for col_idx, td in enumerate(row.select("td.base-rates")):
                if col_idx >= len(months):
                    break

                # Primary: GST-inclusive rate value
                text: str | None = None
                gst_el = td.select_one("span.rate.gst_inclusive div") or td.select_one("span.rate.gst_inclusive")
                if gst_el is not None:
                    text = gst_el.get_text(strip=True)

                # Fallback: any bare numeric text in the cell
                if text is None:
                    cell_text = td.get_text(strip=True)
                    if cell_text and re.match(r"^\d+\.?\d*$", cell_text):
                        text = cell_text

                if text is None:
                    continue

                try:
                    cents = float(text)
                    if cents < 0 or cents > 500:  # sanity check
                        _LOGGER.warning(
                            "Rate value %.2f cents out of range for %s col %d",
                            cents, period_key, col_idx,
                        )
                        continue
                    dollars = round(cents / 100, 6)
                    result[months[col_idx]][period_key] = dollars
                    rate_cells_parsed += 1
                except (ValueError, TypeError):
                    _LOGGER.debug(
                        "Could not parse rate '%s' for %s col %d",
                        text, period_key, col_idx,
                    )

            if rate_cells_parsed == 0:
                _LOGGER.debug(
                    "Period '%s' found but no rate values parsed from row",
                    period_key,
                )

        if unclassified_labels:
            _LOGGER.debug(
                "Unclassified row labels on rates page: %s",
                unclassified_labels,
            )

        return result

    def _effective_bucket(self, rate_type: str) -> dict:
        """Return the rate bucket for *rate_type*, auto-falling back.

        If the requested bucket (e.g. ``"special"``) is empty but the
        other one (``"base"``) has data, use that instead.  This
        handles accounts that only have base rates without requiring
        the user to change the option manually.
        """
        bucket = self._data.get(rate_type, {})
        if bucket:
            return bucket
        alt = "base" if rate_type == "special" else "special"
        alt_bucket = self._data.get(alt, {})
        if alt_bucket:
            _LOGGER.info(
                "Requested '%s' rates empty — falling back to '%s'",
                rate_type, alt,
            )
            return alt_bucket
        return bucket  # both empty — caller will hit emergency defaults

    def _lookup_month(
        self, year: int, month: int, rate_type: str
    ) -> dict[str, float] | None:
        """Find the best matching month entry using the 4-tier fallback.

        Returns the raw month dict (including ``daily`` key if present),
        or ``None`` when no cached data exists at all.
        """
        key = _month_key(year, month)
        bucket: dict = self._effective_bucket(rate_type)

        # 1. exact
        if key in bucket:
            return bucket[key]

        # 2. same month, earlier years
        for y in range(year - 1, year - 5, -1):
            fk = _month_key(y, month)
            if fk in bucket:
                return bucket[fk]

        # 3. same month, later years
        for y in range(year + 1, year + 5):
            fk = _month_key(y, month)
            if fk in bucket:
                return bucket[fk]

        # 4. oldest month in cache
        all_keys = sorted(bucket.keys())
        if all_keys:
            return bucket[all_keys[0]]

        return None

    def get_rates(
        self, year: int, month: int, rate_type: str = "special"
    ) -> dict[str, float]:
        """Return ``{period: $/kWh}`` for the given month.

        Lookup order
        ~~~~~~~~~~~~
        1. Exact match in cache
        2. Same calendar month from earlier years (seasonal fallback)
        3. Same calendar month from later years (for old history)
        4. Oldest available month in cache
        5. Hard-coded emergency defaults
        """
        entry = self._lookup_month(year, month, rate_type)
        if entry is not None:
            return {k: v for k, v in entry.items() if k != "daily"}

        _LOGGER.warning(
            "No cached rates for %04d-%02d — using emergency defaults",
            year,
            month,
        )
        return {
            "night": 0.17,
            "peak": 0.34,
            "offpeak": 0.22,
            "weekend_offpeak": 0.22,
            "controlled": 0.17,
        }

    def get_daily_charge(
        self, year: int, month: int, rate_type: str = "special"
    ) -> float:
        """Return the daily connection charge (NZD/day) for a month."""
        entry = self._lookup_month(year, month, rate_type)
        if entry is not None and "daily" in entry:
            return entry["daily"]

        _LOGGER.warning(
            "No cached daily charge for %04d-%02d — using emergency default",
            year,
            month,
        )
        return 4.14
