# Meridian Energy — Home Assistant Integration

[![Validate](https://github.com/danfickling/ha-meridian-energy/actions/workflows/validate.yml/badge.svg)](https://github.com/danfickling/ha-meridian-energy/actions/workflows/validate.yml)
[![HACS](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Home Assistant custom integration for **Powershop** and **Meridian Energy** customers in New Zealand. Provides accurate electricity cost tracking with time-of-use (TOU) period classification, rate scraping, and full Energy Dashboard support.

> **Disclaimer:** This is an independent, personal project. It is **not affiliated with, endorsed by, or connected to Meridian Energy, Powershop, or Flux Federation** in any way. It interacts with publicly accessible web portals via web scraping. All code was generated with AI assistance — the maintainer directed the design, reviewed outputs, and validated behaviour against a live Home Assistant instance. Use at your own risk — no warranty or liability is provided.

> **Tested with Powershop only.** Meridian Energy uses the same Flux Federation platform and should work, but has not been verified.

## Features

- **Automatic TOU classification** — classifies consumption into Night, Peak, Off-Peak, Weekend Off-Peak, and Controlled periods
- **Dynamic rate scraping** — scrapes current and historical rates from your supplier portal, refreshed monthly
- **Schedule detection** — monitors the Get Shifty page for TOU boundary changes across 29 network regions
- **Smart schedule history** — preserves TOU boundary history for accurate historical reclassification; de-duplicates identical schedules on network changes
- **Energy Dashboard ready** — publishes external statistics (energy + cost) for all TOU periods, with retroactive history
- **Incremental updates** — only fetches new CSV data since the last run (24-hour cycle)
- **Account balance tracking** — credit balance, future packs, and estimated daily cost
- **Solar export support** — tracks energy returned to grid when solar data is present
- **Multi-supplier** — works with both Powershop and Meridian Energy (same Flux Federation platform)

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Go to **Integrations** → three-dot menu → **Custom repositories**
3. Add `https://github.com/danfickling/ha-meridian-energy` with category **Integration**
4. Search for "Meridian Energy" and install
5. Restart Home Assistant

### Manual

1. Copy the `custom_components/meridian_energy/` folder into your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings** → **Devices & Services** → **Add Integration**
2. Search for **Meridian Energy**
3. Enter your Powershop or Meridian credentials
4. Select your supplier and network region

### Options

After setup, click **Configure** on the integration to adjust:

| Option | Description |
|--------|-------------|
| Supplier | Powershop or Meridian Energy |
| Network | Your electricity network region (29 available) |
| Rate type | Special (default) or base rates |
| History start | Fixed date (DD/MM/YYYY) or rolling 365-day window |
| Browser cookie | Optional auth fallback for captcha/2FA |

## Entities

The integration creates a single device with the following sensors. Entity names are derived from the configured supplier — Powershop examples shown below (`sensor.powershop_*`); Meridian Energy users will see `sensor.meridian_energy_*`.

| Entity | Description |
|--------|-------------|
| `sensor.powershop_current_rate` | Live NZD/kWh for the active TOU period |
| `sensor.powershop_current_period` | Current TOU period name |
| `sensor.powershop_night_rate` | Night rate (NZD/kWh) |
| `sensor.powershop_peak_rate` | Peak rate (NZD/kWh) |
| `sensor.powershop_off_peak_rate` | Off-Peak rate (NZD/kWh) |
| `sensor.powershop_weekend_off_peak_rate` | Weekend Off-Peak rate (NZD/kWh) |
| `sensor.powershop_controlled_rate` | Controlled/hot water rate (NZD/kWh) |
| `sensor.powershop_daily_charge` | Daily connection charge (NZD/day) |
| `sensor.powershop_solar_export` | Solar export (kWh) — disabled by default |
| `sensor.powershop_account_balance` | Account credit balance (NZD) |
| `sensor.powershop_future_packs` | Pre-purchased Future Packs value (NZD) |
| `sensor.powershop_daily_cost` | Estimated daily cost (NZD/day) |

## Energy Dashboard Setup

Add these grid consumption sources in **Settings** → **Dashboards** → **Energy**:

| Energy Statistic | Cost Statistic |
|-----------------|----------------|
| `meridian_energy:consumption_night` | `meridian_energy:cost_night` |
| `meridian_energy:consumption_peak` | `meridian_energy:cost_peak` |
| `meridian_energy:consumption_offpeak` | `meridian_energy:cost_offpeak` |
| `meridian_energy:consumption_weekend_offpeak` | `meridian_energy:cost_weekend_offpeak` |
| `meridian_energy:consumption_controlled` | `meridian_energy:cost_controlled` |
| `meridian_energy:consumption_daily_charge` | `meridian_energy:cost_daily_charge` |

If solar data is present, a `meridian_energy:return_to_grid` statistic is also published.

Statistics update every 24 hours with full historical backfill. Cost accuracy is within ~1% of actual invoices.

## Services

| Service | Description |
|---------|-------------|
| `meridian_energy.refresh_rates` | Force re-scrape of supplier rates |
| `meridian_energy.reimport_history` | Purge and reimport all historical statistics |
| `meridian_energy.check_schedule` | Check for TOU schedule changes |
| `meridian_energy.update_schedule` | Update TOU schedule boundaries manually |

## Limitations

- **Web scraping** — This integration scrapes the Flux Federation portal. It may break without notice if the supplier changes their website structure or authentication flow.
- **Kraken migration** — Meridian announced in June 2025 that it will migrate from Flux to Kraken Technologies. Once complete, this integration will stop working for affected customers.
- **Rate accuracy** — Rates come from the supplier's `/rates` page, which shows a rolling 13-month window. Historical months outside that window use a seasonal fallback. Actual billed rates may differ slightly.
- **Half-hour resolution** — Usage data comes from half-hourly CSV exports. Real-time consumption is not available.
- **Read-only** — Cannot make payments, change plans, or modify account settings.
- **Single account** — One supplier account per integration instance. Multiple accounts require multiple entries.
- **Captcha/2FA** — If the supplier enables captcha or two-factor authentication, automatic login may fail. A browser cookie can be provided as a workaround.

## Testing

```bash
pip install pytest beautifulsoup4 requests
python -m pytest tests/ -v
```

242 tests covering TOU classification, rate parsing, schedule handling, API interactions, coordinator logic, config flow, sensors, and diagnostics. CI runs on Python 3.12 and 3.13 via GitHub Actions.

## License

[MIT](LICENSE) — Copyright (c) 2025 Dan Fickling
