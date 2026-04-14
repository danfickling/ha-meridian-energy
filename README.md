# Meridian Energy — Home Assistant Integration

[![Validate](https://github.com/danfickling/ha-meridian-energy/actions/workflows/validate.yml/badge.svg)](https://github.com/danfickling/ha-meridian-energy/actions/workflows/validate.yml)
[![HACS](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Home Assistant custom integration for **Powershop** and **Meridian Energy** customers in New Zealand. Provides electricity cost tracking with time-of-use (TOU) period classification, rate retrieval, and full Energy Dashboard support via the Kraken GraphQL API.

> **Disclaimer:** This is an independent, personal project. It is **not affiliated with, endorsed by, or connected to Meridian Energy, Powershop, or Kraken Technologies** in any way. All code was generated with AI assistance — the maintainer directed the design, reviewed outputs, and validated behaviour against a live Home Assistant instance. Use at your own risk — no warranty or liability is provided.

> **Tested with Powershop only.** Meridian Energy uses the same Kraken platform and should work, but has not been verified.

## Features

- **Automatic TOU classification** — classifies consumption into dynamic TOU periods (Night, Peak, Off-Peak, Controlled, etc.) based on your plan
- **Dynamic rate retrieval** — fetches current rates from the Kraken API, refreshed every 24 hours
- **TOU schedule detection** — parses your plan's time-of-use scheme for accurate period boundaries
- **Energy Dashboard ready** — publishes external statistics (energy + cost) for all detected TOU periods, with historical backfill
- **Half-hourly updates** — consumption data refreshed every 30 minutes
- **Account balance tracking** — credit balance, future packs, and estimated daily cost
- **Billing cycle tracking** — current billing period start/end dates and next billing date
- **Solar export support** — tracks energy returned to grid when solar data is present
- **Multi-supplier** — works with both Powershop and Meridian Energy (same Kraken platform)
- **Dynamic periods** — rate sensors and statistics are created automatically from whatever TOU periods your plan defines; no hardcoded period list

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
3. Select your supplier (**Powershop** or **Meridian Energy**) and enter your email address
4. Enter the OTP code sent to your email
5. If your email is linked to multiple accounts, select which account to use

## Entities

The integration creates a single device with the following sensors. Entity names are derived from the configured supplier — Powershop examples shown below (`sensor.powershop_*`); Meridian Energy users will see `sensor.meridian_energy_*`.

### Fixed sensors (always created)

| Entity | Description |
|--------|-------------|
| `sensor.powershop_current_rate` | Live NZD/kWh for the active TOU period |
| `sensor.powershop_current_period` | Current TOU period name |
| `sensor.powershop_daily_charge` | Daily connection charge (NZD/day) |
| `sensor.powershop_account_balance` | Account credit balance (NZD) |
| `sensor.powershop_future_packs` | Pre-purchased Future Packs value (NZD) |
| `sensor.powershop_solar_export` | Solar export (kWh) — disabled by default |
| `sensor.powershop_billing_period_start` | Current billing period start date |
| `sensor.powershop_billing_period_end` | Current billing period end date |
| `sensor.powershop_next_billing_date` | Next billing date |

### Dynamic rate sensors (created from your plan)

Rate sensors are created automatically based on your plan's TOU periods. Typical examples:

| Entity | Description |
|--------|-------------|
| `sensor.powershop_night_rate` | Night rate (NZD/kWh) |
| `sensor.powershop_peak_rate` | Peak rate (NZD/kWh) |
| `sensor.powershop_off_peak_rate` | Off-Peak rate (NZD/kWh) |
| `sensor.powershop_controlled_rate` | Controlled/hot water rate (NZD/kWh) |

If your plan defines different or additional periods, corresponding rate sensors will be created automatically.

> **Note:** Rate sensors are created when the integration loads. If your plan changes to include new TOU periods, reload the integration (Settings → Devices & Services → Meridian Energy → three-dot menu → Reload) to pick up the new sensors.

## Energy Dashboard Setup

Add these grid consumption sources in **Settings** → **Dashboards** → **Energy**:

| Energy Statistic | Cost Statistic |
|-----------------|----------------|
| `meridian_energy:consumption_night` | `meridian_energy:cost_night` |
| `meridian_energy:consumption_peak` | `meridian_energy:cost_peak` |
| `meridian_energy:consumption_offpeak` | `meridian_energy:cost_offpeak` |
| `meridian_energy:consumption_controlled` | `meridian_energy:cost_controlled` |
| `meridian_energy:consumption_daily_charge` | `meridian_energy:cost_daily_charge` |

The exact statistics depend on your plan's TOU periods. If your plan includes additional periods, corresponding statistics will be published automatically.

If solar data is present, a `meridian_energy:return_to_grid` statistic is also published.

Statistics are published on each data refresh (30 minutes) and include historical backfill up to 10 years (limited only by data available from the API).

## Services

| Service | Description |
|---------|-------------|
| `meridian_energy.refresh_rates` | Force re-fetch of supplier rates and TOU schedule, bypassing the 24-hour cache |
| `meridian_energy.backfill` | Re-fetch and re-publish energy statistics from a specific date. Use to fill gaps after an outage or pick up late-arriving data. Accepts `start_date` (required) and `end_date` (optional, defaults to today). |

## Limitations

- **API dependency** — This integration uses the Kraken GraphQL API. It may break if the API changes without notice.
- **Half-hour resolution** — Usage data is available in half-hourly intervals. Real-time consumption is not available.
- **Read-only** — Cannot make payments, change plans, or modify account settings.
- **Multi-account** — If your login has multiple accounts you will be prompted to choose one. Each account requires a separate integration entry.
- **NZ only** — Designed for New Zealand Meridian Energy / Powershop customers on the Kraken platform.

## Testing

```bash
pip install pytest
python -m pytest tests/ -v
```

323 tests covering API interactions, rate parsing, TOU schedule handling, coordinator logic, config flow (including behavioral async flow and reconfigure flow tests), sensors, and diagnostics. CI runs on Python 3.12 and 3.13 via GitHub Actions.

## License

[MIT](LICENSE) — Copyright (c) 2025 Dan Fickling
