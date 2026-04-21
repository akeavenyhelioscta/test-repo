# ice_python_cleaned marts

## Purpose

Consumer-facing ICE natural gas views exposing daily BALMO prices and hourly/daily next-day gas cash prices across 15 U.S. hubs. Each mart is a view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `ice_python_balmo` | `trade_date` (one row per day) |
| `ice_python_next_day_gas_hourly` | `date x hour_ending` (one row per hour) |
| `ice_python_next_day_gas_daily` | `gas_day x trade_date` (one row per day) |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| ICE BALMO | `staging_v1_ice_balmo` |
| ICE Next-Day Gas (Hourly) | `staging_v1_ice_next_day_gas_hourly` |
| ICE Next-Day Gas (Daily) | `staging_v1_ice_next_day_gas_daily` |

## Key Columns

| Column | Description |
|--------|-------------|
| `date` | Calendar / trade date |
| `trade_date` | Date the ICE trade was executed |
| `gas_day` | Gas delivery date (trade_date + 1) |
| `datetime` | Hourly timestamp (hourly models only) |
| `hour_ending` | Hour ending 1-24 (hourly models only) |
| `hh_balmo` | Henry Hub BALMO settle price ($/MMBtu) |
| `hh_cash` | Henry Hub next-day cash price ($/MMBtu) |

## Transformation Notes

- All marts are materialized as **views**.
- Business logic (forward-fill, symbol pivot) lives entirely in the source and staging layers.
- Forward-fill uses cumulative-sum grouping with `FIRST_VALUE()` window function to propagate last known values through weekends and holidays.
- Each hub column is independently forward-filled so a missing value for one hub does not block others.
- Daily next-day gas is derived from the hour ending 10 (10 AM) snapshot, aligning with the gas day transition time.

## Data Quality Checks

- `not_null` on `date`, `trade_date` in BALMO model.
- `not_null` on `datetime`, `date`, `hour_ending`, `trade_date` in hourly gas model.
- `not_null` on `gas_day`, `trade_date` in daily gas model.
