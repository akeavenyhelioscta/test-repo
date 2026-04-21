# ercot_cleaned marts

## Purpose

Consumer-facing ERCOT power market views exposing LMPs, fuel mix, load, combined forecasts, and outages. Each mart is a thin view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `ercot_lmps_hourly` | `date x hour_ending x hub x market` |
| `ercot_lmps_daily` | `date` |
| `ercot_fuel_mix_hourly` | `date x hour_ending` |
| `ercot_fuel_mix_daily` | `date x period` |
| `ercot_load_hourly` | `date x hour_ending` |
| `ercot_load_daily` | `date x period` |
| `ercot_forecasts_hourly` | `rank_forecast_execution_timestamps x forecast_date x hour_ending` |
| `ercot_forecasts_daily` | `rank_forecast_execution_timestamps x forecast_date x period` |
| `ercot_forecasts_hourly_current` | `forecast_date x hour_ending` |
| `ercot_forecasts_daily_current` | `forecast_date x period` |
| `ercot_outages_hourly` | `date x hour_ending` |

## Source Relations

| Source | Upstream Model |
|--------|---------------|
| GridStatus (open-source) | `source_v1_ercot_da_hrl_lmps`, `source_v1_ercot_spp_real_time_15_min`, `source_v1_ercot_fuel_mix_hourly`, `source_v1_ercot_energy_storage_hourly`, `source_v1_ercot_load_hourly`, `source_v1_ercot_outages_hourly` |
| GridStatus.io (paid API) | `source_v1_ercot_gridstatus_load_forecast_hourly`, `source_v1_ercot_gridstatus_solar_forecast_hourly`, `source_v1_ercot_gridstatus_wind_forecast_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | CPT timestamp |
| `date` | Calendar date (CPT) |
| `hour_ending` | Hour ending 1-24 (CPT) |
| `hub` | Pricing hub name (HB_HOUSTON, HB_NORTH, HB_SOUTH, HB_WEST) |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total LMP ($/MWh) |
| `lmp_system_energy_price` | Always NULL (ERCOT does not publish decomposition) |
| `lmp_congestion_price` | Always NULL (ERCOT does not publish decomposition) |
| `lmp_marginal_loss_price` | Always NULL (ERCOT does not publish decomposition) |
| `load_total` | Total ERCOT load (MW) |
| `forecast_load_total` | Forecasted total load (MW) |
| `forecast_net_load_total` | Forecasted net load: load - solar - wind (MW) |
| `period` | Time-of-day period: flat, onpeak, offpeak |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Hourly LMPs are normalized to long format: one row per hub x market.
- `ercot_load_hourly` and `ercot_outages_hourly` wrap **source** models directly (no staging layer needed).
- On-peak hours: HE07-HE22 (Central Prevailing Time).
- Only `lmp_total` is available (no energy/congestion/loss decomposition).
