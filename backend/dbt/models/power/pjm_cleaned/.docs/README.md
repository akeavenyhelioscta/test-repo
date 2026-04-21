# pjm_cleaned marts

## Purpose

Consumer-facing PJM power market views exposing LMPs, load (DA, RT metered, RT preliminary, RT instantaneous, forecasts), fuel mix, outages, tie flows, and renewable (solar/wind) forecasts. Each mart is a thin view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `pjm_lmps_hourly` | `date x hour_ending x hub x market` |
| `pjm_lmps_daily` | `date x hub x period x market` |
| `pjm_lmps_rt_hourly` | `date x hour_ending x hub` |
| `pjm_load_da_hourly` | `date x hour_ending x region` |
| `pjm_load_rt_metered_hourly` | `date x hour_ending x region` |
| `pjm_load_rt_prelim_hourly` | `date x hour_ending x region` |
| `pjm_load_rt_instantaneous_hourly` | `date x hour_ending x region` |
| `pjm_load_forecast_hourly` | `date x hour_ending x region x forecast_date` |
| `pjm_gridstatus_load_forecast_hourly` | `date x hour_ending x region x forecast_date` |
| `pjm_fuel_mix_hourly` | `date x hour_ending x fuel_type` |
| `pjm_outages_actual_daily` | `date x fuel_type` |
| `pjm_outages_forecast_daily` | `date x fuel_type` |
| `pjm_tie_flows_hourly` | `date x hour_ending x interface` |
| `pjm_gridstatus_solar_forecast_hourly` | `date x hour_ending x region x forecast_date` |
| `pjm_gridstatus_wind_forecast_hourly` | `date x hour_ending x region x forecast_date` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| PJM Data Miner 2 | `staging_v1_pjm_lmps_hourly`, `staging_v1_pjm_lmps_daily`, `staging_v1_pjm_load_da_*`, `staging_v1_pjm_load_rt_*`, `staging_v1_pjm_load_forecast_*`, `staging_v1_pjm_outages_*`, `staging_v1_pjm_tie_flows_*` |
| GridStatus (open-source) | `staging_v1_pjm_fuel_mix_*`, `staging_v1_gridstatus_pjm_solar_forecast_hourly`, `staging_v1_gridstatus_pjm_wind_forecast_hourly`, `staging_v1_gridstatus_pjm_load_forecast_hourly` |
| GridStatus.io (paid API) | `staging_v1_pjm_lmps_rt_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime` | EPT timestamp |
| `date` | Calendar date (EPT) |
| `hour_ending` | Hour ending 1-24 (EPT) |
| `hub` / `region` / `interface` | Dimensional identifier for the data slice |
| `market` | Market type: `da`, `rt`, or `dart` |
| `lmp_total` | Total locational marginal price ($/MWh) |
| `lmp_system_energy_price` | System energy component of LMP |
| `lmp_congestion_price` | Congestion component of LMP |
| `lmp_marginal_loss_price` | Marginal loss component of LMP |
| `load_mw` | Load in megawatts |
| `forecast_load_mw` | Forecasted load in megawatts |
| `generation_mw` | Generation output in megawatts |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`).
- Business logic (DA/RT spread calculation, regional aggregation, hour-ending derivation) lives entirely in the staging layer.
- Load regions: RTO, MIDATL, WEST, SOUTH (SOUTH = RTO - MIDATL - WEST).
- LMPs include DA, RT, and computed DART (DA minus RT) spreads.
- Fuel mix and renewable forecasts sourced from GridStatus; all other data from PJM Data Miner 2.

## Data Quality Checks

- `not_null` on `date`, `hour_ending`, `hub`/`region`, `market` across all models.
- `accepted_values` on `market` column: `['da', 'rt', 'dart']`.
- `accepted_values` on `region` column: `['RTO', 'MIDATL', 'WEST', 'SOUTH']` for load models.
- Schema tests defined in `schema.yml` for all 20+ mart models.
