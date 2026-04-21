# meteologica_cleaned marts

## Purpose

Consumer-facing views for Meteologica xTraders API PJM forecasts, observations, projections, and normals covering demand, generation (solar/wind/hydro), and day-ahead prices. Each mart exposes ranked forecast/update vintages with EPT-converted timestamps for downstream analysis.

## Grain

| Model | Grain |
|-------|-------|
| `meteologica_pjm_demand_forecast_hourly` | `forecast_date x hour_ending x region x forecast_rank` |
| `meteologica_pjm_demand_forecast_ecmwf_ens_hourly` | `forecast_date x hour_ending x region x forecast_rank` |
| `meteologica_pjm_generation_forecast_hourly` | `forecast_date x hour_ending x region x generation_type x forecast_rank` |
| `meteologica_pjm_da_price_forecast_hourly` | `forecast_date x hour_ending x hub x forecast_rank` |
| `meteologica_pjm_demand_observation_5min` | `observation_datetime_ending_local x region x update_rank` |
| `meteologica_pjm_generation_observation` | `observation_date x hour_ending x source x region x update_rank` |
| `meteologica_pjm_da_price_observation` | `observation_date x hour_ending x hub x update_rank` |
| `meteologica_pjm_demand_projection_hourly` | `projection_date x hour_ending x region x update_rank` |
| `meteologica_pjm_generation_normal_hourly` | `normal_date x hour_ending x source x region x update_rank` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_demand_forecast_hourly` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteo_pjm_demand_fcst_ecmwf_ens_hourly` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_gen_forecast_hourly` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_da_price_forecast_hourly` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_demand_observation` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_gen_observation` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_da_price_observation` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_demand_projection_hourly` |
| Meteologica xTraders API (ISO account) | `staging_v1_meteologica_pjm_gen_normal_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `forecast_date` | Target forecast date (EPT) |
| `hour_ending` | Hour ending 1-24 (EPT) |
| `forecast_execution_datetime_utc` / `forecast_execution_datetime_local` | When the forecast model was run (UTC / EPT triplet with `timezone`) |
| `forecast_datetime_ending_utc` / `forecast_datetime_ending_local` | Hour-ending target timestamp (UTC / EPT triplet) |
| `forecast_rank` | Recency rank (1 = most recent vintage) via `DENSE_RANK` |
| `region` | PJM region or utility-level sub-region (36 demand, 17 generation) |
| `hub` | PJM pricing hub (13 hubs for DA price forecasts) |
| `forecast_load_mw` | Forecasted demand in megawatts |
| `forecast_load_average_mw` | ECMWF-ENS ensemble average demand (MW) |
| `forecast_load_bottom_mw` | ECMWF-ENS ensemble minimum demand (MW) |
| `forecast_load_top_mw` | ECMWF-ENS ensemble maximum demand (MW) |
| `ens_00_mw`–`ens_50_mw` | Individual ECMWF-ENS ensemble member forecasts (51 members) |
| `forecast_generation_mw` | Forecasted generation in megawatts |
| `forecast_da_price` | Forecasted day-ahead price ($/MWh) |
| `update_rank` | Recency rank (1 = first update) for observations/normals/projections via `DENSE_RANK` |
| `update_datetime_utc` / `update_datetime_local` | When the observation/normal/projection was issued (UTC / EPT triplet with `timezone`) |
| `observation_datetime_ending_utc` / `observation_datetime_ending_local` | Hour-ending observation timestamp (UTC / EPT triplet; 5-min demand obs exception: values still represent block-start) |
| `projection_datetime_ending_utc` / `projection_datetime_ending_local` | Hour-ending projection timestamp (UTC / EPT triplet) |
| `normal_datetime_ending_utc` / `normal_datetime_ending_local` | Hour-ending normal timestamp (UTC / EPT triplet) |
| `observation_date` | The date the observation covers (EPT) |
| `observation_load_mw` | Observed actual demand in megawatts |
| `observation_generation_mw` | Observed actual generation in megawatts |
| `observation_da_price` | Observed actual day-ahead price ($/MWh) |
| `projection_date` | The date the demand projection covers (EPT) |
| `projection_load_mw` | Projected demand in megawatts |
| `normal_date` | The date the climatological normal covers (EPT) |
| `normal_generation_mw` | Climatological normal generation in megawatts |

## Transformation Notes

- All marts are materialized as **views** (`SELECT * FROM staging`), with a handful of incremental marts.
- Staging UNIONs ~200 raw Meteologica source tables, produces UTC/timezone/local triplets for issue times and hour-ending target times, and ranks vintages by `DENSE_RANK` on `forecast_execution_datetime_local` (forecasts) or `update_datetime_local` (observations/projections/normals).
- Demand forecasts cover RTO + 3 macro regions + 32 utility sub-regions.
- Generation forecasts span solar (4 regions), wind (12 regions), and hydro (1 region).
- DA price forecasts cover SYSTEM + 12 pricing hubs.
- Observation models use `update_rank` instead of `forecast_rank`, and `update_datetime_*` instead of `forecast_execution_datetime_*`.
- Demand observations cover same 36 regions as demand forecasts.
- Demand projections cover RTO + 32 utility sub-regions (no MIDATL/SOUTH/WEST macro aggregates).
- Generation normals cover same 9 source/region combinations as generation observations.
- No completeness filter applied: `forecast_rank = 1` may reference a vintage with fewer than 24 hours for the first/last forecast dates in its horizon.

## Data Quality Checks

- `not_null` on `forecast_rank`, `forecast_date`, `hour_ending`, `region`/`hub`, and forecast value columns.
- `not_null` on `update_rank`, observation/projection/normal date columns, `hour_ending`, `region`/`hub`/`source`, and value columns.
- `accepted_values` on `forecast_rank`: `[1, 2, 3, 4, 5]`.
- `accepted_values` on `update_rank`: `[1, 2, 3, 4, 5]`.
- Schema tests defined in `schema.yml` for all 9 mart models.
