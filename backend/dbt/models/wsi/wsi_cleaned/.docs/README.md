# wsi_cleaned marts

## Purpose

Consumer-facing views and tables for WSI weather data: weighted degree days (WDD) — observed actuals, 10/30-year normals, and combined model/WSI forecasts with run-over-run differences — plus station-level hourly temperature observations and forecasts. Used by the HeliosCTA desk for gas-weather analysis and heating/cooling demand modeling.

## Grain

| Model | Grain |
|-------|-------|
| `wdd_observed_daily` | `date x region` |
| `wdd_normals_daily` | `mm_dd x region x period` (long format: 10_year / 30_year) |
| `wdd_forecasts_daily` | `forecast_execution_datetime x forecast_date x model x cycle x bias_corrected x region` |
| `temp_observed_hourly` | `datetime x region x site_id x station_name` |
| `temp_forecast_hourly` | `local_time x region x site_id x station_name` |

## Source Relations

| Source | Upstream Model |
|--------|---------------|
| WSI daily observed WDD | `source_v1_daily_observed_wdd` |
| WSI WDD observed (30-year history) | `source_v1_daily_observed_wdd` (filtered to 30 years) |
| WSI WDD model run forecasts | `staging_v1_wdd_forecast_models` |
| WSI WDD blend forecasts | `staging_v1_wdd_forecast_wsi` |
| WSI hourly observed temps | `source_v1_hourly_observed_temp` → `staging_v1_temp_observed_hourly` |
| WSI hourly forecast temps | `source_v1_hourly_forecast_temp` → `staging_v1_temp_forecast_hourly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `date` / `forecast_date` | Observation or forecast target date |
| `mm_dd` | Calendar month-day for normals (e.g., `02-28`) |
| `region` | Geographic region (EAST, MIDWEST, MOUNTAIN, PACIFIC, SOUTHCENTRAL, CONUS for WDD; PJM, ERCOT, MISO, etc. for hourly temps) |
| `site_id` / `station_name` | WSI weather station identifier and name (hourly temp models) |
| `hour_ending` | Observation or forecast hour ending (0–23, hourly temp models) |
| `datetime` / `local_time` | Combined timestamp: `datetime` for observed, `local_time` for forecast |
| `temperature` | Temperature in °F (observed or forecast) |
| `temperature_normal` / `temperature_diff` | Normal and deviation from normal (forecast only) |
| `period` | Normal lookback period: `10_year` or `30_year` (normals only) |
| `gas_hdd` | Gas heating degree days |
| `pw_cdd` / `population_cdd` | Population-weighted cooling degree days |
| `tdd` / `tdd_normal` | Total degree days (`gas_hdd + population_cdd`) |
| `model` | Forecast model: `GFS_OP`, `GFS_ENS`, `ECMWF_OP`, `ECMWF_ENS`, or `WSI` |
| `cycle` | Model run cycle: `00Z` or `12Z` (model runs); NULL for WSI blend |
| `forecast_rank` | Recency rank (1 = most recent vintage) |
| `forecast_label` | Human-readable label: `Current Forecast`, `12hrs Ago`, `24hrs Ago`, `Friday 12z` |
| `*_diff_run_over_run` | Run-over-run change vs prior forecast run (12hr for models, 24hr for WSI) |

## Transformation Notes

- `wdd_observed_daily` is a **view** directly over the source observed WDD table.
- `wdd_normals_daily` is a **table** in long format (one row per `mm_dd x region x period`). Computes 10-year and 30-year rolling normals from observed history. Feb 29 values folded into Feb 28. Includes `normal`, `min`, `max`, `stddev`, and year count metadata per WDD type.
- `wdd_forecasts_daily` is a **view** that unions model run forecasts (GFS/ECMWF with 00Z/12Z cycles) and WSI proprietary blend (no cycle). Includes run-over-run differences. Ranked by execution time via `DENSE_RANK`.
- `temp_observed_hourly` is a **view** over station-level hourly observed weather data. Source layer casts and aliases raw column names; staging adds a combined `datetime` column (date + hour_ending interval). Includes temperature, dewpoint, cloud cover, wind, heat index, wind chill, humidity, and precipitation.
- `temp_forecast_hourly` is a **view** over station-level hourly temperature forecasts. Source layer casts and aliases raw column names; staging derives `date` and `hour_ending` from `local_time`. Includes temperature (with normal and diff), feels-like, dewpoint, cloud cover, wind, precipitation, and GHI irradiance.

## Data Quality Checks

- `not_null` on `date`, `region` for observed data; `accepted_values` on `region`.
- `not_null` on `mm_dd`, `region`, `period` for normals; `accepted_values` on `period` (`10_year`, `30_year`).
- `unique_combination_of_columns` on (`mm_dd`, `region`, `period`) for normals.
- `not_null` on `forecast_date`, `region`, `model`, `forecast_rank` for forecasts.
- `accepted_values` on `model`: NWP models (`GFS_OP`, `GFS_ENS`, `ECMWF_OP`, `ECMWF_ENS`) and `WSI`.
- `accepted_values` on `cycle`: `00Z`, `12Z` for NWP models.
- `not_null` on `datetime`, `date`, `hour_ending`, `region`, `site_id`, `station_name` for `temp_observed_hourly`.
- `not_null` on `local_time`, `date`, `hour_ending`, `region`, `site_id`, `station_name` for `temp_forecast_hourly`.
- Schema tests defined in `schema.yml` for all 5 mart models.
