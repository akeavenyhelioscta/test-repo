# WSI Module

This module contains WSI Trader (AG2) ingestion scripts organized by API
documentation section. See `.documentation/WSI/WSI Trader API Documentation.pdf`
for the full endpoint reference.

## Layout

- `utils.py`: Shared WSI Trader auth, HTTP client, and CSV pull helpers.
- `wsi_trader_city_ids.json`: Site ID lookup used by forecast scripts.
- `reference/`: Reference-only assets (not executed by domain `runs.py` files).

### Domain Subfolders (aligned to API doc sections)

| Folder | API Doc Section | Endpoint |
|---|---|---|
| `homepage_forecast_table/` | Sec 1 - Homepage Forecast Table | `GetCityTableForecast` |
| `hourly_forecast/` | Sec 2 - Hourly Forecast | `GetHourlyForecast` |
| `weighted_forecast_iso/` | Sec 3 - Weighted Forecast ISO/Country | `GetModelForecast` |
| `weighted_degree_day/` | Sec 4 - Weighted Forecast Degree Days | `GetWeightedDegreeDayForecast` |
| `historical_observations/` | Sec 10 - Historical Observations | `GetHistoricalObservations` |
| `weighted_forecast_city/` | Sec 26 - Weighted Forecast Individual City | `GetWsiForecastForDDModelCities` |

Each domain subfolder contains:
- Scrape scripts (`_pull()`, `_format()`, `_upsert()`, `main()`)
- `runs.py` - manual runner
- `flows.py` - Prefect flow wrappers

There is no module-level `backend/scrapes/wsi/runs.py` or `backend/scrapes/wsi/flows.py`.

## Scripts and Target Tables

- `hourly_forecast/hourly_forecast_temp_v4_2025_jan_12.py` -> `wsi.hourly_forecast_temp_v4_2025_jan_12`
- `historical_observations/hourly_observed_temp_v2_2025_07_22.py` -> `wsi.hourly_observed_temp_v2_20250722`
- `weighted_degree_day/wsi_wdd_day_forecast_v2_2025_dec_17.py` -> `wsi.wsi_wdd_day_forecast_v2_2025_dec_17`
- `weighted_degree_day/gfs_op_wdd_day_forecast_v2_2025_dec_17.py` -> `wsi.gfs_op_wdd_day_forecast_v2_2025_dec_17`
- `weighted_degree_day/gfs_ens_wdd_day_forecast_v2_2025_dec_17.py` -> `wsi.gfs_ens_wdd_day_forecast_v2_2025_dec_17`
- `weighted_degree_day/ecmwf_op_wdd_day_forecast_v2_2025_dec_17.py` -> `wsi.ecmwf_op_wdd_day_forecast_v2_2025_dec_17`
- `weighted_degree_day/ecmwf_ens_wdd_day_forecast_v2_2025_dec_17.py` -> `wsi.ecmwf_ens_wdd_day_forecast_v2_2025_dec_17`
- `weighted_degree_day/aifs_ens_wdd_day_forecast_v1_2026_feb_12.py` -> `wsi.aifs_ens_wdd_day_forecast_v1_2026_feb_12`
- `weighted_forecast_iso/weighted_temp_daily_forecast_iso_models_v2_2026_jan_12.py` -> `wsi.weighted_temp_daily_forecast_iso_models_v2_2026_jan_12`
- `weighted_forecast_iso/weighted_temp_daily_forecast_iso_wsi_v2_2026_jan_12.py` -> `wsi.weighted_temp_daily_forecast_iso_wsi_v2_2026_jan_12`
- `weighted_forecast_city/weighted_temp_daily_forecast_city_v2_2026_jan_12.py` -> `wsi.weighted_temp_daily_forecast_city_v2_2026_jan_12`
- `homepage_forecast_table/wsi_homepage_forecast_table_minmax_v1_2026_jan_12.py` -> `wsi.wsi_homepage_forecast_table_minmax_v1_2026_jan_12`
- `homepage_forecast_table/wsi_homepage_forecast_table_hddcdd_v1_2026_jan_12.py` -> `wsi.wsi_homepage_forecast_table_hddcdd_v1_2026_jan_12`
- `homepage_forecast_table/wsi_homepage_forecast_table_avg_v1_2026_jan_12.py` -> `wsi.wsi_homepage_forecast_table_avg_v1_2026_jan_12`

## Runtime Requirements

Environment variables consumed from `backend.secrets`:

- `WSI_TRADER_USERNAME`
- `WSI_TRADER_NAME`
- `WSI_TRADER_PASSWORD`

Database writes use the shared Azure PostgreSQL helpers under `backend.utils`.

## Running Scripts

From the repo root:

- List scripts for a domain: `python backend/scrapes/wsi/<domain>/runs.py --list`
- Run all scripts in a domain: `python backend/scrapes/wsi/<domain>/runs.py all`
- Run one script in a domain by menu index: `python backend/scrapes/wsi/<domain>/runs.py 2`
- Example: `python backend/scrapes/wsi/weighted_degree_day/runs.py --list`

## Conventions

- Script constants use `API_SCRAPE_NAME` and map to the target DB table name.
- Pipelines use `PipelineRunLogger` for run tracking and failure logging.
- Script entry points are plain `main()` functions (no Prefect decorators).
- Prefect wrappers live only in `flows.py` within each domain subfolder.
