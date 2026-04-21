# genscape_cleaned marts

## Purpose

Consumer-facing views for Genscape (Wood Mackenzie) natural gas data: monthly gas production forecasts by region, daily pipeline production estimates, and daily power burn estimates. Used by the HeliosCTA trading desk for gas supply analysis and production trend tracking.

## Grain

| Model | Grain |
|-------|-------|
| `genscape_gas_production_forecast` | `date x region x revision` |
| `genscape_daily_pipeline_production` | `date x revision` (22 regional columns per row) |
| `genscape_daily_power_estimate` | `gas_day x power_burn_variable x model_type_based_on_noms` (regional columns per row) |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| Genscape Gas Production Forecast | `staging_v2_genscape_gas_production_forecast` |
| Genscape Daily Pipeline Production | `staging_v2_daily_pipeline_production` |
| Genscape Daily Power Estimate | `staging_v2_daily_power_estimate` |

## Key Columns

### Gas Production Forecast

| Column | Description |
|--------|-------------|
| `date` | Target production month |
| `region` | Geographic tier (22 tiers aggregated from 67 raw regions) |
| `revision` / `max_revision` | Forecast revision tracking (1 = oldest) |
| `report_date` | When the forecast was issued |
| `production` | Forecasted gas production |
| `dry_gas_production_yoy` | Year-over-year percentage change |
| `oil_rig_count` / `gas_rig_count` | Operational rig counts |

### Daily Pipeline Production

| Column | Description |
|--------|-------------|
| `date` | Production date |
| `revision` / `max_revision` | Report revision tracking (1 = oldest) |
| `report_date` | When the production estimate was reported |
| `lower_48` through `western_canada` | Regional production in MMCF/d (22 columns) |
| `permian_flare_counts` / `permian_flare_volume` | Permian flaring metrics |

### Daily Power Estimate

| Column | Description |
|--------|-------------|
| `gas_day` | Gas flow day |
| `power_burn_variable` | Power burn metric name |
| `model_type_based_on_noms` | Model type based on nomination data availability |
| `max_model_type_based_on_noms` | Maximum model type for the same gas day and variable |
| `conus`, `east`, `midwest`, `mountain`, `pacific`, `south_central` | Power burn values by US region |

## Transformation Notes

- All three marts are materialized as **views** (`SELECT * FROM staging`).
- Gas production forecast staging pivots raw item/value pairs into typed metric columns and aggregates 67 raw regions into 22 geographic tiers.
- Daily pipeline production staging computes 5 composite aggregate regions (gulf_coast, mid_con, permian, rockies, east) from sub-regional columns and adds revision tracking via `ROW_NUMBER`.
- Daily power estimate staging passes through the pre-pivoted regional columns from the Pipeline Fundamentals API.
- All production values are in MMCF/d (million cubic feet per day).

## Data Quality Checks

- `not_null` tests on all primary key columns across all three mart models (date, report_date, revision, max_revision for production models; gas_day, power_burn_variable, model_type_based_on_noms for power estimate).
- `accepted_values` test on `region` column in the gas production forecast source model.
- Data quality is enforced at the staging and source layers through type casting, composite region computation with `COALESCE` NULL safety, and `not_null` tests on key columns.
- Revision tracking ensures multiple forecast/report vintages are preserved and identifiable.
