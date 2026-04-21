# eia_cleaned marts

## Purpose

Consumer-facing EIA-930 generation views exposing hourly and daily fuel-type generation by balancing authority across all U.S. regions (60+ BAs). Each mart is a view wrapper over vetted staging logic so downstream users query stable view names while internal transforms remain ephemeral.

## Grain

| Model | Grain |
|-------|-------|
| `eia_930_hourly` | `date x hour_ending x respondent x fuel_type` |
| `eia_930_daily` | `date x respondent x fuel_type` |
| `eia_natural_gas_consumption_by_end_use_monthly` | `year x month x area_name_standardized` |

## Source Relations

| Source | Upstream Staging Model |
|--------|----------------------|
| EIA-930 Fuel Type Hourly Generation | `staging_v1_eia_930_hourly` |
| EIA Natural Gas Consumption by End Use | `staging_v1_eia_ng_consumption_by_end_use_monthly` |

## Key Columns

| Column | Description |
|--------|-------------|
| `datetime_utc` | Original UTC timestamp from EIA |
| `datetime` | Eastern Prevailing Time timestamp |
| `date` | Calendar date (EST) |
| `hour_ending` | Hour ending 1-24 (EST) |
| `respondent` | Normalized balancing authority code (e.g., ISONE, NYISO, ERCOT, CAISO) |
| `region` | EIA grid region (US48, NE, NY, MIDW, MIDA, TEN, CAR, SE, FLA, CENT, TEX, NW, SW, CAL) |
| `is_iso` | Whether the respondent is an ISO/RTO |
| `total` / `total_mw` | Total generation (MW) |
| `renewables` / `renewables_mw` | Wind + solar (MW) |
| `thermal` / `thermal_mw` | Natural gas + coal (MW) |
| `natural_gas_pct_of_thermal` | Gas share of thermal generation (daily only) |
| `coal_pct_of_thermal` | Coal share of thermal generation (daily only) |

## Natural Gas Consumption Key Columns

| Column | Description |
|--------|-------------|
| `year` | Calendar year of the observation |
| `month` | Calendar month (1–12) |
| `area_name_standardized` | US state name or `US48` for national aggregate |
| `consumption_unit` | Unit of measurement (`MMCF`) |
| `lease_and_plant_fuel` | Gas used in extraction and processing (MMCF) |
| `pipeline_and_distribution_use` | Gas used for pipeline infrastructure (MMCF) |
| `volumes_delivered_to_consumers` | Total end-use deliveries (MMCF) |
| `residential` | Residential sector consumption (MMCF) |
| `commercial` | Commercial sector consumption (MMCF) |
| `industrial` | Industrial sector consumption (MMCF) |
| `vehicle_fuel` | CNG/LNG for transportation (MMCF) |
| `electric_power` | Utility and IPP generation (MMCF) |

## Transformation Notes

- All marts are materialized as **views**.
- Business logic (UTC→EST conversion, respondent normalization, hourly aggregation) lives entirely in the staging layer.
- Respondent codes are normalized: ISNE→ISONE, NYIS→NYISO, ERCO→ERCOT, CISO→CAISO.
- Respondent metadata (region, is_iso, time_zone, balancing_authority_name) joined from `utils_v1_eia_respondent_lookup`.
- Daily mart computes AVG of hourly values and adds thermal percentage columns; fuel columns are suffixed with `_mw`.
- NG consumption mart pivots row-per-category source data into one row per year/month/area with separate end-use columns.
- Area names standardized via `utils_v1_eia_area_name_lookup` (e.g., `USA-AL` → `ALABAMA`, `U.S.` → `US48`).

## Data Quality Checks

- `not_null` on `datetime_utc`, `datetime`, `date`, `hour_ending`, `respondent` in hourly model.
- `not_null` on `date`, `respondent` in daily model.
- `accepted_values` on `region`: `['US48', 'NE', 'NY', 'MIDW', 'MIDA', 'TEN', 'CAR', 'SE', 'FLA', 'CENT', 'TEX', 'NW', 'SW', 'CAL']`.
- `not_null` on `year`, `month`, `area_name_standardized`, `consumption_unit` in NG consumption model.
- `accepted_values` on `consumption_unit`: `['MMCF']`.
