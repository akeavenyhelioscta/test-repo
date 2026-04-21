{% docs meteologica_pjm_demand_forecast %}

## Demand Forecast

Hourly demand (load) forecasts for PJM by region, from Meteologica's weather-driven model.

### Data Source
- Meteologica xTraders API — 36 raw tables (RTO + 3 macro regions + 32 utility-level sub-regions)

### Key Transformations
- UNIONs 36 region-specific tables with a `region` label
- Produces `forecast_execution_datetime_utc` / `timezone` / `forecast_execution_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending target triplet `forecast_datetime_ending_utc` / `forecast_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `forecast_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Ranks vintages by issue time (earliest first) via `DENSE_RANK()` partitioned by `(forecast_date, region)`, ordered on `forecast_execution_datetime_local ASC`
- No completeness filter — partial vintages are retained (see overview for rationale)

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_demand_forecast_hourly` | ephemeral |
| Mart | `meteologica_pjm_demand_forecast_hourly` | view |

**Grain:** forecast_rank x forecast_date x hour_ending x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `forecast_execution_datetime_utc`, `timezone`, `forecast_execution_datetime_local`, `forecast_execution_date` |
| `forecast_period_start` | `forecast_date`, `hour_ending`, `forecast_datetime_ending_utc`, `forecast_datetime_ending_local` |
| `forecast_mw` | `forecast_load_mw` |

{% enddocs %}


{% docs meteologica_pjm_demand_forecast_ecmwf_ens %}

## ECMWF-ENS Demand Forecast

Hourly ensemble demand (load) forecasts for PJM by region, from Meteologica's ECMWF-ENS model.
Provides 51 individual ensemble members (ENS00–ENS50) plus summary statistics (Average, Bottom, Top)
for quantifying forecast uncertainty.

### Data Source
- Meteologica xTraders API — 36 raw tables (RTO + 3 macro regions + 32 utility-level sub-regions)
- Content IDs: 2724–2759

### Key Transformations
- UNIONs 36 region-specific tables with a `region` label
- Produces `forecast_execution_datetime_utc` / `timezone` / `forecast_execution_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending target triplet `forecast_datetime_ending_utc` / `forecast_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `forecast_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Casts all 54 MW columns (3 summary + 51 ensemble) from VARCHAR to NUMERIC
- Ranks vintages by issue time (earliest first) via `DENSE_RANK()` partitioned by `(forecast_date, region)`, ordered on `forecast_execution_datetime_local ASC`
- No completeness filter — partial vintages are retained (see overview for rationale)

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteo_pjm_demand_fcst_ecmwf_ens_hourly` | ephemeral |
| Mart | `meteologica_pjm_demand_forecast_ecmwf_ens_hourly` | view |

**Grain:** forecast_rank x forecast_date x hour_ending x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `forecast_execution_datetime_utc`, `timezone`, `forecast_execution_datetime_local`, `forecast_execution_date` |
| `forecast_period_start` | `forecast_date`, `hour_ending`, `forecast_datetime_ending_utc`, `forecast_datetime_ending_local` |
| `average_mw` | `forecast_load_average_mw` |
| `bottom_mw` | `forecast_load_bottom_mw` |
| `top_mw` | `forecast_load_top_mw` |
| `ens_00_mw` ... `ens_50_mw` | `ens_00_mw` ... `ens_50_mw` (passed through) |

{% enddocs %}


{% docs meteologica_pjm_generation_forecast %}

## Generation Forecast

Hourly generation forecasts for PJM by source type and region, from Meteologica's
weather-driven model.

### Data Source
- Meteologica xTraders API — 17 raw tables:
  - **Solar (4):** RTO, MIDATL, SOUTH, WEST
  - **Wind — regional (4):** RTO, MIDATL, SOUTH, WEST
  - **Wind — utility-level (8):** MIDATL_AE, MIDATL_PL, MIDATL_PN, SOUTH_DOM, WEST_AEP, WEST_AP, WEST_ATSI, WEST_CE
  - **Hydro (1):** RTO only

### Key Transformations
- UNIONs 17 tables with `source` (solar/wind/hydro) and `region` labels
- Same UTC/timezone/local triplet normalization and ranking as the demand model
- Ranked by issue time (earliest first) via `DENSE_RANK()` partitioned by `(forecast_date, source, region)`, ordered on `forecast_execution_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_gen_forecast_hourly` | ephemeral |
| Mart | `meteologica_pjm_generation_forecast_hourly` | view |

**Grain:** forecast_rank x forecast_date x hour_ending x source x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `forecast_execution_datetime_utc`, `timezone`, `forecast_execution_datetime_local`, `forecast_execution_date` |
| `forecast_period_start` | `forecast_date`, `hour_ending`, `forecast_datetime_ending_utc`, `forecast_datetime_ending_local` |
| `forecast_mw` | `forecast_generation_mw` |

{% enddocs %}


{% docs meteologica_pjm_da_price_forecast %}

## Day-Ahead Price Forecast

Hourly DA electricity price forecasts for PJM by pricing hub, from Meteologica's model.

### Data Source
- Meteologica xTraders API — 13 raw tables (SYSTEM + 12 pricing hubs)

### Pricing Hubs
`SYSTEM`, `AEP DAYTON`, `AEP GEN`, `ATSI GEN`, `CHICAGO GEN`, `CHICAGO`, `DOMINION`,
`EASTERN`, `NEW JERSEY`, `N ILLINOIS`, `OHIO`, `WESTERN`, `WEST INT`

### Key Transformations
- UNIONs 13 hub-specific tables with a `hub` label
- Same UTC/timezone/local triplet normalization and ranking as the demand model
- Ranked by issue time (earliest first) via `DENSE_RANK()` partitioned by `(forecast_date, hub)`, ordered on `forecast_execution_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_da_price_forecast_hourly` | ephemeral |
| Mart | `meteologica_pjm_da_price_forecast_hourly` | view |

**Grain:** forecast_rank x forecast_date x hour_ending x hub

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `forecast_execution_datetime_utc`, `timezone`, `forecast_execution_datetime_local`, `forecast_execution_date` |
| `forecast_period_start` | `forecast_date`, `hour_ending`, `forecast_datetime_ending_utc`, `forecast_datetime_ending_local` |
| `day_ahead_price` | `forecast_da_price` |

{% enddocs %}


{% docs meteologica_pjm_demand_observation %}

## Demand Observation

5-minute observed actual demand (load) for PJM by region, from Meteologica's xTraders API.

### Data Source
- Meteologica xTraders API — 36 raw tables (RTO + 3 macro regions + 32 utility-level sub-regions)

### Key Transformations
- UNIONs 36 region-specific tables with a `region` label
- Produces `update_datetime_utc` / `timezone` / `update_datetime_local` triplet from UTC `issue_date`
- Produces `observation_datetime_ending_utc` / `observation_datetime_ending_local` triplet from `forecast_period_start` (treated as naive UTC → naive EPT)
- Ranks updates by issue time (earliest first) via `DENSE_RANK()` partitioned by `(observation_date, region)`, ordered on `update_datetime_local ASC`

**5-min exception:** the `observation_datetime_ending_*` values are *not* shifted forward one slot — they preserve the prior naive timestamp semantics (actually start-of-5-min-block) so the hourly rollup in `meteologica_pjm_demand_observation_hourly` continues to produce correct `hour_ending` values via `EXTRACT(HOUR FROM observation_datetime_ending_local) + 1`. The `_ending` suffix is used for naming consistency with the hourly marts.

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_demand_observation` | ephemeral |
| Mart | `meteologica_pjm_demand_observation_5min` | incremental (delete+insert) |

**Grain:** update_rank x observation_datetime_ending_local x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `update_datetime_utc`, `timezone`, `update_datetime_local`, `update_date` |
| `forecast_period_start` | `observation_datetime_ending_utc`, `observation_datetime_ending_local`, `observation_date` |
| `observation_mw` | `observation_load_mw` |

{% enddocs %}


{% docs meteologica_pjm_generation_observation %}

## Generation Observation

Hourly observed actual generation for PJM by source type and region, from Meteologica's
xTraders API.

### Data Source
- Meteologica xTraders API — 9 raw tables:
  - **Solar (4):** RTO, MIDATL, WEST, SOUTH
  - **Wind (3):** RTO, MIDATL, SOUTH
  - **Hydro (1):** RTO only
  - Plus 1 additional source table

### Key Transformations
- UNIONs 9 tables with `source` (solar/wind/hydro) and `region` labels
- Produces `update_datetime_utc` / `timezone` / `update_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending observation triplet `observation_datetime_ending_utc` / `observation_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `observation_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Ranks updates by issue time (earliest first) via `DENSE_RANK()` partitioned by `(observation_date, source, region)`, ordered on `update_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_gen_observation` | ephemeral |
| Mart | `meteologica_pjm_generation_observation` | view |

**Grain:** update_rank x observation_date x hour_ending x source x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `update_datetime_utc`, `timezone`, `update_datetime_local`, `update_date` |
| `forecast_period_start` | `observation_date`, `hour_ending`, `observation_datetime_ending_utc`, `observation_datetime_ending_local` |
| `observation_mw` | `observation_generation_mw` |

{% enddocs %}


{% docs meteologica_pjm_da_price_observation %}

## Day-Ahead Price Observation

Hourly observed actual day-ahead electricity prices for PJM by pricing hub, from Meteologica's
xTraders API.

### Data Source
- Meteologica xTraders API — 13 raw tables (SYSTEM + 12 pricing hubs)

### Pricing Hubs
`SYSTEM`, `AEP DAYTON`, `AEP GEN`, `ATSI GEN`, `CHICAGO GEN`, `CHICAGO`, `DOMINION`,
`EASTERN`, `NEW JERSEY`, `N ILLINOIS`, `OHIO`, `WESTERN`, `WEST INT`

### Key Transformations
- UNIONs 13 hub-specific tables with a `hub` label
- Produces `update_datetime_utc` / `timezone` / `update_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending observation triplet `observation_datetime_ending_utc` / `observation_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `observation_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Ranks updates by issue time (earliest first) via `DENSE_RANK()` partitioned by `(observation_date, hub)`, ordered on `update_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_da_price_observation` | ephemeral |
| Mart | `meteologica_pjm_da_price_observation` | view |

**Grain:** update_rank x observation_date x hour_ending x hub

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `update_datetime_utc`, `timezone`, `update_datetime_local`, `update_date` |
| `forecast_period_start` | `observation_date`, `hour_ending`, `observation_datetime_ending_utc`, `observation_datetime_ending_local` |
| `dayahead` | `observation_da_price` |

{% enddocs %}


{% docs meteologica_pjm_demand_projection %}

## Demand Projection

Hourly demand (load) projections for PJM by region, from Meteologica's demand normal model
(labeled "projection" by Meteologica).

### Data Source
- Meteologica xTraders API — 33 raw tables (RTO + 32 utility-level sub-regions, no MIDATL/SOUTH/WEST macro aggregates)

### Key Transformations
- UNIONs 33 region-specific tables with a `region` label
- Produces `update_datetime_utc` / `timezone` / `update_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending projection triplet `projection_datetime_ending_utc` / `projection_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `projection_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Ranks updates by issue time (earliest first) via `DENSE_RANK()` partitioned by `(projection_date, region)`, ordered on `update_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_demand_projection_hourly` | ephemeral |
| Mart | `meteologica_pjm_demand_projection_hourly` | view |

**Grain:** update_rank x projection_date x hour_ending x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `update_datetime_utc`, `timezone`, `update_datetime_local`, `update_date` |
| `forecast_period_start` | `projection_date`, `hour_ending`, `projection_datetime_ending_utc`, `projection_datetime_ending_local` |
| `normal_mw` | `projection_load_mw` |

{% enddocs %}


{% docs meteologica_pjm_generation_normal %}

## Generation Normal

Hourly climatological normal generation for PJM by source type and region, from Meteologica's
xTraders API.

### Data Source
- Meteologica xTraders API — 9 raw tables:
  - **Solar (4):** RTO, MIDATL, WEST, SOUTH
  - **Wind (4):** RTO, WEST, MIDATL, SOUTH
  - **Hydro (1):** RTO only

### Key Transformations
- UNIONs 9 tables with `source` (solar/wind/hydro) and `region` labels
- Produces `update_datetime_utc` / `timezone` / `update_datetime_local` triplet from UTC `issue_date`
- Produces hour-ending normal triplet `normal_datetime_ending_utc` / `normal_datetime_ending_local` from `forecast_period_start + INTERVAL '1 hour'`
- Extracts `normal_date` + `hour_ending` from `forecast_period_start` (already EPT)
- Ranks updates by issue time (earliest first) via `DENSE_RANK()` partitioned by `(normal_date, source, region)`, ordered on `update_datetime_local ASC`

### Model

| Layer | Model | Materialization |
|-------|-------|-----------------|
| Staging | `staging_v1_meteologica_pjm_gen_normal_hourly` | ephemeral |
| Mart | `meteologica_pjm_generation_normal_hourly` | view |

**Grain:** update_rank x normal_date x hour_ending x source x region

### Column Mapping (raw -> staging)

| Raw Column | Staging Column |
|------------|---------------|
| `issue_date` | `update_datetime_utc`, `timezone`, `update_datetime_local`, `update_date` |
| `forecast_period_start` | `normal_date`, `hour_ending`, `normal_datetime_ending_utc`, `normal_datetime_ending_local` |
| `normal_mw` | `normal_generation_mw` |

{% enddocs %}
