{% docs ercot_lmps %}

## Locational Marginal Prices (LMPs)

LMP models provide day-ahead, real-time, and DA-RT spread pricing by hub.

### Data Sources
- **DA LMPs** — GridStatus `ercot_lmp_by_settlement_point` (settlement point prices)
- **RT SPPs** — GridStatus `ercot_spp_real_time_15_min` (15-min real-time, averaged to hourly)

### Key Transformations
- RT 15-minute data is **averaged to hourly** in the source model
- **DART spread** = DA LMP - RT LMP, computed per hour/hub
- Only **lmp_total** is available (no energy/congestion/loss decomposition)

- Normalized long format: one row per `date x hour_ending x hub x market`

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_ercot_lmps_hourly_wide` | date x hour_ending | ephemeral |
| `staging_v1_ercot_lmps_hourly` | date x hour_ending x hub x market | ephemeral |
| `staging_v1_ercot_lmps_daily` | date | ephemeral |

### Hubs
HB_HOUSTON, HB_NORTH, HB_SOUTH, HB_WEST

{% enddocs %}


{% docs ercot_fuel_mix %}

## Fuel Mix

Hourly generation by fuel type with energy storage data.

### Data Sources
- **Fuel mix** — GridStatus `ercot_fuel_mix` (5-min data, averaged to hourly)
- **Energy storage** — GridStatus `ercot_energy_storage_resources` (5-min data, averaged to hourly)

### Key Transformations
- 5-minute data is **averaged to hourly** in source models
- Fuel mix and storage data are **joined** on date x hour_ending
- Derived columns: `total` (sum of all fuels), `renewables` (wind + solar), `thermal` (gas + coal)
- `gas_pct_thermal` and `coal_pct_thermal` compute thermal fuel share

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_ercot_fuel_mix_hourly` | date x hour_ending | ephemeral |
| `staging_v1_ercot_fuel_mix_daily` | date x period | ephemeral |

### Fuel Types
`nuclear`, `hydro`, `wind`, `solar`, `natural_gas`, `coal_and_lignite`, `power_storage`, `other`,
`storage_net_output`, `storage_total_charging`, `storage_total_discharging`

{% enddocs %}


{% docs ercot_load %}

## Load

Actual load by forecast zone.

### Data Source
- **Load** — GridStatus `ercot_load_by_forecast_zone` (hourly)

### Key Transformations
- `load_total` is computed as sum of all zones (north + south + west + houston)
- Daily model aggregates hourly data into **flat**, **onpeak** (HE07-HE22), and **offpeak** periods

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `source_v1_ercot_load_hourly` | date x hour_ending | ephemeral |
| `staging_v1_ercot_load_daily` | date x period | ephemeral |

### Zones
`load_total`, `load_north`, `load_south`, `load_west`, `load_houston`

{% enddocs %}


{% docs ercot_forecasts %}

## Combined Forecasts

Combined load, solar, and wind forecasts with net load calculation.

### Data Sources
- **Load forecast** — GridStatus `ercot_load_forecast_by_forecast_zone`
- **Solar forecast** — GridStatus `ercot_solar_actual_and_forecast_by_geo_region_hourly` (STPPF system-wide)
- **Wind forecast** — GridStatus `ercot_wind_actual_and_forecast_by_geo_region_hourly` (STWPF system-wide)

### Key Transformations
- Three forecast streams are **joined** on interval_start and forecast_execution_date
- **net_load** = load - solar - wind
- Forecast revisions are ranked by `DENSE_RANK() OVER (ORDER BY forecast_execution_datetime DESC)`
- "Current" variants filter to `labelled_forecast_execution_timestamp = 'Current Forecast'`
- Daily models aggregate into **flat**, **onpeak** (HE07-HE22), and **offpeak** periods

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_ercot_gridstatus_forecasts_hourly` | rank x forecast_date x hour_ending | ephemeral |
| `staging_v1_ercot_gridstatus_forecasts_daily` | rank x forecast_date x period | ephemeral |
| `staging_v1_ercot_gridstatus_forecasts_hourly_current` | forecast_date x hour_ending | ephemeral |
| `staging_v1_ercot_gridstatus_forecasts_daily_current` | forecast_date x period | ephemeral |

{% enddocs %}


{% docs ercot_outages %}

## Reported Outages

Hourly reported outages aggregated from sub-hourly data.

### Data Source
- **Outages** — GridStatus `ercot_reported_outages` (sub-hourly, averaged to hourly)

### Key Transformations
- Sub-hourly data is **averaged to hourly**
- Combined outages = sum across all zones (south + north + west + houston)
- Planned vs unplanned breakdown: `combined_unplanned = combined_total - combined_planned`

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `source_v1_ercot_outages_hourly` | date x hour_ending | ephemeral |

{% enddocs %}
