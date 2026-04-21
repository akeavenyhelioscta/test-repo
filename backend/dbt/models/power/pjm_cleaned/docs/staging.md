{% docs pjm_lmps %}

## Locational Marginal Prices (LMPs)

LMP models provide day-ahead, real-time, and DA-RT spread pricing by hub.

### Data Sources
- **DA LMPs** — PJM Data Miner 2 `da_hrl_lmps` (next-day morning)
- **RT verified LMPs** — PJM `rt_settlements_verified_hourly_lmps` (~60 day lag)
- **RT unverified LMPs** — PJM `rt_unverified_hourly_lmps` (same-day)

### Key Transformations
- RT hourly model merges verified and unverified sources, **preferring verified** via
  `ROW_NUMBER() OVER (... ORDER BY verified DESC)` deduplication
- **DART spread** = DA LMP - RT LMP, computed per hour/hub
- Three market types (DA, RT, DART) are UNION'd into a single table

### Models

| Model | Grain | Materialization |
|-------|-------|-----------------|
| `staging_v1_pjm_lmps_hourly` | date × hour_ending × hub × market | ephemeral |
| `staging_v1_pjm_lmps_rt_hourly` | date × hour_ending × hub | ephemeral |
| `staging_v1_pjm_lmps_daily` | date × hub × period × market | ephemeral |

### Price Components
Each LMP is decomposed into: **total**, **system energy price**, **congestion price**, **marginal loss price**.

{% enddocs %}


{% docs pjm_load_da %}

## Day-Ahead Load

Day-ahead cleared load from PJM demand bids, broken down by region.

### Data Source
- PJM Data Miner 2 `hrl_dmd_bids` — hourly day-ahead demand bids by load area

### Key Transformations
- Load areas are mapped to parent regions (RTO, MIDATL, WEST) via the region mapping utility
- **SOUTH is computed as RTO - MIDATL - WEST** using conditional aggregation
- Daily models aggregate hourly data into **flat**, **peak**, **onpeak**, and **offpeak** periods

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_load_da_hourly` | date × hour_ending × region |

{% enddocs %}


{% docs pjm_load_rt %}

## Real-Time Load

Three tiers of real-time load data, each with different latency and accuracy:

| Tier | Source | Lag | Use Case |
|------|--------|-----|----------|
| **Instantaneous** | 5-min API, averaged to hourly | ~5 min | Live monitoring |
| **Preliminary** | PJM Data Miner 2 | ~1 hour | Same-day analysis |
| **Metered (actual)** | PJM Data Miner 2 | ~2 days | Settlement, backtesting |

### Key Transformations
- All tiers share the same regional breakdown: RTO, MIDATL, WEST, SOUTH
- SOUTH is computed as RTO - MIDATL - WEST (same as DA load)
- Daily models use **holiday-aware OnPeak/OffPeak** definitions (NERC holidays treated as off-peak)
- Peak hours: HE08–HE23 on weekdays excluding NERC holidays

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_load_rt_metered_hourly` | date × hour_ending × region |
| `staging_v1_pjm_load_rt_prelim_hourly` | date × hour_ending × region |
| `staging_v1_pjm_load_rt_instantaneous_hourly` | date × hour_ending × region |

{% enddocs %}


{% docs pjm_load_forecast %}

## Load Forecast

7-day hourly load forecasts from both PJM API and GridStatus. Multiple forecast
revisions are issued per day; models retain all revisions ranked by issue time
(earliest first).

### Data Sources
- **PJM API** — `seven_day_load_forecast_v1_2025_08_13`
- **GridStatus** — `pjm_load_forecast` (regions pre-normalized)

### Key Transformations
- Only **complete forecasts** are kept (all 24 hours × all regions present)
- `forecast_rank = 1` is the first-issued forecast for a given `forecast_date`
- Daily models aggregate to holiday-aware OnPeak/OffPeak periods

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_load_forecast_hourly` | forecast_rank × forecast_date × hour_ending × region |
| `staging_v1_gridstatus_pjm_load_forecast_hourly` | forecast_rank × forecast_date × hour_ending × region |

{% enddocs %}


{% docs pjm_fuel_mix %}

## Fuel Mix

Hourly and daily generation by fuel type across the PJM RTO footprint.

### Data Source
- GridStatus `pjm_fuel_mix_hourly`

### Fuel Types
`coal`, `gas`, `hydro`, `multiple_fuels`, `nuclear`, `oil`, `solar`, `storage`, `wind`, `other`, `other_renewables`

### Computed Aggregates
- **total** — sum of all fuel types
- **thermal** — coal + gas + oil + nuclear + multiple_fuels
- **renewables** — solar + wind + hydro + other_renewables
- **gas_pct_thermal** / **coal_pct_thermal** — fuel share within thermal generation

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_fuel_mix_hourly` | date × hour_ending |

{% enddocs %}


{% docs pjm_outages %}

## Generation Outages

Planned, maintenance, and forced generation outage data from PJM's 7-day outage forecast.

### Data Source
- PJM API `seven_day_outage_forecast` — reports total, planned, maintenance, and forced outage MW by region

### Models

| Model | Description | Grain |
|-------|-------------|-------|
| `staging_v1_pjm_outages_actual_daily` | Actuals only (`execution_date = forecast_date`) | date × region |
| `staging_v1_pjm_outages_forecast_daily` | Full 7-day forecast, ranked by issue time (earliest first) | forecast_rank × execution_date × forecast_date × region |

### Outage Categories
- **Total** — all outage types combined
- **Planned** — scheduled maintenance windows
- **Maintenance** — routine maintenance
- **Forced** — unplanned/emergency outages

{% enddocs %}


{% docs pjm_tie_flows %}

## Tie Flows

Actual and scheduled power flows across PJM's interconnection interfaces with
neighboring systems.

### Data Source
- PJM API `five_min_tie_flows` — 5-minute granularity

### Key Transformations
- 5-minute data is averaged to hourly
- Daily models aggregate to flat, onpeak, and offpeak periods

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_tie_flows_hourly` | date × hour_ending × tie_flow_name |

### Columns
- **actual_mw** — measured power flow
- **scheduled_mw** — day-ahead scheduled flow

{% enddocs %}


{% docs pjm_ancillary_services %}

## Ancillary Services Prices

Hourly ancillary services prices pivoted from long format into wide columns.

### Data Source
- PJM Data Miner 2 `ancillary_services` — SR, non-SR, secondary reserve, regulation prices and mileage ratio

### Key Transformations
- Long-to-wide pivot: one row per `date × hour_ending` with named price columns
- Deduplication via `row_is_current` and `version_nbr` (prefer current, latest version)
- **Scarcity flag**: `scarcity_adder_active = TRUE` when MAD Synchronized Reserve price > $0

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_ancillary_services_hourly` | date × hour_ending |

### Columns
- **sr_price** — MAD Synchronized Reserve clearing price ($/MWh). > $0 = scarcity adder active
- **non_sr_price** — MAD Non-Synchronized Reserve price ($/MWh)
- **secondary_reserve_price** — MAD Secondary Reserve price ($/MWh)
- **regulation_price** — RTO Regulation Capability price ($/MWh)
- **mileage_ratio** — RTO Mileage Ratio (dimensionless)

{% enddocs %}


{% docs pjm_reserves %}

## Dispatched Reserves

Hourly dispatched reserve data with clearing price, shortage indicator, and reserve margin.

### Data Sources
- PJM Data Miner 2 `dispatched_reserves` — 5-min updates, aggregated to hourly

### Key Transformations
- 5-minute `dispatched_reserves` aggregated to hourly: AVG for MW, MAX for clearing price, `BOOL_OR` for shortage
- **Reserve margin** computed as `reserve_quantity_mw - reserve_requirement_mw` (negative = deficit)

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_reserves_hourly` | date × hour_ending × area × reserve_type |

### Key Columns
- **reserve_quantity_mw** — reserve MW available
- **reserve_requirement_mw** — reserve MW required
- **reserve_margin_mw** — quantity minus requirement (negative = deficit)
- **market_clearing_price** — reserve clearing price ($/MWh)
- **shortage_indicator** — TRUE if shortage pricing fired in any 5-min interval

{% enddocs %}


{% docs pjm_lmps_rt_five_min %}

## 5-Minute Real-Time LMPs

5-minute RT LMPs by hub, exposed at the native 5-min grain (no hourly aggregation).

### Data Source
- PJM Data Miner 2 `unverified_five_min_lmps` — filtered to `type = HUB` at ingestion

### Models

| Model | Grain |
|-------|-------|
| `staging_v1_pjm_lmps_rt_five_min` | datetime_beginning_utc × hub |

### Key Columns
- **rt_lmp** — 5-minute real-time LMP ($/MWh)

{% enddocs %}


{% docs pjm_solar_wind_forecast %}

## Solar & Wind Generation Forecasts

2-day ahead hourly renewable generation forecasts for the PJM RTO footprint.

### Data Source
- GridStatus `pjm_solar_forecast_hourly` and `pjm_wind_forecast_hourly`

### Key Transformations
- Only complete forecasts are retained (all hours present)
- `forecast_rank = 1` is the first-issued forecast for a given date

### Models

| Model | Grain | Key Columns |
|-------|-------|-------------|
| `staging_v1_gridstatus_pjm_solar_forecast_hourly` | forecast_rank × forecast_date × hour_ending | `solar_forecast`, `solar_forecast_btm` |
| `staging_v1_gridstatus_pjm_wind_forecast_hourly` | forecast_rank × forecast_date × hour_ending | `wind_forecast` |

### Solar Components
- **solar_forecast** — front-of-meter (utility-scale) solar generation
- **solar_forecast_btm** — behind-the-meter (rooftop/distributed) solar generation

{% enddocs %}

