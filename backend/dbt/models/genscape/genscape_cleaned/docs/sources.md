{% docs genscape_source %}

Raw Genscape gas production forecast data stored in the `genscape` schema.

The source table contains item-value pairs at the granular region level. Each row
represents a single metric (`item`) for a given region and forecast month, reported
on a specific `reportdate`.

**Raw columns:** `reportdate`, `month`, `region`, `item`, `value`

**Item values:**
- Production
- Dry Gas Production YoY
- Oil Rig Count
- Gas Rig Count
- Dry Gas Production Actual
- Wet Gas Production Actual
- Wet Gas Production

**Regions:** 67 distinct values spanning US states, Texas Railroad Commission districts,
sub-basins (Permian, San Juan, Rockies), Canadian provinces, and aggregate regions
(Lower 48, United States).

{% enddocs %}

{% docs genscape_daily_pipeline_production_source %}

Raw Genscape daily pipeline production data stored in the `genscape` schema.

The source table contains pre-pivoted regional production columns. Each row
represents a single production date reported on a specific `reportdate`, with
dry gas production values (MMCF/d) for each region as separate columns.

**Raw columns:** `reportdate`, `date`, plus 22 regional columns

**Regional columns:**
- `lower_48` — Lower 48 aggregate
- `gulf_of_mexico` — Gulf of Mexico
- `north_louisiana`, `south_louisiana`, `other_gulf_coast` — Gulf Coast sub-regions
- `texas` — Texas aggregate
- `east_texas`, `south_texas` — Texas sub-regions
- `oklahoma`, `kansas`, `arkansas` — Mid-Continent sub-regions
- `permian_new_mexico`, `permian_texas` — Permian sub-regions
- `permian_flare_counts`, `permian_flare_volume` — Permian flaring metrics
- `san_juan` — San Juan Basin
- `piceance_basin`, `colorado_denver_julesberg`, `north_dakota_montana`, `utah_uinta`, `wyoming_green_wind_ot`, `wyoming_powder`, `other_rockies` — Rockies sub-regions
- `west` — West aggregate
- `ohio`, `southwest_pennsylvania`, `northeast_pennsylvania`, `west_virginia`, `other_east_ga_il_in_md_nc_tn_ky_mi_ny_va` — East sub-regions
- `western_canada` — Western Canada aggregate

**Primary Key:** `[reportdate, date]`

**Ingestion:** Python script `daily_pipeline_production_v2_2026_mar_10.py` pulls from
`https://api.genscape.com/natgas/supply-demand/v1/daily-pipeline-production`
with a 7-day lookback window and upserts to PostgreSQL.

{% enddocs %}

{% docs genscape_daily_power_estimate_source %}

Raw Genscape daily power burn estimate data stored in the `genscape` schema.

The source table contains pivoted regional power burn estimates. Each row
represents a single gas day, power burn variable, and model type, with
regional values as separate columns.

**Primary Key:** `[gasday, power_burn_variable, modeltype]`

**Key columns:**
- `gasday` — the gas flow day
- `power_burn_variable` — the metric being reported
- `modeltype` — the model type (forecast vs actuals)
- Regional columns vary by API response (e.g., `east`, `midwest`, `south`, `west`)

**Ingestion:** Python script `daily_power_estimate.py` pulls from
`https://api.genscape.com/natgas/pipeline-fundamentals/v1/power-estimate/daily`
with a 7-day lookback window and upserts to PostgreSQL.

{% enddocs %}
