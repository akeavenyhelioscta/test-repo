{% docs eia_930_source %}

Raw EIA-930 hourly generation by fuel type, ingested from the EIA Open Data API.

Data is stored in the `eia` schema as `fuel_type_hrl_gen_v2_20250626`. Each row
represents one hour of generation for one balancing authority, broken down by
16 fuel types.

**Raw columns:**
- `datetime_utc` — UTC timestamp of the observation
- `date` — UTC date
- `hour` — UTC hour (0–23)
- `respondent` — EIA balancing authority code (e.g., `ISNE`, `ERCO`, `CISO`)
- 16 fuel type columns (MW): `battery_storage`, `coal`, `geothermal`, `hydro`,
  `natural_gas`, `nuclear`, `other`, `other_energy_storage`, `petroleum`,
  `pumped_storage`, `solar`, `solar_with_integrated_battery_storage`, `unknown`,
  `unknown_energy_storage`, `wind`, `wind_with_integrated_battery_storage`

**Primary key:** `datetime_utc` + `respondent`

**Ingestion:** Python script in `backend/scrapes/eia/` upserts into Azure PostgreSQL.

{% enddocs %}


{% docs eia_nat_gas_consumption_end_use_source %}

Raw EIA natural gas consumption by end use, ingested from the EIA Open Data API.

Data is stored in the `eia` schema as `nat_gas_consumption_end_use_v2_2025_dec_28`.
Each row represents one month of consumption for one geographic area and one
end-use category.

**Raw columns:**
- `period` — Reporting period in `YYYY-MM` format
- `duoarea` — EIA area code (not used in downstream models)
- `area_name` — Geographic area name (state or national)
- `product` — EIA product code (not used)
- `product_name` — Product description (not used)
- `process` — EIA process code (not used)
- `process_name` — Consumption category (e.g., `Residential Consumption`)
- `series` — EIA series identifier (not used)
- `series_description` — Series description (not used)
- `units` — Unit of measurement (`MMCF`)
- `value` — Consumption value
- `created_at` / `updated_at` — Ingestion timestamps (not used)

**Primary key:** `period` + `area_name` + `process_name`

**Ingestion:** Python script in `backend/scrapes/eia/` upserts into Azure PostgreSQL.

{% enddocs %}


{% docs eia_nat_gas_consumption_end_use_source_model %}

Ephemeral extraction of the raw EIA natural gas consumption end-use table.
Selects and casts the five columns needed for downstream transformations:
`period`, `area_name`, `process_name`, `units`, and `value`.

{% enddocs %}

