{% docs eia_col_datetime_utc %}
Original UTC timestamp from the EIA-930 data feed. Preserved for cross-referencing
with the raw source table.
{% enddocs %}

{% docs eia_col_datetime %}
Timestamp in Eastern Prevailing Time (America/New_York), DST-aware. Derived from
the UTC timestamp by converting to ET and subtracting one hour to align with the
hour-ending convention.
{% enddocs %}

{% docs eia_col_date %}
Operating date in Eastern Prevailing Time. This is the calendar date the energy
was generated, derived from the EST-converted timestamp.
{% enddocs %}

{% docs eia_col_hour_ending %}
Hour ending in Eastern Prevailing Time (1–24). Hour ending 1 covers midnight to
1 AM, hour ending 24 covers 11 PM to midnight.
{% enddocs %}

{% docs eia_col_respondent %}
EIA balancing authority code, normalized to standard ISO names where applicable:
`ISNE` → `ISONE`, `NYIS` → `NYISO`, `ERCO` → `ERCOT`, `CISO` → `CAISO`.
All other codes are passed through unchanged.
{% enddocs %}

{% docs eia_col_region %}
EIA grid region grouping. One of: US48, NE, NY, MIDW, MIDA, TEN, CAR, SE, FLA,
CENT, TEX, NW, SW, CAL. Each balancing authority maps to exactly one region.
{% enddocs %}

{% docs eia_col_is_iso %}
Boolean flag indicating whether the respondent is an ISO/RTO (TRUE) or a
non-ISO balancing authority (FALSE). ISOs: ISONE, NYISO, MISO, PJM, ERCOT, CAISO.
{% enddocs %}

{% docs eia_col_time_zone %}
Primary time zone of the balancing authority: Eastern, Central, Mountain,
Pacific, or Arizona.
{% enddocs %}

{% docs eia_col_balancing_authority_name %}
Full legal name of the balancing authority, sourced from the EIA respondent
lookup table.
{% enddocs %}

{% docs eia_col_total %}
Total generation across all fuel types (MW). Computed as the sum of all 16 fuel
type averages with COALESCE to prevent NULL propagation.
{% enddocs %}

{% docs eia_col_renewables %}
Renewable generation (MW). Computed as wind + solar.
{% enddocs %}

{% docs eia_col_thermal %}
Thermal generation (MW). Computed as natural gas + coal.
{% enddocs %}

{% docs eia_col_wind %}
Wind generation (MW).
{% enddocs %}

{% docs eia_col_solar %}
Solar generation (MW), excluding integrated battery storage.
{% enddocs %}

{% docs eia_col_natural_gas %}
Natural gas generation (MW).
{% enddocs %}

{% docs eia_col_coal %}
Coal generation (MW).
{% enddocs %}

{% docs eia_col_oil %}
Petroleum/oil generation (MW). Renamed from `petroleum` in the raw source.
{% enddocs %}

{% docs eia_col_nuclear %}
Nuclear generation (MW).
{% enddocs %}

{% docs eia_col_hydro %}
Conventional hydroelectric generation (MW).
{% enddocs %}

{% docs eia_col_pumped_storage %}
Pumped-storage hydroelectric generation (MW). Can be negative when pumping.
{% enddocs %}

{% docs eia_col_geothermal %}
Geothermal generation (MW).
{% enddocs %}

{% docs eia_col_battery %}
Battery storage generation/discharge (MW). Can be negative when charging.
{% enddocs %}

{% docs eia_col_solar_battery %}
Solar with integrated battery storage generation (MW).
{% enddocs %}

{% docs eia_col_wind_battery %}
Wind with integrated battery storage generation (MW).
{% enddocs %}

{% docs eia_col_other_energy_storage %}
Other energy storage generation (MW). Can be negative when charging.
{% enddocs %}

{% docs eia_col_unknown_energy_storage %}
Unknown/unclassified energy storage generation (MW).
{% enddocs %}

{% docs eia_col_other %}
Other generation from unclassified fuel types (MW).
{% enddocs %}

{% docs eia_col_unknown %}
Generation from unknown fuel types (MW).
{% enddocs %}

{% docs eia_col_natural_gas_pct_of_thermal %}
Natural gas share of thermal generation (0–1 ratio). Computed as
natural_gas_mw / thermal_mw. NULL when thermal_mw is zero.
{% enddocs %}

{% docs eia_col_coal_pct_of_thermal %}
Coal share of thermal generation (0–1 ratio). Computed as
coal_mw / thermal_mw. NULL when thermal_mw is zero.
{% enddocs %}


{% docs eia_ng_col_period %}
Raw reporting period from the EIA API in `YYYY-MM` format. Parsed into `year`
and `month` integer columns in staging.
{% enddocs %}

{% docs eia_ng_col_year %}
Calendar year of the consumption observation, parsed from the `period` field.
{% enddocs %}

{% docs eia_ng_col_month %}
Calendar month (1–12) of the consumption observation, parsed from the `period` field.
{% enddocs %}

{% docs eia_ng_col_area_name %}
Raw geographic area name from the EIA API. Uses mixed conventions: full state names
for some states (e.g., `CALIFORNIA`), `USA-XX` postal codes for others (e.g., `USA-AL`),
and `U.S.` for the national aggregate.
{% enddocs %}

{% docs eia_ng_col_area_name_standardized %}
Standardized geographic area name. State postal codes are expanded to full uppercase
names (e.g., `USA-AL` → `ALABAMA`), and `U.S.` is mapped to `US48`.
{% enddocs %}

{% docs eia_ng_col_process_name %}
Raw EIA consumption category (e.g., `Residential Consumption`, `Electric Power Consumption`).
Standardized in staging to shorter labels.
{% enddocs %}

{% docs eia_ng_col_consumption_unit %}
Unit of measurement for consumption values. Expected value: `MMCF`
(million cubic feet).
{% enddocs %}

{% docs eia_ng_col_consumption %}
Raw consumption value from the EIA API, in the unit specified by `consumption_unit`.
{% enddocs %}

{% docs eia_ng_col_lease_and_plant_fuel %}
Natural gas consumed as lease and plant fuel (MMCF). Gas used in the field for
extraction operations and processing plant fuel.
{% enddocs %}

{% docs eia_ng_col_pipeline_and_distribution_use %}
Natural gas consumed for pipeline and distribution operations (MMCF). Gas used to
power compressor stations and other pipeline infrastructure.
{% enddocs %}

{% docs eia_ng_col_volumes_delivered_to_consumers %}
Total natural gas volumes delivered to all end-use consumer sectors (MMCF).
This is the sum of residential, commercial, industrial, vehicle fuel, and
electric power consumption.
{% enddocs %}

{% docs eia_ng_col_residential %}
Natural gas delivered to the residential sector (MMCF). Includes gas used for
heating, cooking, and other household purposes.
{% enddocs %}

{% docs eia_ng_col_commercial %}
Natural gas delivered to the commercial sector (MMCF). Includes gas used by
non-manufacturing business establishments such as offices, hotels, and restaurants.
{% enddocs %}

{% docs eia_ng_col_industrial %}
Natural gas delivered to the industrial sector (MMCF). Includes gas used for
manufacturing, mining, and construction operations.
{% enddocs %}

{% docs eia_ng_col_vehicle_fuel %}
Natural gas consumed as vehicle fuel (MMCF). Includes compressed natural gas (CNG)
and liquefied natural gas (LNG) for transportation.
{% enddocs %}

{% docs eia_ng_col_electric_power %}
Natural gas consumed by the electric power sector (MMCF). Includes gas burned at
electric utilities and independent power producers for electricity generation.
{% enddocs %}
