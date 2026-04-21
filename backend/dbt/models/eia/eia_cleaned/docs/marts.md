{% docs eia_930_mart_hourly %}

Hourly EIA-930 generation by balancing authority, materialized as a view in the
`eia_cleaned` schema.

**Grain:** One row per EST hour per respondent.

**Key columns:**
- `datetime_utc` — Original UTC timestamp
- `datetime` — Eastern Prevailing Time timestamp
- `date` / `hour_ending` — EST date and hour ending (1–24)
- `respondent` — Normalized BA code
- `region` — EIA grid region
- `total`, `renewables`, `thermal` — Composite MW metrics
- Individual fuel types in MW

**Validation:** Compare against
[EIA Grid Monitor](https://www.eia.gov/electricity/gridmonitor/dashboard/daily_generation_mix/US48/US48).

{% enddocs %}


{% docs eia_930_mart_daily %}

Daily average EIA-930 generation by balancing authority, materialized as a view
in the `eia_cleaned` schema.

**Grain:** One row per date per respondent.

**Key columns:**
- `date` — EST operating date
- `respondent` — Normalized BA code
- `region` — EIA grid region
- All fuel type columns suffixed with `_mw` (daily average MW)
- `natural_gas_pct_of_thermal` — Gas share of thermal generation
- `coal_pct_of_thermal` — Coal share of thermal generation

**Validation:** Compare against
[EIA Grid Monitor Expanded View](https://www.eia.gov/electricity/gridmonitor/expanded-view/custom/pending/RegionBaEnergymix-14).

{% enddocs %}


{% docs eia_nat_gas_consumption_mart %}

Monthly natural gas consumption by end-use sector and state, materialized as
a view in the `eia_cleaned` schema.

**Grain:** One row per year × month × state/area.

**Key columns:**
- `year` / `month` — Calendar period of the observation
- `area_name_standardized` — US state or `US48` for the national aggregate
- `consumption_unit` — Always `MMCF` (million cubic feet)
- 8 end-use columns: `lease_and_plant_fuel`, `pipeline_and_distribution_use`,
  `volumes_delivered_to_consumers`, `residential`, `commercial`, `industrial`,
  `vehicle_fuel`, `electric_power`

**Validation:** Compare against
[EIA Natural Gas Consumption](https://www.eia.gov/dnav/ng/ng_cons_sum_dcu_nus_m.htm).

{% enddocs %}
