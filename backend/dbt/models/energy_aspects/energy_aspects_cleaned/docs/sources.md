{% docs ea_source_iso_dispatch_costs %}

**ISO Dispatch Costs** — 95 datasets covering dispatch costs and fuel costs by
fuel type, plant type, and hub across all ISOs.

Covers natural gas, diesel, fuel oil, bituminous coal, and sub-bituminous coal
dispatch and fuel costs for CCGT, CT, and ST plant types. PJM hubs include
PJM W, PJM Dominion, PJM Nihub, and PJM Adhub.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/iso_dispatch_costs.py`

{% enddocs %}


{% docs ea_source_us_regional_power_model %}

**US Regional Power Model** — 175 datasets covering generation by fuel type,
demand, natural gas demand, and net imports across all ISOs/regions.

Includes EA price and forward price variants for gas generation, plus
normal-weather-adjusted demand and thermal generation forecasts.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/us_regional_power_model.py`

{% enddocs %}


{% docs ea_source_na_power_price_hr_spark %}

**North American Power Price, Heat Rate & Spark Forecasts** — 15 datasets
covering on-peak power prices ($/MWh), heat rates (MMBtu/MWh), and spark
spreads ($/MWh) for ERCOT North, ISONE Mass, NYISO G, PJM West, and CAISO SP15.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/na_power_price_heat_rate_spark_forecasts.py`

{% enddocs %}


{% docs ea_source_monthly_iso_load_model %}

**Monthly ISO Load Model** — 16 datasets covering weather-normalized load
forecasts and actual load by ISO (PJM, ERCOT, NYISO, CAISO, ISONE, SPP, MISO, US48).

Two series per ISO: EA modeled historical + forecast load under normal weather,
and actual load with forecast under normal weather.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/monthly_iso_load_model.py`

{% enddocs %}


{% docs ea_source_us_installed_capacity %}

**US Installed Capacity by ISO and Fuel Type** — 84 datasets covering installed
generation capacity (MW) by fuel type across all ISOs/regions.

Fuel types: natural gas, coal, nuclear, oil, solar, onshore wind, offshore wind,
hydro, battery.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/us_installed_capacity_by_iso_and_fuel_type.py`

{% enddocs %}


{% docs ea_source_lower_48_avg_power_demand %}

**Lower 48 Average Power Demand** — 7 datasets (one per major ISO) for
average power demand (MW). Subset of the US Regional Power Model.

- **Grain:** 1 row per month
- **Primary key:** `date`
- **Ingestion:** `backend/scrapes/energy_aspects/timeseries/lower_48_average_power_demand_mw.py`

{% enddocs %}
