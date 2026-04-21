{% docs ea_staging_pjm_power_model %}

PJM-specific extraction from the US Regional Power Model source.

Selects the 13 PJM columns covering generation by fuel type, demand,
natural gas demand (bcf/d), and net imports. Renames auto-generated
column names to clean, semantic aliases.

- **Grain:** 1 row per month
- **Key columns:** `date`, generation by fuel, demand, gas demand, net imports

{% enddocs %}


{% docs ea_staging_pjm_price_hr_spark %}

PJM West extraction from the NA Power Price/HR/Spark source.

Selects the 3 PJM West columns: on-peak power price, on-peak heat rate,
and on-peak dirty spark spread.

- **Grain:** 1 row per month
- **Key columns:** `date`, on-peak price, heat rate, spark spread

{% enddocs %}


{% docs ea_staging_pjm_load_model %}

PJM extraction from the Monthly ISO Load Model source.

Selects the 2 PJM columns: EA modeled/forecast load under normal weather,
and actual load with forecast under normal weather.

- **Grain:** 1 row per month
- **Key columns:** `date`, load_norm_weather_mw, actual_load_norm_weather_mw

{% enddocs %}


{% docs ea_staging_pjm_installed_capacity %}

PJM extraction from the US Installed Capacity by ISO and Fuel Type source.

Selects the 9 PJM columns covering installed capacity by fuel type:
natural gas, coal, nuclear, oil, solar, onshore wind, offshore wind, hydro,
and battery.

- **Grain:** 1 row per month
- **Key columns:** `date`, capacity by fuel type (MW)

{% enddocs %}


{% docs ea_staging_pjm_dispatch_costs %}

PJM extraction from the ISO Dispatch Costs source.

Selects 36 PJM columns covering dispatch costs and fuel costs by fuel type
(NG, diesel, fuel oil, bituminous coal, sub-bituminous coal), plant type
(CCGT, CT, ST), and hub (PJM W, PJM Dominion, PJM Nihub, PJM Adhub).

- **Grain:** 1 row per month
- **Key columns:** `date`, dispatch/fuel costs by fuel×plant×hub ($/MWh)

{% enddocs %}
