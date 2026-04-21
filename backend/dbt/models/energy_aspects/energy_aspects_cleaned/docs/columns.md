{% docs ea_col_date %}
Monthly observation/forecast date from Energy Aspects. All EA timeseries data
is at monthly grain.
{% enddocs %}

{% docs ea_col_demand_mw %}
EA actual load and forecast load under normal weather for PJM power balances (MW).
Combines historical actuals with forward-looking EA forecast under normal weather
assumptions.
{% enddocs %}

{% docs ea_col_coal_generation_ea_price_mw %}
EA forecast for PJM coal generation (MW) under EA's proprietary forecast price
assumptions.
{% enddocs %}

{% docs ea_col_coal_generation_fwd_price_mw %}
EA forecast for PJM coal generation (MW) under forward/market price assumptions.
{% enddocs %}

{% docs ea_col_ng_generation_ea_price_mw %}
EA forecast for PJM natural gas generation (MW) under EA's proprietary gas price
assumptions.
{% enddocs %}

{% docs ea_col_ng_generation_fwd_price_mw %}
EA forecast for PJM natural gas generation (MW) under forward/market gas price
assumptions.
{% enddocs %}

{% docs ea_col_nuclear_generation_mw %}
EA forecast for PJM nuclear generation (MW).
{% enddocs %}

{% docs ea_col_solar_generation_mw %}
EA forecast for PJM solar generation (MW).
{% enddocs %}

{% docs ea_col_wind_generation_mw %}
EA forecast for PJM wind generation (MW).
{% enddocs %}

{% docs ea_col_hydro_generation_mw %}
EA forecast for PJM hydro generation (MW).
{% enddocs %}

{% docs ea_col_other_generation_mw %}
EA forecast for PJM other generation (MW). Includes fuel types not separately
categorized (biomass, waste, etc.).
{% enddocs %}

{% docs ea_col_thermal_generation_norm_weather_mw %}
EA forecast for PJM thermal generation under normal weather conditions (MW).
{% enddocs %}

{% docs ea_col_net_imports_mw %}
EA forecast for PJM net imports (MW). Positive values indicate net imports into PJM.
{% enddocs %}

{% docs ea_col_ng_demand_ea_price_bcf_per_d %}
EA forecast for PJM natural gas demand (bcf/d) for power generation under EA's
proprietary gas price assumptions.
{% enddocs %}

{% docs ea_col_ng_demand_fwd_price_bcf_per_d %}
EA forecast for PJM natural gas demand (bcf/d) for power generation under
forward/market gas price assumptions.
{% enddocs %}

{% docs ea_col_ng_equiv_demand_norm_weather_bcf_per_d %}
EA forecast for PJM natural gas equivalent demand (bcf/d) under normal weather
conditions. Represents the weather-normalized gas burn for power generation.
{% enddocs %}

{% docs ea_col_on_peak_power_price %}
EA monthly forecast for PJM West on-peak power price ($/MWh).
{% enddocs %}

{% docs ea_col_on_peak_heat_rate %}
EA monthly forecast for PJM West on-peak implied heat rate (MMBtu/MWh).
{% enddocs %}

{% docs ea_col_on_peak_dirty_spark_spread %}
EA monthly forecast for PJM West on-peak dirty spark spread ($/MWh).
Dirty spark = power price minus gas cost (without carbon adder).
{% enddocs %}

{% docs ea_col_load_norm_weather_mw %}
EA modeled historical and forecast load under normal weather for PJM (MW).
Weather-normalized series that removes temperature effects from load.
{% enddocs %}

{% docs ea_col_actual_load_norm_weather_mw %}
EA actual load combined with forecast load under normal weather for PJM (MW).
Historical portion reflects actual load; forward portion reflects EA's
weather-normalized load forecast.
{% enddocs %}
