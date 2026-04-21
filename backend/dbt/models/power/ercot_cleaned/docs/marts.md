{% docs ercot_marts_overview %}

# ERCOT Marts

This layer exposes consumer-facing ERCOT views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs ercot_mart_lmps_hourly %}
Hourly ERCOT LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing. Only `lmp_total` is available (no energy/congestion/loss decomposition).
{% enddocs %}

{% docs ercot_mart_lmps_daily %}
Daily ERCOT LMP mart by `date` with flat/onpeak/offpeak averages for 4 hubs x 3 markets.
{% enddocs %}

{% docs ercot_mart_fuel_mix_hourly %}
Hourly ERCOT fuel mix mart by `date x hour_ending` with generation by fuel type and energy storage.
{% enddocs %}

{% docs ercot_mart_fuel_mix_daily %}
Daily ERCOT fuel mix mart by `date x period` with generation averages by fuel type.
{% enddocs %}

{% docs ercot_mart_load_hourly %}
Hourly ERCOT actual load mart by `date x hour_ending` with zonal breakdown (total, north, south, west, houston).
{% enddocs %}

{% docs ercot_mart_load_daily %}
Daily ERCOT load mart by `date x period` with zonal averages.
{% enddocs %}

{% docs ercot_mart_forecasts_hourly %}
Hourly ERCOT combined forecast mart by `rank x forecast_date x hour_ending` with load, solar, wind, and net load.
{% enddocs %}

{% docs ercot_mart_forecasts_daily %}
Daily ERCOT combined forecast mart by `rank x forecast_date x period`.
{% enddocs %}

{% docs ercot_mart_forecasts_hourly_current %}
Hourly ERCOT current forecast mart by `forecast_date x hour_ending` -- most recent forecast only.
{% enddocs %}

{% docs ercot_mart_forecasts_daily_current %}
Daily ERCOT current forecast mart by `forecast_date x period` -- most recent forecast only.
{% enddocs %}

{% docs ercot_mart_outages_hourly %}
Hourly ERCOT reported outages mart by `date x hour_ending` with combined planned/unplanned/total.
{% enddocs %}
