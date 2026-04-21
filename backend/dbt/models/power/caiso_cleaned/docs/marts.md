{% docs caiso_marts_overview %}

# CAISO Marts

This layer exposes consumer-facing CAISO views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs caiso_mart_lmps_hourly %}
Hourly CAISO LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing.
{% enddocs %}

{% docs caiso_mart_lmps_daily %}
Daily CAISO LMP mart by `date` with flat/onpeak/offpeak periods for NP15 and SP15.
{% enddocs %}
