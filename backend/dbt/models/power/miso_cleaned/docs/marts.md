{% docs miso_marts_overview %}

# MISO Marts

This layer exposes consumer-facing MISO views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs miso_mart_lmps_hourly %}
Hourly MISO LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing.
{% enddocs %}

{% docs miso_mart_lmps_daily %}
Daily MISO LMP mart by `date x period` with DA, RT, and DART pricing across 8 hubs in wide format.
{% enddocs %}
