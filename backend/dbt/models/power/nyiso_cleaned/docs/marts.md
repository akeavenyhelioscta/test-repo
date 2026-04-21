{% docs nyiso_marts_overview %}

# NYISO Marts

This layer exposes consumer-facing NYISO views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs nyiso_mart_lmps_hourly %}
Hourly NYISO LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing for all 15 zones.
{% enddocs %}
