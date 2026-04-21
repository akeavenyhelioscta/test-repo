{% docs spp_marts_overview %}

# SPP Marts

This layer exposes consumer-facing SPP views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs spp_mart_lmps_hourly %}
Hourly SPP LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing.
{% enddocs %}
