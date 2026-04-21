{% docs isone_marts_overview %}

# ISO-NE Marts

This layer exposes consumer-facing ISO-NE views. Each mart model is a thin wrapper
over vetted staging logic so downstream users query stable view names while
internal transforms remain ephemeral.

{% enddocs %}

{% docs isone_mart_lmps_hourly %}
Hourly ISO-NE LMP mart by `date x hour_ending x hub x market` with DA, RT, and DART pricing.
{% enddocs %}

{% docs isone_mart_lmps_daily %}
Daily ISO-NE LMP mart by `date` with flat/onpeak/offpeak averages in wide format.
{% enddocs %}
