{% docs ice_python_mart_balmo %}

ICE balance-of-month (BALMO) gas swap settle prices across 15 U.S. hubs.
Values are forward-filled through weekends and holidays. One row per
trade date.

Use this view to track remaining-month gas basis differentials and
compare BALMO prices across regional hubs.

{% enddocs %}

{% docs ice_python_mart_next_day_gas_hourly %}

ICE next-day firm physical gas hourly cash prices across 15 U.S. hubs.
Values are forward-filled through weekends and holidays. One row per
datetime (date x hour_ending).

Use this view to track intraday gas price movements at major trading hubs.

{% enddocs %}

{% docs ice_python_mart_next_day_gas_daily %}

ICE next-day firm physical gas daily cash prices across 15 U.S. hubs,
derived from the 10 AM hour snapshot of hourly data. One row per
gas_day x trade_date.

Use this view to track daily gas settlement prices and basis differentials.

{% enddocs %}
