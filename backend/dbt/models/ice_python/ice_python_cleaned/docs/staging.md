{% docs ice_python_staging_balmo %}

Forward-fills ICE BALMO settle prices across weekends and holidays using
a cumulative-sum grouping technique. Each hub column is independently
forward-filled so a missing value for one hub does not block others.

The model is filtered to only include rows up to the latest date with
actual Henry Hub BALMO data, preventing exposure of stale forward-filled
values beyond the data horizon.

{% enddocs %}

{% docs ice_python_staging_next_day_gas_hourly %}

Forward-fills ICE next-day gas hourly cash prices across weekends and
holidays. Each of the 15 hub columns is independently forward-filled.

Filtered to rows up to the latest datetime with actual Henry Hub cash data.

{% enddocs %}

{% docs ice_python_staging_next_day_gas_daily %}

Aggregates hourly next-day gas prices to daily by extracting the hour
ending 10 (10 AM) snapshot. This aligns with the natural gas day
convention where the gas day transitions at 10 AM CT.

{% enddocs %}
