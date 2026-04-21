{% docs spp_col_date %}
Operating date in Central Prevailing Time (CPT). This is the calendar date the
energy was produced or consumed, not the settlement date.
{% enddocs %}

{% docs spp_col_hour_ending %}
Hour ending in Central Prevailing Time (1-24). Hour ending 1 covers midnight to 1 AM,
hour ending 24 covers 11 PM to midnight.
{% enddocs %}

{% docs spp_col_datetime %}
Timestamp computed as `date + hour_ending` interval. Represents the end of each
hourly interval in Central Prevailing Time.
{% enddocs %}
