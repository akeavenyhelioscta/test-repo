{% docs miso_col_datetime %}
Computed timestamp from `date + hour_ending` in Central Prevailing Time.
Derived as `(date + (hour_ending || ' hours')::INTERVAL)::TIMESTAMP`.
{% enddocs %}
