{% docs ercot_col_datetime %}
Timestamp in Central Prevailing Time (CPT) derived from `date + hour_ending`.
{% enddocs %}

{% docs ercot_col_forecast_date %}
Target operating date being forecasted.
{% enddocs %}

{% docs ercot_col_forecast_rank %}
Recency rank of the forecast revision. `1` = most recent forecast for a given
`forecast_date`. Computed via `DENSE_RANK()` ordered by `forecast_execution_datetime DESC`.
{% enddocs %}

{% docs ercot_col_period %}
Time-of-day aggregation period for daily models:

- **flat** -- all 24 hours
- **onpeak** -- HE07-HE22
- **offpeak** -- HE01-HE06 and HE23-HE24
{% enddocs %}
