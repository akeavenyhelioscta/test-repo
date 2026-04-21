{% docs col_date %}
Operating date in Eastern Prevailing Time (EPT). This is the calendar date the
energy was produced or consumed, not the settlement date.
{% enddocs %}

{% docs col_hour_ending %}
Hour ending in Eastern Prevailing Time (1–24). Hour ending 1 covers midnight to 1 AM,
hour ending 24 covers 11 PM to midnight. Follows the NERC convention used by PJM.
{% enddocs %}

{% docs col_region %}
PJM load region. One of: **RTO** (full footprint), **MIDATL** (Mid-Atlantic zones),
**WEST** (Western zones including ComEd, AEP), or **SOUTH** (Southern zones including
Dominion). SOUTH is always computed as `RTO - MIDATL - WEST`.
{% enddocs %}

{% docs col_period %}
Time-of-day aggregation period for daily models:

- **flat** — all 24 hours
- **peak** — HE08–HE23 regardless of day type
- **onpeak** — HE08–HE23 on weekdays excluding NERC holidays
- **offpeak** — all hours not in onpeak (nights, weekends, NERC holidays)
{% enddocs %}

{% docs col_forecast_rank %}
Issue-time rank of the forecast revision. `1` = first-issued forecast for a given
`forecast_date`. Computed via `DENSE_RANK()` ordered by `forecast_execution_datetime ASC`.
Only complete forecasts (all hours and regions present) are ranked.
{% enddocs %}

{% docs col_market %}
Market type for LMP pricing:

- **da** — Day-Ahead market
- **rt** — Real-Time market
- **dart** — DA-RT spread (Day-Ahead minus Real-Time)
{% enddocs %}
