{% docs col_timezone %}
IANA timezone string identifying how to interpret the `_local` timestamp columns in this
model. Always `'US/Eastern'` for Meteologica PJM models, which corresponds to Eastern
Prevailing Time (EPT). The paired `_utc` columns are the same instant in UTC.
{% enddocs %}

{% docs col_forecast_execution_datetime_utc %}
Timestamp (UTC, naive) when the forecast was issued by Meteologica. Source is the raw UTC
`issue_date` VARCHAR cast to TIMESTAMP without timezone conversion.
{% enddocs %}

{% docs col_forecast_execution_datetime_local %}
Timestamp (EPT, naive) when the forecast was issued by Meteologica. Converted from UTC
`issue_date` via `AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'`. Ranking key —
`forecast_rank = 1` corresponds to the earliest `forecast_execution_datetime_local` for a
given forecast date.
{% enddocs %}

{% docs col_forecast_execution_date %}
Date portion (EPT) of `forecast_execution_datetime_local`. Useful for filtering to forecasts
issued on a specific day.
{% enddocs %}

{% docs col_forecast_datetime_ending_utc %}
Target hour-ending timestamp (UTC, naive) for the forecast: the end of the clock hour the
row covers. Computed as `(forecast_period_start + INTERVAL '1 hour') AT TIME ZONE
'America/New_York' AT TIME ZONE 'UTC'`. For hour_ending = 1 on 2026-03-04 EPT this yields
`2026-03-04 05:00:00` UTC (or `06:00:00` outside DST).
{% enddocs %}

{% docs col_forecast_datetime_ending_local %}
Target hour-ending timestamp (EPT, naive) for the forecast: the end of the clock hour the
row covers. Equal to `forecast_period_start + INTERVAL '1 hour'`. For hour_ending = 1 on
2026-03-04 this yields `2026-03-04 01:00:00`.
{% enddocs %}

{% docs col_forecast_date %}
The date being forecasted, in Eastern Prevailing Time (EPT). Derived directly from
`forecast_period_start` which is already stored in EPT.
{% enddocs %}

{% docs col_meteologica_hour_ending %}
Hour ending in Eastern Prevailing Time (1-24). Derived directly from `forecast_period_start`
which is already stored in EPT: `EXTRACT(HOUR FROM forecast_period_start) + 1`.
{% enddocs %}

{% docs col_meteologica_forecast_rank %}
Issue-time rank of the forecast vintage. `1` = first-issued forecast for a given target date.
Computed via `DENSE_RANK()` ordered by `forecast_execution_datetime_local ASC`, so all hours
within the same vintage share the same rank. No completeness filter is applied — partial
vintages are included.
{% enddocs %}

{% docs col_forecast_load_mw %}
Forecasted demand/load in MW from Meteologica's weather-driven demand model.
{% enddocs %}

{% docs col_forecast_generation_mw %}
Forecasted generation in MW from Meteologica's weather-driven generation model.
Covers solar, wind, and hydro sources.
{% enddocs %}

{% docs col_forecast_da_price %}
Forecasted day-ahead electricity price in $/MWh from Meteologica's price model.
{% enddocs %}

{% docs col_generation_source %}
Generation fuel/technology type. One of: **solar** (PV), **wind**, or **hydro**.
{% enddocs %}

{% docs col_meteologica_region %}
PJM region for demand and generation forecasts. Macro regions: **RTO**, **MIDATL**,
**WEST**, **SOUTH**.

Demand forecasts include 32 utility-level sub-regions across all three macro regions
(e.g., MIDATL_AE, MIDATL_DPL, SOUTH_DOM, WEST_AEP, WEST_ATSI, WEST_EKPC).

Wind generation forecasts include 8 utility-level sub-regions:
MIDATL_AE, MIDATL_PL, MIDATL_PN, SOUTH_DOM, WEST_AEP, WEST_AP, WEST_ATSI, WEST_CE.
{% enddocs %}

{% docs col_forecast_load_average_mw %}
Ensemble average (mean of 51 ECMWF-ENS members) demand forecast in MW.
{% enddocs %}

{% docs col_forecast_load_bottom_mw %}
Ensemble minimum (lowest of 51 ECMWF-ENS members) demand forecast in MW.
{% enddocs %}

{% docs col_forecast_load_top_mw %}
Ensemble maximum (highest of 51 ECMWF-ENS members) demand forecast in MW.
{% enddocs %}

{% docs col_meteologica_hub %}
PJM pricing hub for DA price forecasts and observations. **SYSTEM** is the system-wide price.
Hub nodes: AEP DAYTON, AEP GEN, ATSI GEN, CHICAGO GEN, CHICAGO, DOMINION, EASTERN,
NEW JERSEY, N ILLINOIS, OHIO, WESTERN, WEST INT.
{% enddocs %}

{% docs col_update_rank %}
Issue-time rank of the data update. `1` = first-issued update for a given target date.
Computed via `DENSE_RANK()` ordered by `update_datetime_local ASC`, so all hours within the
same update share the same rank.
{% enddocs %}

{% docs col_update_datetime_utc %}
Timestamp (UTC, naive) when the data was issued. Source is the raw UTC `issue_date` VARCHAR
cast to TIMESTAMP without timezone conversion. Used as the ranking key (UTC equivalent of
`update_datetime_local`) for observations, projections, and normals.
{% enddocs %}

{% docs col_update_datetime_local %}
Timestamp (EPT, naive) when the data was issued. Converted from UTC `issue_date` via
`AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'`. Ranking key —
`update_rank = 1` corresponds to the earliest `update_datetime_local` for a given target.
{% enddocs %}

{% docs col_update_date %}
Date portion (EPT) of `update_datetime_local`. Useful for filtering to data issued on a
specific day.
{% enddocs %}

{% docs col_observation_datetime_ending_utc %}
Observation hour-ending timestamp (UTC, naive): the end of the clock hour the row covers.
Computed as `(forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York'
AT TIME ZONE 'UTC'`.
{% enddocs %}

{% docs col_observation_datetime_ending_local %}
Observation hour-ending timestamp (EPT, naive): the end of the clock hour the row covers.
Equal to `forecast_period_start + INTERVAL '1 hour'`.
{% enddocs %}

{% docs col_observation_datetime_ending_utc_5min %}
5-minute observation timestamp (UTC, naive). **Exception to the `_ending` convention:** the
value preserves prior naive-UTC semantics (actually the start of each 5-minute block, not
the end) to avoid regressing the hourly rollup in `meteologica_pjm_demand_observation_hourly`
(which relies on `EXTRACT(HOUR) + 1 = hour_ending`). The column is named `_ending` for
consistency with the hourly marts. Derived from `forecast_period_start::TIMESTAMP`.
{% enddocs %}

{% docs col_observation_datetime_ending_local_5min %}
5-minute observation timestamp (EPT, naive). **Exception to the `_ending` convention:** the
value preserves prior naive-EPT semantics (actually the start of each 5-minute block, not
the end) to avoid regressing the hourly rollup in `meteologica_pjm_demand_observation_hourly`
(which relies on `EXTRACT(HOUR) + 1 = hour_ending`). The column is named `_ending` for
consistency with the hourly marts. Derived via `forecast_period_start AT TIME ZONE 'UTC'
AT TIME ZONE 'America/New_York'`.
{% enddocs %}

{% docs col_observation_load_mw %}
Observed actual demand/load in MW from Meteologica's xTraders API.
{% enddocs %}

{% docs col_observation_generation_mw %}
Observed actual generation in MW from Meteologica's xTraders API.
Covers solar, wind, and hydro sources.
{% enddocs %}

{% docs col_observation_da_price %}
Observed actual day-ahead electricity price in $/MWh from Meteologica's xTraders API.
{% enddocs %}

{% docs col_projection_datetime_ending_utc %}
Projection hour-ending timestamp (UTC, naive): the end of the clock hour the row covers.
Computed as `(forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York'
AT TIME ZONE 'UTC'`.
{% enddocs %}

{% docs col_projection_datetime_ending_local %}
Projection hour-ending timestamp (EPT, naive): the end of the clock hour the row covers.
Equal to `forecast_period_start + INTERVAL '1 hour'`.
{% enddocs %}

{% docs col_projection_load_mw %}
Projected demand/load in MW from Meteologica's demand normal model (labeled "projection"
by Meteologica).
{% enddocs %}

{% docs col_normal_datetime_ending_utc %}
Climatological-normal hour-ending timestamp (UTC, naive): the end of the clock hour the row
covers. Computed as `(forecast_period_start + INTERVAL '1 hour') AT TIME ZONE
'America/New_York' AT TIME ZONE 'UTC'`.
{% enddocs %}

{% docs col_normal_datetime_ending_local %}
Climatological-normal hour-ending timestamp (EPT, naive): the end of the clock hour the row
covers. Equal to `forecast_period_start + INTERVAL '1 hour'`.
{% enddocs %}

{% docs col_normal_generation_mw %}
Climatological normal generation in MW from Meteologica's xTraders API.
Covers solar, wind, and hydro sources.
{% enddocs %}
