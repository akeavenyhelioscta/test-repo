{{
  config(
    materialized='incremental',
    unique_key=['forecast_rank', 'forecast_date', 'hour_ending', 'region'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['forecast_datetime_ending_utc', 'region'], 'type': 'btree'},
      {'columns': ['forecast_date', 'region'], 'type': 'btree'},
      {'columns': ['forecast_execution_datetime_utc'], 'type': 'btree'}
    ]
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

-- Only retain forecasts issued in the last 7 days. We only care about the
-- latest forecast for modelling, and a 7-day buffer absorbs weekends, holidays,
-- and late data deliveries.
SELECT * FROM {{ ref('staging_v1_meteo_pjm_load_forecast_hourly') }}
WHERE forecast_execution_date >= CURRENT_DATE - 7

{% if is_incremental() %}
    -- Lookback on forecast_date (the DENSE_RANK partition column) so all issue
    -- dates for the covered forecast_date are reloaded together and ranks stay
    -- consistent when new issue_dates arrive. Must be >= the full forecast
    -- horizon (Meteologica PJM demand = up to +15 days from issue) so a newly
    -- issued forecast's near-term target dates are not dropped; 20 adds buffer.
    AND forecast_date >= (SELECT MAX(forecast_date) - 20 FROM {{ this }})
{% endif %}
