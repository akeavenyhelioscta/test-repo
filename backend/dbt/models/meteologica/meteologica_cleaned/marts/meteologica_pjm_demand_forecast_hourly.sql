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

SELECT * FROM {{ ref('staging_v1_meteologica_pjm_demand_forecast_hourly') }}

{% if is_incremental() %}
-- Lookback on forecast_date (the DENSE_RANK partition column) so all issue
-- dates for the covered forecast_date are reloaded together and ranks stay
-- consistent when new issue_dates arrive. Must be >= the full forecast
-- horizon (Meteologica PJM demand = up to +15 days from issue) so a newly
-- issued forecast's near-term target dates are not dropped; 20 adds buffer.
WHERE forecast_date >= (SELECT MAX(forecast_date) - 20 FROM {{ this }})
{% endif %}
