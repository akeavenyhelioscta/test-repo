{{
  config(
    materialized='incremental',
    unique_key=['date', 'region', 'site_id', 'station_name']
  )
}}

---------------------------
-- DAILY OBSERVED TEMPERATURES
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('source_v1_daily_observed_temp') }}
    {% if is_incremental() %}
    WHERE date >= (SELECT MAX(date) FROM {{ this }}) - INTERVAL '7 days'
    {% endif %}
)

SELECT * FROM FINAL
ORDER BY date DESC, region, station_name
