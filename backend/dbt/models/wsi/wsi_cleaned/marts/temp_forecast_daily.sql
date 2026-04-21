{{
  config(
    materialized='view'
  )
}}

---------------------------
-- DAILY FORECAST TEMPERATURES — CURRENT
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_wsi_homepage_forecast_table_current') }}
)

SELECT * FROM FINAL
ORDER BY forecast_execution_datetime DESC, forecast_date ASC, region ASC, site_id ASC, station_name ASC
