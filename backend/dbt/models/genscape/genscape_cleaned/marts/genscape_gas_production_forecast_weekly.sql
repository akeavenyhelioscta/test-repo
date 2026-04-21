{{
  config(
    materialized='view'
  )
}}

-------------------------------------------------------------
-- Weekly Gas Production Forecast
-- Interim weekly API updates only — excludes official monthly
-- report dates (defined in the seed).
-------------------------------------------------------------

SELECT s.*
FROM {{ ref('staging_v2_genscape_gas_production_forecast') }} s
LEFT JOIN {{ ref('genscape_gas_production_forecast_report_dates') }} m
    ON s.report_date = m.report_date::DATE
WHERE m.report_date IS NULL