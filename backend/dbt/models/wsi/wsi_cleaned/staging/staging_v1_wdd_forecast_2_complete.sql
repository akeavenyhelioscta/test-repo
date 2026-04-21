{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- COMPLETENESS FILTER
---------------------------

WITH DEDUPLICATED AS (
    SELECT * FROM {{ ref('staging_v1_wdd_forecast_1_deduplicated') }}
),

FORECAST_DAYS AS (
    SELECT
        *,
        (forecast_date - forecast_execution_date) + 1 AS count_forecast_days
    FROM DEDUPLICATED
),

FORECAST_DAYS_MAX AS (
    SELECT
        *,
        MAX(count_forecast_days) OVER (
            PARTITION BY forecast_execution_datetime, model, region, bias_corrected
        ) AS max_forecast_days
    FROM FORECAST_DAYS
),

FINAL AS (
    SELECT * FROM FORECAST_DAYS_MAX
    WHERE max_forecast_days = 15
)

SELECT * FROM FINAL
