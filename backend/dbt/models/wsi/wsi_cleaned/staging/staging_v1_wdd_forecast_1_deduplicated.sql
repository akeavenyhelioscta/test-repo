{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- REVISION DEDUP
---------------------------

WITH SOURCE AS (
    SELECT * FROM {{ ref('source_v1_daily_forecast_wdd') }}
),

REVISIONS AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY region, forecast_execution_datetime, forecast_date, model, bias_corrected
            ORDER BY updated_at DESC
        ) AS forecast_revision
    FROM SOURCE
),

FINAL AS (
    SELECT * FROM REVISIONS
    WHERE forecast_revision = 1
)

SELECT * FROM FINAL
