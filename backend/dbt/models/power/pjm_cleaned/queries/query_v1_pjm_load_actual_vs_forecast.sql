{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Actual RT Load vs Forecast (yesterday)
-- Grain: 1 row per hour × region
-- Run: dbt show --select query_v1_pjm_load_actual_vs_forecast
---------------------------

WITH ACTUAL AS (
    SELECT
        date
        ,hour_ending
        ,region
        ,rt_load_mw AS actual_load_mw
    -- FROM dbt_pjm_v1_2026_feb_19.staging_v1_pjm_load_rt_hourly
    FROM {{ ref('staging_v1_pjm_load_rt_hourly') }}
    WHERE
        date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
),

FORECAST AS (
    SELECT
        forecast_rank
        ,forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
    -- FROM dbt_pjm_v1_2026_feb_19.staging_v1_gridstatus_pjm_load_forecast_hourly
    FROM {{ ref('staging_v1_gridstatus_pjm_load_forecast_hourly') }}
    WHERE
        forecast_date = (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
        AND forecast_rank = 1
),

COMPARISON AS (
    SELECT
        a.date
        ,a.hour_ending
        ,a.region

        ,a.actual_load_mw
        ,f.forecast_load_mw

        ,(a.actual_load_mw - f.forecast_load_mw) AS load_error_mw
        ,CASE
            WHEN f.forecast_load_mw != 0
            THEN ROUND((((a.actual_load_mw - f.forecast_load_mw) / f.forecast_load_mw) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS load_error_pct

    FROM ACTUAL a
    LEFT JOIN FORECAST f
        ON a.date = f.forecast_date
        AND a.hour_ending = f.hour_ending
        AND a.region = f.region
)

SELECT * FROM COMPARISON
ORDER BY date, hour_ending, region
