{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Load Forecast Performance (Hourly)
-- Compares PJM 7-day first-issued forecast (rank=1) vs RT metered actuals
-- Grain: 1 row per date × hour_ending × region
---------------------------

WITH ACTUAL AS (
    SELECT
        date
        ,hour_ending
        ,region
        ,rt_load_mw AS actual_load_mw
    FROM {{ ref('staging_v1_pjm_load_rt_metered_hourly') }}
),

FORECAST AS (
    SELECT
        forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
    FROM {{ ref('staging_v1_pjm_load_forecast_hourly') }}
    WHERE forecast_rank = 1
),

COMPARISON AS (
    SELECT
        a.date
        ,a.hour_ending
        ,a.region

        ,a.actual_load_mw
        ,f.forecast_load_mw

        ,(a.actual_load_mw - f.forecast_load_mw) AS error_mw
        ,ABS(a.actual_load_mw - f.forecast_load_mw) AS abs_error_mw
        ,CASE
            WHEN a.actual_load_mw != 0
            THEN ROUND((ABS(a.actual_load_mw - f.forecast_load_mw) / a.actual_load_mw * 100)::NUMERIC, 2)
            ELSE NULL
        END AS error_pct

    FROM ACTUAL a
    INNER JOIN FORECAST f
        ON a.date = f.forecast_date
        AND a.hour_ending = f.hour_ending
        AND a.region = f.region
)

SELECT
    date + (hour_ending || ' hours')::interval AS datetime,
    *
FROM COMPARISON
ORDER BY date DESC, hour_ending, region
