{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Load Forecast Performance (Daily)
-- MAE, MAPE, and Bias by date × region × period
-- Grain: 1 row per date × region × period
---------------------------

WITH HOURLY AS (
    SELECT
        h.date
        ,h.hour_ending
        ,h.region
        ,h.actual_load_mw
        ,h.forecast_load_mw
        ,h.error_mw
        ,h.abs_error_mw
        ,d.period
    FROM {{ ref('query_v1_pjm_load_forecast_performance_hourly') }} h
    INNER JOIN {{ ref('utils_v1_pjm_dates_hourly') }} d
        ON h.date = d.date
        AND h.hour_ending = d.hour_ending
),

FLAT AS (
    SELECT
        date
        ,region
        ,'flat' AS period
        ,AVG(abs_error_mw) AS mae
        ,CASE
            WHEN AVG(actual_load_mw) != 0
            THEN ROUND((AVG(abs_error_mw / NULLIF(actual_load_mw, 0)) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS mape
        ,AVG(error_mw) AS bias
        ,AVG(actual_load_mw) AS avg_actual
        ,AVG(forecast_load_mw) AS avg_forecast
    FROM HOURLY
    GROUP BY date, region
),

ONPEAK AS (
    SELECT
        date
        ,region
        ,'onpeak' AS period
        ,AVG(abs_error_mw) AS mae
        ,CASE
            WHEN AVG(actual_load_mw) != 0
            THEN ROUND((AVG(abs_error_mw / NULLIF(actual_load_mw, 0)) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS mape
        ,AVG(error_mw) AS bias
        ,AVG(actual_load_mw) AS avg_actual
        ,AVG(forecast_load_mw) AS avg_forecast
    FROM HOURLY
    WHERE period = 'OnPeak'
    GROUP BY date, region
),

OFFPEAK AS (
    SELECT
        date
        ,region
        ,'offpeak' AS period
        ,AVG(abs_error_mw) AS mae
        ,CASE
            WHEN AVG(actual_load_mw) != 0
            THEN ROUND((AVG(abs_error_mw / NULLIF(actual_load_mw, 0)) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS mape
        ,AVG(error_mw) AS bias
        ,AVG(actual_load_mw) AS avg_actual
        ,AVG(forecast_load_mw) AS avg_forecast
    FROM HOURLY
    WHERE period = 'OffPeak'
    GROUP BY date, region
),

DAILY AS (
    SELECT * FROM FLAT
    UNION ALL
    SELECT * FROM ONPEAK
    UNION ALL
    SELECT * FROM OFFPEAK
)

SELECT * FROM DAILY
ORDER BY date DESC, region, period
