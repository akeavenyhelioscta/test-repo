{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Solar Generation Forecast (Hourly)
-- UNIONs 4 raw tables (RTO + 3 macro regions), produces UTC/timezone/local
-- triplets for issue time and hour-ending target time, ranks by issue time (earliest first).
-- Grain: 1 row per forecast_rank × forecast_date × hour_ending × region
---------------------------

WITH UNIONED AS (

    ---------------------------
    -- RTO + 3 macro regions
    ---------------------------

    SELECT
        'RTO' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_pv_power_generation_forecast_hourly') }}

    UNION ALL

    SELECT
        'MIDATL' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pv_power_generation_forecast_hourly') }}

    UNION ALL

    SELECT
        'SOUTH' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_south_pv_power_generation_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST' AS region
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,forecast_mw
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_pv_power_generation_forecast_hourly') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        region
        ,issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS forecast_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS forecast_datetime_ending_local
        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,forecast_mw::NUMERIC AS forecast_mw
    FROM UNIONED
),

--------------------------------
-- Rank forecasts per (forecast_date, region) by issue time (earliest first)
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,region
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, region
            ORDER BY forecast_execution_datetime_local ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, region
        FROM NORMALIZED
    ) sub
),

--------------------------------
--------------------------------

FINAL AS (
    SELECT
        r.forecast_rank

        ,n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,n.forecast_execution_date

        ,n.forecast_datetime_ending_utc
        ,n.forecast_datetime_ending_local
        ,n.forecast_date
        ,n.hour_ending

        ,n.region
        ,n.forecast_mw AS forecast_generation_mw

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, region
