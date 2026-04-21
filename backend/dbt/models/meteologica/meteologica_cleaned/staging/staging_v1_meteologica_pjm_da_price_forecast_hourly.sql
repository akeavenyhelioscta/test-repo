{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Day-Ahead Price Forecast (Hourly)
-- UNIONs 13 raw tables (system + 12 hubs), produces UTC/timezone/local triplets for issue time
-- and hour-ending target time, ranks by issue time (earliest first).
-- Grain: 1 row per forecast_rank × forecast_date × hour_ending × hub
---------------------------

WITH UNIONED AS (

    SELECT
        'SYSTEM' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_da_power_price_system_forecast_hourly') }}

    UNION ALL

    SELECT
        'AEP DAYTON' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_aep_dayton_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'AEP GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_aep_gen_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'ATSI GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_atsi_gen_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'CHICAGO GEN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_chicago_gen_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'CHICAGO' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_chicago_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'DOMINION' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_dominion_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'EASTERN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_eastern_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'NEW JERSEY' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_new_jersey_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'N ILLINOIS' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_n_illinois_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'OHIO' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_ohio_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'WESTERN' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_western_hub_da_power_price_forecast_hourly') }}

    UNION ALL

    SELECT
        'WEST INT' AS hub
        ,content_id
        ,update_id
        ,issue_date
        ,forecast_period_start
        ,forecast_period_end
        ,day_ahead_price
    FROM {{ source('meteologica_pjm_v1', 'usa_pjm_west_int_hub_da_power_price_forecast_hourly') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
---------------------------

NORMALIZED AS (
    SELECT
        hub
        ,issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,((forecast_period_start + INTERVAL '1 hour') AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS forecast_datetime_ending_utc
        ,(forecast_period_start + INTERVAL '1 hour') AS forecast_datetime_ending_local
        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,day_ahead_price::NUMERIC AS forecast_da_price
    FROM UNIONED
),

--------------------------------
-- Rank forecasts per (forecast_date, hub) by issue time (earliest first)
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,hub
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, hub
            ORDER BY forecast_execution_datetime_local ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, hub
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

        ,n.hub
        ,n.forecast_da_price

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
        AND n.hub = r.hub
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, hub


