{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ERCOT Forecasts Hourly (Current Forecast Only)
-- Grain: 1 row per forecast_date x hour_ending
-- Filtered to labelled_forecast_execution_timestamp = 'Current Forecast'
-- Simplified join (no forecast_execution_date, just interval_start)
---------------------------

WITH load_forecast AS (
    SELECT
        rank_forecast_execution_timestamps
        ,labelled_forecast_execution_timestamp
        ,forecast_execution_datetime
        ,forecast_execution_date
        ,interval_start
        ,forecast_date
        ,hour_ending
        ,load_total AS forecast_load_total
        ,load_north AS forecast_load_north
        ,load_south AS forecast_load_south
        ,load_west AS forecast_load_west
        ,load_houston AS forecast_load_houston
    FROM {{ ref('source_v1_ercot_gridstatus_load_forecast_hourly') }}
    WHERE labelled_forecast_execution_timestamp = 'Current Forecast'
),

solar_forecast AS (
    SELECT
        interval_start
        ,stppf_system_wide AS forecast_solar_total
    FROM {{ ref('source_v1_ercot_gridstatus_solar_forecast_hourly') }}
    WHERE labelled_forecast_execution_timestamp = 'Current Forecast'
),

wind_forecast AS (
    SELECT
        interval_start
        ,stwpf_system_wide AS forecast_wind_total
    FROM {{ ref('source_v1_ercot_gridstatus_wind_forecast_hourly') }}
    WHERE labelled_forecast_execution_timestamp = 'Current Forecast'
),

combined AS (
    SELECT
        lf.rank_forecast_execution_timestamps
        ,lf.labelled_forecast_execution_timestamp
        ,lf.forecast_execution_datetime
        ,lf.forecast_execution_date
        ,lf.interval_start
        ,lf.forecast_date
        ,lf.hour_ending

        ,lf.forecast_load_total
        ,lf.forecast_load_north
        ,lf.forecast_load_south
        ,lf.forecast_load_west
        ,lf.forecast_load_houston

        ,sf.forecast_solar_total
        ,wf.forecast_wind_total

        ,(lf.forecast_load_total
            - COALESCE(sf.forecast_solar_total, 0)
            - COALESCE(wf.forecast_wind_total, 0)
        ) AS forecast_net_load_total

    FROM load_forecast lf
    LEFT JOIN solar_forecast sf
        ON lf.interval_start = sf.interval_start
    LEFT JOIN wind_forecast wf
        ON lf.interval_start = wf.interval_start
)

SELECT * FROM combined
