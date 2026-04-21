{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Forecasts Hourly (Combined)
-- Grain: 1 row per rank_forecast_execution_timestamps x forecast_date x hour_ending
-- Combines load, solar (STPPF), and wind (STWPF) forecasts
-- Computes net_load = load - (solar + wind)
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
),

solar_forecast AS (
    SELECT
        forecast_execution_date
        ,interval_start
        ,stppf_system_wide AS forecast_solar_total
    FROM {{ ref('source_v1_ercot_gridstatus_solar_forecast_hourly') }}
    WHERE rank_forecast_execution_timestamps = 1
),

wind_forecast AS (
    SELECT
        forecast_execution_date
        ,interval_start
        ,stwpf_system_wide AS forecast_wind_total
    FROM {{ ref('source_v1_ercot_gridstatus_wind_forecast_hourly') }}
    WHERE rank_forecast_execution_timestamps = 1
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
        ON lf.forecast_execution_date = sf.forecast_execution_date
        AND lf.interval_start = sf.interval_start
    LEFT JOIN wind_forecast wf
        ON lf.forecast_execution_date = wf.forecast_execution_date
        AND lf.interval_start = wf.interval_start
)

SELECT * FROM combined
