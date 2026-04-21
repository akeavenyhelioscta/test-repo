{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT RT 15-min SPP aggregated to hourly (pivoted to wide format)
-- Grain: 1 row per date x hour_ending
-- Filters to 4 trading hubs and pivots SPP to hub columns
---------------------------

WITH raw_data AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending
        ,location AS location_name
        ,spp::NUMERIC AS rt_spp
    FROM {{ source('gridstatus_v1', 'ercot_spp_real_time_15_min') }}
    WHERE
        location IN ('HB_HOUSTON', 'HB_NORTH', 'HB_SOUTH', 'HB_WEST')
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        ,AVG(CASE WHEN location_name = 'HB_HOUSTON' THEN rt_spp END) AS rt_lmp_total_houston_hub
        ,AVG(CASE WHEN location_name = 'HB_NORTH' THEN rt_spp END) AS rt_lmp_total_north_hub
        ,AVG(CASE WHEN location_name = 'HB_SOUTH' THEN rt_spp END) AS rt_lmp_total_south_hub
        ,AVG(CASE WHEN location_name = 'HB_WEST' THEN rt_spp END) AS rt_lmp_total_west_hub
    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
