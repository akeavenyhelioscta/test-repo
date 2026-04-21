{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT DA Hourly LMPs (pivoted to wide format)
-- Grain: 1 row per date x hour_ending
-- Filters to 4 trading hubs and pivots SPP to hub columns
---------------------------

WITH raw_data AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending
        ,location AS location_name
        ,lmp::NUMERIC AS da_lmp_total
    FROM {{ source('gridstatus_v1', 'ercot_lmp_by_settlement_point') }}
    WHERE
        market = 'DAM'
        AND location IN ('HB_HOUSTON', 'HB_NORTH', 'HB_SOUTH', 'HB_WEST')
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        ,AVG(CASE WHEN location_name = 'HB_HOUSTON' THEN da_lmp_total END) AS da_lmp_total_houston_hub
        ,AVG(CASE WHEN location_name = 'HB_NORTH' THEN da_lmp_total END) AS da_lmp_total_north_hub
        ,AVG(CASE WHEN location_name = 'HB_SOUTH' THEN da_lmp_total END) AS da_lmp_total_south_hub
        ,AVG(CASE WHEN location_name = 'HB_WEST' THEN da_lmp_total END) AS da_lmp_total_west_hub
    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
