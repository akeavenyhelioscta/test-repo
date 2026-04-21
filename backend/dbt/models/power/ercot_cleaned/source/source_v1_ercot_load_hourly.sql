{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Load Hourly
-- Grain: 1 row per date x hour_ending
-- Load zones: NORTH, SOUTH, WEST, HOUSTON, TOTAL (ERCOT)
---------------------------

WITH raw_data AS (
    SELECT
        interval_start_local::TIMESTAMP AS interval_start
        ,interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending

        ,north::NUMERIC AS load_north
        ,south::NUMERIC AS load_south
        ,west::NUMERIC AS load_west
        ,houston::NUMERIC AS load_houston
        ,(COALESCE(north::NUMERIC, 0)
            + COALESCE(south::NUMERIC, 0)
            + COALESCE(west::NUMERIC, 0)
            + COALESCE(houston::NUMERIC, 0)
        ) AS load_total

    FROM {{ source('gridstatus_v1', 'ercot_load_by_forecast_zone') }}
    WHERE
        EXTRACT(YEAR FROM interval_start_local::DATE) >= 2020
)

SELECT * FROM raw_data
