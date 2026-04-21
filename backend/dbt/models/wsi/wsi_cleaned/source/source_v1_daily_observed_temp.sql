{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH DAILY_OBSERVED_TEMP AS (
    SELECT

        date

        ,CASE
            WHEN region = 'US_NATIONAL' THEN 'CONUS'
            WHEN region = 'SOUTH CENTRAL' THEN 'SOUTHCENTRAL'
            ELSE region
        END as region
        ,CASE
            WHEN site_id = 'US_NATIONAL' THEN 'CONUS'
            WHEN site_id = 'SOUTH CENTRAL' THEN 'SOUTHCENTRAL'
            ELSE site_id
        END as site_id
        ,CASE
            WHEN station_name = 'US_NATIONAL' THEN 'CONUS'
            WHEN station_name = 'SOUTH CENTRAL' THEN 'SOUTHCENTRAL'
            ELSE station_name
        END as station_name

        ,avg as temp
        ,min as temp_min
        ,max as temp_max

        ,cdd
        ,hdd

    FROM {{ source('wsi_v1', 'daily_observed_temp_v3_2025_09_08') }}
    WHERE
        EXTRACT(year from date) >= EXTRACT(year from current_date) - (30+1)
)

SELECT * FROM DAILY_OBSERVED_TEMP
ORDER BY date DESC, region, station_name
