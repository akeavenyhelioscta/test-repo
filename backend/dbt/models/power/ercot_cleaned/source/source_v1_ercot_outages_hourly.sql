{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Reported Outages Hourly
-- Grain: 1 row per date x hour_ending
-- Aggregates sub-hourly outage data to hourly
---------------------------

WITH raw_data AS (
    SELECT
        time_local::DATE AS date
        ,(EXTRACT(HOUR FROM time_local::TIMESTAMP) + 1)::INT AS hour_ending

        ,combined_total::NUMERIC AS combined_total_raw
        ,combined_planned::NUMERIC AS combined_planned_raw
        ,combined_unplanned::NUMERIC AS combined_unplanned_raw

        ,dispatchable_total::NUMERIC AS dispatchable_total_raw
        ,dispatchable_planned::NUMERIC AS dispatchable_planned_raw
        ,dispatchable_unplanned::NUMERIC AS dispatchable_unplanned_raw

        ,renewable_total::NUMERIC AS renewable_total_raw
        ,renewable_planned::NUMERIC AS renewable_planned_raw
        ,renewable_unplanned::NUMERIC AS renewable_unplanned_raw

    FROM {{ source('gridstatus_v1', 'ercot_reported_outages') }}
    WHERE
        EXTRACT(YEAR FROM time_local::DATE) >= 2020
),

hourly AS (
    SELECT
        date
        ,hour_ending

        ,AVG(combined_unplanned_raw) AS combined_unplanned
        ,AVG(combined_planned_raw) AS combined_planned
        ,AVG(combined_total_raw) AS combined_total

        ,AVG(dispatchable_unplanned_raw) AS dispatchable_unplanned
        ,AVG(dispatchable_planned_raw) AS dispatchable_planned
        ,AVG(dispatchable_total_raw) AS dispatchable_total

        ,AVG(renewable_unplanned_raw) AS renewable_unplanned
        ,AVG(renewable_planned_raw) AS renewable_planned
        ,AVG(renewable_total_raw) AS renewable_total

    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM hourly
