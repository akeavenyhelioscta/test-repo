{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Energy Storage Hourly (5-min aggregated to hourly)
-- Grain: 1 row per date x hour_ending
---------------------------

WITH five_min AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending

        ,net_output::NUMERIC AS storage_net_output
        ,total_charging::NUMERIC AS storage_total_charging
        ,total_discharging::NUMERIC AS storage_total_discharging

    FROM {{ source('gridstatus_v1', 'ercot_energy_storage_resources') }}
    WHERE
        EXTRACT(YEAR FROM interval_start_local::DATE) >= 2023
),

hourly AS (
    SELECT
        date
        ,hour_ending

        ,AVG(storage_net_output) AS storage_net_output
        ,AVG(storage_total_charging) AS storage_total_charging
        ,AVG(storage_total_discharging) AS storage_total_discharging

    FROM five_min
    GROUP BY date, hour_ending
)

SELECT * FROM hourly
