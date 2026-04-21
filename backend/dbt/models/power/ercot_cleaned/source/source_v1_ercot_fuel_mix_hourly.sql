{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Fuel Mix Hourly (5-min aggregated to hourly)
-- Grain: 1 row per date x hour_ending
---------------------------

WITH five_min AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending

        ,nuclear::NUMERIC AS nuclear
        ,hydro::NUMERIC AS hydro
        ,wind::NUMERIC AS wind
        ,solar::NUMERIC AS solar
        ,natural_gas::NUMERIC AS natural_gas
        ,coal_and_lignite::NUMERIC AS coal_and_lignite
        ,power_storage::NUMERIC AS power_storage
        ,other::NUMERIC AS other

    FROM {{ source('gridstatus_v1', 'ercot_fuel_mix') }}
    WHERE
        EXTRACT(YEAR FROM interval_start_local::DATE) >= 2020
),

hourly AS (
    SELECT
        date
        ,hour_ending

        ,AVG(nuclear) AS nuclear
        ,AVG(hydro) AS hydro
        ,AVG(wind) AS wind
        ,AVG(solar) AS solar
        ,AVG(natural_gas) AS natural_gas
        ,AVG(coal_and_lignite) AS coal_and_lignite
        ,AVG(power_storage) AS power_storage
        ,AVG(other) AS other

    FROM five_min
    GROUP BY date, hour_ending
)

SELECT * FROM hourly
