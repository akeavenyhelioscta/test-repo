{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Fuel Mix Hourly
-- Grain: 1 row per date x hour_ending
-- Joins fuel mix with energy storage, computes derived columns
---------------------------

WITH fuel_mix AS (
    SELECT * FROM {{ ref('source_v1_ercot_fuel_mix_hourly') }}
),

storage AS (
    SELECT * FROM {{ ref('source_v1_ercot_energy_storage_hourly') }}
),

hourly AS (
    SELECT
        (fm.date + (fm.hour_ending || ' hours')::INTERVAL)::TIMESTAMP AS datetime
        ,fm.date
        ,fm.hour_ending

        ,fm.nuclear
        ,fm.hydro
        ,fm.wind
        ,fm.solar
        ,fm.natural_gas
        ,fm.coal_and_lignite
        ,fm.power_storage
        ,fm.other

        ,s.storage_net_output
        ,s.storage_total_charging
        ,s.storage_total_discharging

        ,(
            COALESCE(fm.nuclear, 0)
            + COALESCE(fm.hydro, 0)
            + COALESCE(fm.wind, 0)
            + COALESCE(fm.solar, 0)
            + COALESCE(fm.natural_gas, 0)
            + COALESCE(fm.coal_and_lignite, 0)
            + COALESCE(fm.power_storage, 0)
            + COALESCE(fm.other, 0)
        ) AS total

        ,(COALESCE(fm.wind, 0) + COALESCE(fm.solar, 0)) AS renewables
        ,(COALESCE(fm.natural_gas, 0) + COALESCE(fm.coal_and_lignite, 0)) AS thermal

        ,CASE
            WHEN (COALESCE(fm.natural_gas, 0) + COALESCE(fm.coal_and_lignite, 0)) > 0
            THEN fm.natural_gas / (COALESCE(fm.natural_gas, 0) + COALESCE(fm.coal_and_lignite, 0))
        END AS gas_pct_thermal
        ,CASE
            WHEN (COALESCE(fm.natural_gas, 0) + COALESCE(fm.coal_and_lignite, 0)) > 0
            THEN fm.coal_and_lignite / (COALESCE(fm.natural_gas, 0) + COALESCE(fm.coal_and_lignite, 0))
        END AS coal_pct_thermal

    FROM fuel_mix fm
    LEFT JOIN storage s
        ON fm.date = s.date
        AND fm.hour_ending = s.hour_ending
)

SELECT * FROM hourly
