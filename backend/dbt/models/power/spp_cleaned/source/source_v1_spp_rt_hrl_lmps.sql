{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- SPP RT Hourly LMPs
-- (5-min averaged to hourly)
-- ===========================

WITH five_min AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending
        ,location AS location_name
        ,lmp AS rt_lmp_total
        ,energy AS rt_lmp_system_energy_price
        ,congestion AS rt_lmp_congestion_price
        ,loss AS rt_lmp_marginal_loss_price
    FROM {{ source('gridstatus_v1', 'spp_lmp_real_time_5_min') }}
    WHERE
        location IN (
            'AECI', 'EES', 'ERCOTE', 'ERCOTN', 'KCPLHUB',
            'MISO', 'NSP', 'OKGE_OKGE', 'PJM', 'SECI_SECI',
            'SOCO', 'SPA', 'SPPNORTH_HUB', 'SPPSOUTH_HUB', 'TVA'
        )
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN rt_lmp_total END) AS rt_lmp_total_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN rt_lmp_total END) AS rt_lmp_total_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_south_hub
    FROM five_min
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
