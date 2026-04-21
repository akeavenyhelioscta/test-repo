{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- SPP DA Hourly LMPs
-- ===========================

WITH raw_data AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending
        ,location AS location_name
        ,lmp AS da_lmp_total
        ,energy AS da_lmp_system_energy_price
        ,congestion AS da_lmp_congestion_price
        ,loss AS da_lmp_marginal_loss_price
    FROM {{ source('gridstatus_v1', 'spp_lmp_day_ahead_hourly') }}
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
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN da_lmp_total END) AS da_lmp_total_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN da_lmp_system_energy_price END) AS da_lmp_system_energy_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN da_lmp_congestion_price END) AS da_lmp_congestion_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPNORTH_HUB' THEN da_lmp_marginal_loss_price END) AS da_lmp_marginal_loss_price_north_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN da_lmp_total END) AS da_lmp_total_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN da_lmp_system_energy_price END) AS da_lmp_system_energy_price_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN da_lmp_congestion_price END) AS da_lmp_congestion_price_south_hub
        ,AVG(CASE WHEN location_name = 'SPPSOUTH_HUB' THEN da_lmp_marginal_loss_price END) AS da_lmp_marginal_loss_price_south_hub
    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
