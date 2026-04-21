{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- MISO RT Hourly LMPs
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
    FROM {{ source('gridstatus_v1', 'miso_lmp_real_time_5_min') }}
    WHERE
        location IN (
            'ARKANSAS.HUB', 'ILLINOIS.HUB', 'INDIANA.HUB', 'LOUISIANA.HUB',
            'MICHIGAN.HUB', 'MINN.HUB', 'MS.HUB', 'TEXAS.HUB'
        )
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        ,AVG(CASE WHEN location_name = 'ARKANSAS.HUB' THEN rt_lmp_total END) AS rt_lmp_total_arkansas_hub
        ,AVG(CASE WHEN location_name = 'ARKANSAS.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_arkansas_hub
        ,AVG(CASE WHEN location_name = 'ARKANSAS.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_arkansas_hub
        ,AVG(CASE WHEN location_name = 'ARKANSAS.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_arkansas_hub
        ,AVG(CASE WHEN location_name = 'ILLINOIS.HUB' THEN rt_lmp_total END) AS rt_lmp_total_illinois_hub
        ,AVG(CASE WHEN location_name = 'ILLINOIS.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_illinois_hub
        ,AVG(CASE WHEN location_name = 'ILLINOIS.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_illinois_hub
        ,AVG(CASE WHEN location_name = 'ILLINOIS.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_illinois_hub
        ,AVG(CASE WHEN location_name = 'INDIANA.HUB' THEN rt_lmp_total END) AS rt_lmp_total_indiana_hub
        ,AVG(CASE WHEN location_name = 'INDIANA.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_indiana_hub
        ,AVG(CASE WHEN location_name = 'INDIANA.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_indiana_hub
        ,AVG(CASE WHEN location_name = 'INDIANA.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_indiana_hub
        ,AVG(CASE WHEN location_name = 'LOUISIANA.HUB' THEN rt_lmp_total END) AS rt_lmp_total_louisiana_hub
        ,AVG(CASE WHEN location_name = 'LOUISIANA.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_louisiana_hub
        ,AVG(CASE WHEN location_name = 'LOUISIANA.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_louisiana_hub
        ,AVG(CASE WHEN location_name = 'LOUISIANA.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_louisiana_hub
        ,AVG(CASE WHEN location_name = 'MICHIGAN.HUB' THEN rt_lmp_total END) AS rt_lmp_total_michigan_hub
        ,AVG(CASE WHEN location_name = 'MICHIGAN.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_michigan_hub
        ,AVG(CASE WHEN location_name = 'MICHIGAN.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_michigan_hub
        ,AVG(CASE WHEN location_name = 'MICHIGAN.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_michigan_hub
        ,AVG(CASE WHEN location_name = 'MINN.HUB' THEN rt_lmp_total END) AS rt_lmp_total_minn_hub
        ,AVG(CASE WHEN location_name = 'MINN.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_minn_hub
        ,AVG(CASE WHEN location_name = 'MINN.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_minn_hub
        ,AVG(CASE WHEN location_name = 'MINN.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_minn_hub
        ,AVG(CASE WHEN location_name = 'MS.HUB' THEN rt_lmp_total END) AS rt_lmp_total_ms_hub
        ,AVG(CASE WHEN location_name = 'MS.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_ms_hub
        ,AVG(CASE WHEN location_name = 'MS.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_ms_hub
        ,AVG(CASE WHEN location_name = 'MS.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_ms_hub
        ,AVG(CASE WHEN location_name = 'TEXAS.HUB' THEN rt_lmp_total END) AS rt_lmp_total_texas_hub
        ,AVG(CASE WHEN location_name = 'TEXAS.HUB' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_texas_hub
        ,AVG(CASE WHEN location_name = 'TEXAS.HUB' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_texas_hub
        ,AVG(CASE WHEN location_name = 'TEXAS.HUB' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_texas_hub
    FROM five_min
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
