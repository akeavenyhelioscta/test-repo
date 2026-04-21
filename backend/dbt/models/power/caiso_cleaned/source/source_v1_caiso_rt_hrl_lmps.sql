{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- CAISO RT Hourly LMPs
-- ===========================

WITH raw_data AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local) + 1)::INT AS hour_ending
        ,location AS location_name
        ,lmp AS rt_lmp_total
        ,energy AS rt_lmp_system_energy_price
        ,congestion AS rt_lmp_congestion_price
        ,loss AS rt_lmp_marginal_loss_price
    FROM {{ source('gridstatus_v1', 'caiso_lmp_real_time_15_min') }}
    WHERE
        location IN ('TH_NP15_GEN-APND', 'TH_SP15_GEN-APND')
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        ,AVG(CASE WHEN location_name = 'TH_NP15_GEN-APND' THEN rt_lmp_total END) AS rt_lmp_total_np15_hub
        ,AVG(CASE WHEN location_name = 'TH_NP15_GEN-APND' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_np15_hub
        ,AVG(CASE WHEN location_name = 'TH_NP15_GEN-APND' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_np15_hub
        ,AVG(CASE WHEN location_name = 'TH_NP15_GEN-APND' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_np15_hub
        ,AVG(CASE WHEN location_name = 'TH_SP15_GEN-APND' THEN rt_lmp_total END) AS rt_lmp_total_sp15_hub
        ,AVG(CASE WHEN location_name = 'TH_SP15_GEN-APND' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_sp15_hub
        ,AVG(CASE WHEN location_name = 'TH_SP15_GEN-APND' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_sp15_hub
        ,AVG(CASE WHEN location_name = 'TH_SP15_GEN-APND' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_sp15_hub
    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
