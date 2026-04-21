{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- CAISO LMPs Hourly
-- ===========================

WITH hourly AS (
    SELECT
        (da.date + (da.hour_ending || ' hours')::INTERVAL)::TIMESTAMP AS datetime
        ,da.date
        ,da.hour_ending::INT AS hour_ending

        -- DA
        ,da_lmp_total_np15_hub
        ,da_lmp_system_energy_price_np15_hub
        ,da_lmp_congestion_price_np15_hub
        ,da_lmp_marginal_loss_price_np15_hub
        ,da_lmp_total_sp15_hub
        ,da_lmp_system_energy_price_sp15_hub
        ,da_lmp_congestion_price_sp15_hub
        ,da_lmp_marginal_loss_price_sp15_hub

        -- RT
        ,rt_lmp_total_np15_hub
        ,rt_lmp_system_energy_price_np15_hub
        ,rt_lmp_congestion_price_np15_hub
        ,rt_lmp_marginal_loss_price_np15_hub
        ,rt_lmp_total_sp15_hub
        ,rt_lmp_system_energy_price_sp15_hub
        ,rt_lmp_congestion_price_sp15_hub
        ,rt_lmp_marginal_loss_price_sp15_hub

        -- DART (DA - RT)
        ,(da_lmp_total_np15_hub - rt_lmp_total_np15_hub) AS dart_lmp_total_np15_hub
        ,(da_lmp_system_energy_price_np15_hub - rt_lmp_system_energy_price_np15_hub) AS dart_lmp_system_energy_price_np15_hub
        ,(da_lmp_congestion_price_np15_hub - rt_lmp_congestion_price_np15_hub) AS dart_lmp_congestion_price_np15_hub
        ,(da_lmp_marginal_loss_price_np15_hub - rt_lmp_marginal_loss_price_np15_hub) AS dart_lmp_marginal_loss_price_np15_hub
        ,(da_lmp_total_sp15_hub - rt_lmp_total_sp15_hub) AS dart_lmp_total_sp15_hub
        ,(da_lmp_system_energy_price_sp15_hub - rt_lmp_system_energy_price_sp15_hub) AS dart_lmp_system_energy_price_sp15_hub
        ,(da_lmp_congestion_price_sp15_hub - rt_lmp_congestion_price_sp15_hub) AS dart_lmp_congestion_price_sp15_hub
        ,(da_lmp_marginal_loss_price_sp15_hub - rt_lmp_marginal_loss_price_sp15_hub) AS dart_lmp_marginal_loss_price_sp15_hub

    FROM {{ ref('source_v1_caiso_da_hrl_lmps') }} da
    LEFT JOIN {{ ref('source_v1_caiso_rt_hrl_lmps') }} rt
        ON da.date = rt.date
        AND da.hour_ending = rt.hour_ending
)

SELECT * FROM hourly
