{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- ISO-NE RT Hourly LMPs (Final + Prelim)
-- Final takes precedence over prelim
-- ===========================

WITH final_rt AS (
    SELECT * FROM {{ ref('source_v1_isone_rt_hrl_lmps_final') }}
),

prelim_rt AS (
    SELECT * FROM {{ ref('source_v1_isone_rt_hrl_lmps_prelim') }}
),

combined AS (
    SELECT * FROM final_rt
    UNION ALL
    SELECT
        date
        ,hour_ending
        ,rt_lmp_total_internal_hub
        ,rt_lmp_system_energy_price_internal_hub
        ,rt_lmp_congestion_price_internal_hub
        ,rt_lmp_marginal_loss_price_internal_hub
    FROM prelim_rt
    WHERE (date, hour_ending) NOT IN (SELECT date, hour_ending FROM final_rt)
)

SELECT * FROM combined
