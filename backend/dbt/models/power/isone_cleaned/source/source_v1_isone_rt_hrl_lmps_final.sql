{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- ISO-NE RT Hourly LMPs (Final)
-- ===========================

WITH raw_data AS (
    SELECT
        date::DATE AS date
        ,hour_ending::INT AS hour_ending
        ,locational_marginal_price::NUMERIC AS rt_lmp_total_internal_hub
        ,energy_component::NUMERIC AS rt_lmp_system_energy_price_internal_hub
        ,congestion_component::NUMERIC AS rt_lmp_congestion_price_internal_hub
        ,marginal_loss_component::NUMERIC AS rt_lmp_marginal_loss_price_internal_hub
    FROM {{ source('isone_v1', 'rt_hrl_lmps_final') }}
    WHERE location_name = '.H.INTERNAL_HUB'
)

SELECT * FROM raw_data
