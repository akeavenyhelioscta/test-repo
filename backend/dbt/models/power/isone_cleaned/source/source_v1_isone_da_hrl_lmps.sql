{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- ISO-NE DA Hourly LMPs
-- ===========================

WITH raw_data AS (
    SELECT
        date::DATE AS date
        ,hour_ending::INT AS hour_ending
        ,locational_marginal_price::NUMERIC AS da_lmp_total_internal_hub
        ,energy_component::NUMERIC AS da_lmp_system_energy_price_internal_hub
        ,congestion_component::NUMERIC AS da_lmp_congestion_price_internal_hub
        ,marginal_loss_component::NUMERIC AS da_lmp_marginal_loss_price_internal_hub
    FROM {{ source('isone_v1', 'da_hrl_lmps') }}
    WHERE location_name = '.H.INTERNAL_HUB'
)

SELECT * FROM raw_data
