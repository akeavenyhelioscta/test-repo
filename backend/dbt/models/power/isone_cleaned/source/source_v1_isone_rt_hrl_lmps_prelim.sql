{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- ISO-NE RT Hourly LMPs (Preliminary)
-- ===========================

WITH raw_data AS (
    SELECT
        date::DATE AS date
        ,hour_ending::INT AS hour_ending
        ,lmp::NUMERIC AS rt_lmp_total_internal_hub
        ,energy::NUMERIC AS rt_lmp_system_energy_price_internal_hub
        ,congestion::NUMERIC AS rt_lmp_congestion_price_internal_hub
        ,loss::NUMERIC AS rt_lmp_marginal_loss_price_internal_hub
    FROM {{ source('isone_v1', 'rt_hrl_lmps_prelim') }}
    WHERE location = '.H.INTERNAL_HUB'
        AND date >= (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE - 10
)

SELECT * FROM raw_data
