{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- NYISO RT Hourly LMPs
-- (5-min averaged to hourly)
-- ===========================

WITH raw_data AS (
    SELECT
        interval_start_local::DATE AS date
        ,(EXTRACT(HOUR FROM interval_start_local::TIMESTAMP) + 1)::INT AS hour_ending
        ,location AS location_name
        ,lmp AS rt_lmp_total
        ,energy AS rt_lmp_system_energy_price
        ,congestion AS rt_lmp_congestion_price
        ,loss AS rt_lmp_marginal_loss_price
    FROM {{ source('gridstatus_v1', 'nyiso_lmp_real_time_5_min') }}
    WHERE
        location IN (
            'CAPITL', 'CENTRL', 'DUNWOD', 'GENESE', 'H Q',
            'HUD VL', 'LONGIL', 'MHK VL', 'MILLWD', 'NORTH',
            'NPX', 'N.Y.C.', 'O H', 'PJM', 'WEST'
        )
),

pivoted AS (
    SELECT
        date
        ,hour_ending
        -- CAPITL
        ,AVG(CASE WHEN location_name = 'CAPITL' THEN rt_lmp_total END) AS rt_lmp_total_capitl
        ,AVG(CASE WHEN location_name = 'CAPITL' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_capitl
        ,AVG(CASE WHEN location_name = 'CAPITL' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_capitl
        ,AVG(CASE WHEN location_name = 'CAPITL' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_capitl
        -- CENTRL
        ,AVG(CASE WHEN location_name = 'CENTRL' THEN rt_lmp_total END) AS rt_lmp_total_centrl
        ,AVG(CASE WHEN location_name = 'CENTRL' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_centrl
        ,AVG(CASE WHEN location_name = 'CENTRL' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_centrl
        ,AVG(CASE WHEN location_name = 'CENTRL' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_centrl
        -- DUNWOD
        ,AVG(CASE WHEN location_name = 'DUNWOD' THEN rt_lmp_total END) AS rt_lmp_total_dunwod
        ,AVG(CASE WHEN location_name = 'DUNWOD' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_dunwod
        ,AVG(CASE WHEN location_name = 'DUNWOD' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_dunwod
        ,AVG(CASE WHEN location_name = 'DUNWOD' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_dunwod
        -- GENESE
        ,AVG(CASE WHEN location_name = 'GENESE' THEN rt_lmp_total END) AS rt_lmp_total_genese
        ,AVG(CASE WHEN location_name = 'GENESE' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_genese
        ,AVG(CASE WHEN location_name = 'GENESE' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_genese
        ,AVG(CASE WHEN location_name = 'GENESE' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_genese
        -- H Q
        ,AVG(CASE WHEN location_name = 'H Q' THEN rt_lmp_total END) AS rt_lmp_total_hq
        ,AVG(CASE WHEN location_name = 'H Q' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_hq
        ,AVG(CASE WHEN location_name = 'H Q' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_hq
        ,AVG(CASE WHEN location_name = 'H Q' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_hq
        -- HUD VL
        ,AVG(CASE WHEN location_name = 'HUD VL' THEN rt_lmp_total END) AS rt_lmp_total_hud_vl
        ,AVG(CASE WHEN location_name = 'HUD VL' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_hud_vl
        ,AVG(CASE WHEN location_name = 'HUD VL' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_hud_vl
        ,AVG(CASE WHEN location_name = 'HUD VL' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_hud_vl
        -- LONGIL
        ,AVG(CASE WHEN location_name = 'LONGIL' THEN rt_lmp_total END) AS rt_lmp_total_longil
        ,AVG(CASE WHEN location_name = 'LONGIL' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_longil
        ,AVG(CASE WHEN location_name = 'LONGIL' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_longil
        ,AVG(CASE WHEN location_name = 'LONGIL' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_longil
        -- MHK VL
        ,AVG(CASE WHEN location_name = 'MHK VL' THEN rt_lmp_total END) AS rt_lmp_total_mhk_vl
        ,AVG(CASE WHEN location_name = 'MHK VL' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_mhk_vl
        ,AVG(CASE WHEN location_name = 'MHK VL' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_mhk_vl
        ,AVG(CASE WHEN location_name = 'MHK VL' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_mhk_vl
        -- MILLWD
        ,AVG(CASE WHEN location_name = 'MILLWD' THEN rt_lmp_total END) AS rt_lmp_total_millwd
        ,AVG(CASE WHEN location_name = 'MILLWD' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_millwd
        ,AVG(CASE WHEN location_name = 'MILLWD' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_millwd
        ,AVG(CASE WHEN location_name = 'MILLWD' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_millwd
        -- NORTH
        ,AVG(CASE WHEN location_name = 'NORTH' THEN rt_lmp_total END) AS rt_lmp_total_north
        ,AVG(CASE WHEN location_name = 'NORTH' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_north
        ,AVG(CASE WHEN location_name = 'NORTH' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_north
        ,AVG(CASE WHEN location_name = 'NORTH' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_north
        -- NPX
        ,AVG(CASE WHEN location_name = 'NPX' THEN rt_lmp_total END) AS rt_lmp_total_npx
        ,AVG(CASE WHEN location_name = 'NPX' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_npx
        ,AVG(CASE WHEN location_name = 'NPX' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_npx
        ,AVG(CASE WHEN location_name = 'NPX' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_npx
        -- N.Y.C.
        ,AVG(CASE WHEN location_name = 'N.Y.C.' THEN rt_lmp_total END) AS rt_lmp_total_nyc
        ,AVG(CASE WHEN location_name = 'N.Y.C.' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_nyc
        ,AVG(CASE WHEN location_name = 'N.Y.C.' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_nyc
        ,AVG(CASE WHEN location_name = 'N.Y.C.' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_nyc
        -- O H
        ,AVG(CASE WHEN location_name = 'O H' THEN rt_lmp_total END) AS rt_lmp_total_oh
        ,AVG(CASE WHEN location_name = 'O H' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_oh
        ,AVG(CASE WHEN location_name = 'O H' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_oh
        ,AVG(CASE WHEN location_name = 'O H' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_oh
        -- PJM
        ,AVG(CASE WHEN location_name = 'PJM' THEN rt_lmp_total END) AS rt_lmp_total_pjm
        ,AVG(CASE WHEN location_name = 'PJM' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_pjm
        ,AVG(CASE WHEN location_name = 'PJM' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_pjm
        ,AVG(CASE WHEN location_name = 'PJM' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_pjm
        -- WEST
        ,AVG(CASE WHEN location_name = 'WEST' THEN rt_lmp_total END) AS rt_lmp_total_west
        ,AVG(CASE WHEN location_name = 'WEST' THEN rt_lmp_system_energy_price END) AS rt_lmp_system_energy_price_west
        ,AVG(CASE WHEN location_name = 'WEST' THEN rt_lmp_congestion_price END) AS rt_lmp_congestion_price_west
        ,AVG(CASE WHEN location_name = 'WEST' THEN rt_lmp_marginal_loss_price END) AS rt_lmp_marginal_loss_price_west
    FROM raw_data
    GROUP BY date, hour_ending
)

SELECT * FROM pivoted
