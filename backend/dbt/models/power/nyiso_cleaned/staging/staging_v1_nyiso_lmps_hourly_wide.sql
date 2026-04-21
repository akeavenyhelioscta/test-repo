{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- NYISO Hourly LMPs (wide format)
-- Grain: 1 row per date x hour_ending
-- DA, RT, and DART for all 15 zones x 4 price components
-- ===========================

WITH DA AS (
    SELECT * FROM {{ ref('source_v1_nyiso_da_hrl_lmps') }}
),

RT AS (
    SELECT * FROM {{ ref('source_v1_nyiso_rt_hrl_lmps') }}
),

joined AS (
    SELECT
        COALESCE(da.date, rt.date) AS date
        ,COALESCE(da.hour_ending, rt.hour_ending) AS hour_ending

        -- ===========================
        -- DA LMPs
        -- ===========================

        -- CAPITL
        ,da.da_lmp_total_capitl
        ,da.da_lmp_system_energy_price_capitl
        ,da.da_lmp_congestion_price_capitl
        ,da.da_lmp_marginal_loss_price_capitl
        -- CENTRL
        ,da.da_lmp_total_centrl
        ,da.da_lmp_system_energy_price_centrl
        ,da.da_lmp_congestion_price_centrl
        ,da.da_lmp_marginal_loss_price_centrl
        -- DUNWOD
        ,da.da_lmp_total_dunwod
        ,da.da_lmp_system_energy_price_dunwod
        ,da.da_lmp_congestion_price_dunwod
        ,da.da_lmp_marginal_loss_price_dunwod
        -- GENESE
        ,da.da_lmp_total_genese
        ,da.da_lmp_system_energy_price_genese
        ,da.da_lmp_congestion_price_genese
        ,da.da_lmp_marginal_loss_price_genese
        -- H Q
        ,da.da_lmp_total_hq
        ,da.da_lmp_system_energy_price_hq
        ,da.da_lmp_congestion_price_hq
        ,da.da_lmp_marginal_loss_price_hq
        -- HUD VL
        ,da.da_lmp_total_hud_vl
        ,da.da_lmp_system_energy_price_hud_vl
        ,da.da_lmp_congestion_price_hud_vl
        ,da.da_lmp_marginal_loss_price_hud_vl
        -- LONGIL
        ,da.da_lmp_total_longil
        ,da.da_lmp_system_energy_price_longil
        ,da.da_lmp_congestion_price_longil
        ,da.da_lmp_marginal_loss_price_longil
        -- MHK VL
        ,da.da_lmp_total_mhk_vl
        ,da.da_lmp_system_energy_price_mhk_vl
        ,da.da_lmp_congestion_price_mhk_vl
        ,da.da_lmp_marginal_loss_price_mhk_vl
        -- MILLWD
        ,da.da_lmp_total_millwd
        ,da.da_lmp_system_energy_price_millwd
        ,da.da_lmp_congestion_price_millwd
        ,da.da_lmp_marginal_loss_price_millwd
        -- NORTH
        ,da.da_lmp_total_north
        ,da.da_lmp_system_energy_price_north
        ,da.da_lmp_congestion_price_north
        ,da.da_lmp_marginal_loss_price_north
        -- NPX
        ,da.da_lmp_total_npx
        ,da.da_lmp_system_energy_price_npx
        ,da.da_lmp_congestion_price_npx
        ,da.da_lmp_marginal_loss_price_npx
        -- N.Y.C.
        ,da.da_lmp_total_nyc
        ,da.da_lmp_system_energy_price_nyc
        ,da.da_lmp_congestion_price_nyc
        ,da.da_lmp_marginal_loss_price_nyc
        -- O H
        ,da.da_lmp_total_oh
        ,da.da_lmp_system_energy_price_oh
        ,da.da_lmp_congestion_price_oh
        ,da.da_lmp_marginal_loss_price_oh
        -- PJM
        ,da.da_lmp_total_pjm
        ,da.da_lmp_system_energy_price_pjm
        ,da.da_lmp_congestion_price_pjm
        ,da.da_lmp_marginal_loss_price_pjm
        -- WEST
        ,da.da_lmp_total_west
        ,da.da_lmp_system_energy_price_west
        ,da.da_lmp_congestion_price_west
        ,da.da_lmp_marginal_loss_price_west

        -- ===========================
        -- RT LMPs
        -- ===========================

        -- CAPITL
        ,rt.rt_lmp_total_capitl
        ,rt.rt_lmp_system_energy_price_capitl
        ,rt.rt_lmp_congestion_price_capitl
        ,rt.rt_lmp_marginal_loss_price_capitl
        -- CENTRL
        ,rt.rt_lmp_total_centrl
        ,rt.rt_lmp_system_energy_price_centrl
        ,rt.rt_lmp_congestion_price_centrl
        ,rt.rt_lmp_marginal_loss_price_centrl
        -- DUNWOD
        ,rt.rt_lmp_total_dunwod
        ,rt.rt_lmp_system_energy_price_dunwod
        ,rt.rt_lmp_congestion_price_dunwod
        ,rt.rt_lmp_marginal_loss_price_dunwod
        -- GENESE
        ,rt.rt_lmp_total_genese
        ,rt.rt_lmp_system_energy_price_genese
        ,rt.rt_lmp_congestion_price_genese
        ,rt.rt_lmp_marginal_loss_price_genese
        -- H Q
        ,rt.rt_lmp_total_hq
        ,rt.rt_lmp_system_energy_price_hq
        ,rt.rt_lmp_congestion_price_hq
        ,rt.rt_lmp_marginal_loss_price_hq
        -- HUD VL
        ,rt.rt_lmp_total_hud_vl
        ,rt.rt_lmp_system_energy_price_hud_vl
        ,rt.rt_lmp_congestion_price_hud_vl
        ,rt.rt_lmp_marginal_loss_price_hud_vl
        -- LONGIL
        ,rt.rt_lmp_total_longil
        ,rt.rt_lmp_system_energy_price_longil
        ,rt.rt_lmp_congestion_price_longil
        ,rt.rt_lmp_marginal_loss_price_longil
        -- MHK VL
        ,rt.rt_lmp_total_mhk_vl
        ,rt.rt_lmp_system_energy_price_mhk_vl
        ,rt.rt_lmp_congestion_price_mhk_vl
        ,rt.rt_lmp_marginal_loss_price_mhk_vl
        -- MILLWD
        ,rt.rt_lmp_total_millwd
        ,rt.rt_lmp_system_energy_price_millwd
        ,rt.rt_lmp_congestion_price_millwd
        ,rt.rt_lmp_marginal_loss_price_millwd
        -- NORTH
        ,rt.rt_lmp_total_north
        ,rt.rt_lmp_system_energy_price_north
        ,rt.rt_lmp_congestion_price_north
        ,rt.rt_lmp_marginal_loss_price_north
        -- NPX
        ,rt.rt_lmp_total_npx
        ,rt.rt_lmp_system_energy_price_npx
        ,rt.rt_lmp_congestion_price_npx
        ,rt.rt_lmp_marginal_loss_price_npx
        -- N.Y.C.
        ,rt.rt_lmp_total_nyc
        ,rt.rt_lmp_system_energy_price_nyc
        ,rt.rt_lmp_congestion_price_nyc
        ,rt.rt_lmp_marginal_loss_price_nyc
        -- O H
        ,rt.rt_lmp_total_oh
        ,rt.rt_lmp_system_energy_price_oh
        ,rt.rt_lmp_congestion_price_oh
        ,rt.rt_lmp_marginal_loss_price_oh
        -- PJM
        ,rt.rt_lmp_total_pjm
        ,rt.rt_lmp_system_energy_price_pjm
        ,rt.rt_lmp_congestion_price_pjm
        ,rt.rt_lmp_marginal_loss_price_pjm
        -- WEST
        ,rt.rt_lmp_total_west
        ,rt.rt_lmp_system_energy_price_west
        ,rt.rt_lmp_congestion_price_west
        ,rt.rt_lmp_marginal_loss_price_west

        -- ===========================
        -- DART LMPs (DA - RT)
        -- ===========================

        -- CAPITL
        ,(da.da_lmp_total_capitl - rt.rt_lmp_total_capitl) AS dart_lmp_total_capitl
        ,(da.da_lmp_system_energy_price_capitl - rt.rt_lmp_system_energy_price_capitl) AS dart_lmp_system_energy_price_capitl
        ,(da.da_lmp_congestion_price_capitl - rt.rt_lmp_congestion_price_capitl) AS dart_lmp_congestion_price_capitl
        ,(da.da_lmp_marginal_loss_price_capitl - rt.rt_lmp_marginal_loss_price_capitl) AS dart_lmp_marginal_loss_price_capitl
        -- CENTRL
        ,(da.da_lmp_total_centrl - rt.rt_lmp_total_centrl) AS dart_lmp_total_centrl
        ,(da.da_lmp_system_energy_price_centrl - rt.rt_lmp_system_energy_price_centrl) AS dart_lmp_system_energy_price_centrl
        ,(da.da_lmp_congestion_price_centrl - rt.rt_lmp_congestion_price_centrl) AS dart_lmp_congestion_price_centrl
        ,(da.da_lmp_marginal_loss_price_centrl - rt.rt_lmp_marginal_loss_price_centrl) AS dart_lmp_marginal_loss_price_centrl
        -- DUNWOD
        ,(da.da_lmp_total_dunwod - rt.rt_lmp_total_dunwod) AS dart_lmp_total_dunwod
        ,(da.da_lmp_system_energy_price_dunwod - rt.rt_lmp_system_energy_price_dunwod) AS dart_lmp_system_energy_price_dunwod
        ,(da.da_lmp_congestion_price_dunwod - rt.rt_lmp_congestion_price_dunwod) AS dart_lmp_congestion_price_dunwod
        ,(da.da_lmp_marginal_loss_price_dunwod - rt.rt_lmp_marginal_loss_price_dunwod) AS dart_lmp_marginal_loss_price_dunwod
        -- GENESE
        ,(da.da_lmp_total_genese - rt.rt_lmp_total_genese) AS dart_lmp_total_genese
        ,(da.da_lmp_system_energy_price_genese - rt.rt_lmp_system_energy_price_genese) AS dart_lmp_system_energy_price_genese
        ,(da.da_lmp_congestion_price_genese - rt.rt_lmp_congestion_price_genese) AS dart_lmp_congestion_price_genese
        ,(da.da_lmp_marginal_loss_price_genese - rt.rt_lmp_marginal_loss_price_genese) AS dart_lmp_marginal_loss_price_genese
        -- H Q
        ,(da.da_lmp_total_hq - rt.rt_lmp_total_hq) AS dart_lmp_total_hq
        ,(da.da_lmp_system_energy_price_hq - rt.rt_lmp_system_energy_price_hq) AS dart_lmp_system_energy_price_hq
        ,(da.da_lmp_congestion_price_hq - rt.rt_lmp_congestion_price_hq) AS dart_lmp_congestion_price_hq
        ,(da.da_lmp_marginal_loss_price_hq - rt.rt_lmp_marginal_loss_price_hq) AS dart_lmp_marginal_loss_price_hq
        -- HUD VL
        ,(da.da_lmp_total_hud_vl - rt.rt_lmp_total_hud_vl) AS dart_lmp_total_hud_vl
        ,(da.da_lmp_system_energy_price_hud_vl - rt.rt_lmp_system_energy_price_hud_vl) AS dart_lmp_system_energy_price_hud_vl
        ,(da.da_lmp_congestion_price_hud_vl - rt.rt_lmp_congestion_price_hud_vl) AS dart_lmp_congestion_price_hud_vl
        ,(da.da_lmp_marginal_loss_price_hud_vl - rt.rt_lmp_marginal_loss_price_hud_vl) AS dart_lmp_marginal_loss_price_hud_vl
        -- LONGIL
        ,(da.da_lmp_total_longil - rt.rt_lmp_total_longil) AS dart_lmp_total_longil
        ,(da.da_lmp_system_energy_price_longil - rt.rt_lmp_system_energy_price_longil) AS dart_lmp_system_energy_price_longil
        ,(da.da_lmp_congestion_price_longil - rt.rt_lmp_congestion_price_longil) AS dart_lmp_congestion_price_longil
        ,(da.da_lmp_marginal_loss_price_longil - rt.rt_lmp_marginal_loss_price_longil) AS dart_lmp_marginal_loss_price_longil
        -- MHK VL
        ,(da.da_lmp_total_mhk_vl - rt.rt_lmp_total_mhk_vl) AS dart_lmp_total_mhk_vl
        ,(da.da_lmp_system_energy_price_mhk_vl - rt.rt_lmp_system_energy_price_mhk_vl) AS dart_lmp_system_energy_price_mhk_vl
        ,(da.da_lmp_congestion_price_mhk_vl - rt.rt_lmp_congestion_price_mhk_vl) AS dart_lmp_congestion_price_mhk_vl
        ,(da.da_lmp_marginal_loss_price_mhk_vl - rt.rt_lmp_marginal_loss_price_mhk_vl) AS dart_lmp_marginal_loss_price_mhk_vl
        -- MILLWD
        ,(da.da_lmp_total_millwd - rt.rt_lmp_total_millwd) AS dart_lmp_total_millwd
        ,(da.da_lmp_system_energy_price_millwd - rt.rt_lmp_system_energy_price_millwd) AS dart_lmp_system_energy_price_millwd
        ,(da.da_lmp_congestion_price_millwd - rt.rt_lmp_congestion_price_millwd) AS dart_lmp_congestion_price_millwd
        ,(da.da_lmp_marginal_loss_price_millwd - rt.rt_lmp_marginal_loss_price_millwd) AS dart_lmp_marginal_loss_price_millwd
        -- NORTH
        ,(da.da_lmp_total_north - rt.rt_lmp_total_north) AS dart_lmp_total_north
        ,(da.da_lmp_system_energy_price_north - rt.rt_lmp_system_energy_price_north) AS dart_lmp_system_energy_price_north
        ,(da.da_lmp_congestion_price_north - rt.rt_lmp_congestion_price_north) AS dart_lmp_congestion_price_north
        ,(da.da_lmp_marginal_loss_price_north - rt.rt_lmp_marginal_loss_price_north) AS dart_lmp_marginal_loss_price_north
        -- NPX
        ,(da.da_lmp_total_npx - rt.rt_lmp_total_npx) AS dart_lmp_total_npx
        ,(da.da_lmp_system_energy_price_npx - rt.rt_lmp_system_energy_price_npx) AS dart_lmp_system_energy_price_npx
        ,(da.da_lmp_congestion_price_npx - rt.rt_lmp_congestion_price_npx) AS dart_lmp_congestion_price_npx
        ,(da.da_lmp_marginal_loss_price_npx - rt.rt_lmp_marginal_loss_price_npx) AS dart_lmp_marginal_loss_price_npx
        -- N.Y.C.
        ,(da.da_lmp_total_nyc - rt.rt_lmp_total_nyc) AS dart_lmp_total_nyc
        ,(da.da_lmp_system_energy_price_nyc - rt.rt_lmp_system_energy_price_nyc) AS dart_lmp_system_energy_price_nyc
        ,(da.da_lmp_congestion_price_nyc - rt.rt_lmp_congestion_price_nyc) AS dart_lmp_congestion_price_nyc
        ,(da.da_lmp_marginal_loss_price_nyc - rt.rt_lmp_marginal_loss_price_nyc) AS dart_lmp_marginal_loss_price_nyc
        -- O H
        ,(da.da_lmp_total_oh - rt.rt_lmp_total_oh) AS dart_lmp_total_oh
        ,(da.da_lmp_system_energy_price_oh - rt.rt_lmp_system_energy_price_oh) AS dart_lmp_system_energy_price_oh
        ,(da.da_lmp_congestion_price_oh - rt.rt_lmp_congestion_price_oh) AS dart_lmp_congestion_price_oh
        ,(da.da_lmp_marginal_loss_price_oh - rt.rt_lmp_marginal_loss_price_oh) AS dart_lmp_marginal_loss_price_oh
        -- PJM
        ,(da.da_lmp_total_pjm - rt.rt_lmp_total_pjm) AS dart_lmp_total_pjm
        ,(da.da_lmp_system_energy_price_pjm - rt.rt_lmp_system_energy_price_pjm) AS dart_lmp_system_energy_price_pjm
        ,(da.da_lmp_congestion_price_pjm - rt.rt_lmp_congestion_price_pjm) AS dart_lmp_congestion_price_pjm
        ,(da.da_lmp_marginal_loss_price_pjm - rt.rt_lmp_marginal_loss_price_pjm) AS dart_lmp_marginal_loss_price_pjm
        -- WEST
        ,(da.da_lmp_total_west - rt.rt_lmp_total_west) AS dart_lmp_total_west
        ,(da.da_lmp_system_energy_price_west - rt.rt_lmp_system_energy_price_west) AS dart_lmp_system_energy_price_west
        ,(da.da_lmp_congestion_price_west - rt.rt_lmp_congestion_price_west) AS dart_lmp_congestion_price_west
        ,(da.da_lmp_marginal_loss_price_west - rt.rt_lmp_marginal_loss_price_west) AS dart_lmp_marginal_loss_price_west

    FROM DA
    FULL OUTER JOIN RT ON da.date = rt.date AND da.hour_ending = rt.hour_ending
)

SELECT
    date + (hour_ending || ' hours')::interval AS datetime
    ,*
FROM joined
