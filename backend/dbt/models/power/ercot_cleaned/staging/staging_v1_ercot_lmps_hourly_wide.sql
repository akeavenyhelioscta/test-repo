{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT LMPs Hourly
-- Grain: 1 row per date x hour_ending
-- Joins DA and RT, computes DART for 4 hubs (lmp_total only)
---------------------------

WITH hourly AS (
    SELECT
        (da.date + (da.hour_ending || ' hours')::INTERVAL)::TIMESTAMP AS datetime
        ,da.date
        ,da.hour_ending::INT AS hour_ending

        -- DA
        ,da_lmp_total_houston_hub
        ,da_lmp_total_north_hub
        ,da_lmp_total_south_hub
        ,da_lmp_total_west_hub

        -- RT
        ,rt_lmp_total_houston_hub
        ,rt_lmp_total_north_hub
        ,rt_lmp_total_south_hub
        ,rt_lmp_total_west_hub

        -- DART (DA - RT)
        ,(da_lmp_total_houston_hub - rt_lmp_total_houston_hub) AS dart_lmp_total_houston_hub
        ,(da_lmp_total_north_hub - rt_lmp_total_north_hub) AS dart_lmp_total_north_hub
        ,(da_lmp_total_south_hub - rt_lmp_total_south_hub) AS dart_lmp_total_south_hub
        ,(da_lmp_total_west_hub - rt_lmp_total_west_hub) AS dart_lmp_total_west_hub

    FROM {{ ref('source_v1_ercot_da_hrl_lmps') }} da
    LEFT JOIN {{ ref('source_v1_ercot_spp_real_time_15_min') }} rt
        ON da.date = rt.date
        AND da.hour_ending = rt.hour_ending
)

SELECT * FROM hourly
