{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE NEXT-DAY GAS HOURLY MART
-- 10 PJM-relevant hubs, ordered by PJM-linked gas generation capacity.
---------------------------

SELECT
    datetime
    ,date
    ,hour_ending
    ,gas_day
    ,trade_date
    ,tetco_m3_cash
    ,columbia_tco_cash
    ,transco_z6_ny_cash
    ,dominion_south_cash
    ,nng_ventura_cash
    ,tetco_m2_cash
    ,transco_z5_north_cash
    ,tenn_z4_marcellus_cash
    ,transco_leidy_cash
    ,chicago_cg_cash
FROM {{ ref('staging_v1_ice_next_day_gas_hourly') }}
ORDER BY datetime DESC
