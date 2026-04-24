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
    -- Delivery-axis timestamp/date so downstream feature code that
    -- groups by `date` (or a `datetime` bucket) aggregates per gas_day, not
    -- per trade_date. `hour_ending` is the source's trade-hour and stays
    -- passthrough; it is retained for traceability of which snapshot within
    -- the D-1 HE10 through D HE9 trading window this row came from.
    (gas_day::TIMESTAMP + (hour_ending || ' hours')::INTERVAL) AS datetime
    ,gas_day AS date
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
-- Delivery-day first, then most-recent trade-time within that day. Ordering
-- by `datetime` alone splits each gas_day into two blocks because HE10-24
-- come from trade_date = D - 1 and HE1-9 come from trade_date = D.
ORDER BY gas_day DESC, trade_date DESC, hour_ending DESC
