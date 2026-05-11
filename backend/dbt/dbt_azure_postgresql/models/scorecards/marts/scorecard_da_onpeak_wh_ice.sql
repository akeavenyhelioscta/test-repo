{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- DA SCORECARD — PJM Western Hub DA HE 8-23 averages vs ICE settle.
-- Spine: every delivery_date ICE has priced via PDA D1-IUS (weekday)
-- or PDO P1-IUS (weekend). Forward-dated rows show up with NULL DA
-- columns until PJM clears.
--
-- DA columns: HE 8-23 averages of total / system-energy / congestion /
-- loss components from pjm_lmps_hourly. Same 16-hour window applied to
-- weekday and weekend deliveries (matches ICE settlement convention).
-- NERC `period` is NOT used — would zero out weekends.
--
-- Symbol routing:
--   PDA D1-IUS — single-day DA next-day strip (start = end).
--   PDO P1-IUS — DA weekend 2x16 strip; unrolled into Sat + Sun rows.
--
-- ice_start_date / ice_end_date = the ICE strip's delivery window.
-- For PDA D1 they equal each other and the delivery_date. For PDO
-- weekends both Sat and Sun unrolled rows share the same ice_start
-- (Sat) and ice_end (Sun), so consumers can group back to strip-level.
--
-- ice_error = da_lmp_total - ice_vwap (positive = market under-priced).
--
-- Trade attribution (our fills, our P&L) lives in the sibling view
-- scorecard_da_trades_wh, which refs this one for DA + ICE context.
----------------------------------------------------

WITH ICE_BY_DELIVERY AS (
    SELECT
        delivery_date::DATE       AS delivery_date
        ,ice.start_date           AS ice_start_date
        ,ice.end_date             AS ice_end_date
        ,ice.symbol               AS product
        ,ice.description          AS ice_description
        ,ice.trade_date_start     AS ice_trade_date_start
        ,ice.trade_date_last      AS ice_trade_date_last
        ,ice.open                 AS ice_open
        ,ice.high                 AS ice_high
        ,ice.low                  AS ice_low
        ,ice.close                AS ice_close
        ,ice.vwap                 AS ice_vwap
        ,ice.volume               AS ice_volume
        ,ice.buy_volume           AS ice_buy_volume
        ,ice.sell_volume          AS ice_sell_volume
    FROM {{ ref('ice_python_ticker_data_by_delivery') }} ice
    CROSS JOIN LATERAL generate_series(
        ice.start_date::timestamp,
        ice.end_date::timestamp,
        INTERVAL '1 day'
    ) AS delivery_date
    WHERE ice.symbol IN ('PDA D1-IUS', 'PDO P1-IUS')
),

-- DA HE 8-23 daily averages for WHUB are pre-aggregated in
-- pjm_lmps_daily (one row per date x hub x period x market). Reading
-- the indexed table is a sub-100ms lookup vs the prior full scan of
-- pjm_lmps_hourly (~3.9M rows).
DA_DAILY AS (
    SELECT
        date AS delivery_date
        ,lmp_total                AS da_lmp_total
        ,lmp_system_energy_price  AS da_lmp_system_energy
        ,lmp_congestion_price     AS da_lmp_congestion
        ,lmp_marginal_loss_price  AS da_lmp_loss
    FROM {{ ref('pjm_lmps_daily') }}
    WHERE hub = 'WESTERN HUB'
      AND market = 'da'
      AND period = 'onpeak'
)

SELECT
    ice.delivery_date

    ,ROUND(da.da_lmp_total::NUMERIC,         2) AS da_lmp_total
    ,ROUND(da.da_lmp_system_energy::NUMERIC, 2) AS da_lmp_system_energy
    ,ROUND(da.da_lmp_congestion::NUMERIC,    2) AS da_lmp_congestion
    ,ROUND(da.da_lmp_loss::NUMERIC,          2) AS da_lmp_loss

    ,ice.ice_start_date
    ,ice.ice_end_date
    ,ice.product
    ,ice.ice_description
    ,ice.ice_trade_date_start
    ,ice.ice_trade_date_last

    ,ROUND(ice.ice_open::NUMERIC,  2) AS ice_open
    ,ROUND(ice.ice_high::NUMERIC,  2) AS ice_high
    ,ROUND(ice.ice_low::NUMERIC,   2) AS ice_low
    ,ROUND(ice.ice_close::NUMERIC, 2) AS ice_close
    ,ROUND(ice.ice_vwap::NUMERIC,  2) AS ice_vwap
    ,ice.ice_volume
    ,ice.ice_buy_volume
    ,ice.ice_sell_volume

    ,ROUND((da.da_lmp_total - ice.ice_vwap)::NUMERIC, 2) AS actual_vs_ice

FROM ICE_BY_DELIVERY ice
LEFT JOIN DA_DAILY da
    ON ice.delivery_date = da.delivery_date
ORDER BY ice.delivery_date DESC
