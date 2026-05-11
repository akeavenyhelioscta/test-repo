{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- ICE TICKER DATA — by delivery period
-- Grain: 1 row per symbol × start_date × end_date.
-- Collapses every trade_date that priced a given delivery period into
-- a single row spanning the product's full trading lifetime.
--
-- Examples of expansion vs ice_python_ticker_data_eod:
--   PDA D1-IUS  : 1 EOD row → 1 by-delivery row    (single-session product)
--   PDO P1-IUS  : 5 EOD rows → 1 by-delivery row   (Mon-Fri prices same weekend)
--   PDP W1-IUS  : N EOD rows → 1 by-delivery row   (multi-week price evolution
--                                                    collapsed to product-life)
--
-- Column semantics (vs EOD which is per-session):
--   open  = first tick price across the product's trading life
--   close = last tick price (final settle)
--   high  / low = range across all sessions
--   volume / vwap / counts = aggregated across all ticks
--   trade_date_start / trade_date_last = window the product traded
--
-- Use this mart when you want one row per ICE delivery product, not
-- one row per session. Downstream marts that need per-day delivery
-- granularity (scorecard splitting weekend strips into Sat + Sun) do
-- their own unrolling.
--
-- Rows with NULL start_date or end_date are dropped (no delivery
-- period defined).
----------------------------------------------------

WITH ticks AS (
    SELECT *
    FROM {{ ref('staging_v1_ticker_data') }}
    WHERE price IS NOT NULL
      AND quantity IS NOT NULL
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
),

opens AS (
    SELECT DISTINCT ON (symbol, start_date, end_date)
        symbol
        ,start_date
        ,end_date
        ,price AS open_price
    FROM ticks
    ORDER BY symbol, start_date, end_date, exec_time_local ASC
),

closes AS (
    SELECT DISTINCT ON (symbol, start_date, end_date)
        symbol
        ,start_date
        ,end_date
        ,price AS close_price
    FROM ticks
    ORDER BY symbol, start_date, end_date, exec_time_local DESC
),

-- Second-to-last tick price across the product's full trading life.
-- NULL when only one tick exists for the strip. Useful for spotting
-- when `close` is an outlier vs the prior trade.
lasts_before_close AS (
    SELECT symbol, start_date, end_date, price AS last_before_close_price
    FROM (
        SELECT
            symbol
            ,start_date
            ,end_date
            ,price
            ,ROW_NUMBER() OVER (
                PARTITION BY symbol, start_date, end_date
                ORDER BY exec_time_local DESC
            ) AS rn
        FROM ticks
    ) ranked
    WHERE rn = 2
),

aggs AS (
    SELECT
        symbol
        ,start_date
        ,end_date

        ,MIN(trade_date)     AS trade_date_start
        ,MAX(trade_date)     AS trade_date_last

        ,MAX(description)    AS description
        ,MAX(product_type)   AS product_type
        ,MAX(contract_type)  AS contract_type
        ,MAX(strip)          AS strip

        ,MAX(price)          AS high
        ,MIN(price)          AS low

        ,SUM(quantity)                                       AS volume
        ,SUM(price * quantity) / NULLIF(SUM(quantity), 0)    AS vwap

        ,COUNT(*)                                              AS trade_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Lift')  AS lift_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Hit')   AS hit_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Leg')   AS leg_count
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Lift')  AS buy_volume
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Hit')   AS sell_volume
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Leg')   AS leg_volume

        ,COUNT(*)      FILTER (WHERE conditions ILIKE '%block%')  AS block_trade_count
        ,SUM(quantity) FILTER (WHERE conditions ILIKE '%block%')  AS block_volume
    FROM ticks
    GROUP BY symbol, start_date, end_date
)

SELECT
    a.start_date
    ,a.end_date
    ,a.trade_date_start
    ,a.trade_date_last

    ,a.symbol
    ,a.description
    ,a.product_type
    ,a.contract_type
    ,a.strip

    -- ────── OHLC ──────
    ,o.open_price  AS open
    ,a.high
    ,a.low
    ,c.close_price AS close
    ,lbc.last_before_close_price AS last_before_close

    -- ────── Volume & VWAP ──────
    ,a.volume
    ,a.vwap

    -- ────── Trade flow ──────
    ,a.trade_count
    ,a.lift_count
    ,a.hit_count
    ,a.leg_count
    ,a.buy_volume
    ,a.sell_volume
    ,a.leg_volume

    -- ────── Block trades ──────
    ,a.block_trade_count
    ,a.block_volume

FROM aggs a
LEFT JOIN opens              o   ON a.symbol = o.symbol   AND a.start_date = o.start_date   AND a.end_date = o.end_date
LEFT JOIN closes             c   ON a.symbol = c.symbol   AND a.start_date = c.start_date   AND a.end_date = c.end_date
LEFT JOIN lasts_before_close lbc ON a.symbol = lbc.symbol AND a.start_date = lbc.start_date AND a.end_date = lbc.end_date
ORDER BY a.start_date DESC, a.end_date DESC, a.symbol
