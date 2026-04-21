{{
  config(
    materialized='view'
  )
}}

-- ICE TRADE BLOTTER — SHORT-TERM POWER PnL (PDP/PDA)
--
-- Two mark sources:
--   1. Settled mark: recent_settlement from delivery date snapshot (final PnL)
--   2. Live mark: last from trade date snapshot (intraday unrealized PnL)
-- Uses COALESCE(settled, live) so settled trades show final PnL,
-- open trades show live PnL.

WITH TRADES AS (

    SELECT * FROM {{ ref('staging_v1_ice_trade_blotter_short_term_pnl') }}
    WHERE ice_product_code IS NOT NULL

),

-------------------------------------------------------------
-- SETTLED MARK: recent_settlement from settlement_date
-- Available after the product settles (delivery date)
-------------------------------------------------------------

SETTLED_RANKED AS (

    SELECT
         trade_date       AS mark_date
        ,symbol
        ,start_date
        ,end_date
        ,recent_settlement
        ,ROW_NUMBER() OVER (
            PARTITION BY trade_date, symbol
            ORDER BY snapshot_at DESC
        ) AS rn
    FROM {{ source('ice_python_market_data', 'ice_blotter_settles_v1_2026_apr_02') }}
    WHERE trade_date >= CURRENT_DATE - INTERVAL '14 days'

),

SETTLED_MARKS AS (
    SELECT * FROM SETTLED_RANKED WHERE rn = 1
),

-------------------------------------------------------------
-- LIVE MARK: last from trade_date (intraday price)
-- Available same-day for open trades not yet settled
-------------------------------------------------------------

LIVE_RANKED AS (

    SELECT
         trade_date       AS mark_date
        ,symbol
        ,last
        ,ROW_NUMBER() OVER (
            PARTITION BY trade_date, symbol
            ORDER BY snapshot_at DESC
        ) AS rn
    FROM {{ source('ice_python_market_data', 'ice_blotter_settles_v1_2026_apr_02') }}
    WHERE trade_date >= CURRENT_DATE - INTERVAL '14 days'

),

LIVE_MARKS AS (
    SELECT * FROM LIVE_RANKED WHERE rn = 1
),

-------------------------------------------------------------
-- JOIN TRADES TO BOTH MARKS
-------------------------------------------------------------

TRADE_WITH_MARKS AS (

    SELECT

        -- Trade identity
         t.deal_type
        ,t.deal_id
        ,t.orig_id

        -- Trade
        ,t.trade_date_parsed AS trade_date
        ,t.trade_time
        ,t.trader

        -- Product
        ,t.cc AS product_code
        ,t.ice_product_code
        ,t.product
        ,t.hub
        ,t.contract AS strip
        ,t.delivery_date
        ,t.settlement_date

        -- Contract dates from ICE
        ,sm.start_date
        ,sm.end_date

        -- Trade economics (signed: positive = long, negative = short)
        ,t.b_s
        ,t.lots * t.direction           AS lots
        ,t.total_quantity * t.direction  AS total_quantity
        ,t.price AS trade_price

        -- Settlement status (strict < so same-day products use live mark until EOD)
        ,(t.delivery_date < CURRENT_DATE) AS is_settled

        -- Marks
        ,sm.recent_settlement   AS settled_mark
        ,lm.last                AS live_mark

        -- Mark price: use settled only when truly settled, otherwise live
        ,(CASE
            WHEN t.delivery_date < CURRENT_DATE AND sm.recent_settlement IS NOT NULL
                THEN sm.recent_settlement
            ELSE lm.last
        END) AS mark_price

        -- PnL
        ,(CASE
            WHEN t.delivery_date < CURRENT_DATE AND sm.recent_settlement IS NOT NULL
                THEN ROUND(((sm.recent_settlement - t.price) * t.total_quantity * t.direction)::NUMERIC, 2)
            WHEN lm.last IS NOT NULL
                THEN ROUND(((lm.last - t.price) * t.total_quantity * t.direction)::NUMERIC, 2)
            ELSE NULL
        END) AS pnl

    FROM TRADES t

    -- Settled mark: join on settlement_date (= delivery_date for Next Day)
    LEFT JOIN SETTLED_MARKS sm
        ON t.ice_product_code = sm.symbol
        AND t.settlement_date = sm.mark_date

    -- Live mark: join on trade_date (today's last price for open trades)
    LEFT JOIN LIVE_MARKS lm
        ON t.ice_product_code = lm.symbol
        AND t.trade_date_parsed = lm.mark_date

)

SELECT * FROM TRADE_WITH_MARKS
ORDER BY trade_date DESC, trade_time DESC
