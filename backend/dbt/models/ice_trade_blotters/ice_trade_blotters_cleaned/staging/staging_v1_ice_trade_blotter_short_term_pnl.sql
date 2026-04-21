{{
  config(
    materialized='ephemeral'
  )
}}

-- ICE TRADE BLOTTER — SHORT-TERM PDP/PDA TRADES WITH PRODUCT CODES
-- Filters to PDP and PDA trades, parses dates, derives ICE symbols,
-- and deduplicates across file uploads (which have different date formats).

WITH TRADES AS (

    SELECT * FROM {{ ref('ice_trade_blotter') }}
    WHERE cc IN ('PDP', 'PDA')

),

-------------------------------------------------------------
-- PARSE DATES
-------------------------------------------------------------
-- trade_date: "1-Apr-26" (DD-Mon-YY) or "April 1, 2026" (FMMonth DD, YYYY)
-- begin_date: "1-Apr-26" (DD-Mon-YY) or "01-Apr-2026" (DD-Mon-YYYY)

PARSED AS (

    SELECT
        *,

        (CASE
            WHEN trade_date ~ '^\d' THEN TO_DATE(trade_date, 'DD-Mon-YY')
            ELSE TO_DATE(trade_date, 'FMMonth DD, YYYY')
        END) AS trade_date_parsed,

        (CASE
            WHEN begin_date ~ '^\d{1,2}-[A-Za-z]+-\d{4}$' THEN TO_DATE(begin_date, 'DD-Mon-YYYY')
            WHEN begin_date ~ '^\d' THEN TO_DATE(begin_date, 'DD-Mon-YY')
            ELSE NULL
        END) AS delivery_date

    FROM TRADES

),

-------------------------------------------------------------
-- DEDUP ACROSS FILE UPLOADS
-------------------------------------------------------------
-- Same trade appears with different date formats across files.
-- Keep latest ingested_at per unique trade identity.

DEDUPED AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                 deal_type
                ,trade_date_parsed
                ,trade_time_micros
                ,deal_id
                ,orig_id
                ,b_s
                ,product
                ,hub
                ,contract
                ,price
                ,lots
            ORDER BY ingested_at DESC
        ) AS dedup_rn

    FROM PARSED

),

-------------------------------------------------------------
-- ADD PRODUCT CODES
-------------------------------------------------------------

FINAL AS (

    SELECT
         deal_type
        ,trade_date_parsed
        ,trade_time
        ,trade_time_micros
        ,deal_id
        ,leg_id
        ,orig_id
        ,b_s
        ,product
        ,hub
        ,cc
        ,contract
        ,delivery_date
        ,price
        ,price_units
        ,lots
        ,total_quantity
        ,qty_units
        ,trader
        ,clearing_acct

        -- Direction: +1 for Buy, -1 for Sell
        ,(CASE WHEN b_s = 'Bought' THEN 1 ELSE -1 END) AS direction

        -- ICE product code
        ,(CASE
            WHEN cc = 'PDP' AND contract = 'HE 0800-HE 2300' THEN 'PDP D0-IUS'
            WHEN cc = 'PDP' AND contract = 'Next Day'         THEN 'PDP D1-IUS'
            WHEN cc = 'PDP' AND contract = 'Bal Week'          THEN 'PDP W0-IUS'
            WHEN cc = 'PDA' AND contract = 'Next Day'          THEN 'PDA D1-IUS'
            ELSE NULL
        END) AS ice_product_code

        -- Settlement date: which date's ICE snapshot has the settle for this trade
        -- Next Day (D1): settle published on delivery date (the next day)
        -- Same-day RT (D0): settle on delivery day itself
        -- Bal Week (W0): settle on trade day
        ,(CASE
            WHEN contract = 'Next Day'          THEN delivery_date
            WHEN contract = 'HE 0800-HE 2300'   THEN delivery_date
            WHEN contract = 'Bal Week'           THEN trade_date_parsed
            ELSE trade_date_parsed
        END)::DATE AS settlement_date

    FROM DEDUPED
    WHERE dedup_rn = 1

)

SELECT * FROM FINAL
