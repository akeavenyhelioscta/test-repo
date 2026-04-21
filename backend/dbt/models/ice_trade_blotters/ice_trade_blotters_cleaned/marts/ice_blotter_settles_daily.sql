{{
  config(
    materialized='view'
  )
}}

-- ICE BLOTTER SETTLES — DAILY SUMMARY
-- Latest snapshot per day for PDP/PDA products with contract dates from ICE.

WITH RANKED AS (

    SELECT
         trade_date
        ,symbol          AS ice_product_code
        ,strip
        ,start_date
        ,end_date
        ,last
        ,settle
        ,recent_settlement
        ,ROW_NUMBER() OVER (
            PARTITION BY trade_date, symbol
            ORDER BY snapshot_at DESC
        ) AS rn

    FROM {{ source('ice_python_market_data', 'ice_blotter_settles_v1_2026_apr_02') }}
    WHERE trade_date >= DATE_TRUNC('week', CURRENT_DATE)

)

SELECT
     ice_product_code
    ,strip
    ,start_date
    ,end_date
    ,trade_date       AS settlement_date
    ,last
    ,settle
    ,recent_settlement

FROM RANKED
WHERE rn = 1
ORDER BY settlement_date DESC, ice_product_code
