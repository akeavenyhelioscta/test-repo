{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Join the slim product_lookup for product_code_grouping/region.
-- Derive ice_product_code as the ICE ticker that traded:
--   PDA -> 'PDA D1-IUS' (weekday DA on-peak daily strip)
--   PDO -> 'PDO P1-IUS' (weekend DA off-peak 2x16 strip)
-- These are the SAME literal tickers regardless of delivery_date —
-- the per-delivery resolution happens via the delivery_date column.
-- Use (ice_product_code, delivery_date) as the JOIN key into
-- ice_python_ticker_data_by_delivery.
-------------------------------------------------------------

WITH TRADES AS (
    SELECT * FROM {{ ref('staging_v2_clear_street_trades_1_additional_cols') }}
),

PRODUCT_LOOKUP_TABLE AS (
    SELECT DISTINCT
        exchange_code
        ,exchange_code_grouping AS product_code_grouping
        ,exchange_code_region AS product_code_region
    FROM {{ ref('utils_v1_positions_and_trades_product_lookup') }}
),

TRADES_WITH_PRODUCT_LOOKUP AS (
    SELECT
        trades.*
        ,lookup.product_code_grouping
        ,lookup.product_code_region
    FROM TRADES trades
    LEFT JOIN PRODUCT_LOOKUP_TABLE lookup
        ON trades.exch_comm_cd = lookup.exchange_code
),

TRADES_WITH_ICE_PRODUCT_CODE AS (
    SELECT
        trades.*
        ,(CASE
            WHEN exchange_name_cleaned = 'IFED' AND exch_comm_cd = 'PDA' THEN 'PDA D1-IUS'
            WHEN exchange_name_cleaned = 'IFED' AND exch_comm_cd = 'PDO' THEN 'PDO P1-IUS'
            ELSE NULL
         END) AS ice_product_code
    FROM TRADES_WITH_PRODUCT_LOOKUP trades
)

SELECT * FROM TRADES_WITH_ICE_PRODUCT_CODE
ORDER BY sftp_date DESC, contract_year_month, contract_day
