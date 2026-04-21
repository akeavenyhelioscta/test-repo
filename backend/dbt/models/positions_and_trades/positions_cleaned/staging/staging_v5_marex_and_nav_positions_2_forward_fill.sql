{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH COMBINED AS (
    select * FROM {{ ref('staging_v5_marex_and_nav_positions_1_combined') }}
),

-- SELECT * FROM COMBINED
-- ORDER BY sftp_date desc, contract_yyyymm ASC

-------------------------------------------------------------
-- FORWARD FILL
-------------------------------------------------------------

IGNORE_NULLS_ORDERING AS (
    SELECT
        *

        -- NOTE: work around for ignore nulls
        ,SUM(CASE WHEN marex_delta IS NOT NULL THEN 1 ELSE 0 END)
        OVER (
            PARTITION BY exchange_name, exchange_code, contract_year, contract_month, contract_day, put_call, strike_price
            ORDER BY sftp_date, source_table
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ignore_nulls_ordering

    FROM COMBINED
),

COMBINED_FILLED AS (
    SELECT

        *

        -- last_trade_date
        -- TODO: DDP, and PJL FROM NAV
        ,MAX(last_trade_date) OVER (PARTITION BY exchange_name, exchange_code, put_call, strike_price, contract_year, contract_month, contract_day, ignore_nulls_ordering) as last_trade_date_filled

        -- marex_delta
        ,MAX(marex_delta) OVER (PARTITION BY exchange_name, exchange_code, contract_year, contract_month, contract_day, put_call, strike_price, ignore_nulls_ordering) as marex_delta_filled

    FROM IGNORE_NULLS_ORDERING
)

SELECT * FROM COMBINED_FILLED
ORDER BY sftp_date desc, exchange_code, contract_yyyymm, put_call, strike_price