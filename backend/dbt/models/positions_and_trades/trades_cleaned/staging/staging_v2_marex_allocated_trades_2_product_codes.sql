{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- MAREX BASE
-------------------------------------------------------------

WITH TRADES AS (
    SELECT * FROM {{ ref('staging_v2_marex_allocated_trades_1_additional_cols') }}
),

-- SELECT * FROM TRADES
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-------------------------------------------------------------
-- PRODUCT_LOOKUP
-------------------------------------------------------------

PRODUCT_LOOKUP_TABLE AS (
    SELECT distinct
        bbg_exchange_code
        ,exchange_code
        ,exchange_code_grouping as product_code_grouping
        ,exchange_code_region as product_code_region
        ,exchange_code_underlying as product_code_underlying
    FROM {{ ref('utils_v1_positions_and_trades_product_lookup') }}
),

TRADES_WITH_PRODUCT_LOOKUP AS (
    select

        trades.*

        -- product_code_grouping
        ,(CASE
            WHEN trades.is_option = TRUE AND trades.exchange_code IN ('PMI') THEN 'POWER_OPTIONS'
            -- PJM RT
            WHEN trades.exchange_code IN ('PDP', 'PWA', 'DDP') THEN 'SHORT_TERM_POWER_RT'
            ELSE lookup.product_code_grouping
        END) as product_code_grouping

        -- product_code_region
        ,lookup.product_code_region as product_code_region

        -- product_code_underlying
        ,(CASE
            WHEN trades.is_option = TRUE THEN lookup.product_code_underlying
            ELSE NULL
        END)::VARCHAR as product_code_underlying

    FROM TRADES trades
    LEFT JOIN PRODUCT_LOOKUP_TABLE lookup ON trades.exchange_code = lookup.exchange_code
),

-- SELECT * FROM TRADES_WITH_PRODUCT_LOOKUP
-- ORDER BY sftp_date desc, contract_yyyymm ASC

-------------------------------------------------------------
-- ICE XL
-------------------------------------------------------------

TRADES_WITH_ICE_XL AS (
    SELECT
        trades.*

        -- ice_product_code
        ,(CASE
            -- FUTURES
            -- e.g. PMI X25-IUS
            WHEN exchange_name in ('IFED') AND product_code_grouping = 'POWER_FUTURES' THEN
                CONCAT(
                    exchange_code,
                    ' ',
                    futures_contract_month_yy,
                    '-IUS'
                )

            -- OPTION
            -- e.g. PMI Z25P50-IUS
            WHEN exchange_name in ('IFED') AND product_code_grouping = 'POWER_OPTIONS' THEN
                CONCAT(
                    exchange_code,
                    ' ',
                    futures_contract_month_yy,
                    put_call,
                    strike_price::INTEGER,
                    '-IUS'
                )

            -- BALMO
            -- e.g. HHD B0-IUS
            WHEN exchange_name in ('IFED') AND exchange_code = 'HHD' THEN
                CONCAT(
                    exchange_code,
                    ' ',
                    'B0-IUS'
                )

            -- RT
            -- e.g. PDP D0-IUS
            WHEN product_code_grouping = 'SHORT_TERM_POWER_RT' THEN
                CONCAT(
                    exchange_code,
                    ' ',
                    'D0-IUS'
                )

            -- TODO: SHORT TERM POWER

            ELSE NULL
        END) as ice_product_code

        -- ice_product_code_underlying
        ,(CASE
            -- OPTION
            -- e.g. PMI X25-IUS
            WHEN exchange_name = 'IFED' AND product_code_grouping = 'POWER_FUTURES' AND product_code_underlying IS NOT NULL THEN
                CONCAT(
                    product_code_underlying,
                    ' ',
                    futures_contract_month_yy,
                    '-IUS'
                )

            ELSE NULL
        END) as ice_product_code_underlying

    FROM TRADES_WITH_PRODUCT_LOOKUP trades
),

-- SELECT * FROM TRADES_WITH_ICE_XL
-- ORDER BY sftp_date DESC, contract_yyyymm ASC

-------------------------------------------------------------
-- CME EXCEL
-------------------------------------------------------------

TRADES_WITH_CME_EXCEL AS (
    SELECT
        trades.*

        ,(CASE

            -- HP: Penultimate (EXPIRES DAY BEFORE)
            -- https://www.cmegroup.com/markets/energy/natural-gas/natural-gas-penultimate.contractSpecs.html
            WHEN exchange_code in ('HP', 'PHH') THEN CONCAT('1|', 'G', '|', 'XNYM:F:NG', ':', contract_yyyymm)

            -- HH: Last Day Financial
            -- https://www.cmegroup.com/markets/energy/natural-gas/natural-gas-last-day.contractSpecs.html
            WHEN exchange_code in ('HH', 'H') THEN CONCAT('1|', 'G', '|', 'XNYM:F:NG', ':', contract_yyyymm)

            -- NG
            WHEN exchange_code = 'NG' THEN CONCAT('1|', 'G', '|', 'XNYM:F:NG', ':', contract_yyyymm)

            -- OPTION
            WHEN exchange_code in ('LN', 'PHE') THEN CONCAT('1|', 'G', '|', 'XNYM:O:LN', ':', contract_yyyymm, ':', put_call, ':', RTRIM(TO_CHAR(strike_price, 'FM999999999.999'), '.'))

            -- WEEKLY
            WHEN exchange_code in ('LN1', 'LN2', 'LN3', 'LN4', 'LN5') THEN CONCAT('1|', 'G', '|', 'XNYM:O:KN', SUBSTRING(exchange_code, 3, 2), ':', contract_yyyymm, ':', put_call, ':', RTRIM(TO_CHAR(strike_price, 'FM999999999.999'), '.'))

            -- TODO: CAL SPREAD
            WHEN exchange_code in ('G3', 'G4') THEN ''

            ELSE NULL
        END) as cme_product_code

    FROM TRADES_WITH_ICE_XL trades
)

SELECT * FROM TRADES_WITH_CME_EXCEL
ORDER BY sftp_date DESC, contract_yyyymm, put_call, strike_price