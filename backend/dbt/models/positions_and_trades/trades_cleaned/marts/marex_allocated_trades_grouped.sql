-------------------------------------------------------------
-------------------------------------------------------------

WITH TRADES AS (
    select

        -- DATES
        sftp_date

        -- TRADE DATES
        ,clear_date as trade_date

        -- ACCOUNTS
        ,account
        ,account_name

        -- EXCHANGE
        ,exchange_name
        ,product_code_grouping
        ,product_code_region
        ,exchange_code

        -- OPTIONS
        ,is_option
        ,put_call
        ,strike_price

        -- CONTRACT DATES
        ,contract_yyyymm
        ,contract_year
        ,contract_month

        -- DESCRIPTION
        ,contract_description

        -- prod
        ,ice_product_code
        ,ice_product_code_underlying
        ,cme_product_code

        -- PRICES
        ,trade_price

        -- BUY/SELL
        ,buy_sell

        -- QTY
        ,qty

    FROM {{ ref('marex_allocated_trades') }}
),

-- SELECT * FROM TRADES
-- ORDER BY sftp_date DESC, contract_yyyymm

-------------------------------------------------------------
-- GROUPED
-------------------------------------------------------------

GROUPED AS (
    select

        -- DATES
        sftp_date

        -- TRADE DATES
        ,trade_date

        -- EXCHANGE
        ,exchange_name
        ,product_code_grouping
        ,product_code_region
        ,exchange_code

        -- OPTIONS
        ,is_option
        ,put_call
        ,strike_price

        -- CONTRACT DATES
        ,contract_yyyymm
        ,contract_year
        ,contract_month

        -- DESCRIPTION
        ,contract_description

        -- prod
        ,ice_product_code
        ,ice_product_code_underlying
        ,cme_product_code

        -- PRICES
        ,AVG(trade_price) as trade_price_total
        ,AVG(CASE WHEN account_name = 'ACIM' THEN trade_price ELSE NULL END) AS trade_price_acim
        ,AVG(CASE WHEN account_name = 'ANDY' THEN trade_price ELSE NULL END) AS trade_price_andy
        ,AVG(CASE WHEN account_name = 'MAC' THEN trade_price ELSE NULL END) AS trade_price_mac
        ,AVG(CASE WHEN account_name = 'PNT' THEN trade_price ELSE NULL END) AS trade_price_pnt
        ,AVG(CASE WHEN account_name = 'DICKSON' THEN trade_price ELSE NULL END) AS trade_price_dickson

        -- QTY
        ,SUM(qty) as qty_total
        ,SUM(CASE WHEN account_name = 'ACIM' THEN qty ELSE 0 END) AS qty_acim
        ,SUM(CASE WHEN account_name = 'ANDY' THEN qty ELSE 0 END) AS qty_andy
        ,SUM(CASE WHEN account_name = 'MAC' THEN qty ELSE 0 END) AS qty_mac
        ,SUM(CASE WHEN account_name = 'PNT' THEN qty ELSE 0 END) AS qty_pnt
        ,SUM(CASE WHEN account_name = 'DICKSON' THEN qty ELSE 0 END) AS qty_dickson


    from TRADES
    GROUP BY

        -- DATES
        sftp_date

        -- TRADE DATES
        ,trade_date

        -- EXCHANGE
        ,exchange_name
        ,product_code_grouping
        ,product_code_region
        ,exchange_code

        -- OPTIONS
        ,is_option
        ,put_call
        ,strike_price

        -- CONTRACT DATES
        ,contract_yyyymm
        ,contract_year
        ,contract_month

        -- DESCRIPTION
        ,contract_description

        -- prod
        ,ice_product_code
        ,ice_product_code_underlying
        ,cme_product_code

)

SELECT * FROM GROUPED
ORDER BY sftp_date DESC, contract_yyyymm