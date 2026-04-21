{{
  config(
    materialized='table'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH COMBINED AS (
    -- select * FROM {{ ref('staging_v5_marex_and_nav_positions_4_exchange_codes') }}
    select distinct * FROM {{ ref('staging_v5_marex_and_nav_positions_4_exchange_codes') }}
)

SELECT * FROM COMBINED
ORDER BY sftp_date desc, contract_yyyymm ASC

-------------------------------------------------------------
-------------------------------------------------------------

-- WITH COMBINED AS (
--     SELECT

--         -- DATES
--         sftp_date

--         -- ACCOUNTS
--         ,source_table
--         ,reference_number
--         ,account
--         ,account_name

--         -- TRADE DATES
--         ,trade_date
--         -- last_trade_date
--         ,last_trade_date_filled as last_trade_date
--         ,days_to_expiry

--         -- EXCHANGE
--         ,exchange_name
--         ,exchange_code_grouping
--         ,exchange_code_region
--         ,exchange_code
--         ,exchange_code_underlying

--         -- OPTIONS
--         ,is_option
--         ,put_call
--         ,strike_price
--         -- ,marex_delta
--         ,marex_delta_filled as marex_delta

--         -- DATES
--         ,contract_yyyymm
--         ,contract_yyyymmdd
--         ,contract_year
--         ,contract_month
--         ,contract_day
--         ,futures_contract_month
--         ,futures_contract_month_y
--         ,futures_contract_month_yy

--         -- DESCRIPTIONS
--         ,nav_product
--         ,marex_description

--         -- EXCEL CODES
--         ,ice_xl_symbol
--         ,ice_xl_symbol_underlying
--         ,cme_excel_symbol
--         ,bbg_symbol
--         ,bbg_option_description

--         -- LOTS
--         -- ,lots
--         ,gas_lots as lots

--         -- BUY/SELL
--         ,buy_sell

--         -- QUANTITY
--         -- ,qty
--         ,gas_qty as qty

--         -- PRICES
--         ,settlement_price
--         ,trade_price
--         ,market_value

--     FROM {{ ref('staging_v5_marex_and_nav_positions_4_exchange_codes') }}
-- )

-- SELECT * FROM COMBINED
-- ORDER BY sftp_date desc, contract_yyyymm, exchange_code, put_call, strike_price
