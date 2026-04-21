{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH MAREX_TRADES as (
    select

        -- DATES
        trade_date_from_sftp::DATE as sftp_date
        ,sftp_upload_timestamp::TIMESTAMP as sftp_upload_timestamp

        -- REFERENCE NUMBER

        -- ACCOUNTS
        ,clear_date::DATE as clear_date
        ,clear_status::VARCHAR
        ,clear_info1::VARCHAR as account
        ,trader::VARCHAR

        -- EXCHANGE
        ,CASE
            WHEN TRIM(market::VARCHAR) = 'NYMEX' THEN 'NYME'
            WHEN TRIM(market::VARCHAR) = 'IFED' THEN 'IFED'
            ELSE NULL
        END AS exchange_name
        ,contract::VARCHAR as exchange_code

        -- OPTIONS
        ,CASE
            WHEN TRIM(call_put) = 'P' THEN 'P'
            WHEN TRIM(call_put) = 'C' THEN 'C'
            ELSE NULL
        END AS put_call
        ,CASE
            WHEN TRIM(call_put) = 'P' OR TRIM(call_put) = 'C' THEN strike
            ELSE NULL
        END AS strike_price

        -- CONTRACT DATES
        ,TO_CHAR(TO_DATE(expiry, 'MON-YY'), 'YYYYMM')::VARCHAR as contract_yyyymm
        ,TO_CHAR(TO_DATE(expiry, 'MON-YY'), 'YYYY')::INT as contract_year
        ,TO_CHAR(TO_DATE(expiry, 'MON-YY'), 'MM')::INT as contract_month

        -- TRADE DATES
        ,clear_date::DATE as trade_date

        -- DESCRIPTION
        ,regexp_replace(contract_description::VARCHAR, '^\s+', '') as contract_description

        -- BUY/SELL, QTY, LOTS
        ,buy_sell
        ,CASE
            WHEN buy_sell = 'S' THEN (volume * -1)::INTEGER
            ELSE volume::INTEGER
        END AS qty
        ,ROUND(price::NUMERIC, 5) as trade_price

    FROM {{ source('marex_v2', 'helios_allocated_trades_v2_2026_feb_23') }}
),

-- SELECT * FROM MAREX_TRADES
-- ORDER BY sftp_date DESC, contract_description, account


-------------------------------------------------------------
-------------------------------------------------------------

LATEST_SFTP_UPLOAD_TIMESTAMP as (
    SELECT
        sftp_date
        ,MAX(sftp_upload_timestamp) as max_timestamp
    FROM MAREX_TRADES
    GROUP BY sftp_date
),

-- SELECT * FROM LATEST_SFTP_UPLOAD_TIMESTAMP
-- ORDER BY sftp_date desc


-------------------------------------------------------------
-------------------------------------------------------------

FINAL AS (
    SELECT

        -- DATES
        a.sftp_date
        ,a.sftp_upload_timestamp

        -- REFERENCE NUMBER

        -- ACCOUNTS
        ,clear_date
        ,clear_status
        ,account
        ,trader

        -- EXCHANGE
        ,exchange_name
        ,exchange_code

        -- OPTIONS
        ,put_call
        ,strike_price

        ,contract_yyyymm
        ,contract_year
        ,contract_month

        ,trade_date

        ,contract_description

        ,buy_sell
        ,qty

        ,trade_price

    FROM MAREX_TRADES a
    INNER JOIN LATEST_SFTP_UPLOAD_TIMESTAMP b ON a.sftp_date = b.sftp_date AND a.sftp_upload_timestamp = b.max_timestamp
)

-------------------------------------------------------------
-------------------------------------------------------------

SELECT * FROM FINAL
ORDER BY sftp_date DESC, contract_description, account
