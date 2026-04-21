{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH MAREX_POSITIONS as (
    select

        -- DATES
        marex_date_from_sftp::DATE as sftp_date
        ,sftp_upload_timestamp::TIMESTAMP as sftp_upload_timestamp

        -- REFERENCE NUMBER
        ,dprefno::VARCHAR as marex_reference_number

        -- ACCOUNTS
        ,dpacct::VARCHAR as account

        -- EXCHANGE
        ,dpexch3::VARCHAR as exchange_name
        ,dpexfc::VARCHAR as exchange_code

        -- OPTIONS
        ,CASE
            WHEN dpsubt::VARCHAR IS NULL OR TRIM(dpsubt::VARCHAR) = '' THEN FALSE::BOOLEAN
            ELSE TRUE::BOOLEAN
        END AS is_option
        ,CASE
            WHEN dpsubt::VARCHAR IS NULL OR TRIM(dpsubt::VARCHAR) = '' THEN NULL
            ELSE dpsubt::VARCHAR
        END as put_call
        ,CASE
            WHEN dpsubt::VARCHAR IS NULL OR TRIM(dpsubt::VARCHAR) = '' THEN NULL
            ELSE ROUND(dpstrik::NUMERIC, 3)
        END as strike_price
        ,CASE
            WHEN dpsubt::VARCHAR IS NULL OR TRIM(dpsubt::VARCHAR) = '' THEN NULL
            ELSE dpdelta::FLOAT
        END as marex_delta

        -- CONTRACT DATES
        -- ,TO_CHAR(TO_DATE(dpctym, 'YYYYMMDD'), 'YYYY-MM') AS contract_yyyymm
        ,TO_CHAR(TO_DATE(dpctym, 'YYYYMMDD'), 'YYYYMM') AS contract_yyyymm
        ,CASE
            WHEN NULLIF(NULLIF(TRIM(dpday), ''), 'nan') IS NOT NULL
            THEN dpctym || LPAD(dpday::FLOAT::VARCHAR, 2, '0')
            ELSE NULL
        END as contract_yyyymmdd

        ,LEFT(dpctym, 4)::INT as contract_year
        ,TRIM(RIGHT(dpctym, 2))::INT as contract_month
        ,NULLIF(NULLIF(TRIM(dpday), ''), 'nan')::FLOAT::INT as contract_day

        -- TRADE DATES
        ,dptdat::VARCHAR::DATE as trade_date
        ,dplsttrdt::VARCHAR::DATE as last_trade_date

        -- DESCRIPTION
        ,REGEXP_REPLACE(TRIM(dpsdsc), '(\d+\.\d{2})\d+\s*$', '\1')::VARCHAR as marex_description

        -- BUY/SELL, QTY, LOTS
        ,CASE
            WHEN dpbs = 2 THEN 'S'
            ELSE 'B'
        END AS buy_sell
        ,CASE
            WHEN dpbs = 2 THEN (dpqty * -1)::INTEGER
            ELSE dpqty::INTEGER
        END AS qty
        ,dpmultf::NUMERIC::INTEGER as lots

        -- PRICES
        ,dpclos::NUMERIC as settlement_price
        ,dptprc::NUMERIC as trade_price
        ,dpmkvl::NUMERIC as market_value

    -- from marex.marex_sftp_positions_v2_2026_feb_23
    FROM {{ source('marex_v5', 'marex_sftp_positions_v2_2026_feb_23') }}
),

-- SELECT * FROM MAREX_POSITIONS
-- ORDER BY sftp_date DESC, contract_yyyymm, last_trade_date::DATE, marex_description

-------------------------------------------------------------
-------------------------------------------------------------

LATEST_SFTP_UPLOAD_TIMESTAMP as (
    SELECT
        sftp_date
        ,MAX(sftp_upload_timestamp) as max_timestamp
    FROM MAREX_POSITIONS
    GROUP BY sftp_date
),

-- SELECT * FROM LATEST_SFTP_UPLOAD_TIMESTAMP
-- ORDER BY sftp_date desc

-------------------------------------------------------------
-------------------------------------------------------------

FINAL AS (
    SELECT

        a.sftp_date
        ,a.sftp_upload_timestamp
        ,marex_reference_number
        ,account
        ,exchange_name
        ,exchange_code
        ,is_option
        ,put_call
        ,strike_price
        ,marex_delta
        ,contract_yyyymm
        ,contract_yyyymmdd
        ,contract_year
        ,contract_month
        ,contract_day
        ,trade_date
        ,last_trade_date
        ,marex_description
        ,buy_sell
        ,qty
        ,lots
        ,settlement_price
        ,trade_price
        ,market_value

    FROM MAREX_POSITIONS a
    INNER JOIN LATEST_SFTP_UPLOAD_TIMESTAMP b ON a.sftp_date = b.sftp_date AND a.sftp_upload_timestamp = b.max_timestamp
)

-------------------------------------------------------------
-------------------------------------------------------------

SELECT * FROM FINAL
ORDER BY sftp_date DESC, contract_yyyymm, last_trade_date, marex_description
