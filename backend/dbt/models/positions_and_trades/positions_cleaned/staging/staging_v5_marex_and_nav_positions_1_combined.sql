{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- MAREX
-------------------------------------------------------------

WITH MAREX AS (
    select

        sftp_date::DATE
        ,sftp_upload_timestamp::TIMESTAMP

        ,'MAREX'::VARCHAR as source_table
        ,marex_reference_number::VARCHAR as reference_number
        ,account::VARCHAR

        ,exchange_name::VARCHAR
        ,exchange_code::VARCHAR

        ,is_option::BOOLEAN
        ,put_call::VARCHAR
        ,strike_price::FLOAT as strike_price
        ,marex_delta::FLOAT as marex_delta

        ,contract_yyyymm::VARCHAR
        ,contract_yyyymmdd::DATE
        ,contract_year::INT
        ,contract_month::INT
        ,contract_day::INT

        ,trade_date::DATE
        ,last_trade_date::DATE

        -- NOTE
        ,NULL::VARCHAR as nav_product
        ,marex_description::VARCHAR

        ,buy_sell::VARCHAR
        ,qty::FLOAT as qty
        ,lots::FLOAT as lots

        ,settlement_price::FLOAT as settlement_price
        ,trade_price::FLOAT as trade_price
        ,market_value::FLOAT as market_value

    FROM {{ ref('source_v5_marex_positions') }}
),

-- SELECT * FROM MAREX
-- ORDER BY sftp_date desc, contract_yyyymm ASC

-------------------------------------------------------------
-- NAV
-------------------------------------------------------------

NAV AS (
    select

        sftp_date::DATE
        ,sftp_upload_timestamp::TIMESTAMP

        ,'NAV'::VARCHAR as source_table
        ,nav_reference_number::VARCHAR as reference_number
        ,account::VARCHAR

        ,exchange_name::VARCHAR
        ,exchange_code::VARCHAR

        ,is_option::BOOLEAN
        ,put_call::VARCHAR
        ,strike_price::FLOAT as strike_price
        -- TODO:
        ,NULL::NUMERIC as marex_delta

        ,contract_yyyymm::VARCHAR
        ,contract_yyyymmdd::DATE
        ,contract_year::INT
        ,contract_month::INT
        ,contract_day::INT

        ,trade_date::DATE
        -- TODO:
        ,NULL::DATE as last_trade_date

        ,nav_product::VARCHAR
        ,marex_description::VARCHAR

        ,buy_sell::VARCHAR
        ,qty::FLOAT as qty
        ,lots::FLOAT as lots

        ,settlement_price::FLOAT as settlement_price
        ,trade_price::FLOAT as trade_price
        ,market_value::FLOAT as market_value

    -- from positions_v5_2026_feb_23.staging_v5_nav_positions
    FROM {{ ref('staging_v5_nav_positions') }}

    WHERE
        account in (
            -- 'UBE 10051',
            'ABN AMRO_1251PT034',
            'RJO_35511229',
            '969 ESKHL'
        )
),

-- SELECT * FROM NAV

-------------------------------------------------------------
-------------------------------------------------------------

COMBINED AS (
    SELECT * FROM MAREX
    UNION ALL
    SELECT * FROM NAV
)

SELECT * FROM COMBINED