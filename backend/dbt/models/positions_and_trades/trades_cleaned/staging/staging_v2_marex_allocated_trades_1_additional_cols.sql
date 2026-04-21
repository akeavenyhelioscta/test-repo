{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- MAREX BASE
-------------------------------------------------------------

WITH TRADES AS (
    SELECT * FROM {{ ref('source_v2_marex_allocated_trades') }}
),

-------------------------------------------------------------
-------------------------------------------------------------

TRADES_WITH_ADDITIONAL_COLS AS (
    SELECT
        *
        -- Missing the following columns:
        -- - contract_day
        -- - lots
        -- - last_trade_date
        -- - marex_delta
        -- - marex_reference_number

        -- source_table
        ,'Marex Allocated Trades'::VARCHAR as source_table

        -- is_option
        ,CASE
            WHEN put_call in ('C', 'P') THEN TRUE
            ELSE FALSE
        END AS is_option

    FROM TRADES
),

-- SELECT * FROM ADDITIONAL_COLS
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-----------------------------------------------------------
-- futures_contract_month_code
-----------------------------------------------------------

futures_month_lookup AS (
    SELECT * FROM (VALUES
        (1, 'Jan', 'F'),
        (2, 'Feb', 'G'),
        (3, 'Mar', 'H'),
        (4, 'Apr', 'J'),
        (5, 'May', 'K'),
        (6, 'Jun', 'M'),
        (7, 'Jul', 'N'),
        (8, 'Aug', 'Q'),
        (9, 'Sep', 'U'),
        (10, 'Oct', 'V'),
        (11, 'Nov', 'X'),
        (12, 'Dec', 'Z')
    ) AS t(month_number, month_name, contract_code)
),

TRADES_WITH_FUTURES_CODES AS (
    SELECT
        *

        -- futures_month_code
        ,(SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month) as futures_contract_month
        ,CONCAT((SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month), RIGHT(contract_year::VARCHAR, 1)) as futures_contract_month_y
        ,CONCAT((SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month), RIGHT(contract_year::VARCHAR, 2)) as futures_contract_month_yy

    FROM TRADES_WITH_ADDITIONAL_COLS
),

-- SELECT * FROM TRADES_WITH_FUTURES_CODES
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-----------------------------------------------------------
-- account names
-----------------------------------------------------------

ACCOUNTS_LOOKUP_TABLE AS (
    select distinct

        account_name
        ,account
        ,source

    FROM {{ ref('utils_v1_positions_and_trades_accounts_lookup') }}
),

TRADES_WITH_ACCOUNT_NAMES AS (
    SELECT
        trades.*

        -- account_name
        ,lookup.account_name as account_name

    FROM TRADES_WITH_FUTURES_CODES trades
    LEFT JOIN ACCOUNTS_LOOKUP_TABLE lookup ON trades.account = lookup.account
)

SELECT * FROM TRADES_WITH_ACCOUNT_NAMES
ORDER BY sftp_date DESC, sftp_upload_timestamp DESC
