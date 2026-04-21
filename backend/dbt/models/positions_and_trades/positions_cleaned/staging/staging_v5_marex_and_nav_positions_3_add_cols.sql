{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH COMBINED AS (
    select * FROM {{ ref('staging_v5_marex_and_nav_positions_2_forward_fill') }}
),

-- SELECT * FROM COMBINED
-- ORDER BY sftp_date desc, contract_yyyymm ASC

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

COMBINED_WITH_ACCOUNT_NAMES AS (
    SELECT
        combined.*

        -- account_name
        ,lookup.account_name as account_name

    FROM COMBINED combined
    LEFT JOIN ACCOUNTS_LOOKUP_TABLE lookup ON combined.account = lookup.account
),

-- SELECT * FROM COMBINED_WITH_ACCOUNT_NAMES
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-------------------------------------------------------------
-- DAYS TO EXPIRY
-------------------------------------------------------------

COMBINED_WITH_DAYS_TO_EXPIRY AS (
    SELECT
        combined.*

        -- days_to_expiry
        ,(last_trade_date_filled - (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE)::INT as days_to_expiry

    FROM COMBINED_WITH_ACCOUNT_NAMES combined
),

-- SELECT * FROM COMBINED_WITH_DAYS_TO_EXPIRY
-- ORDER BY sftp_date DESC, contract_yyyymm, contract_yyyymmdd, last_trade_date, marex_description, source_table

-------------------------------------------------------------
-- GAS LOTS
-------------------------------------------------------------

COMBINED_WITH_GAS_LOTS AS (
    SELECT
        combined.*

        -- cme_gas_qty
        ,(CASE
            WHEN lots = 2500 and exchange_code in ('HHD', 'H', 'PHH', 'PHE') THEN qty/4
            ELSE qty
        END) as gas_qty

        -- cme_gas_lots
        ,(CASE
            WHEN lots = 2500 and exchange_code in ('HHD', 'H', 'PHH', 'PHE') THEN lots*4
            ELSE lots
        END) as gas_lots

    FROM COMBINED_WITH_DAYS_TO_EXPIRY combined
),

-- SELECT * FROM COMBINED_WITH_GAS_LOTS
-- ORDER BY sftp_date DESC, contract_yyyymm, contract_yyyymmdd, last_trade_date, marex_description, source_table

-------------------------------------------------------------
-- futures_contract_month_code
-------------------------------------------------------------

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

COMBINED_FUTURES_CODES AS (
    SELECT
        combined.*

        -- futures_month_code
        ,(SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month) as futures_contract_month
        ,CONCAT((SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month), RIGHT(contract_year::VARCHAR, 1)) as futures_contract_month_y
        ,CONCAT((SELECT contract_code FROM futures_month_lookup WHERE month_number = contract_month), RIGHT(contract_year::VARCHAR, 2)) as futures_contract_month_yy

    FROM COMBINED_WITH_GAS_LOTS combined
)

SELECT * FROM COMBINED_FUTURES_CODES
ORDER BY sftp_date DESC, contract_yyyymm, contract_yyyymmdd, last_trade_date_filled, marex_description, source_table