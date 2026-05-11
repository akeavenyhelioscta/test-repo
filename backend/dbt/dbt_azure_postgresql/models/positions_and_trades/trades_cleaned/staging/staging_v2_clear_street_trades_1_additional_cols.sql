{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- Derive contract calendar parts, signed quantity, cleaned exchange
-- name, cleaned account, and delivery_date (the join key into ICE
-- marts and the DA scorecard).
-- Resolve account_name from give_in_out_firm_num via the lookup.
-------------------------------------------------------------

WITH TRADES AS (
    SELECT * FROM {{ ref('source_v2_clear_street_trades') }}
),

TRADES_WITH_ADDITIONAL_COLS AS (
    SELECT
        *

        -- CONTRACT DATES
        ,CASE
            WHEN contract_year_month::TEXT ~ '^\d{6}$' THEN LEFT(contract_year_month::TEXT, 4)::INT
            ELSE NULL
         END AS contract_year
        ,CASE
            WHEN contract_year_month::TEXT ~ '^\d{6}$' THEN RIGHT(contract_year_month::TEXT, 2)::INT
            ELSE NULL
         END AS contract_month
        ,CASE
            WHEN prompt_day > 0 THEN prompt_day::INT
            ELSE NULL
         END AS contract_day

        -- exchange_name cleaned (NYM/IFE/IPE all collapse to canonical IFED for DA-scope)
        ,CASE
            WHEN exchange_name = 'NYM' THEN 'NYME'
            WHEN exchange_name IN ('IFE', 'IFED', 'IPE') THEN 'IFED'
            ELSE NULL
         END AS exchange_name_cleaned

        -- buy_sell as B/S
        ,CASE
            WHEN buy_sell::INT = 1 THEN 'B'
            WHEN buy_sell::INT = 2 THEN 'S'
            ELSE NULL
         END AS buy_sell_cleaned

        -- signed quantity: positive for buys, negative for sells
        ,CASE
            WHEN buy_sell::INT = 1 THEN quantity
            WHEN buy_sell::INT = 2 THEN -1 * quantity
            ELSE NULL
         END AS quantity_cleaned

        -- account_number: GHELI is Helios' Clear Street account
        ,CASE
            WHEN account_number = 'GHELI' THEN 'HELIOS'
            ELSE account_number
         END AS account_number_cleaned

    FROM TRADES
),

TRADES_WITH_DELIVERY_DATE AS (
    SELECT
        trades.*

        -- delivery_date: calendar date this contract delivers on. Joins
        -- to ice_python_ticker_data_by_delivery.start_date and to
        -- scorecard_da_onpeak_wh.delivery_date. NULL guard avoids
        -- make_date raising on missing components.
        ,CASE
            WHEN contract_year IS NOT NULL
             AND contract_month IS NOT NULL
             AND contract_day IS NOT NULL
            THEN make_date(contract_year, contract_month, contract_day)
            ELSE NULL
         END AS delivery_date

    FROM TRADES_WITH_ADDITIONAL_COLS trades
),

ACCOUNTS_LOOKUP_TABLE AS (
    SELECT DISTINCT
        account_name
        ,account
        ,source
    FROM {{ ref('utils_v1_positions_and_trades_accounts_lookup') }}
),

TRADES_WITH_ACCOUNT_NAMES AS (
    SELECT
        trades.*
        ,lookup.account_name AS account_name
    FROM TRADES_WITH_DELIVERY_DATE trades
    LEFT JOIN ACCOUNTS_LOOKUP_TABLE lookup
        ON trades.give_in_out_firm_num = lookup.account
)

SELECT * FROM TRADES_WITH_ACCOUNT_NAMES
ORDER BY sftp_date DESC, sftp_upload_timestamp DESC
