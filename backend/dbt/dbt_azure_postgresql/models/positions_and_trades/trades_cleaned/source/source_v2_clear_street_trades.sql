{{
  config(
    materialized='ephemeral'
  )
}}

-- DA-SCOPED FORK: filtered to exch_comm_cd IN ('PDA','PDO') and trimmed
-- to the column subset needed for the trades-vs-ICE scorecard. See
-- sister repo helioscta-backend for the full multi-product version.

-------------------------------------------------------------
-- 1. Project the slim column set + cast types.
-- 2. INNER JOIN against the latest sftp_upload_timestamp per sftp_date
--    so same-day re-uploads supersede earlier intraday previews.
-- 3. Filter to DA exchange-commodity codes.
-------------------------------------------------------------

WITH SFTP_TRADES_UPSERT AS (
    SELECT
        trade_date_from_sftp::DATE                              AS sftp_date
        ,sftp_upload_timestamp::TIMESTAMP                       AS sftp_upload_timestamp

        ,trade_date::DATE                                       AS trade_date
        ,NULLIF(record_id::VARCHAR, 'nan')                      AS record_id
        ,NULLIF(order_number::VARCHAR, 'nan')                   AS order_number
        ,NULLIF(trace_num_or_unique_identifier::VARCHAR, 'nan') AS trace_num_or_unique_identifier

        ,NULLIF(account_number::VARCHAR, 'nan')                 AS account_number
        ,NULLIF(give_in_out_code::VARCHAR, 'nan')               AS give_in_out_code
        ,NULLIF(give_in_out_firm_num::VARCHAR, 'nan')           AS give_in_out_firm_num
        ,NULLIF(executing_broker::VARCHAR, 'nan')               AS executing_broker
        ,NULLIF(opposing_broker::VARCHAR, 'nan')                AS opposing_broker
        ,NULLIF(oppos_firm::VARCHAR, 'nan')                     AS oppos_firm

        ,NULLIF(exch_comm_cd::VARCHAR, 'nan')                   AS exch_comm_cd
        ,NULLIF(exchange_name::VARCHAR, 'nan')                  AS exchange_name
        ,contract_year_month::INTEGER                           AS contract_year_month
        ,prompt_day::INTEGER                                    AS prompt_day
        ,NULLIF(security_description::VARCHAR, 'nan')           AS security_description

        ,buy_sell::INTEGER                                      AS buy_sell
        ,quantity::INTEGER                                      AS quantity
        ,trade_price::FLOAT                                     AS trade_price
        ,settlement_price::FLOAT                                AS settlement_price

    FROM {{ source('clear_street_v2', 'helios_transactions_v2_2026_feb_23') }}
),

LATEST_SFTP_UPLOAD_TIMESTAMP AS (
    SELECT
        sftp_date
        ,MAX(sftp_upload_timestamp) AS max_timestamp
    FROM SFTP_TRADES_UPSERT
    GROUP BY sftp_date
),

CLEAR_STREET_TRADES_FILE AS (
    SELECT a.*
    FROM SFTP_TRADES_UPSERT a
    INNER JOIN LATEST_SFTP_UPLOAD_TIMESTAMP b
        ON a.sftp_date = b.sftp_date
       AND a.sftp_upload_timestamp = b.max_timestamp
    WHERE a.exch_comm_cd IN ('PDA', 'PDO')
)

SELECT * FROM CLEAR_STREET_TRADES_FILE
ORDER BY sftp_date DESC, sftp_upload_timestamp DESC
