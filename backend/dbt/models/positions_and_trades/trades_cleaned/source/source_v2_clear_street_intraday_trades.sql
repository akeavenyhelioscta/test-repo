{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-------------------------------------------------------------

WITH SFTP_TRADES_UPSERT AS (
    select

        -- DATES
        trade_date_from_sftp::DATE as sftp_date
        ,sftp_upload_timestamp::TIMESTAMP as sftp_upload_timestamp

        -- PRIMARY KEYS
        ,trade_date::DATE as trade_date
        ,NULLIF(cusip::VARCHAR, 'nan') as cusip
        ,NULLIF(tracer::VARCHAR, 'nan') as tracer
        ,NULLIF(order_number::VARCHAR, 'nan') as order_number
        ,NULLIF(instrument_description::VARCHAR, 'nan') as instrument_description
        ,NULLIF(description::VARCHAR, 'nan') as description
        ,NULLIF(NULLIF(contract_year, 'nan'), '')::NUMERIC::INTEGER::TEXT || LPAD(NULLIF(NULLIF(contract_month, 'nan'), '')::NUMERIC::INTEGER::TEXT, 2, '0') as contract_year_month
        ,NULLIF(NULLIF(contract_year, 'nan'), '')::NUMERIC::INTEGER as contract_year
        ,NULLIF(NULLIF(contract_month, 'nan'), '')::NUMERIC::INTEGER as contract_month
        ,NULLIF(NULLIF(contract_day, 'nan'), '')::NUMERIC::INTEGER as contract_day
        ,NULLIF(put_call::VARCHAR, 'nan') as put_call
        ,NULLIF(strike_price, 'NaN'::FLOAT) as strike_price
        ,b_s::VARCHAR as b_s
        ,NULLIF(qty, '')::INTEGER as qty
        ,NULLIF(trade_price, 'NaN'::FLOAT) as trade_price

        -- COLS
        ,NULLIF(record_i_d::VARCHAR, 'nan') as record_i_d
        ,NULLIF(firm::VARCHAR, 'nan') as firm
        ,NULLIF(office::VARCHAR, 'nan') as office
        ,NULLIF(rr::VARCHAR, 'nan') as rr
        ,NULLIF(account::VARCHAR, 'nan') as account
        ,NULLIF(exchange::VARCHAR, 'nan') as exchange_name
        ,NULLIF(exchange_name_3dig::VARCHAR, 'nan') as exchange_name_3dig
        ,NULLIF(futures_code::VARCHAR, 'nan') as futures_code
        ,NULLIF(symbol::VARCHAR, 'nan') as symbol
        ,NULLIF(printable_trade_price, 'NaN'::FLOAT) as printable_trade_price
        ,NULLIF(cash_amount::VARCHAR, 'nan') as cash_amount
        ,NULLIF(sub_account::VARCHAR, 'nan') as sub_account
        ,NULLIF(spread_code::VARCHAR, 'nan') as spread_code
        ,NULLIF(trade_type::VARCHAR, 'nan') as trade_type
        ,NULLIF(comment_code::VARCHAR, 'nan') as comment_code
        ,NULLIF(exec_broker::VARCHAR, 'nan') as exec_broker
        ,NULLIF(opp_broker::VARCHAR, 'nan') as opp_broker
        ,NULLIF(opp_firm::VARCHAR, 'nan') as opp_firm
        ,NULLIF(gi_code::VARCHAR, 'nan') as gi_code
        ,NULLIF(gi_firm::VARCHAR, 'nan') as gi_firm
        ,NULLIF(curr_symbol::VARCHAR, 'nan') as curr_symbol
        ,NULLIF(prod_curr::VARCHAR, 'nan') as prod_curr
        ,NULLIF(prod_type::VARCHAR, 'nan') as prod_type
        ,NULLIF(exec_time::VARCHAR, 'nan') as exec_time
        ,TO_DATE(NULLIF(NULLIF(opt_exp_date, 'nan'), '')::NUMERIC::INTEGER::VARCHAR, 'YYYYMMDD') as opt_exp_date
        ,TO_DATE(NULLIF(NULLIF(last_trd_date, 'nan'), '')::NUMERIC::INTEGER::VARCHAR, 'YYYYMMDD') as last_trd_date
        ,NULLIF(traded_exchg::VARCHAR, 'nan') as traded_exchg
        ,NULLIF(sub_exchange::VARCHAR, 'nan') as sub_exchange
        ,NULLIF(source_refrnce::VARCHAR, 'nan') as source_refrnce
        ,NULLIF(exch_comm_cd::VARCHAR, 'nan') as exch_comm_cd
        ,NULLIF(mult_factor::VARCHAR, 'nan') as mult_factor
        ,NULLIF(chit_number::VARCHAR, 'nan') as chit_number
        ,NULLIF(isin::VARCHAR, 'nan') as isin
        ,NULLIF(mic::VARCHAR, 'nan') as mic
        ,row_number_for_trades::INTEGER

        -- MY COLUMNS
        ,created_at::TIMESTAMP as created_at
        ,updated_at::TIMESTAMP as updated_at

    -- from clear_street.helios_intraday_transactions_v2_2026_feb_23
    FROM {{ source('clear_street_v2', 'helios_intraday_transactions_v2_2026_feb_23') }}
),

-- SELECT * FROM SFTP_TRADES_UPSERT
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-------------------------------------------------------------
-------------------------------------------------------------

LATEST_SFTP_UPLOAD_TIMESTAMP as (
    SELECT
        sftp_date
        ,MAX(sftp_upload_timestamp) as max_timestamp
    FROM SFTP_TRADES_UPSERT
    GROUP BY sftp_date
),

-- SELECT * FROM LATEST_SFTP_UPLOAD_TIMESTAMP
-- ORDER BY sftp_date DESC, sftp_upload_timestamp DESC

-------------------------------------------------------------
-------------------------------------------------------------

FINAL as (
    SELECT
        a.*

    FROM SFTP_TRADES_UPSERT a
    INNER JOIN LATEST_SFTP_UPLOAD_TIMESTAMP b ON a.sftp_date = b.sftp_date AND a.sftp_upload_timestamp = b.max_timestamp
)

SELECT * FROM FINAL
ORDER BY sftp_date DESC, sftp_upload_timestamp DESC
