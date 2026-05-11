{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['delivery_date', 'ice_product_code'], 'type': 'btree'},
      {'columns': ['delivery_date'], 'type': 'btree'}
    ]
  )
}}

-------------------------------------------------------------
-- Clear Street overnight trades — DA-only, slim table.
-- Grain: 1 row per Clear Street trade fill (parent + allocation
-- children both appear; allocation chain is the give_in_out_*
-- columns).
-- Joins to ICE marts via (ice_product_code, delivery_date).
-- Joins to scorecard via delivery_date.
-- Materialised as a table refreshed daily by the
-- `Clear Street Trades` Prefect flow at 5am MT, after the sister
-- helioscta-backend repo's overnight SFTP ingest completes.
-------------------------------------------------------------

SELECT
    sftp_date
    ,sftp_upload_timestamp

    ,trade_date
    ,delivery_date

    ,record_id
    ,order_number
    ,trace_num_or_unique_identifier

    ,account_number_cleaned
    ,give_in_out_code
    ,give_in_out_firm_num
    ,account_name

    ,executing_broker
    ,opposing_broker
    ,oppos_firm

    ,exch_comm_cd
    ,exchange_name_cleaned
    ,product_code_grouping
    ,product_code_region
    ,ice_product_code

    ,security_description
    ,contract_year_month
    ,contract_year
    ,contract_month
    ,contract_day

    ,buy_sell_cleaned
    ,quantity_cleaned
    ,trade_price
    ,settlement_price

FROM {{ ref('staging_v2_clear_street_trades_2_product_codes') }}
ORDER BY sftp_date DESC, trade_date DESC, give_in_out_firm_num
