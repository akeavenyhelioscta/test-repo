{{
  config(
    materialized='view'
  )
}}

-- ICE TRADE BLOTTER
-- Deduplicates across multiple file uploads, keeping the latest ingested row.

WITH DEDUPED AS (

    SELECT
         deal_type
        ,trade_date
        ,trade_time
        ,deal_id
        ,leg_id
        ,orig_id
        ,b_s
        ,product
        ,hub
        ,contract
        ,begin_date
        ,end_date
        ,clearing_acct
        ,cust_acct
        ,clearing_firm
        ,price
        ,price_units
        ,option
        ,strike
        ,strike2
        ,style
        ,lots
        ,total_quantity
        ,qty_units
        ,tt
        ,brk
        ,trader
        ,memo
        ,clearing_venue
        ,user_id
        ,source
        ,link_id
        ,usi
        ,authorized_trader_id
        ,location
        ,meter
        ,lead_time
        ,waiver_ind
        ,trade_time_micros
        ,cdi_override
        ,by_pass_mqr
        ,broker_name
        ,trading_company
        ,mic
        ,cc
        ,ingested_at
        ,created_at
        ,updated_at
        ,ROW_NUMBER() OVER (
            PARTITION BY
                 deal_type
                ,trade_date
                ,trade_time_micros
                ,deal_id
                ,orig_id
                ,b_s
                ,product
                ,hub
                ,contract
                ,price
                ,lots
            ORDER BY ingested_at DESC
        ) AS rn

    FROM {{ ref('source_v1_ice_trade_blotter') }}

)

SELECT
     deal_type
    ,trade_date
    ,trade_time
    ,deal_id
    ,leg_id
    ,orig_id
    ,b_s
    ,product
    ,hub
    ,contract
    ,begin_date
    ,end_date
    ,clearing_acct
    ,cust_acct
    ,clearing_firm
    ,price
    ,price_units
    ,option
    ,strike
    ,strike2
    ,style
    ,lots
    ,total_quantity
    ,qty_units
    ,tt
    ,brk
    ,trader
    ,memo
    ,clearing_venue
    ,user_id
    ,source
    ,link_id
    ,usi
    ,authorized_trader_id
    ,location
    ,meter
    ,lead_time
    ,waiver_ind
    ,trade_time_micros
    ,cdi_override
    ,by_pass_mqr
    ,broker_name
    ,trading_company
    ,mic
    ,cc
    ,ingested_at
    ,created_at
    ,updated_at

FROM DEDUPED
WHERE rn = 1
ORDER BY trade_date DESC, trade_time_micros DESC
