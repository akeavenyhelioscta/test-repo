SELECT  

    record_id
    ,firm
    ,organization
    ,account_number
    ,account_type
    ,currency_symbol
    ,rr
    ,trade_date
    ,buy_sell
    ,quantity
    ,exchange
    ,futures_code
    ,symbol
    ,contract_year_month
    ,prompt_day
    ,strike_price
    ,put_call
    ,security_description
    ,trade_price
    ,printable_price
    ,trade_type
    ,order_number
    ,security_type_code
    ,cusip
    ,comment_code
    ,give_in_out_code
    ,give_in_out_firm_num
    ,spread_code
    ,open_close_code
    ,trace_num_or_unique_identifier
    ,round_turn_half_turn_account
    ,executing_broker
    ,opposing_broker
    ,oppos_firm
    ,commission
    ,comm_act_type
    ,fee_amt_1
    ,fee_1_atype
    ,fee_amt_2
    ,fee_2_atype
    ,fee_amt_3
    ,fee_3_atype
    ,brokerage
    ,brkrage_atype
    ,give_io_charge
    ,give_io_atype
    ,other_charges
    ,other_atype
    ,wire_charge
    ,wire_chg_atype
    ,fee_type_6
    ,fee_type_6_atype
    ,date
    ,CASE WHEN option_exp_date < '0001-01-01' THEN NULL ELSE option_exp_date END AS option_exp_date
    ,last_trd_date
    ,net_amount
    ,traded_exchg
    ,sub_exchange
    ,exchange_name
    ,exch_comm_cd
    ,multiplication_factor
    ,subaccount
    ,instr_type
    ,cash_settled
    ,instrument_description
    ,fee_amt_4
    ,fee_4_atype
    ,fee_amt_5
    ,fee_5_atype
    ,fee_amt_7
    ,fee_7_atype
    ,fee_amt_8
    ,fee_8_atype
    ,fee_amt_9
    ,fee_9_atype
    ,fee_amt_10
    ,fee_10_atype
    ,fee_amt_11
    ,fee_11_atype
    ,fee_amt_12
    ,fee_12_atype
    ,fee_amt_13
    ,fee_13_atype
    ,clearing_time_hhmmss
    ,settlement_price
    ,broker
    ,isin
    ,mic

    ,sftp_date::DATE as sftp_date
    ,sftp_upload_timestamp::TIMESTAMP as sftp_upload_timestamp
    ,trade_status
    ,product_code_grouping
    ,product_code_region
    ,product_code_underlying
    ,ice_product_code
    ,cme_product_code
    ,bbg_product_code

FROM trades_cleaned.clear_street_trades

WHERE 
    give_in_out_firm_num in ('ADU', '905')
    AND sftp_date = (SELECT MAX(sftp_date) FROM trades_cleaned.clear_street_trades)

ORDER BY 
  sftp_date DESC, 
  sftp_upload_timestamp DESC,
  product_code_grouping,
  product_code_region