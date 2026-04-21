{% docs positions_col_sftp_date %}
Business date associated with the SFTP position snapshot.
{% enddocs %}

{% docs positions_col_sftp_upload_timestamp %}
Timestamp when the SFTP file was ingested.
{% enddocs %}

{% docs positions_col_source_table %}
Originating source family for the normalized record (`MAREX` or `NAV`).
{% enddocs %}

{% docs positions_col_reference_number %}
Normalized source reference identifier used to track individual position rows.
{% enddocs %}

{% docs positions_col_account %}
Raw broker/account identifier from the source file.
{% enddocs %}

{% docs positions_col_account_name %}
Mapped internal account label from the account lookup table.
{% enddocs %}

{% docs positions_col_exchange_name %}
Normalized exchange name.
{% enddocs %}

{% docs positions_col_exchange_code %}
Primary exchange/product code used for downstream symbol mappings.
{% enddocs %}

{% docs positions_col_exchange_code_grouping %}
Higher-level product grouping derived from lookup logic.
{% enddocs %}

{% docs positions_col_exchange_code_region %}
Region classification derived from product lookup metadata.
{% enddocs %}

{% docs positions_col_exchange_code_underlying %}
Underlying product code for option contracts.
{% enddocs %}

{% docs positions_col_is_option %}
Boolean indicator for option contracts.
{% enddocs %}

{% docs positions_col_put_call %}
Option side code (`C` or `P`) when the record is an option.
{% enddocs %}

{% docs positions_col_strike_price %}
Option strike price.
{% enddocs %}

{% docs positions_col_marex_delta %}
Option delta metric sourced from Marex when available and forward-filled in staging.
{% enddocs %}

{% docs positions_col_contract_yyyymm %}
Contract month in `YYYYMM` format.
{% enddocs %}

{% docs positions_col_contract_yyyymmdd %}
Contract date in `YYYYMMDD` date form when a day-level prompt exists.
{% enddocs %}

{% docs positions_col_contract_year %}
Contract year component.
{% enddocs %}

{% docs positions_col_contract_month %}
Contract month number.
{% enddocs %}

{% docs positions_col_contract_day %}
Contract day number for prompt-dated products.
{% enddocs %}

{% docs positions_col_trade_date %}
Trade date associated with the position entry.
{% enddocs %}

{% docs positions_col_last_trade_date %}
Last tradable date from source when provided.
{% enddocs %}

{% docs positions_col_last_trade_date_filled %}
Forward-filled last trade date used for expiry and PnL calculations.
{% enddocs %}

{% docs positions_col_days_to_expiry %}
Days between current MST date and the filled last trade date.
{% enddocs %}

{% docs positions_col_nav_product %}
NAV product label as delivered by the NAV feed.
{% enddocs %}

{% docs positions_col_marex_description %}
Normalized contract description used for analysis and reporting.
{% enddocs %}

{% docs positions_col_buy_sell %}
Trade direction code (`B` buy or `S` sell).
{% enddocs %}

{% docs positions_col_qty %}
Signed quantity at record grain.
{% enddocs %}

{% docs positions_col_lots %}
Contract lot size at record grain.
{% enddocs %}

{% docs positions_col_gas_qty %}
Quantity normalized to gas-equivalent sizing rules.
{% enddocs %}

{% docs positions_col_gas_lots %}
Lot size normalized to gas-equivalent sizing rules.
{% enddocs %}

{% docs positions_col_settlement_price %}
Settlement price used for valuation and PnL.
{% enddocs %}

{% docs positions_col_trade_price %}
Trade price observed in source positions data.
{% enddocs %}

{% docs positions_col_market_value %}
Source-level market value.
{% enddocs %}

{% docs positions_col_futures_contract_month %}
Single-letter futures month code.
{% enddocs %}

{% docs positions_col_futures_contract_month_y %}
Futures month code plus one-digit year suffix.
{% enddocs %}

{% docs positions_col_futures_contract_month_yy %}
Futures month code plus two-digit year suffix.
{% enddocs %}

{% docs positions_col_ice_xl_symbol %}
ICE XL symbol generated from product mapping rules.
{% enddocs %}

{% docs positions_col_ice_xl_symbol_underlying %}
ICE XL symbol for an option's underlying product.
{% enddocs %}

{% docs positions_col_cme_excel_symbol %}
CME Excel-compatible symbol generated from contract attributes.
{% enddocs %}

{% docs positions_col_bbg_symbol %}
Bloomberg-style symbol for supported options products.
{% enddocs %}

{% docs positions_col_bbg_option_description %}
Human-readable Bloomberg option description.
{% enddocs %}

{% docs positions_col_previous_sftp_date %}
Prior snapshot date used for day-over-day comparisons.
{% enddocs %}

{% docs positions_col_previous_marex_delta %}
Prior snapshot option delta value.
{% enddocs %}

{% docs positions_col_daily_change_total %}
Daily settlement price change versus previous reference price.
{% enddocs %}

{% docs positions_col_daily_pnl_total %}
Calculated daily PnL from settlement move, quantity, and lots.
{% enddocs %}
