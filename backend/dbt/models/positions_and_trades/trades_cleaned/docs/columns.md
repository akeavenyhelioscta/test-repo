{% docs trades_col_sftp_date %}
Business date associated with the SFTP trade snapshot.
{% enddocs %}

{% docs trades_col_sftp_upload_timestamp %}
Timestamp when the SFTP trade file was ingested.
{% enddocs %}

{% docs trades_col_trade_date %}
Trade date for the transaction.
{% enddocs %}

{% docs trades_col_clear_date %}
Clear/booking date used in Marex allocated trade files.
{% enddocs %}

{% docs trades_col_account %}
Raw account identifier from the broker/source feed.
{% enddocs %}

{% docs trades_col_account_name %}
Mapped internal account label from account lookup metadata.
{% enddocs %}

{% docs trades_col_clear_street_account %}
Normalized Clear Street account identifier used in grouped marts.
{% enddocs %}

{% docs trades_col_exchange_name %}
Normalized exchange name.
{% enddocs %}

{% docs trades_col_exchange_code %}
Product-level exchange code used for contract mappings.
{% enddocs %}

{% docs trades_col_exch_comm_cd %}
Clear Street exchange commodity code.
{% enddocs %}

{% docs trades_col_product_code_grouping %}
High-level product grouping from lookup and business overrides.
{% enddocs %}

{% docs trades_col_product_code_region %}
Regional product classification from lookup metadata.
{% enddocs %}

{% docs trades_col_product_code_underlying %}
Underlying product code for options products.
{% enddocs %}

{% docs trades_col_is_option %}
Boolean indicator for option trades.
{% enddocs %}

{% docs trades_col_put_call %}
Option side code (`C` or `P`) for option products.
{% enddocs %}

{% docs trades_col_strike_price %}
Option strike price.
{% enddocs %}

{% docs trades_col_contract_yyyymm %}
Contract month in `YYYYMM` format.
{% enddocs %}

{% docs trades_col_contract_year %}
Contract year component.
{% enddocs %}

{% docs trades_col_contract_month %}
Contract month number.
{% enddocs %}

{% docs trades_col_contract_day %}
Prompt day component for short-dated contracts.
{% enddocs %}

{% docs trades_col_contract_description %}
Human-readable contract/security description.
{% enddocs %}

{% docs trades_col_buy_sell %}
Trade side indicator (`B` buy or `S` sell).
{% enddocs %}

{% docs trades_col_qty %}
Signed quantity.
{% enddocs %}

{% docs trades_col_trade_price %}
Trade execution price.
{% enddocs %}

{% docs trades_col_settlement_price %}
Settlement reference price.
{% enddocs %}

{% docs trades_col_trade_status %}
Derived trade lifecycle status.
{% enddocs %}

{% docs trades_col_ice_product_code %}
ICE XL compatible symbol generated from product mapping logic.
{% enddocs %}

{% docs trades_col_ice_product_code_underlying %}
ICE XL symbol for the mapped option underlying.
{% enddocs %}

{% docs trades_col_cme_product_code %}
CME Excel-compatible symbol generated from contract attributes.
{% enddocs %}

{% docs trades_col_bbg_product_code %}
Bloomberg-style product code where mapping is supported.
{% enddocs %}

{% docs trades_col_source_table %}
Normalized label of the originating source family.
{% enddocs %}
