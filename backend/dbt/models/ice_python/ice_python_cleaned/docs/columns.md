{% docs ice_python_col_date %}
Calendar date. For daily models this is the trade date; for hourly models
this is the date component of the timestamp.
{% enddocs %}

{% docs ice_python_col_datetime %}
Hourly timestamp combining date and hour_ending. Used as the primary time
dimension in hourly models.
{% enddocs %}

{% docs ice_python_col_hour_ending %}
Hour ending (1-24). Hour ending 1 covers midnight to 1 AM; hour ending 24
covers 11 PM to midnight.
{% enddocs %}

{% docs ice_python_col_trade_date %}
The date on which the ICE trade was executed. For next-day gas, the trade
date is one day before the gas delivery day.
{% enddocs %}

{% docs ice_python_col_gas_day %}
The natural gas delivery date. Gas day = trade_date + 1 calendar day.
The gas day runs from 10:00 AM CT to 10:00 AM CT the following day.
{% enddocs %}

{% docs ice_python_col_hh_cash %}
Henry Hub next-day firm physical gas VWAP close price ($/MMBtu).
ICE symbol: XGF D1-IPG.
{% enddocs %}

{% docs ice_python_col_hh_balmo %}
Henry Hub balance-of-month gas swap settle price ($/MMBtu).
ICE symbol: HHD B0-IUS.
{% enddocs %}

