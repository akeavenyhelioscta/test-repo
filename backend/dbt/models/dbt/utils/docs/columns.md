{% docs utils_col_date %}
Calendar date. The primary grain of the daily date spine.
{% enddocs %}

{% docs utils_col_year %}
Four-digit calendar year extracted from the date.
{% enddocs %}

{% docs utils_col_year_month %}
First day of the month (`YYYY-MM-01`) for the given date. Useful for monthly grouping.
{% enddocs %}

{% docs utils_col_summer_winter %}
Season classification: `SUMMER` (Apr–Oct) or `WINTER` (Nov–Mar).
{% enddocs %}

{% docs utils_col_summer_winter_yyyy %}
Season strip code combining season and delivery year.
`JV-YY` for summer, `XH-YY` for winter (year of the winter's end).
{% enddocs %}

{% docs utils_col_month %}
Numeric month (1–12) extracted from the date.
{% enddocs %}

{% docs utils_col_mm_dd %}
Month-day string in `MM-DD` format, useful for year-over-year comparisons.
{% enddocs %}

{% docs utils_col_mm_dd_cy %}
The `MM-DD` mapped onto the current calendar year as a `DATE`.
Enables overlaying historical patterns on the current year.
{% enddocs %}

{% docs utils_col_eia_storage_week %}
The Friday date that ends the EIA natural gas storage reporting week
containing this date. EIA weeks run Friday-to-Thursday.
{% enddocs %}

{% docs utils_col_eia_storage_week_number %}
ISO week number of the EIA storage week Friday.
{% enddocs %}

{% docs utils_col_day_of_week %}
Full day name (e.g., `Monday`, `Tuesday`).
{% enddocs %}

{% docs utils_col_day_of_week_number %}
Numeric day of week (0 = Sunday, 6 = Saturday).
{% enddocs %}

{% docs utils_col_is_weekend %}
Weekend flag: `1` if Saturday or Sunday, `0` otherwise.
{% enddocs %}

{% docs utils_col_is_nerc_holiday %}
NERC holiday flag. In the daily model: `1` if the date is a NERC holiday, `0` otherwise.
In the weekly model: count of NERC holidays within the EIA storage week.
{% enddocs %}

{% docs utils_col_nerc_holiday %}
The NERC holiday date. One of six annual US electricity market holidays:
New Year's Day, Memorial Day, Independence Day, Labor Day, Thanksgiving, Christmas.
{% enddocs %}
