{% docs genscape_col_date %}
The forecast target date, derived from the `month` field in the raw data.
Represents the monthly period being forecasted.
{% enddocs %}

{% docs genscape_col_report_date %}
The date the forecast report was issued. Multiple reports can exist for the same
target `date`, tracked via `revision` and `max_revision`.
{% enddocs %}

{% docs genscape_col_region %}
Granular Genscape production region. One of 67 values spanning US states,
Texas Railroad Commission districts, sub-basins (Permian, San Juan, Rockies),
Canadian provinces, and aggregate regions (Lower 48, United States).
{% enddocs %}

{% docs genscape_col_revision %}
Sequential revision number for a given forecast date. `1` = oldest report,
`max_revision` = most recent. Computed via `ROW_NUMBER() OVER (PARTITION BY date
ORDER BY report_date)`.
{% enddocs %}

{% docs genscape_col_production %}
Forecasted gas production for the region/tier. Units vary by Genscape reporting
convention.
{% enddocs %}

{% docs genscape_col_dry_gas_production_yoy %}
Year-over-year percentage change in dry gas production for the region/tier.
{% enddocs %}

{% docs genscape_col_oil_rig_count %}
Count of operational oil drilling rigs in the region/tier.
{% enddocs %}

{% docs genscape_col_gas_rig_count %}
Count of operational gas drilling rigs in the region/tier.
{% enddocs %}

{% docs genscape_dpp_col_date %}
The actual production date. Represents the day for which pipeline flow-based
gas production was estimated.
{% enddocs %}

{% docs genscape_dpp_col_lower_48 %}
Daily dry gas production for the Lower 48 US aggregate (MMCF/d).
Pre-aggregated by Genscape from pipeline flow monitoring.
{% enddocs %}

{% docs genscape_dpp_col_gulf_of_mexico %}
Daily dry gas production for the Gulf of Mexico (MMCF/d).
Pre-aggregated by Genscape.
{% enddocs %}

{% docs genscape_dpp_col_texas %}
Daily dry gas production for Texas aggregate (MMCF/d).
Pre-aggregated by Genscape.
{% enddocs %}

{% docs genscape_dpp_col_san_juan %}
Daily dry gas production for the San Juan Basin (MMCF/d).
Pre-aggregated by Genscape.
{% enddocs %}

{% docs genscape_dpp_col_west %}
Daily dry gas production for the Western US aggregate (MMCF/d).
Pre-aggregated by Genscape.
{% enddocs %}

{% docs genscape_dpp_col_western_canada %}
Daily dry gas production for Western Canada aggregate (MMCF/d).
Pre-aggregated by Genscape.
{% enddocs %}

{% docs genscape_dpe_col_gasday %}
The gas day for which the power burn estimate applies. Represents the
24-hour gas flow day (typically 10:00 AM CT to 10:00 AM CT).
{% enddocs %}

{% docs genscape_dpe_col_power_burn_variable %}
The power burn metric being reported. Derived from the Genscape API response
columns (e.g., power burn volume, power burn estimate).
{% enddocs %}

{% docs genscape_dpe_col_modeltype %}
The model type that produced the estimate (e.g., forecast vs actuals).
Part of the primary key to differentiate model versions for the same gas day.
{% enddocs %}
