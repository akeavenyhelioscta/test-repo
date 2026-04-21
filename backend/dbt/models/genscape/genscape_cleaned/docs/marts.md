{% docs genscape_gas_production_forecast_weekly_mart %}

## Gas Production Forecast (Weekly) — Mart

Interim weekly forecast updates only — excludes official monthly report dates (defined
in the `genscape_gas_production_forecast_report_dates` seed). The two views (weekly and
monthly) are mutually exclusive.

### Grain
One row per **year x month x date x report_date** — each forecast period can have
multiple revisions (one per weekly report date).

### Key Consumers
- Trading desk: tracking week-over-week forecast revisions
- Downstream dbt queries joining on `date` and `report_date`

{% enddocs %}

{% docs genscape_gas_production_forecast_monthly_mart %}

## Gas Production Forecast (Monthly) — Mart

Official published monthly report releases only, filtered using the
`genscape_gas_production_forecast_report_dates` seed. Matches the "Spring Rock Natural
Gas Production Forecast" PDF report cadence (~every 6-8 weeks, irregular schedule).
Revision numbers are recalculated scoped to monthly reportDates only.

**The seed must be manually updated when new official reports are published.**

### Grain
One row per **year x month x date x report_date** — each forecast period can have
multiple revisions (one per official monthly report date).

### Key Consumers
- Trading desk: official gas supply outlook aligned with published reports
- Downstream dbt queries joining on `date` and `report_date`

{% enddocs %}

{% docs genscape_daily_pipeline_production_mart %}

## Daily Pipeline Production — Mart

Consumer-facing view of daily dry gas pipeline production estimates with 22 regional
columns, 5 computed composites, Permian flaring metrics, and revision tracking.
Direct pass-through from the staging model.

### Grain
One row per **date x report_date** — each production date can have multiple revisions.

### Key Consumers
- Trading desk: actual vs forecast production comparison, supply trend monitoring
- Downstream dbt queries joining on `date`

{% enddocs %}

{% docs genscape_daily_power_estimate_mart %}

## Daily Power Estimate — Mart

Consumer-facing view of daily power generation burn estimates by region and model type.
Direct pass-through from the staging model.

### Grain
One row per **gas_day x power_burn_variable x model_type_based_on_noms**.

### Key Consumers
- Trading desk: gas-to-power demand tracking, gas/power spread analysis
- Downstream dbt queries joining on `gas_day`

{% enddocs %}
