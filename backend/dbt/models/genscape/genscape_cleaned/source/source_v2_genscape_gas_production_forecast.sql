{{
  config(
    materialized='incremental',
    unique_key=['report_date', 'date', 'region'],
    on_schema_change='sync_all_columns'
  )
}}

-------------------------------------------------------------
-- Source: Genscape Gas Production Forecast
-- Pivots raw item-value pairs into typed metric columns.
-- Grain: 1 row per report_date x date x region
-------------------------------------------------------------

WITH GENSCAPE_PROD_FORECAST AS (
    SELECT

        reportdate::DATE as report_date

        ,EXTRACT(YEAR from month::DATE) as year
        ,EXTRACT(MONTH from month::DATE) as month
        ,month::DATE as date

        ,region

        ,item
        ,value

    FROM {{ source('genscape_v2', 'gas_production_forecast_v2_2025_09_23') }}

    {% if is_incremental() %}
    WHERE reportdate::DATE > (SELECT MAX(report_date) FROM {{ this }})
    {% endif %}
),

-------------------------------------------------------------
-------------------------------------------------------------

GENSCAPE_PROD_FORECAST_ITEMS AS (
    SELECT

        report_date

        ,year
        ,month
        ,date

        ,region

        ,SUM(CASE WHEN item = 'Production' THEN value END) as production
        ,SUM(CASE WHEN item = 'Dry Gas Production YoY' THEN value END) as dry_gas_production_yoy
        ,SUM(CASE WHEN item = 'Oil Rig Count' THEN value END) as oil_rig_count
        ,SUM(CASE WHEN item = 'Gas Rig Count' THEN value END) as gas_rig_count
        ,SUM(CASE WHEN item = 'Dry Gas Production Actual' THEN value END) as dry_gas_production_actual
        ,SUM(CASE WHEN item = 'Wet Gas Production Actual' THEN value END) as wet_gas_production_actual
        ,SUM(CASE WHEN item = 'Wet Gas Production' THEN value END) as wet_gas_production

    FROM GENSCAPE_PROD_FORECAST
    GROUP BY report_date, year, month, date, region
),

FINAL AS (
    SELECT * FROM GENSCAPE_PROD_FORECAST_ITEMS
)

SELECT * FROM FINAL
ORDER BY report_date DESC, year DESC, month DESC, region DESC