{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT Fuel Mix Daily
-- Grain: 1 row per date x period
---------------------------

{% set onpeak_start = 7 %}
{% set onpeak_end = 22 %}

{% set fuel_types = [
    'nuclear', 'hydro', 'wind', 'solar', 'natural_gas', 'coal_and_lignite',
    'power_storage', 'other', 'storage_net_output', 'storage_total_charging',
    'storage_total_discharging', 'total', 'renewables', 'thermal',
    'gas_pct_thermal', 'coal_pct_thermal'
] %}

WITH HOURLY AS (
    SELECT * FROM {{ ref('staging_v1_ercot_fuel_mix_hourly') }}
),

---------------------------
-- FLAT (all hours)
---------------------------

FLAT AS (
    SELECT
        date
        ,'flat' AS period
        {% for fuel in fuel_types %}
        ,AVG({{ fuel }}) AS {{ fuel }}
        {% endfor %}
    FROM HOURLY
    GROUP BY date
),

---------------------------
-- ONPEAK (hours 7-22)
---------------------------

ONPEAK AS (
    SELECT
        date
        ,'onpeak' AS period
        {% for fuel in fuel_types %}
        ,AVG({{ fuel }}) AS {{ fuel }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
),

---------------------------
-- OFFPEAK (hours 1-6, 23-24)
---------------------------

OFFPEAK AS (
    SELECT
        date
        ,'offpeak' AS period
        {% for fuel in fuel_types %}
        ,AVG({{ fuel }}) AS {{ fuel }}
        {% endfor %}
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
),

---------------------------
-- UNION ALL PERIODS
---------------------------

DAILY AS (
    SELECT * FROM FLAT
    UNION ALL
    SELECT * FROM ONPEAK
    UNION ALL
    SELECT * FROM OFFPEAK
)

SELECT * FROM DAILY
