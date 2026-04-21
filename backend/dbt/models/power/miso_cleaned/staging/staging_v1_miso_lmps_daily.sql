{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- MISO Daily LMPs
-- Grain: 1 row per date x period
-- Wide format: 8 hubs x 4 components x 3 markets = 96 price columns
-- ===========================

{% set onpeak_start = 8 %}
{% set onpeak_end = 23 %}

{% set zones = ['arkansas_hub', 'illinois_hub', 'indiana_hub', 'louisiana_hub', 'michigan_hub', 'minn_hub', 'ms_hub', 'texas_hub'] %}
{% set markets = ['da', 'rt', 'dart'] %}
{% set components = ['lmp_total', 'lmp_system_energy_price', 'lmp_congestion_price', 'lmp_marginal_loss_price'] %}

WITH hourly AS (
    SELECT *
    FROM {{ ref('staging_v1_miso_lmps_hourly_wide') }}
),

-- RT and DART should not include incomplete current day
hourly_filtered AS (
    SELECT
        date
        ,hour_ending
        {% for zone in zones %}
        {% for comp in components %}
        ,da_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% for zone in zones %}
        {% for comp in components %}
        ,CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')::DATE THEN rt_{{ comp }}_{{ zone }} END AS rt_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% for zone in zones %}
        {% for comp in components %}
        ,CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')::DATE THEN dart_{{ comp }}_{{ zone }} END AS dart_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
    FROM hourly
),

flat AS (
    SELECT
        date
        ,'flat' AS period
        {% for market in markets %}
        {% for zone in zones %}
        {% for comp in components %}
        ,AVG({{ market }}_{{ comp }}_{{ zone }}) AS {{ market }}_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% endfor %}
    FROM hourly_filtered
    GROUP BY date
),

onpeak AS (
    SELECT
        date
        ,'onpeak' AS period
        {% for market in markets %}
        {% for zone in zones %}
        {% for comp in components %}
        ,AVG({{ market }}_{{ comp }}_{{ zone }}) AS {{ market }}_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% endfor %}
    FROM hourly_filtered
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
),

offpeak AS (
    SELECT
        date
        ,'offpeak' AS period
        {% for market in markets %}
        {% for zone in zones %}
        {% for comp in components %}
        ,AVG({{ market }}_{{ comp }}_{{ zone }}) AS {{ market }}_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% endfor %}
    FROM hourly_filtered
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date
),

daily AS (
    SELECT * FROM flat
    UNION ALL
    SELECT * FROM onpeak
    UNION ALL
    SELECT * FROM offpeak
)

SELECT * FROM daily
