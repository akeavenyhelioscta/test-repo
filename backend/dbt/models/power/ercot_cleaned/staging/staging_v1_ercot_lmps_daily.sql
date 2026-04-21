{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ERCOT LMPs Daily
-- Grain: 1 row per date
-- Jinja loops: zones x markets x lmp_total with flat/onpeak/offpeak (7-22)
---------------------------

{% set zones = ['houston_hub', 'north_hub', 'south_hub', 'west_hub'] %}
{% set markets = ['da', 'rt', 'dart'] %}
{% set price_components = ['lmp_total'] %}
{% set onpeak_start = 7 %}
{% set onpeak_end = 22 %}

WITH daily AS (
    SELECT
        date

        {% for zone in zones %}
            {% for market in markets %}
                {% for price in price_components %}
                    {% if market == 'da' %}
                        ,AVG({{ market }}_{{ price }}_{{ zone }}) AS {{ market }}_{{ price }}_{{ zone }}_flat
                        ,AVG(CASE WHEN hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_onpeak
                        ,AVG(CASE WHEN hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_offpeak
                    {% else %}
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')::DATE THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_flat
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')::DATE AND hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_onpeak
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')::DATE AND hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_offpeak
                    {% endif %}
                {% endfor %}
            {% endfor %}
        {% endfor %}

    FROM {{ ref('staging_v1_ercot_lmps_hourly_wide') }}
    GROUP BY date
)

SELECT * FROM daily
