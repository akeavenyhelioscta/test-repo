{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- CAISO LMPs Daily
-- ===========================

{% set zones = ['np15_hub', 'sp15_hub'] %}
{% set markets = ['da', 'rt', 'dart'] %}
{% set price_components = [
    'lmp_total',
    'lmp_system_energy_price',
    'lmp_congestion_price',
    'lmp_marginal_loss_price'
] %}
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
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Pacific')::DATE THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_flat
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Pacific')::DATE AND hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_onpeak
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Pacific')::DATE AND hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_offpeak
                    {% endif %}
                {% endfor %}
            {% endfor %}
        {% endfor %}

    FROM {{ ref('staging_v1_caiso_lmps_hourly_wide') }}
    GROUP BY date
)

SELECT * FROM daily
