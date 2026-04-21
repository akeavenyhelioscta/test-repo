{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- ISO-NE LMPs Daily
-- Grain: 1 row per date
-- Wide format: 1 hub x 4 components x 3 markets x 3 periods = 36 price columns
-- ===========================

{% set zones = ['internal_hub'] %}
{% set markets = ['da', 'rt', 'dart'] %}
{% set price_components = [
    'lmp_total',
    'lmp_system_energy_price',
    'lmp_congestion_price',
    'lmp_marginal_loss_price'
] %}
{% set onpeak_start = 8 %}
{% set onpeak_end = 23 %}

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
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_flat
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE AND hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_onpeak
                        ,AVG(CASE WHEN date < (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE AND hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }} THEN {{ market }}_{{ price }}_{{ zone }} END) AS {{ market }}_{{ price }}_{{ zone }}_offpeak
                    {% endif %}
                {% endfor %}
            {% endfor %}
        {% endfor %}

    FROM {{ ref('staging_v1_isone_lmps_hourly_wide') }}
    GROUP BY date
)

SELECT * FROM daily
