{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Hourly LMPs (normalized)
-- Grain: 1 row per date × hour × hub × market
---------------------------

{% set hubs = [
    ('north_hub', 'SPPNORTH_HUB'),
    ('south_hub', 'SPPSOUTH_HUB'),
] %}
{% set markets = ['da', 'rt', 'dart'] %}

WITH WIDE AS (
    SELECT * FROM {{ ref('staging_v1_spp_lmps_hourly_wide') }}
),

LMPS AS (
    {% for suffix, name in hubs %}
    {% set outer_loop = loop %}
    {% for market in markets %}
    {% if not (outer_loop.first and loop.first) %}UNION ALL{% endif %}
    SELECT
        datetime
        ,date
        ,hour_ending
        ,'{{ name }}' AS hub
        ,'{{ market }}' AS market
        ,{{ market }}_lmp_total_{{ suffix }} AS lmp_total
        ,{{ market }}_lmp_system_energy_price_{{ suffix }} AS lmp_system_energy_price
        ,{{ market }}_lmp_congestion_price_{{ suffix }} AS lmp_congestion_price
        ,{{ market }}_lmp_marginal_loss_price_{{ suffix }} AS lmp_marginal_loss_price
    FROM WIDE
    {% endfor %}
    {% endfor %}
)

SELECT * FROM LMPS
ORDER BY date DESC, hour_ending DESC, hub, market
