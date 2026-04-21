{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Hourly LMPs (normalized)
-- Grain: 1 row per date × hour × hub × market
-- Note: ERCOT only publishes lmp_total (no energy/congestion/loss decomposition)
---------------------------

{% set hubs = [
    ('houston_hub', 'HB_HOUSTON'),
    ('north_hub', 'HB_NORTH'),
    ('south_hub', 'HB_SOUTH'),
    ('west_hub', 'HB_WEST'),
] %}
{% set markets = ['da', 'rt', 'dart'] %}

WITH WIDE AS (
    SELECT * FROM {{ ref('staging_v1_ercot_lmps_hourly_wide') }}
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
        ,NULL::NUMERIC AS lmp_system_energy_price
        ,NULL::NUMERIC AS lmp_congestion_price
        ,NULL::NUMERIC AS lmp_marginal_loss_price
    FROM WIDE
    {% endfor %}
    {% endfor %}
)

SELECT * FROM LMPS
ORDER BY date DESC, hour_ending DESC, hub, market
