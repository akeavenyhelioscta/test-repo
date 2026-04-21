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
    ('capitl', 'CAPITL'),
    ('centrl', 'CENTRL'),
    ('dunwod', 'DUNWOD'),
    ('genese', 'GENESE'),
    ('hq', 'H Q'),
    ('hud_vl', 'HUD VL'),
    ('longil', 'LONGIL'),
    ('mhk_vl', 'MHK VL'),
    ('millwd', 'MILLWD'),
    ('north', 'NORTH'),
    ('npx', 'NPX'),
    ('nyc', 'N.Y.C.'),
    ('oh', 'O H'),
    ('pjm', 'PJM'),
    ('west', 'WEST'),
] %}
{% set markets = ['da', 'rt', 'dart'] %}

WITH WIDE AS (
    SELECT * FROM {{ ref('staging_v1_nyiso_lmps_hourly_wide') }}
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
