{{
  config(
    materialized='ephemeral'
  )
}}

-- ===========================
-- SPP Hourly LMPs (DA + RT + DART)
-- Grain: 1 row per date x hour_ending
-- Wide format: 2 hubs x 4 components x 3 markets = 24 price columns
-- ===========================

{% set zones = ['north_hub', 'south_hub'] %}
{% set components = ['lmp_total', 'lmp_system_energy_price', 'lmp_congestion_price', 'lmp_marginal_loss_price'] %}

WITH da AS (
    SELECT * FROM {{ ref('source_v1_spp_da_hrl_lmps') }}
),

rt AS (
    SELECT * FROM {{ ref('source_v1_spp_rt_hrl_lmps') }}
),

joined AS (
    SELECT
        COALESCE(da.date, rt.date) AS date
        ,COALESCE(da.hour_ending, rt.hour_ending) AS hour_ending
        ,(COALESCE(da.date, rt.date) + (COALESCE(da.hour_ending, rt.hour_ending) || ' hours')::INTERVAL)::TIMESTAMP AS datetime
        {% for zone in zones %}
        {% for comp in components %}
        ,da.da_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% for zone in zones %}
        {% for comp in components %}
        ,rt.rt_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
        {% for zone in zones %}
        {% for comp in components %}
        ,(da.da_{{ comp }}_{{ zone }} - rt.rt_{{ comp }}_{{ zone }}) AS dart_{{ comp }}_{{ zone }}
        {% endfor %}
        {% endfor %}
    FROM da
    FULL OUTER JOIN rt ON da.date = rt.date AND da.hour_ending = rt.hour_ending
)

SELECT
    datetime
    ,date
    ,hour_ending
    {% for zone in zones %}
    {% for comp in components %}
    ,da_{{ comp }}_{{ zone }}
    ,rt_{{ comp }}_{{ zone }}
    ,dart_{{ comp }}_{{ zone }}
    {% endfor %}
    {% endfor %}
FROM joined
