{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- FORWARD-FILL BALMO PRICES
---------------------------

{% set columns = [
    {'col': 'hh_balmo',                    'grp': 'grp_hh'},
    {'col': 'transco_st85_balmo',          'grp': 'grp_transco'},
    {'col': 'pine_prarie_balmo',           'grp': 'grp_pine'},
    {'col': 'houston_ship_channel_balmo',  'grp': 'grp_hsc'},
    {'col': 'waha_balmo',                  'grp': 'grp_waha'},
    {'col': 'ngpl_txok_balmo',             'grp': 'grp_ngpl'},
    {'col': 'transco_zone_5_south_balmo',  'grp': 'grp_transco_zone'},
    {'col': 'tetco_m3_balmo',              'grp': 'grp_tetco'},
    {'col': 'agt_balmo',                   'grp': 'grp_agt'},
    {'col': 'iroquois_z2_balmo',           'grp': 'grp_iroquois'},
    {'col': 'socal_cg_balmo',              'grp': 'grp_socal'},
    {'col': 'pge_cg_balmo',               'grp': 'grp_pge'},
    {'col': 'cig_balmo',                   'grp': 'grp_cig'},
    {'col': 'ngpl_midcon_balmo',           'grp': 'grp_ngpl_midcon'},
    {'col': 'michcon_balmo',               'grp': 'grp_michcon'},
] %}

---------------------------
-- SOURCE DATA
---------------------------

WITH DAILY AS (
    SELECT
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM {{ ref('source_v1_ice_balmo') }}
),

---------------------------
-- NULL GROUP BOUNDARIES
---------------------------

GROUPED_DATA AS (
    SELECT
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
            ,SUM(CASE WHEN {{ c.col }} IS NOT NULL THEN 1 ELSE 0 END) OVER (ORDER BY trade_date) AS {{ c.grp }}
        {% endfor %}

    FROM DAILY
),

---------------------------
-- FORWARD FILL
---------------------------

FILLED_DATA AS (
    SELECT
        gas_day,
        trade_date

        {% for c in columns %}
            ,FIRST_VALUE({{ c.col }}) OVER (PARTITION BY {{ c.grp }} ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ c.col }}
        {% endfor %}

    FROM GROUPED_DATA
),

---------------------------
-- FILTER TO VALID RANGE
---------------------------

FINAL AS (
    SELECT
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM FILLED_DATA
    WHERE trade_date <= (SELECT MAX(trade_date) FROM {{ ref('source_v1_ice_balmo') }} WHERE hh_balmo IS NOT NULL)
)

SELECT * FROM FINAL
ORDER BY trade_date DESC
