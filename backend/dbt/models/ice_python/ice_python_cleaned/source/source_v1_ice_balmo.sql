{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ICE BALMO SYMBOL MAPPING
---------------------------

{% set columns = [
    {'symbol': 'HHD B0-IUS',  'col': 'hh_balmo'},
    {'symbol': 'TRW B0-IUS',  'col': 'transco_st85_balmo'},
    {'symbol': 'CVK B0-IUS',  'col': 'pine_prarie_balmo'},
    {'symbol': 'UCS B0-IUS',  'col': 'houston_ship_channel_balmo'},
    {'symbol': 'WAS B0-IUS',  'col': 'waha_balmo'},
    {'symbol': 'NTS B0-IUS',  'col': 'ngpl_txok_balmo'},
    {'symbol': 'T5C B0-IUS',  'col': 'transco_zone_5_south_balmo'},
    {'symbol': 'TSS B0-IUS',  'col': 'tetco_m3_balmo'},
    {'symbol': 'ALS B0-IUS',  'col': 'agt_balmo'},
    {'symbol': 'IZS B0-IUS',  'col': 'iroquois_z2_balmo'},
    {'symbol': 'SCS B0-IUS',  'col': 'socal_cg_balmo'},
    {'symbol': 'PIG B0-IUS',  'col': 'pge_cg_balmo'},
    {'symbol': 'CRS B0-IUS',  'col': 'cig_balmo'},
    {'symbol': 'MTS B0-IUS',  'col': 'ngpl_midcon_balmo'},
    {'symbol': 'NMS B0-IUS',  'col': 'michcon_balmo'},
] %}

---------------------------
-- GAS DAY DATE SPINE
---------------------------

WITH DATES AS (
    SELECT
        gas_day,
        trade_date
    FROM {{ ref('utils_v1_ice_gas_day_dates_daily') }}
),

---------------------------
-- PIVOT ICE BALMO BY SYMBOL
---------------------------

ICEXL AS (
    SELECT
        trade_date::DATE AS trade_date

        {% for c in columns %}
            ,AVG(CASE WHEN symbol = '{{ c.symbol }}' THEN value END) AS {{ c.col }}
        {% endfor %}

    FROM {{ source('ice_python_v1', 'balmo_v1_2025_dec_16') }}
    GROUP BY trade_date
),

---------------------------
-- JOIN DATES WITH BALMO DATA
---------------------------

FINAL AS (
    SELECT
        dates.gas_day,
        dates.trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM DATES dates
    LEFT JOIN ICEXL icexl ON dates.trade_date = icexl.trade_date
)

SELECT * FROM FINAL
ORDER BY trade_date DESC
