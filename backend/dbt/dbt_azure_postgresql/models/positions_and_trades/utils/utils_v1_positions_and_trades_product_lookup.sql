{{
  config(
    materialized='ephemeral'
  )
}}

-------------------------------------------------------------
-- DA-scoped product lookup. PJM DA products only.
-- PJL kept as future-scope insurance for an off-peak DA scorecard;
-- expand the source filter in source_v2_clear_street_trades.sql to
-- enable trades for it. See sister repo helioscta-backend for the
-- full multi-product lookup.
-------------------------------------------------------------

SELECT * FROM (
    VALUES
        (NULL, 'PDA', 'POWER_FUTURES', 'PJM', 'PJM WEST DAY AHEAD PK DA'),
        (NULL, 'PDO', 'POWER_FUTURES', 'PJM', 'PJM WH DA OFF-PEAK WEEKEND'),
        (NULL, 'PJL', 'POWER_FUTURES', 'PJM', 'PJM WST HUB D APDM FP FU')
) AS lookup_data(
    bbg_exchange_code
    ,exchange_code
    ,exchange_code_grouping
    ,exchange_code_region
    ,marex_product
)
