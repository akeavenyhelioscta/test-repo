{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- LATEST SNAPSHOT PER DAY
---------------------------
-- Filters to the last snapshot_at per trade_date so the full chain
-- view exposes one set of Greeks per symbol per day (the EOD values).
-- Grain: one row per trade_date x symbol.

WITH LATEST_SNAPSHOT AS (
    SELECT
        trade_date,
        MAX(snapshot_at) AS max_snapshot_at
    FROM {{ ref('source_v1_options_greeks') }}
    GROUP BY trade_date
),

FINAL AS (
    SELECT
        src.trade_date,
        src.snapshot_at,
        src.symbol,
        src.product,
        src.expiry_code,
        src.underlying,
        src.underlying_price,
        src.strike,
        src.expiration,
        src.option_type,
        src.days_to_expiry,
        src.settle,
        src.bid,
        src.ask,
        src.mid_price,
        src.last,
        src.volume,
        src.open_interest,
        src.delta,
        src.gamma,
        src.theta,
        src.vega,
        src.rho,
        src.pct_in_out_of_money
    FROM {{ ref('source_v1_options_greeks') }} src
    INNER JOIN LATEST_SNAPSHOT ls
        ON src.trade_date = ls.trade_date
       AND src.snapshot_at = ls.max_snapshot_at
)

SELECT * FROM FINAL
