{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RAW OPTIONS GREEKS
---------------------------
-- Cleans raw ICE options_greeks table.
-- Parses symbol into product, expiry_code components.
-- Casts types and filters out rows with no Greeks.
-- Grain: one row per trade_date x snapshot_at x symbol.

WITH RAW AS (
    SELECT
        trade_date,
        snapshot_at,
        symbol,
        underlying,
        underlying_price,
        strike,
        expiration,
        option_type,
        settle,
        bid,
        ask,
        last,
        volume,
        open_interest,
        delta,
        gamma,
        theta,
        vega,
        rho,
        pct_in_out_of_money
    FROM {{ source('ice_python_v1', 'options_greeks') }}
    WHERE delta IS NOT NULL
       OR gamma IS NOT NULL
       OR theta IS NOT NULL
       OR vega  IS NOT NULL
       OR rho   IS NOT NULL
),

---------------------------
-- PARSE SYMBOL COMPONENTS
---------------------------

PARSED AS (
    SELECT
        *,

        -- product: everything before the first space (e.g. "HNG" from "HNG J26C3-IUS")
        SPLIT_PART(symbol, ' ', 1) AS product,

        -- expiry_code: first 3 chars of the second token (e.g. "J26" from "J26C3-IUS")
        LEFT(SPLIT_PART(symbol, ' ', 2), 3) AS expiry_code,

        -- mid_price
        CASE
            WHEN bid IS NOT NULL AND ask IS NOT NULL
            THEN (bid + ask) / 2.0
        END AS mid_price,

        -- days_to_expiry
        expiration - trade_date AS days_to_expiry

    FROM RAW
)

SELECT * FROM PARSED
