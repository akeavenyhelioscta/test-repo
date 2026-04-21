{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DAILY OPTIONS SUMMARY
---------------------------
-- Aggregates the latest-snapshot chain into one row per
-- trade_date x underlying x expiry_code.
-- Computes put/call volume, OI, P/C ratios, and ATM delta.
-- Grain: one row per trade_date x underlying x expiry_code.

WITH CHAIN AS (
    SELECT * FROM {{ ref('staging_v1_options_greeks') }}
),

---------------------------
-- ATM CALL (closest to 0.50 delta)
---------------------------

ATM_CALL AS (
    SELECT DISTINCT ON (trade_date, underlying, expiry_code)
        trade_date,
        underlying,
        expiry_code,
        delta   AS atm_call_delta,
        strike  AS atm_strike,
        settle  AS atm_call_settle,
        bid     AS atm_call_bid,
        ask     AS atm_call_ask,
        gamma   AS atm_gamma,
        theta   AS atm_theta,
        vega    AS atm_vega
    FROM CHAIN
    WHERE option_type = 'call'
      AND delta IS NOT NULL
    ORDER BY
        trade_date,
        underlying,
        expiry_code,
        ABS(delta - 0.50)
),

---------------------------
-- AGGREGATE BY EXPIRATION
---------------------------

AGGREGATED AS (
    SELECT
        trade_date,
        underlying,
        expiry_code,
        MIN(expiration)     AS expiration,
        MAX(underlying_price) AS underlying_price,
        MAX(days_to_expiry) AS days_to_expiry,

        -- Volume
        COALESCE(SUM(CASE WHEN option_type = 'call' THEN volume END), 0) AS call_volume,
        COALESCE(SUM(CASE WHEN option_type = 'put'  THEN volume END), 0) AS put_volume,
        COALESCE(SUM(volume), 0) AS total_volume,

        -- Open interest
        COALESCE(SUM(CASE WHEN option_type = 'call' THEN open_interest END), 0) AS call_oi,
        COALESCE(SUM(CASE WHEN option_type = 'put'  THEN open_interest END), 0) AS put_oi,
        COALESCE(SUM(open_interest), 0) AS total_oi,

        -- Strike counts
        COUNT(DISTINCT CASE WHEN option_type = 'call' THEN strike END) AS call_strike_count,
        COUNT(DISTINCT CASE WHEN option_type = 'put'  THEN strike END) AS put_strike_count,

        -- Weighted average strike by OI
        CASE
            WHEN SUM(open_interest) > 0
            THEN SUM(strike * COALESCE(open_interest, 0)) / SUM(COALESCE(open_interest, 0))
        END AS oi_weighted_avg_strike

    FROM CHAIN
    GROUP BY trade_date, underlying, expiry_code
),

---------------------------
-- FINAL: JOIN ATM + RATIOS
---------------------------

FINAL AS (
    SELECT
        a.trade_date,
        a.underlying,
        a.expiry_code,
        a.expiration,
        a.underlying_price,
        a.days_to_expiry,

        -- ATM
        atm.atm_strike,
        atm.atm_call_delta,
        atm.atm_call_settle,
        atm.atm_call_bid,
        atm.atm_call_ask,
        atm.atm_gamma,
        atm.atm_theta,
        atm.atm_vega,

        -- Volume
        a.call_volume,
        a.put_volume,
        a.total_volume,
        CASE
            WHEN a.call_volume > 0
            THEN a.put_volume::FLOAT / a.call_volume
        END AS pc_volume_ratio,

        -- Open interest
        a.call_oi,
        a.put_oi,
        a.total_oi,
        CASE
            WHEN a.call_oi > 0
            THEN a.put_oi::FLOAT / a.call_oi
        END AS pc_oi_ratio,

        -- Strikes
        a.call_strike_count,
        a.put_strike_count,
        a.oi_weighted_avg_strike

    FROM AGGREGATED a
    LEFT JOIN ATM_CALL atm
        ON a.trade_date  = atm.trade_date
       AND a.underlying  = atm.underlying
       AND a.expiry_code = atm.expiry_code
)

SELECT * FROM FINAL
