{% docs trades_overview %}

# Trades Cleaned

This domain standardizes daily and intraday trade feeds from Clear Street and
allocated-trade files from Marex into analysis-ready marts for execution tracking,
product exposure, and account-level aggregation.

## Sources

- `clear_street_v2.helios_transactions_v2_2026_feb_23`
- `clear_street_v2.helios_intraday_transactions_v2_2026_feb_23`
- `marex_v2.helios_allocated_trades_v2_2026_feb_23`

## Layer Design

- `source/`: source-specific normalization and typing (ephemeral)
- `staging/`: enrichments for account/product mappings and external symbols (ephemeral)
- `marts/`: consumer-facing daily/intraday/grouped trade views (view)

Only models under `marts/` are exposed as database objects.

{% enddocs %}
