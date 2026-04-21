# positions_cleaned marts

## Purpose

Consumer-facing views and tables combining Marex and NAV SFTP position files into a consistent options/futures positions mart for daily exposure, risk, and PnL workflow on the HeliosCTA desk.

## Grain

| Model | Grain |
|-------|-------|
| `nav_positions` | `sftp_date x contract_yyyymm x account x source row` |
| `nav_positions_agr` | `sftp_date x contract_yyyymm x source row` (AGR account) |
| `nav_positions_moross` | `sftp_date x contract_yyyymm x source row` (MOROSS account) |
| `nav_positions_pnt` | `sftp_date x contract_yyyymm x source row` (PNT account) |
| `nav_positions_titan` | `sftp_date x contract_yyyymm x source row` (TITAN account) |
| `marex_positions` | `sftp_date x contract_yyyymm x source row` |
| `marex_and_nav_positions_grouped` | `sftp_date x exchange_code x contract_yyyymm x is_option x put_call x strike_price` |
| `marex_and_nav_positions_grouped_latest` | Same as grouped but filtered to latest `sftp_date` with day-over-day PnL |

## Source Relations

| Source | Upstream Model |
|--------|---------------|
| Marex SFTP | `source_v5_marex_positions` |
| NAV SFTP (AGR, MOROSS, PNT, TITAN) | `staging_v5_nav_positions`, `source_v5_nav_positions_agr`, `source_v5_nav_positions_moross`, `source_v5_nav_positions_pnt`, `source_v5_nav_positions_titan` |
| Combined staging | `staging_v5_marex_and_nav_positions` |

## Key Columns

| Column | Description |
|--------|-------------|
| `sftp_date` | File delivery date |
| `exchange_code` | Product exchange code |
| `exchange_code_grouping` | Product grouping (e.g., NG, CL) |
| `exchange_code_region` | Regional designation |
| `contract_yyyymm` | Contract month |
| `is_option` / `put_call` / `strike_price` | Option dimensions |
| `marex_delta` | Option delta (averaged in grouped models) |
| `qty_total` | Total net quantity across all accounts |
| `qty_acim`, `qty_andy`, `qty_mac`, `qty_pnt`, `qty_dickson`, `qty_titan` | Account-level net quantities |
| `settlement_price` / `trade_price` / `market_value` | Price and valuation columns |
| `daily_pnl_total` | Day-over-day PnL (latest model only) |
| `dod_qty_total` | Day-over-day quantity change (latest model only) |

## Transformation Notes

- Individual position views (`nav_positions`, `marex_positions`, account-specific) are materialized as **views**.
- `marex_and_nav_positions_grouped` is materialized as a **table** for performance (complex GROUP BY with 6 account-level rollups).
- `marex_and_nav_positions_grouped_latest` uses LAG window functions to compute day-over-day changes in settlement prices, deltas, quantities, and PnL.
- Forward-fill logic, account/product enrichment, and symbol generation all live in the staging layer.

## Data Quality Checks

- `not_null` on `sftp_date`, `exchange_code`, `contract_yyyymm`, `is_option` in `schema.yml`.
- `accepted_values` on `is_option`: `['Y', 'N']`.
- `accepted_values` on `put_call`: `['P', 'C', null]`.
- Account-level quantity columns validated against total: sum of account quantities equals `qty_total`.
