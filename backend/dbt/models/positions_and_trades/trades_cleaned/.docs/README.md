# trades_cleaned marts

## Purpose

Consumer-facing views standardizing daily and intraday trade feeds from Clear Street and allocated-trade files from Marex for execution tracking, product exposure, and account-level aggregation.

## Grain

| Model | Grain |
|-------|-------|
| `clear_street_trades` | `sftp_date x sftp_upload_timestamp x trade row` |
| `clear_street_intraday_trades` | `sftp_date x sftp_upload_timestamp x trade row` |
| `marex_allocated_trades` | `sftp_date x sftp_upload_timestamp x trade row` |
| `clear_street_trades_grouped` | `sftp_date x product_code x contract_yyyymm x is_option x put_call x strike_price` |
| `clear_street_intraday_trades_grouped` | `sftp_date x product_code x contract_yyyymm x is_option x put_call x strike_price` |
| `marex_allocated_trades_grouped` | `sftp_date x product_code x contract_yyyymm x is_option x put_call x strike_price` |

## Source Relations

| Source | Upstream Model |
|--------|---------------|
| Clear Street daily | `staging_v2_clear_street_trades_2_product_codes` |
| Clear Street intraday | `staging_v2_clear_street_intraday_2_product_codes` |
| Marex allocated trades | `staging_v2_marex_allocated_trades_2_product_codes` |

## Key Columns

| Column | Description |
|--------|-------------|
| `sftp_date` | File delivery date |
| `sftp_upload_timestamp` | Upload timestamp for ordering |
| `product_code_grouping` | Product grouping (e.g., NG, CL) |
| `product_code_region` | Regional designation |
| `contract_yyyymm` | Contract month |
| `is_option` / `put_call` / `strike_price` | Option dimensions |
| `qty` / `qty_total` | Trade quantity (individual / grouped total) |
| `settlement_price` | Settlement price (Clear Street models) |
| `trade_price` | Execution price |
| `qty_acim`, `qty_andy`, `qty_mac`, `qty_pnt`, `qty_dickson`, `qty_titan` | Account-level quantities (grouped models) |

## Transformation Notes

- Individual trade views are materialized as **views** ordered by `sftp_date DESC, sftp_upload_timestamp DESC, product_code_grouping, product_code_region`.
- Grouped models aggregate by product and contract dimensions with account-level quantity and price rollups for 6 accounts (ACIM, ANDY, MAC, PNT, DICKSON, TITAN).
- Grouped models filter out rows where `account_name IS NULL`.
- Marex grouped model excludes `settlement_price` (not provided in source).
- Product code enrichment and account mappings live in the staging layer.

## Data Quality Checks

- `not_null` on `sftp_date`, `sftp_upload_timestamp`, `product_code_grouping`, `is_option` in `schema.yml`.
- `accepted_values` on `is_option`: `['Y', 'N']`.
- `accepted_values` on `put_call`: `['P', 'C', null]`.
- Schema tests defined in `schema.yml` for all 6 mart models.
