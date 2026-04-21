{% docs ice_python_next_day_gas_source %}

Raw ICE next-day firm physical gas prices ingested from the ICE Python API.

Data is stored in the `ice_python` schema as `next_day_gas_v1_2025_dec_16`.
Each row represents one trade at one hub with symbol, value, and trade date.

**Raw columns:**
- `trade_date` — Date the trade was executed
- `symbol` — ICE instrument symbol (e.g., `XGF D1-IPG` for Henry Hub)
- `data_type` — Price type identifier
- `value` — Trade price ($/MMBtu)
- `created_at` — Row creation timestamp
- `updated_at` — Row last-update timestamp

**Primary key:** `trade_date` + `symbol`

**Ingestion:** Python script `backend/scrapes/ice_python/next_day_gas/next_day_gas_v1_2025_dec_16.py`
upserts into Azure PostgreSQL.

{% enddocs %}

{% docs ice_python_balmo_source %}

Raw ICE balance-of-month gas swap settle prices ingested from the ICE Python API.

Data is stored in the `ice_python` schema as `balmo_v1_2025_dec_16`.
Each row represents one BALMO settle at one hub.

**Raw columns:**
- `trade_date` — Date the settle was recorded
- `symbol` — ICE instrument symbol (e.g., `HHD B0-IUS` for Henry Hub BALMO)
- `data_type` — Price type identifier
- `value` — Settle price ($/MMBtu)
- `created_at` — Row creation timestamp
- `updated_at` — Row last-update timestamp

**Primary key:** `trade_date` + `symbol`

**Ingestion:** Python script `backend/scrapes/ice_python/balmo/balmo_v1_2025_dec_16.py`
upserts into Azure PostgreSQL.

{% enddocs %}

