{% docs bbg_dapi_tickers_source %}

Bloomberg DAPI ticker registry. Each row represents one Bloomberg security
with its human-readable description.

**Raw columns:**
- `security` — Bloomberg ticker identifier (e.g., `CLA Comdty`)
- `description` — Human-readable security name
- `created_at` — Row creation timestamp
- `updated_at` — Row last-update timestamp

**Primary key:** `security`

**Ingestion:** Python script upserts into `bbg_dapi.bbg_tickers`.

{% enddocs %}


{% docs bbg_dapi_historical_source %}

Bloomberg DAPI historical data. Each row represents one data point for a
given security, date, snapshot timestamp, and data type.

**Raw columns:**
- `security` — Bloomberg ticker identifier (foreign key to `bbg_tickers`)
- `date` — Observation date
- `snapshot_at` — Timestamp when the data snapshot was taken
- `data_type` — Bloomberg field name (e.g., `PX_LAST`, `PX_OPEN`)
- `value` — Data point value
- `created_at` — Row creation timestamp
- `updated_at` — Row last-update timestamp

**Primary key:** `security` + `date` + `snapshot_at` + `data_type`

**Ingestion:** Python script upserts into `bbg_dapi.bbg_historical`.

{% enddocs %}
