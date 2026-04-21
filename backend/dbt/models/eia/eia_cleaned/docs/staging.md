{% docs eia_930_staging_hourly %}

Hourly EIA-930 generation data transformed from UTC to Eastern Prevailing Time,
with respondent codes normalized and fuel types aggregated.

**Key transformations:**

1. **UTC → EST conversion** — Converts `datetime_utc` to Eastern Prevailing Time
   using PostgreSQL `AT TIME ZONE 'America/New_York'`, DST-aware. Subtracts 1 hour
   to align with hour-ending convention.

2. **Respondent normalization** — Maps EIA respondent codes to standard ISO names:
   `ISNE` → `ISONE`, `NYIS` → `NYISO`, `ERCO` → `ERCOT`, `CISO` → `CAISO`.

3. **DST aggregation** — Uses `AVG()` grouped by EST datetime to handle DST
   fall-back duplicate hours.

4. **Composite metrics** — Computes `total` (all fuels), `renewables` (wind + solar),
   and `thermal` (gas + coal) with `COALESCE(..., 0)` for NULL safety.

5. **Respondent join** — Left joins with the respondent lookup utility to add
   `is_iso`, `time_zone`, `region`, and `balancing_authority_name` dimensions.

**Grain:** One row per EST hour per respondent.

{% enddocs %}


{% docs eia_nat_gas_consumption_staging %}

Monthly EIA natural gas consumption by end use, with area names standardized
and consumption categories pivoted into columns.

**Key transformations:**

1. **Period parsing** — Splits `YYYY-MM` period string into integer `year` and
   `month` columns.

2. **Area name standardization** — Joins with the area name lookup utility to
   map EIA area codes to full uppercase state names (e.g., `USA-AL` → `ALABAMA`,
   `U.S.` → `US48`).

3. **Consumption type standardization** — Maps raw `process_name` values to
   shorter labels (e.g., `Residential Consumption` → `Residential`).

4. **Pivot** — Transforms the row-per-category structure into one row per
   year/month/area with separate columns for each end-use category. Uses
   `AVG()` as the aggregate function to handle potential duplicates safely.

**Grain:** One row per year × month × state/area × consumption unit.

{% enddocs %}
