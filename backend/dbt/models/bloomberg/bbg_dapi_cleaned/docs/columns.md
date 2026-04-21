{% docs bbg_dapi_col_security %}
Bloomberg ticker identifier (e.g., `CLA Comdty`). This is the primary key
in the tickers table and the foreign key in the historical table.
{% enddocs %}

{% docs bbg_dapi_col_description %}
Human-readable name for the Bloomberg security, sourced from the
`bbg_tickers` table. May be NULL for securities not yet registered
in the ticker registry.
{% enddocs %}

{% docs bbg_dapi_col_date %}
Observation date for the historical data point.
{% enddocs %}

{% docs bbg_dapi_col_snapshot_at %}
Timestamp indicating when the Bloomberg data snapshot was captured.
Part of the composite grain along with `security`, `date`, and `data_type`.
{% enddocs %}

{% docs bbg_dapi_col_revision %}
Revision number within each (`date`, `security`, `description`, `data_type`)
group, ordered by `snapshot_at` ascending. Revision 1 is the oldest snapshot;
revision equal to `max_revision` is the latest.
{% enddocs %}

{% docs bbg_dapi_col_max_revision %}
Total number of revisions (snapshots) for each (`date`, `security`,
`description`, `data_type`) group. Use `WHERE revision = max_revision`
to get the latest snapshot only.
{% enddocs %}

{% docs bbg_dapi_col_data_type %}
Bloomberg field name identifying the type of data point (e.g., `PX_LAST`,
`PX_OPEN`, `PX_HIGH`, `PX_LOW`, `PX_VOLUME`). Kept in long-form to
support flexible downstream pivoting.
{% enddocs %}

{% docs bbg_dapi_col_value %}
The observed data point value for the given security, date, snapshot, and
data type. Cast to NUMERIC for consistent arithmetic operations.
{% enddocs %}

{% docs bbg_dapi_col_created_at %}
Timestamp when the row was first inserted into the raw table by the
ingestion pipeline.
{% enddocs %}

{% docs bbg_dapi_col_updated_at %}
Timestamp when the row was last updated in the raw table by the
ingestion pipeline. Used as the primary tiebreaker for deduplication.
{% enddocs %}
