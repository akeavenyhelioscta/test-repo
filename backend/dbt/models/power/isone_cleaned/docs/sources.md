{% docs isone_source %}

ISO-NE data ingested via direct ISO-NE API — day-ahead hourly LMPs and real-time
hourly LMPs (final + preliminary) by location.

Tables in this source are stored in the `isone` schema. Data is pulled by
scheduled Python scripts and upserted into Azure PostgreSQL.

**Included datasets:**
- Day-ahead hourly LMPs (Internal Hub)
- Real-time hourly LMPs — final/verified (Internal Hub)
- Real-time hourly LMPs — preliminary (Internal Hub, last 10 days)

{% enddocs %}
