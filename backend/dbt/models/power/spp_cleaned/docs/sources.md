{% docs spp_source %}

SPP data ingested via **GridStatus** — day-ahead hourly LMPs and real-time 5-minute
LMPs by location.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts and upserted into Azure PostgreSQL.

**Included datasets:**
- Day-ahead hourly LMPs (15 locations)
- Real-time 5-minute LMPs (15 locations)

{% enddocs %}
