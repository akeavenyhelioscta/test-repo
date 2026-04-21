{% docs miso_gridstatus_source %}

MISO data ingested via **GridStatus** — the open-source Python library wrapping
MISO public data feeds.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts in `backend/scrapes/power/gridstatus_open_source/miso/`.

**Included datasets:**
- Day-ahead hourly LMPs by location
- Real-time 5-minute LMPs by location

{% enddocs %}
