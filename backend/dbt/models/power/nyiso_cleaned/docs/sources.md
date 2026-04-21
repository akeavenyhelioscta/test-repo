{% docs nyiso_gridstatus_source %}

NYISO data ingested via **GridStatus** open-source library.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts in `backend/scrapes/power/gridstatus_open_source/nyiso/`.

**Included datasets:**
- Day-ahead hourly LMPs by zone
- Real-time 5-minute LMPs by zone

{% enddocs %}
