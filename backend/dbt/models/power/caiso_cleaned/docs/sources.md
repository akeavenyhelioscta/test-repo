{% docs caiso_source %}

Raw CAISO data ingested via **GridStatus** open-source Python library.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts in `backend/scrapes/power/gridstatus_open_source/caiso/`.

**Included datasets:**
- Day-ahead hourly LMPs by pricing hub
- Real-time 15-minute LMPs by pricing hub

{% enddocs %}
