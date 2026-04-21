{% docs pjm_source %}

Raw PJM data ingested via **PJM Data Miner 2** and direct PJM API scrapes.

Tables in this source contain unmodified API responses stored in the `pjm` schema.
Data is pulled by scheduled Python scripts in `backend/scrapes/power/pjm/` and upserted
into Azure PostgreSQL.

**Included datasets:**
- Hourly load (metered, preliminary, instantaneous)
- Day-ahead demand bids (cleared load)
- Day-ahead and real-time LMPs (verified and unverified)
- 5-minute real-time LMPs (unverified, hub-only)
- Ancillary services prices (synchronized reserve, regulation, mileage ratio)
- Dispatched reserves (quantity, requirement, clearing price, shortage indicator)
- Operational reserves (real-time reserve MW by product)
- Real-time dispatched reserves (archival with deficit MW)
- 7-day load forecast
- 7-day outage forecast
- 5-minute tie flows

{% enddocs %}


{% docs gridstatus_source %}

PJM data ingested via **GridStatus** — both the open-source Python library and the
paid GridStatus.io API.

Tables in this source are stored in the `gridstatus` schema. Data is pulled by
scheduled Python scripts in `backend/scrapes/power/gridstatus_open_source/pjm/` and
`backend/scrapes/power/gridstatusio_api_key/pjm/`.

**Included datasets:**
- 7-day load forecast (regions pre-normalized to RTO, MIDATL, WEST, SOUTH)
- Hourly fuel mix by fuel type
- 2-day solar generation forecast (front-of-meter + behind-the-meter)
- 2-day wind generation forecast

{% enddocs %}
