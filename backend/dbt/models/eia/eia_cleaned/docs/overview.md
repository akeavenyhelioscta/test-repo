{% docs eia_overview %}

# EIA-930 Hourly Electric Grid Monitor

This dbt module transforms raw EIA-930 data into analysis-ready mart views
for the HeliosCTA power trading desk.

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **EIA-930** | Hourly generation by fuel type for all US balancing authorities | Python scrape (`backend/scrapes/eia/`) into `eia` schema |

## Geographic Hierarchy

EIA-930 data covers 60+ balancing authorities organized into 14 regions:

- **US48** — Lower 48 aggregate
- **NE** — ISO New England (ISONE)
- **NY** — New York ISO (NYISO)
- **MIDW** — Midwest (MISO, AECI, LGEE)
- **MIDA** — Mid-Atlantic (PJM)
- **TEN** — Tennessee (TVA)
- **CAR** — Carolinas (Duke, Dominion SC, etc.)
- **SE** — Southeast (Southern Company, SEPA)
- **FLA** — Florida (FPL, Duke FL, etc.)
- **CENT** — Central (SPP, SPA)
- **TEX** — Texas (ERCOT)
- **NW** — Northwest (BPA, PacifiCorp, etc.)
- **SW** — Southwest (APS, SRP, PNM, etc.)
- **CAL** — California (CAISO, LADWP, BANC, etc.)

## Pipeline Architecture

```
source/          Raw EIA-930 table extraction (ephemeral)
  |
  v
utils/           Respondent lookup table (ephemeral)
  |
  v
staging/         UTC→EST conversion, respondent normalization,
                 hourly aggregation, respondent join (ephemeral)
  |
  v
marts/           Hourly and daily generation views (views)
```

## Metrics

- **total** — sum of all fuel types (MW)
- **renewables** — wind + solar (MW)
- **thermal** — natural gas + coal (MW)
- Individual fuel types: wind, solar, natural_gas, coal, oil, nuclear, hydro,
  pumped_storage, geothermal, battery, solar_battery, wind_battery,
  other_energy_storage, unknown_energy_storage, other, unknown

## Time Zone Convention

Raw data is in UTC. All staging and mart outputs are converted to
Eastern Prevailing Time (America/New_York) using PostgreSQL
`AT TIME ZONE` with DST awareness. The original `datetime_utc` is preserved
for reference.

{% enddocs %}


{% docs eia_nat_gas_overview %}

# EIA Natural Gas Consumption by End Use

Monthly state-level natural gas consumption data from the EIA Natural Gas API,
broken down by end-use sector.

## Data Source

| Source | Description | Ingestion |
|--------|-------------|-----------|
| **EIA Natural Gas API** | Monthly consumption by end-use category for all US states | Python scrape (`backend/scrapes/eia/`) into `eia` schema |

## Geographic Coverage

- **US48** — Lower 48 aggregate
- **50 US states** + District of Columbia

## End-Use Categories

- **Lease and Plant Fuel** — Field extraction and processing plant fuel
- **Pipeline & Distribution Use** — Compressor stations and pipeline infrastructure
- **Volumes Delivered to Consumers** — Total end-use deliveries
- **Residential** — Household heating, cooking, etc.
- **Commercial** — Offices, hotels, restaurants, etc.
- **Industrial** — Manufacturing, mining, construction
- **Vehicle Fuel** — CNG/LNG for transportation
- **Electric Power** — Utility and independent power producer generation

## Pipeline Architecture

```
source/          Raw EIA table extraction (ephemeral)
  |
  v
utils/           Area name standardization lookup (ephemeral)
  |
  v
staging/         Area join, consumption type standardization,
                 pivot to columns (ephemeral)
  |
  v
marts/           Monthly consumption view (view)
```

{% enddocs %}
