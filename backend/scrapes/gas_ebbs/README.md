# Gas EBBs (Electronic Bulletin Boards)

Scrapes critical and non-critical notices from natural gas pipeline Electronic Bulletin Boards (EBBs). Each pipeline operator publishes notices about force majeure events, operational flow orders, maintenance, capacity reductions, etc.

**Total: 135 configured pipelines (119 enabled, 16 disabled)**

## Architecture

```
gas_ebbs/
├── adapters/           # 15 source-family adapters (one per platform)
├── config/             # 15 YAML configs (one per source family)
├── base_scraper.py     # Abstract base class, adapter registry, factory
├── ebb_utils.py        # Shared utilities (HTML parsing, datetime handling)
├── notice_classifier.py# Notice classification (6 categories + severity 1-5)
├── monitor.py          # Health monitoring (queries logging.pipeline_runs)
├── runs.py             # CLI runner (interactive, parallel by family)
├── flows.py            # Prefect flow definitions (135 flows + gas_ebb_all)
```

**Pipeline pattern:** `_pull()` -> `_parse_listing()` -> `_format()` -> `_upsert()`

**DB tables:** `gas_ebbs.notices` (latest state) and `gas_ebbs.notice_snapshots` (revision history)

**PK:** `(source_family, pipeline_name, notice_identifier)`

## Source Families — Currently Scraping

### PipeRiv (14 pipelines)
Platform: `piperiv.com/ip/{operator}/critical-notices` — simple HTML tables.

| Pipeline | Operator Code |
|----------|---------------|
| Algonquin | algonquin |
| ANR | anr |
| Columbia Gas | columbia |
| El Paso | el-paso |
| Florida Gas | florida |
| Gulf South | gulf-south |
| Iroquois | iroquois |
| Millennium | millennium |
| Mountain Valley | mountain-valley |
| Northern Natural | northern |
| Northwest | northwest |
| Panhandle Eastern | panhandle |
| Rover | rover |
| Southeast Supply | southeast-supply |

### Kinder Morgan (19 pipelines)
Platform: `pipeline2.kindermorgan.com` — Infragistics WebDataGrid, Critical + Non-Critical.

| Pipeline | Pipe Code |
|----------|-----------|
| Tennessee Gas Pipeline | TGP |
| Natural Gas Pipeline of America (NGPL) | NGPL |
| El Paso Natural Gas | EPNG |
| Colorado Interstate Gas | CIG |
| Cheyenne Plains | CP |
| Southern Natural Gas | SNG |
| Elba Express | EEC |
| Midcontinent Express | MEP |
| Mojave Pipeline | MOPC |
| Sierrita Gas Pipeline | SGP |
| Transcolorado | TCP |
| Wyoming Interstate | WIC |
| Southern LNG | SLNG |
| KM Illinois Pipeline | KMIL |
| KM Louisiana Pipeline | KMLP |
| Horizon Pipeline | HORZ |
| Stagecoach | STAG |
| Arlington Storage | ARLS |
| Young Gas Storage | YGS |

### Enbridge (18 pipelines)
Platform: `infopost.enbridge.com` — HTML tables, CRI + NON notice types.

| Pipeline | Pipe Code |
|----------|-----------|
| Algonquin | AG |
| Texas Eastern | TE |
| East Tennessee | ETNG |
| Southeast Supply (SESH) | SESH |
| Sabal Trail | STT |
| Nexus | NXUS |
| Maritimes Northeast | MNUS |
| Big Sandy | BSP |
| Saltville | SGSC |
| Steckman Ridge | SR |
| Valley Crossing | VCP |
| Bobcat Gas Storage | BGS |
| Egan Hub | EHP |
| Garden Banks | GB |
| Mississippi Canyon | MCGP |
| Moss Bluff | MBHP |
| Nautilus | NPC |
| Tres Palacios | TPGS |

### TCE Energy (11 pipelines)
Platform: `tceconnects.com/infopost` — Critical + Non-Critical.

| Pipeline | Pipe Code | Asset ID |
|----------|-----------|----------|
| ANR | ANR | 3005 |
| ANR Storage | ANRS | 3009 |
| Bison | BISON | 3031 |
| Blue Lake | BLGS | 3014 |
| Columbia Gas | TCO | 51 |
| Columbia Gulf | CGUL | 14 |
| Crossroads | CROSS | 44 |
| Hardy Storage | HARDY | 465 |
| Millennium | MILL | 26 |
| Northern Border | NBPL | 3029 |
| Portland Natural Gas | PNGTS | 3037 |

### Energy Transfer (9 pipelines)
Platform: `{subdomain}.energytransfer.com/ipost` — Quorum-based, CRI + NON.

| Pipeline | Subdomain | Code |
|----------|-----------|------|
| Florida Gas | fgttransfer | fgt |
| Panhandle Eastern | peplmessenger | pepl |
| Transwestern | twtransfer | tw |
| Trunkline Gas | tgcmessenger | tgc |
| Sea Robin | sermessenger | ser |
| Tiger | tigertransfer | tgr |
| Fayetteville Express | feptransfer | fep |
| Trunkline LNG | tlngmessenger | tlng |
| Southwest Gas Storage | swgsmessenger | swgs |

### Quorum / MyQuorumCloud (7 pipelines)
Platform: `web-prd.myquorumcloud.com` — Kendo Grid, JSON API with HTML fallback.

| Pipeline | App Code | TSP No |
|----------|----------|--------|
| BBT Alatenn | BBTPA1IPWS | 3 |
| BBT Midla | BBTPA1IPWS | 6 |
| BBT Trans Union | BBTPA1IPWS | 12 |
| Ozark Gas | BBTPA1IPWS | 16 |
| High Point | HPEPA1IPWS | 1 |
| Chandeleur | HPEPA1IPWS | 14 |
| Destin | HPEPA1IPWS | 9 |

### GasNom (6 pipelines)
Platform: `gasnom.com` — standard HTML tables.

| Pipeline | Path |
|----------|------|
| Southern Pines | SOUTHERNPINES |
| Cameron Interstate | cameron |
| Golden Pass | goldenpass |
| Golden Triangle | goldentriangle |
| Mississippi Hub | mississippihub |
| LA Storage | lastorage |

### Williams / 1Line (3 active, 2 disabled)
Platform: `1line.williams.com/xhtml/notice_list.jsf` — JSF endpoint, BUID-based.

| Pipeline | BUID | Status |
|----------|------|--------|
| Transco | 80 | Active |
| Gulfstream | 205 | Active |
| Pine Needle LNG | 82 | Active |
| Northwest | — | Disabled (decommissioned) |
| Discovery | — | Disabled (decommissioned) |

### TC Plus (4 pipelines)
Platform: `tcplus.com` — Critical + NonCritical notice types.

| Pipeline | Path |
|----------|------|
| GTN | gtn |
| Great Lakes | great lakes |
| Tuscarora | tuscarora |
| North Baja | north baja |

### BHEGTS (3 pipelines)
Platform: `infopost.bhegts.com` — JSON API, ISO timestamps.

| Pipeline | Code |
|----------|------|
| Carolina Gas | cgt |
| Cove Point | cpl |
| Eastern Gas | egts |

### DT Midstream (3 pipelines)
Platform: `dtmidstream.trellisenergy.com` — Trellis Energy platform.

| Pipeline | TSP Code |
|----------|----------|
| Guardian | GPL |
| Midwestern | mgt |
| Viking | vgt |

### Cheniere LNG (3 pipelines)
Platform: `lngconnection.cheniere.com` — React SPA, API-based.

| Pipeline | Code |
|----------|------|
| Corpus Christi | ccpl |
| Creole Trail | ctpl |
| Midship | mspl |

### Northern Natural (1 pipeline)
Platform: `northernnaturalgas.com` — separate Critical/NonCritical pages.

| Pipeline | Status |
|----------|--------|
| Northern Natural Gas | Active |

### Standalone (18 enabled, 10 disabled)
Generic HTML table parser — one-off URLs, heuristic extraction.

**Active:**

| Pipeline | URL Domain |
|----------|------------|
| Iroquois | iol.iroquois.com |
| Gulf South | gasquest.com |
| Texas Gas | infopost.txgt.com |
| WBI Energy | transmission.wbienergy.com |
| Alliance | ips.alliance-pipeline.com |
| Paiute | paiutepipeline.com |
| Boardwalk Storage | bwpipelines.com |
| Vector | vector-pipeline.com |
| White River Hub | whiteriverhub.com |
| OneOK OkTex | oneok.com |
| Southern Star | southernstar.com |

**Enabled but flaky (503/timeout — may recover):**

| Pipeline | URL Domain | Issue |
|----------|------------|-------|
| DCP Cimarron | ebb.dcpmidstream.com | 503 |
| DCP Dauphin | ebb.dcpmidstream.com | 503 |
| Florida Southeast | fsc.nexteraenergyresources.com | 503 |
| Mountainwest | questarpipeline.com | 503 |
| Mountainwest Overthrust | questarpipeline.com | 503 |
| Enable Gas | pipelines.enablemidstream.com | Timeout |
| Enable MRT | pipelines.enablemidstream.com | Timeout |

**Disabled — DNS dead / decommissioned:**

| Pipeline | Reason |
|----------|--------|
| Black Marlin | DNS failure — domain decommissioned |
| Enterprise HIOS | DNS failure — ebb.starwebgas.com decommissioned |
| Enterprise Petal | DNS failure — ebb.starwebgas.com decommissioned |
| Kern River | DNS failure — domain decommissioned |
| KO Transmission | DNS failure — domain decommissioned |
| Westgas | DNS failure — domain decommissioned |

**Disabled — 403/404 blocked:**

| Pipeline | Reason |
|----------|--------|
| Empire | 403 Forbidden — auth/IP whitelist required |
| National Fuel | 403 Forbidden — auth/IP whitelist required |
| Sabine | 404 Not Found — page removed |
| Stingray | 404 Not Found — page removed |

## Disabled — Needs Playwright

### Tallgrass Energy (4 pipelines, all disabled)
Platform: `pipeline.tallgrassenergylp.com` — behind Incapsula/Imperva WAF bot challenge.

| Pipeline | Label |
|----------|-------|
| Rockies Express | REX |
| Ruby | Ruby |
| Tallgrass Interstate | Tallgrass Interstate |
| Trailblazer | Trailblazer |

**Blocked by:** Incapsula WAF returns a bot challenge page. Requires Playwright browser automation or an alternate data source.

## Summary Table

| Source Family | Total | Enabled | Disabled | Adapter |
|---------------|:-----:|:-------:|:--------:|---------|
| PipeRiv | 14 | 14 | 0 | piperiv_adapter.py |
| Kinder Morgan | 19 | 19 | 0 | kindermorgan_adapter.py |
| Enbridge | 18 | 18 | 0 | enbridge_adapter.py |
| TCE Energy | 11 | 11 | 0 | tce_adapter.py |
| Energy Transfer | 9 | 9 | 0 | energytransfer_adapter.py |
| Quorum | 7 | 7 | 0 | quorum_adapter.py |
| GasNom | 6 | 6 | 0 | gasnom_adapter.py |
| Williams | 5 | 3 | 2 | williams_adapter.py |
| TC Plus | 4 | 4 | 0 | tcplus_adapter.py |
| Tallgrass | 4 | 0 | 4 | tallgrass_adapter.py |
| BHEGTS | 3 | 3 | 0 | bhegts_adapter.py |
| DT Midstream | 3 | 3 | 0 | dtmidstream_adapter.py |
| Cheniere | 3 | 3 | 0 | cheniere_adapter.py |
| Northern Natural | 1 | 1 | 0 | northern_natural_adapter.py |
| Standalone | 28 | 18 | 10 | standalone_adapter.py |
| **Total** | **135** | **119** | **16** | |

## Usage

```bash
python runs.py                    # Interactive menu
python runs.py --list             # List all pipelines
python runs.py <number>           # Run pipeline by menu index
python runs.py all                # Run all (parallel by family)
python runs.py all --sequential   # Run all (strict sequential)
```

## What's Left / TODO

1. **Tallgrass (4 pipelines)** — Needs Playwright to bypass Incapsula WAF. Rockies Express (REX) is high-priority.
2. **Standalone DNS dead (6)** — Black Marlin, Enterprise HIOS/Petal, Kern River, KO Transmission, Westgas. Need to find alternate EBB URLs or confirm permanently decommissioned.
3. **Standalone 403/404 (4)** — Empire, National Fuel (403 — may need auth), Sabine, Stingray (404 — removed).
4. **Williams (2)** — Northwest and Discovery BUIDs unknown. May be decommissioned or hosted on a different system.
5. **Standalone flaky (7)** — DCP Cimarron/Dauphin, Florida Southeast, Mountainwest/Overthrust (503), Enable Gas/MRT (timeout). Monitor for recovery.
