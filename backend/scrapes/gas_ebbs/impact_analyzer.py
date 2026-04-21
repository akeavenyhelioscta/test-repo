"""
Gas EBB Impact Analyzer — Phase 3: Impact Analysis Layer.

Maps pipeline outages to production sub-regions and calculates
capacity/pricing impact by joining planned_outages with a
pipeline_regions reference table.

Usage:
    python impact_analyzer.py              # run full impact analysis
    python impact_analyzer.py --seed-only  # seed reference table only
    python impact_analyzer.py --dry-run    # compute impacts without upserting
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend import secrets  # noqa: F401
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)


# ── Constants ─────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"

REFERENCE_SCHEMA = "gas_reference"
REFERENCE_TABLE = "pipeline_regions"

REFERENCE_COLUMNS = [
    "pipeline_name",
    "source_family",
    "display_name",
    "operator",
    "primary_basin",
    "secondary_basins",
    "primary_region",
    "direction",
    "design_capacity_bcfd",
    "notes",
    "updated_at",
]

REFERENCE_DATA_TYPES = [
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # source_family
    "VARCHAR",   # display_name
    "VARCHAR",   # operator
    "VARCHAR",   # primary_basin
    "VARCHAR",   # secondary_basins
    "VARCHAR",   # primary_region
    "VARCHAR",   # direction
    "FLOAT",     # design_capacity_bcfd
    "VARCHAR",   # notes
    "VARCHAR",   # updated_at
]

REFERENCE_PRIMARY_KEY = ["source_family", "pipeline_name"]

IMPACT_SCHEMA = "gas_ebbs"
IMPACT_TABLE = "outage_impacts"

IMPACT_COLUMNS = [
    "source_family",
    "pipeline_name",
    "notice_identifier",
    "primary_basin",
    "primary_region",
    "direction",
    "design_capacity_bcfd",
    "capacity_loss_bcfd",
    "capacity_loss_pct",
    "price_impact",
    "impact_summary",
    "computed_at",
]

IMPACT_DATA_TYPES = [
    "VARCHAR",   # source_family
    "VARCHAR",   # pipeline_name
    "VARCHAR",   # notice_identifier
    "VARCHAR",   # primary_basin
    "VARCHAR",   # primary_region
    "VARCHAR",   # direction
    "FLOAT",     # design_capacity_bcfd
    "FLOAT",     # capacity_loss_bcfd
    "FLOAT",     # capacity_loss_pct
    "VARCHAR",   # price_impact
    "VARCHAR",   # impact_summary
    "VARCHAR",   # computed_at
]

IMPACT_PRIMARY_KEY = ["source_family", "pipeline_name", "notice_identifier"]


# ── Reference data: pipeline-to-region mappings ───────────────────────────

# Each tuple: (source_family, pipeline_name, display_name, operator,
#               primary_basin, secondary_basins, primary_region, direction,
#               design_capacity_bcfd, notes)

PIPELINE_REGION_SEED = [
    # ── Enbridge family ──
    ("enbridge", "algonquin", "Algonquin Gas Transmission", "Enbridge",
     "Marcellus", "", "Northeast", "demand_area", 3.023,
     "Serves New England from Ramapo NJ interconnect"),
    ("enbridge", "big_sandy", "Big Sandy Pipeline", "Enbridge",
     "Marcellus", "Utica", "Appalachia", "production_area", 0.2,
     "Eastern Kentucky gathering/transport"),
    ("enbridge", "bobcat_gas_storage", "Bobcat Gas Storage", "Enbridge",
     None, None, "Appalachia", None, None,
     "Storage facility in Appalachian region"),
    ("enbridge", "garden_banks", "Garden Banks Gas Pipeline", "Enbridge",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore Gulf of Mexico deepwater pipeline"),
    ("enbridge", "maritimes_northeast", "Maritimes & Northeast Pipeline", "Enbridge",
     None, None, "Northeast", "demand_area", 0.8,
     "Transports Sable Island gas to New England"),
    ("enbridge", "mississippi_canyon", "Mississippi Canyon Gas Pipeline", "Enbridge",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore Gulf of Mexico deepwater gathering"),
    ("enbridge", "nautilus", "Nautilus Pipeline", "Enbridge",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM to onshore Louisiana"),
    ("enbridge", "nexus", "NEXUS Gas Transmission", "Enbridge",
     "Marcellus", "Utica", "Northeast", "production_area", 1.5,
     "Appalachian Basin to Midwest/Dawn Hub"),
    ("enbridge", "sabal_trail", "Sabal Trail Transmission", "Enbridge",
     None, None, "Southeast", "demand_area", 1.1,
     "Alabama to central Florida"),
    ("enbridge", "saltville", "Saltville Gas Storage", "Enbridge",
     "Marcellus", "", "Appalachia", None, None,
     "Storage facility in southwestern Virginia"),
    ("enbridge", "southeast_supply", "Southeast Supply Header", "Enbridge",
     None, None, "Southeast", "demand_area", 1.0,
     "Perryville LA hub to Southeast markets"),
    ("enbridge", "steckman_ridge", "Steckman Ridge Storage", "Enbridge",
     "Marcellus", "", "Appalachia", None, None,
     "Storage facility in south-central Pennsylvania"),
    ("enbridge", "texas_eastern", "Texas Eastern Transmission", "Enbridge",
     "Marcellus", "Haynesville", "Northeast", "bidirectional", 10.2,
     "Gulf Coast TX/LA through Appalachia to NJ/NYC"),
    ("enbridge", "tres_palacios", "Tres Palacios Gas Storage", "Enbridge",
     None, None, "Gulf Coast", None, None,
     "Salt dome storage on Texas Gulf Coast"),
    ("enbridge", "valley_crossing", "Valley Crossing Pipeline", "Enbridge",
     None, None, "Gulf Coast", "demand_area", 2.6,
     "Agua Dulce TX to US-Mexico border for LNG/export"),
    ("enbridge", "east_tennessee", "East Tennessee Natural Gas", "Enbridge",
     "Marcellus", "", "Southeast", "demand_area", 1.5,
     "Virginia/Tennessee lateral off Texas Eastern"),
    ("enbridge", "egan_hub", "Egan Hub Storage", "Enbridge",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage in southwest Louisiana"),
    ("enbridge", "moss_bluff", "Moss Bluff Hub Storage", "Enbridge",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage near Houston TX area"),

    # ── Piperiv family ──
    ("piperiv", "algonquin", "Algonquin Gas Transmission", "Enbridge",
     "Marcellus", "", "Northeast", "demand_area", 3.023,
     "Piperiv listing for Algonquin; serves New England"),
    ("piperiv", "anr", "ANR Pipeline", "TC Energy",
     None, "Haynesville", "Midwest", "bidirectional", 5.0,
     "Gulf Coast LA to Michigan/Wisconsin; bidirectional"),
    ("piperiv", "columbia_gas", "Columbia Gas Transmission", "TC Energy",
     "Marcellus", "Utica", "Appalachia", "production_area", 4.6,
     "Appalachian Basin gathering and transport"),
    ("piperiv", "el_paso", "El Paso Natural Gas", "Kinder Morgan",
     "Permian", "San Juan", "West", "production_area", 5.59,
     "Permian/San Juan basins to California and Southwest"),
    ("piperiv", "florida_gas", "Florida Gas Transmission", "Energy Transfer",
     None, None, "Southeast", "demand_area", 3.3,
     "Texas/Louisiana to Florida markets"),
    ("piperiv", "gulf_south", "Gulf South Pipeline", "Boardwalk Pipeline",
     "Haynesville", "", "Gulf Coast", "production_area", 2.3,
     "Haynesville Shale gathering and transport"),
    ("piperiv", "iroquois", "Iroquois Gas Transmission", "Iroquois",
     None, None, "Northeast", "demand_area", 1.48,
     "Waddington NY to Long Island/NYC metro"),
    ("piperiv", "millennium", "Millennium Pipeline", "DTE/NiSource/National Fuel",
     "Marcellus", "", "Northeast", "production_area", 0.85,
     "Corning NY to Ramapo NY interconnect"),
    ("piperiv", "mountain_valley", "Mountain Valley Pipeline", "Equitrans",
     "Marcellus", "Utica", "Appalachia", "production_area", 2.0,
     "WV to VA; takeaway from Marcellus/Utica"),
    ("piperiv", "northern_natural", "Northern Natural Gas", "Berkshire Hathaway",
     None, "Anadarko,Permian", "Upper Midwest", "bidirectional", 5.2,
     "Permian/Midcontinent to Upper Midwest (MN, WI, IA)"),
    ("piperiv", "northwest", "Northwest Pipeline", "Williams",
     None, None, "West", "demand_area", 3.7,
     "Colorado/Wyoming to Pacific Northwest (WA, OR)"),
    ("piperiv", "panhandle_eastern", "Panhandle Eastern Pipe Line", "Energy Transfer",
     "Anadarko", "Hugoton", "Midcontinent", "production_area", 2.4,
     "Texas panhandle/Anadarko to eastern Midwest"),
    ("piperiv", "rover", "Rover Pipeline", "Energy Transfer",
     "Marcellus", "Utica", "Midwest", "production_area", 3.25,
     "Appalachian Basin to Midwest/Dawn Hub"),
    ("piperiv", "southeast_supply", "Southeast Supply Header", "Enbridge",
     None, None, "Southeast", "demand_area", 1.0,
     "Piperiv listing for SESH"),

    # ── Kinder Morgan family ──
    ("kindermorgan", "arlington_storage", "Arlington Storage", "Kinder Morgan",
     None, None, "West", None, None,
     "Gas storage in Pacific Northwest (OR)"),
    ("kindermorgan", "cheyenne_plains", "Cheyenne Plains Pipeline", "Kinder Morgan",
     None, "DJ Basin", "West", "production_area", 0.9,
     "Cheyenne Hub WY to interstate connections"),
    ("kindermorgan", "colorado_interstate_gas", "Colorado Interstate Gas", "Kinder Morgan",
     None, "DJ Basin,Green River", "West", "production_area", 2.6,
     "Rocky Mountain basins to Front Range/Cheyenne Hub"),
    ("kindermorgan", "el_paso_natural_gas", "El Paso Natural Gas", "Kinder Morgan",
     "Permian", "San Juan", "West", "production_area", 5.59,
     "Permian/San Juan to California and Southwest"),
    ("kindermorgan", "elba_express", "Elba Express Pipeline", "Kinder Morgan",
     None, None, "Southeast", "demand_area", 0.946,
     "Transco Zone 5 to Elba Island LNG terminal GA"),
    ("kindermorgan", "horizon_pipeline", "Horizon Pipeline", "Kinder Morgan",
     None, None, "Midwest", "demand_area", 0.2,
     "Joliet IL area lateral"),
    ("kindermorgan", "kinder_morgan_illinois_pipeline", "Kinder Morgan Illinois Pipeline", "Kinder Morgan",
     None, None, "Midwest", "bidirectional", 0.7,
     "NGPL interconnect in Illinois"),
    ("kindermorgan", "kinder_morgan_louisiana_pipeline", "Kinder Morgan Louisiana Pipeline", "Kinder Morgan",
     "Haynesville", "", "Gulf Coast", "production_area", 3.6,
     "North Louisiana Haynesville takeaway"),
    ("kindermorgan", "midcontinent_express", "Midcontinent Express Pipeline", "Kinder Morgan",
     None, "Woodford,Fayetteville", "Midcontinent", "production_area", 1.8,
     "Oklahoma to Perryville LA hub"),
    ("kindermorgan", "mojave_pipeline", "Mojave Pipeline", "Kinder Morgan",
     None, None, "West", "demand_area", 0.58,
     "Topock AZ to southern California (SoCal)"),
    ("kindermorgan", "natural_gas_pipeline_of_america", "Natural Gas Pipeline of America (NGPL)", "Kinder Morgan",
     None, "Permian,Anadarko", "Midwest", "bidirectional", 6.1,
     "Gulf Coast TX/LA through Midcontinent to Chicago"),
    ("kindermorgan", "sierrita_gas_pipeline", "Sierrita Gas Pipeline", "Kinder Morgan",
     None, None, "West", "production_area", 0.2,
     "Southern Arizona to US-Mexico border"),
    ("kindermorgan", "southern_lng", "Southern LNG", "Kinder Morgan",
     None, None, "Southeast", "demand_area", None,
     "Elba Island LNG import/export facility GA"),
    ("kindermorgan", "southern_natural_gas", "Southern Natural Gas", "Kinder Morgan",
     None, None, "Southeast", "bidirectional", 4.2,
     "Gulf Coast LA/TX to Southeast US markets"),
    ("kindermorgan", "stagecoach", "Stagecoach Gas Storage", "Kinder Morgan",
     "Marcellus", "", "Northeast", None, None,
     "Storage in south-central New York"),
    ("kindermorgan", "tennessee_gas_pipeline", "Tennessee Gas Pipeline (TGP)", "Kinder Morgan",
     None, "Haynesville,Marcellus", "Northeast", "bidirectional", 7.9,
     "Gulf Coast TX/LA through Appalachia to New England"),
    ("kindermorgan", "transcolorado", "TransColorado Gas Transmission", "Kinder Morgan",
     None, "Piceance", "West", "production_area", 0.3,
     "Western Colorado to Blanco Hub NM"),
    ("kindermorgan", "wyoming_interstate", "Wyoming Interstate Company", "Kinder Morgan",
     None, "Green River,Overthrust", "West", "production_area", 2.5,
     "Wyoming Overthrust to Opal/Cheyenne hubs"),
    ("kindermorgan", "young_gas_storage", "Young Gas Storage", "Kinder Morgan",
     None, None, "West", None, None,
     "Gas storage in Colorado"),

    # ── Williams family ──
    ("williams", "gulfstream", "Gulfstream Natural Gas System", "Williams",
     None, None, "Southeast", "demand_area", 1.3,
     "Mississippi AL to central Florida (subsea)"),
    ("williams", "transco", "Transcontinental Gas Pipe Line (Transco)", "Williams",
     "Marcellus", "Haynesville", "Northeast", "bidirectional", 17.7,
     "Gulf Coast TX to NYC metro; largest US pipeline by capacity"),

    # ── TC Energy (tce) family ──
    ("tce", "anr", "ANR Pipeline", "TC Energy",
     None, "Haynesville", "Midwest", "bidirectional", 5.0,
     "Gulf Coast LA to Michigan/Wisconsin"),
    ("tce", "anr_storage", "ANR Storage", "TC Energy",
     None, None, "Midwest", None, None,
     "Underground gas storage in Michigan"),
    ("tce", "bison", "Bison Pipeline", "TC Energy",
     None, "Powder River", "West", "production_area", 0.4,
     "Powder River Basin WY to Northern Border interconnect"),
    ("tce", "blue_lake", "Blue Lake Gas Storage", "TC Energy",
     None, None, "Midwest", None, None,
     "Storage in Michigan"),
    ("tce", "columbia_gas", "Columbia Gas Transmission", "TC Energy",
     "Marcellus", "Utica", "Appalachia", "production_area", 4.6,
     "Appalachian Basin gathering and transport to East Coast"),
    ("tce", "columbia_gulf", "Columbia Gulf Transmission", "TC Energy",
     None, "Haynesville", "Gulf Coast", "bidirectional", 2.6,
     "Gulf Coast LA to Appalachian Basin interconnect"),
    ("tce", "crossroads", "Crossroads Pipeline", "TC Energy",
     None, None, "Midwest", "demand_area", 0.7,
     "Indiana lateral for Midwest demand"),
    ("tce", "hardy_storage", "Hardy Storage", "TC Energy",
     "Marcellus", "", "Appalachia", None, None,
     "Storage in West Virginia"),
    ("tce", "millennium", "Millennium Pipeline", "TC Energy/DTE/NiSource",
     "Marcellus", "", "Northeast", "production_area", 0.85,
     "Corning NY to Ramapo NY interconnect"),
    ("tce", "northern_border", "Northern Border Pipeline", "TC Energy",
     None, "Bakken,Williston", "Upper Midwest", "production_area", 2.4,
     "Montana/North Dakota to Chicago-area markets"),
    ("tce", "portland_natural_gas", "Portland Natural Gas Transmission", "TC Energy",
     None, None, "Northeast", "demand_area", 0.21,
     "Vermont/New Hampshire to Portland ME; New England supply"),

    # ── Energy Transfer family ──
    ("energytransfer", "fayetteville_express", "Fayetteville Express Pipeline", "Energy Transfer",
     "Fayetteville", "", "Midcontinent", "production_area", 2.0,
     "Fayetteville Shale AR to Perryville LA hub"),
    ("energytransfer", "florida_gas", "Florida Gas Transmission", "Energy Transfer",
     None, None, "Southeast", "demand_area", 3.3,
     "Texas/Louisiana to Florida"),
    ("energytransfer", "panhandle_eastern", "Panhandle Eastern Pipe Line", "Energy Transfer",
     "Anadarko", "Hugoton", "Midcontinent", "production_area", 2.4,
     "Texas panhandle/Anadarko to eastern Midwest"),
    ("energytransfer", "sea_robin", "Sea Robin Pipeline", "Energy Transfer",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM gathering to onshore Louisiana"),
    ("energytransfer", "southwest_gas_storage", "Southwest Gas Storage", "Energy Transfer",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage in Gulf Coast area"),
    ("energytransfer", "tiger", "Tiger Pipeline", "Energy Transfer",
     "Haynesville", "", "Gulf Coast", "production_area", 2.4,
     "Haynesville Shale to Perryville LA hub and markets"),
    ("energytransfer", "transwestern", "Transwestern Pipeline", "Energy Transfer",
     "Permian", "San Juan", "West", "production_area", 2.1,
     "West Texas/Permian to California via Arizona"),
    ("energytransfer", "trunkline_gas", "Trunkline Gas", "Energy Transfer",
     None, None, "Midwest", "bidirectional", 1.9,
     "Gulf Coast LA to Midwest (IL, IN)"),
    ("energytransfer", "trunkline_lng", "Trunkline LNG", "Energy Transfer",
     None, None, "Gulf Coast", "demand_area", None,
     "LNG import terminal at Lake Charles LA"),

    # ── Northern Natural family ──
    ("northern_natural", "northern_natural", "Northern Natural Gas", "Berkshire Hathaway",
     None, "Anadarko,Permian", "Upper Midwest", "bidirectional", 5.2,
     "Permian/Midcontinent to Upper Midwest"),

    # ── Bhegts family ──
    ("bhegts", "cove_point", "Cove Point LNG", "Berkshire Hathaway",
     None, None, "Mid-Atlantic", "demand_area", 1.8,
     "Cove Point MD LNG export/import terminal"),
    ("bhegts", "carolina_gas", "Carolina Gas Transmission", "Berkshire Hathaway",
     None, None, "Southeast", "demand_area", 0.4,
     "South Carolina lateral for Southeast demand"),
    ("bhegts", "eastern_gas", "Eastern Gas Transmission & Storage", "Berkshire Hathaway",
     "Marcellus", "Utica", "Appalachia", "production_area", 3.0,
     "Appalachian Basin gathering and transport (former Dominion)"),

    # ── Gasnom family ──
    ("gasnom", "cameron_interstate", "Cameron Interstate Pipeline", "Sempra",
     None, None, "Gulf Coast", "demand_area", 1.7,
     "Henry Hub/Sabine area to Cameron LNG terminal LA"),
    ("gasnom", "golden_pass", "Golden Pass Pipeline", "Golden Pass LNG",
     None, None, "Gulf Coast", "demand_area", 2.5,
     "Feeds Golden Pass LNG terminal Sabine Pass TX"),
    ("gasnom", "golden_triangle", "Golden Triangle Storage", "Energy Transfer",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage Beaumont TX area"),
    ("gasnom", "la_storage", "Louisiana Storage", "Sempra",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage southern Louisiana"),
    ("gasnom", "mississippi_hub", "Mississippi Hub", "Boardwalk Pipeline",
     None, None, "Gulf Coast", None, None,
     "Market center and storage Butler AL area"),
    ("gasnom", "southern_pines", "Southern Pines Energy Center", "Southern Pines",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage Mississippi"),

    # ── Standalone family ──
    ("standalone", "alliance", "Alliance Pipeline", "Enbridge/Pembina",
     None, "Bakken,Williston", "Upper Midwest", "production_area", 1.6,
     "Western Canada/Bakken ND to Chicago area"),
    ("standalone", "black_marlin", "Black Marlin Pipeline", "Black Marlin",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM to onshore Texas"),
    ("standalone", "boardwalk_storage", "Boardwalk Storage", "Boardwalk Pipeline",
     None, None, "Gulf Coast", None, None,
     "Gas storage facilities in Louisiana area"),
    ("standalone", "dcp_cimarron", "DCP Cimarron Gathering", "DCP Midstream",
     None, "Anadarko", "Midcontinent", "production_area", None,
     "Gathering system in Oklahoma Anadarko Basin"),
    ("standalone", "dcp_dauphin", "DCP Dauphin Island Gathering", "DCP Midstream",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM gathering to Alabama coast"),
    ("standalone", "empire", "Empire State Pipeline", "National Fuel",
     "Marcellus", "", "Northeast", "production_area", 0.75,
     "Western NY to Niagara/Dawn Hub interconnects"),
    ("standalone", "enable_gas", "Enable Gas Transmission", "CenterPoint/OGE",
     "Anadarko", "Arkoma", "Midcontinent", "production_area", 3.3,
     "Oklahoma and Arkansas gathering/transport"),
    ("standalone", "enable_mrt", "Enable Mississippi River Transmission", "CenterPoint/OGE",
     None, None, "Midcontinent", "demand_area", 1.8,
     "Perryville LA to Arkansas/Missouri markets"),
    ("standalone", "enterprise_hios", "Enterprise HIOS", "Enterprise Products",
     None, None, "Gulf Coast", "production_area", None,
     "High Island Offshore System GOM to TX coast"),
    ("standalone", "enterprise_petal", "Enterprise Petal Gas Storage", "Enterprise Products",
     None, None, "Gulf Coast", None, None,
     "Salt cavern storage Hattiesburg MS area"),
    ("standalone", "florida_southeast", "Florida Southeast Connection", "NextEra",
     None, None, "Southeast", "demand_area", 0.64,
     "Connects Sabal Trail to Florida power plants"),
    ("standalone", "gulf_south", "Gulf South Pipeline", "Boardwalk Pipeline",
     "Haynesville", "", "Gulf Coast", "production_area", 2.3,
     "Standalone listing for Gulf South"),
    ("standalone", "iroquois", "Iroquois Gas Transmission", "Iroquois",
     None, None, "Northeast", "demand_area", 1.48,
     "Standalone listing for Iroquois"),
    ("standalone", "kern_river", "Kern River Gas Transmission", "Berkshire Hathaway",
     None, "Overthrust", "West", "production_area", 1.95,
     "Wyoming to southern California/Las Vegas"),
    ("standalone", "ko_transmission", "KO Transmission", "Crestwood Equity",
     None, None, "Midcontinent", "production_area", None,
     "Oklahoma gathering lateral"),
    ("standalone", "mountainwest", "MountainWest Pipelines", "Williams",
     None, "Green River,Overthrust", "West", "production_area", None,
     "Rocky Mountain pipeline system (formerly Questar)"),
    ("standalone", "mountainwest_overthrust", "MountainWest Overthrust", "Williams",
     None, "Overthrust", "West", "production_area", None,
     "Overthrust Belt Wyoming lateral"),
    ("standalone", "national_fuel", "National Fuel Gas Supply", "National Fuel",
     "Marcellus", "Utica", "Northeast", "production_area", 3.5,
     "Western NY/PA Appalachian Basin gathering"),
    ("standalone", "oneok_oktex", "ONEOK OkTex Pipeline", "ONEOK",
     "Anadarko", "", "Midcontinent", "production_area", None,
     "Oklahoma/Texas panhandle NGL/gas gathering"),
    ("standalone", "paiute", "Paiute Pipeline", "Southwest Gas",
     None, None, "West", "demand_area", 0.58,
     "Nevada/Utah distribution lateral"),
    ("standalone", "sabine", "Sabine Pipe Line", "Sabine",
     None, None, "Gulf Coast", "production_area", None,
     "Henry Hub area gathering in Louisiana"),
    ("standalone", "southern_star", "Southern Star Central Gas Pipeline", "Black Hills Energy",
     "Anadarko", "Hugoton", "Midcontinent", "production_area", 1.5,
     "Kansas/Oklahoma Midcontinent production transport"),
    ("standalone", "stingray", "Stingray Pipeline", "Enbridge",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM to onshore Louisiana"),
    ("standalone", "texas_gas", "Texas Gas Transmission", "Boardwalk Pipeline",
     None, None, "Midwest", "bidirectional", 2.6,
     "Gulf Coast LA through Kentucky to Ohio Valley"),
    ("standalone", "vector", "Vector Pipeline", "Enbridge/DTE",
     None, None, "Midwest", "bidirectional", 1.0,
     "Chicago area to Dawn Hub Ontario"),
    ("standalone", "wbi_energy", "WBI Energy Transmission", "MDU Resources",
     None, "Williston,Bakken", "Upper Midwest", "production_area", 0.8,
     "Montana/North Dakota/Wyoming regional pipeline"),
    ("standalone", "westgas", "WestGas InterState", "WestGas",
     None, None, "West", "production_area", None,
     "DJ Basin Colorado gathering"),
    ("standalone", "white_river_hub", "White River Hub", "Enterprise Products",
     None, "Piceance,Uinta", "West", None, None,
     "Market center and storage in Piceance Basin CO"),

    # ── Tallgrass family ──
    ("tallgrass", "rockies_express", "Rockies Express Pipeline (REX)", "Tallgrass Energy",
     None, "Piceance,Green River", "Midwest", "bidirectional", 1.8,
     "Rocky Mountain basins to eastern Ohio; bidirectional"),
    ("tallgrass", "ruby", "Ruby Pipeline", "Tallgrass Energy",
     None, "Overthrust", "West", "production_area", 1.5,
     "Opal WY hub to Malin OR interconnect"),
    ("tallgrass", "tallgrass_interstate", "Tallgrass Interstate Gas Transmission", "Tallgrass Energy",
     "Anadarko", "Hugoton", "Midcontinent", "production_area", 1.7,
     "Kansas/Oklahoma Midcontinent to Colorado/Midwest"),
    ("tallgrass", "trailblazer", "Trailblazer Pipeline", "Tallgrass Energy",
     None, "DJ Basin", "West", "production_area", 0.44,
     "DJ Basin Colorado to Cheyenne Hub WY"),

    # ── Quorum family ──
    ("quorum", "bbt_alatenn", "AlaTenn Pipeline", "Boardwalk Pipeline",
     None, None, "Southeast", "demand_area", 0.6,
     "Mississippi to northern Alabama/Tennessee"),
    ("quorum", "bbt_midla", "MidLa Pipeline", "Boardwalk Pipeline",
     None, None, "Gulf Coast", "production_area", 0.6,
     "Central Louisiana gathering/transport"),
    ("quorum", "bbt_trans_union", "Trans Union Pipeline", "Boardwalk Pipeline",
     None, None, "Gulf Coast", "production_area", None,
     "Louisiana gathering lateral"),
    ("quorum", "chandeleur", "Chandeleur Pipe Line", "Chevron",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM to onshore Louisiana"),
    ("quorum", "destin", "Destin Pipeline", "ArcLight Capital",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM to Pascagoula MS area"),
    ("quorum", "high_point", "High Point Gas Transmission", "Southern Natural Gas",
     None, None, "Southeast", "demand_area", None,
     "North Carolina lateral off Transco"),
    ("quorum", "ozark_gas", "Ozark Gas Transmission", "Boardwalk Pipeline",
     None, "Arkoma", "Midcontinent", "production_area", None,
     "Arkoma Basin Oklahoma gathering"),

    # ── Tcplus family ──
    ("tcplus", "great_lakes", "Great Lakes Gas Transmission", "TC Energy",
     None, None, "Upper Midwest", "bidirectional", 2.4,
     "Canadian gas through upper Michigan/Wisconsin"),
    ("tcplus", "gtn", "Gas Transmission Northwest (GTN)", "TC Energy",
     None, None, "West", "bidirectional", 2.76,
     "Canadian gas through Idaho to Malin OR hub"),
    ("tcplus", "north_baja", "North Baja Pipeline", "TC Energy",
     None, None, "West", "demand_area", 0.5,
     "Arizona to US-Mexico border for Baja CA"),
    ("tcplus", "tuscarora", "Tuscarora Gas Transmission", "TC Energy",
     None, None, "West", "demand_area", 0.11,
     "Malin OR to northern Nevada/California"),

    # ── Dtmidstream family ──
    ("dtmidstream", "guardian", "Guardian Pipeline", "DT Midstream",
     None, None, "Midwest", "demand_area", 0.7,
     "Joliet IL to southeastern Wisconsin"),
    ("dtmidstream", "midwestern", "Midwestern Gas Transmission", "DT Midstream",
     None, None, "Midwest", "demand_area", None,
     "Indiana to Illinois lateral"),
    ("dtmidstream", "viking", "Viking Gas Transmission", "DT Midstream",
     None, None, "Upper Midwest", "demand_area", 0.6,
     "Emerson Manitoba to Minneapolis/St. Paul area"),

    # ── Cheniere family ──
    ("cheniere", "corpus_christi", "Corpus Christi Pipeline", "Cheniere Energy",
     "Eagle Ford", "", "Gulf Coast", "demand_area", 2.5,
     "Feeds Corpus Christi LNG export terminal TX"),
    ("cheniere", "creole_trail", "Creole Trail Pipeline", "Cheniere Energy",
     "Haynesville", "", "Gulf Coast", "demand_area", 4.4,
     "Feeds Sabine Pass LNG export terminal LA"),
    ("cheniere", "midship", "Midship Pipeline", "Cheniere Energy",
     "SCOOP/STACK", "", "Midcontinent", "production_area", 1.44,
     "Oklahoma SCOOP/STACK to SE Oklahoma hub"),

    # ── Williams family (additional) ──
    ("williams", "discovery", "Discovery Gas Transmission", "Williams",
     None, None, "Gulf Coast", "production_area", None,
     "Offshore GOM deepwater gathering to LaRose LA"),
    ("williams", "northwest", "Northwest Pipeline", "Williams",
     None, None, "West", "demand_area", 3.7,
     "Colorado/Wyoming to Pacific Northwest"),
    ("williams", "pine_needle", "Pine Needle LNG", "Williams",
     None, None, "Southeast", None, None,
     "LNG storage facility in North Carolina"),
]


# ── Seeding logic ─────────────────────────────────────────────────────────


def build_reference_df() -> pd.DataFrame:
    """Build a DataFrame of pipeline region mappings from the seed data."""
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entry in PIPELINE_REGION_SEED:
        (source_family, pipeline_name, display_name, operator,
         primary_basin, secondary_basins, primary_region, direction,
         design_capacity_bcfd, notes) = entry
        rows.append({
            "pipeline_name": pipeline_name,
            "source_family": source_family,
            "display_name": display_name or "",
            "operator": operator or "",
            "primary_basin": primary_basin or "",
            "secondary_basins": secondary_basins or "",
            "primary_region": primary_region or "",
            "direction": direction or "",
            "design_capacity_bcfd": design_capacity_bcfd if design_capacity_bcfd is not None else 0.0,
            "notes": notes or "",
            "updated_at": now,
        })

    df = pd.DataFrame(rows)
    df = df[REFERENCE_COLUMNS]

    # Fill NaN
    for col in df.columns:
        if col == "design_capacity_bcfd":
            df[col] = df[col].fillna(0.0)
        else:
            df[col] = df[col].fillna("").astype(str)

    return df


def seed_reference_table() -> int:
    """Upsert pipeline region reference data to gas_reference.pipeline_regions.

    Returns the number of rows seeded.
    """
    df = build_reference_df()
    azure_postgresql.upsert_to_azure_postgresql(
        schema=REFERENCE_SCHEMA,
        table_name=REFERENCE_TABLE,
        df=df,
        columns=REFERENCE_COLUMNS,
        data_types=REFERENCE_DATA_TYPES,
        primary_key=REFERENCE_PRIMARY_KEY,
    )
    return len(df)


# ── Impact computation ────────────────────────────────────────────────────


def pull_planned_outages() -> pd.DataFrame:
    """Pull all planned outages from gas_ebbs.planned_outages."""
    return azure_postgresql.pull_from_db("""
        SELECT
            source_family,
            pipeline_name,
            notice_identifier,
            location,
            sub_region,
            start_date,
            end_date,
            capacity_loss_bcfd,
            outage_type,
            status,
            subject,
            notice_category,
            severity,
            scraped_at
        FROM gas_ebbs.planned_outages
    """)


def pull_pipeline_regions() -> pd.DataFrame:
    """Pull all pipeline region mappings from gas_reference.pipeline_regions."""
    return azure_postgresql.pull_from_db("""
        SELECT
            source_family,
            pipeline_name,
            display_name,
            operator,
            primary_basin,
            secondary_basins,
            primary_region,
            direction,
            design_capacity_bcfd,
            notes
        FROM gas_reference.pipeline_regions
    """)


def _classify_price_impact(direction: str, capacity_loss: float) -> str:
    """Determine price impact based on pipeline direction and capacity loss.

    Logic:
      - production_area + loss > 0: 'bearish' (gas trapped at wellhead,
        oversupply in production area, undersupply in demand area)
      - demand_area + loss > 0: 'bullish' (supply constrained to consumers)
      - bidirectional: 'neutral' (depends on flow direction; needs further analysis)
      - No loss or unknown direction: 'unknown'
    """
    if not direction or capacity_loss <= 0:
        return "unknown"

    direction = str(direction).strip().lower()

    if direction == "production_area":
        return "bearish"
    elif direction == "demand_area":
        return "bullish"
    elif direction == "bidirectional":
        return "neutral"
    else:
        return "unknown"


def _generate_impact_summary(
    pipeline_name: str,
    display_name: str,
    direction: str,
    primary_basin: str,
    primary_region: str,
    capacity_loss_bcfd: float,
    design_capacity_bcfd: float,
    capacity_loss_pct: float,
    price_impact: str,
    subject: str,
) -> str:
    """Generate a human-readable impact summary string."""
    name = display_name if display_name else pipeline_name
    parts = []

    # Pipeline identity
    parts.append(f"{name}")

    # Location context
    location_parts = []
    if primary_basin:
        location_parts.append(primary_basin)
    if primary_region:
        location_parts.append(primary_region)
    if location_parts:
        parts.append(f"({', '.join(location_parts)})")

    # Capacity impact
    if capacity_loss_bcfd > 0 and design_capacity_bcfd > 0:
        parts.append(
            f"— {capacity_loss_bcfd:.3f} Bcf/d loss "
            f"({capacity_loss_pct:.1f}% of {design_capacity_bcfd:.3f} Bcf/d design)"
        )
    elif capacity_loss_bcfd > 0:
        parts.append(f"— {capacity_loss_bcfd:.3f} Bcf/d loss")

    # Direction and price signal
    direction_labels = {
        "production_area": "production-area pipeline",
        "demand_area": "demand-area pipeline",
        "bidirectional": "bidirectional pipeline",
    }
    if direction and direction in direction_labels:
        parts.append(f"[{direction_labels[direction]}]")

    impact_labels = {
        "bullish": "BULLISH: supply constrained to demand area",
        "bearish": "BEARISH: gas trapped in production area",
        "neutral": "NEUTRAL: bidirectional flow; direction-dependent",
        "unknown": "UNKNOWN: insufficient data for classification",
    }
    parts.append(f"-> {impact_labels.get(price_impact, 'UNKNOWN')}")

    return " ".join(parts)


def compute_impacts(outages_df: pd.DataFrame, regions_df: pd.DataFrame) -> pd.DataFrame:
    """Join planned_outages with pipeline_regions to compute impacts.

    1. Left join outages with regions on (source_family, pipeline_name)
    2. Compute capacity_loss_pct = capacity_loss_bcfd / design_capacity_bcfd * 100
    3. Classify price_impact based on direction and capacity loss
    4. Generate impact_summary text

    Returns DataFrame matching outage_impacts columns.
    """
    if outages_df is None or outages_df.empty:
        return pd.DataFrame(columns=IMPACT_COLUMNS)

    # Left join outages with region reference
    merged = outages_df.merge(
        regions_df[["source_family", "pipeline_name", "display_name",
                     "primary_basin", "primary_region", "direction",
                     "design_capacity_bcfd"]],
        on=["source_family", "pipeline_name"],
        how="left",
        suffixes=("", "_ref"),
    )

    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for _, row in merged.iterrows():
        cap_loss = float(row.get("capacity_loss_bcfd", 0) or 0)
        design_cap = float(row.get("design_capacity_bcfd", 0) or 0)
        direction = str(row.get("direction", "") or "")
        primary_basin = str(row.get("primary_basin", "") or "")
        primary_region = str(row.get("primary_region", "") or "")
        display_name = str(row.get("display_name", "") or "")
        subject = str(row.get("subject", "") or "")

        # Compute loss percentage
        if design_cap > 0 and cap_loss > 0:
            loss_pct = round((cap_loss / design_cap) * 100, 2)
        else:
            loss_pct = 0.0

        # Classify price impact
        price_impact = _classify_price_impact(direction, cap_loss)

        # Generate summary
        summary = _generate_impact_summary(
            pipeline_name=row["pipeline_name"],
            display_name=display_name,
            direction=direction,
            primary_basin=primary_basin,
            primary_region=primary_region,
            capacity_loss_bcfd=cap_loss,
            design_capacity_bcfd=design_cap,
            capacity_loss_pct=loss_pct,
            price_impact=price_impact,
            subject=subject,
        )

        rows.append({
            "source_family": row["source_family"],
            "pipeline_name": row["pipeline_name"],
            "notice_identifier": row["notice_identifier"],
            "primary_basin": primary_basin,
            "primary_region": primary_region,
            "direction": direction,
            "design_capacity_bcfd": design_cap,
            "capacity_loss_bcfd": cap_loss,
            "capacity_loss_pct": loss_pct,
            "price_impact": price_impact,
            "impact_summary": summary[:2000],  # cap stored text
            "computed_at": now,
        })

    if not rows:
        return pd.DataFrame(columns=IMPACT_COLUMNS)

    impact_df = pd.DataFrame(rows)[IMPACT_COLUMNS]

    # Fill NaN
    for col in impact_df.columns:
        if col in ("design_capacity_bcfd", "capacity_loss_bcfd", "capacity_loss_pct"):
            impact_df[col] = impact_df[col].fillna(0.0)
        else:
            impact_df[col] = impact_df[col].fillna("").astype(str)

    return impact_df


def upsert_impacts(impact_df: pd.DataFrame) -> int:
    """Upsert computed impact rows to gas_ebbs.outage_impacts.

    Returns the number of rows upserted.
    """
    if impact_df.empty:
        return 0

    azure_postgresql.upsert_to_azure_postgresql(
        schema=IMPACT_SCHEMA,
        table_name=IMPACT_TABLE,
        df=impact_df,
        columns=IMPACT_COLUMNS,
        data_types=IMPACT_DATA_TYPES,
        primary_key=IMPACT_PRIMARY_KEY,
    )
    return len(impact_df)


# ── Runner ────────────────────────────────────────────────────────────────


def run_impact_analysis(dry_run: bool = False) -> dict:
    """Full impact analysis pipeline.

    1. Ensure reference data is seeded
    2. Pull planned_outages + pipeline_regions
    3. Compute impacts
    4. Upsert to gas_ebbs.outage_impacts (unless dry_run)

    Returns a summary dict.
    """
    api_scrape_name = "gas_ebb_impact_analysis"
    logger = logging_utils.PipelineLogger(
        name=api_scrape_name,
        log_dir=LOG_DIR,
        log_to_file=True,
        delete_if_no_errors=True,
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=api_scrape_name,
        source="gas_ebbs",
        target_table=f"{IMPACT_SCHEMA}.{IMPACT_TABLE}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(api_scrape_name)

        # Step 1: Seed reference data
        logger.section("Seeding Reference Data")
        ref_count = seed_reference_table()
        logger.info(f"Seeded {ref_count} pipeline region mappings to {REFERENCE_SCHEMA}.{REFERENCE_TABLE}")

        # Step 2: Pull data
        logger.section("Pulling Data")
        outages_df = pull_planned_outages()
        regions_df = pull_pipeline_regions()

        outage_count = len(outages_df) if outages_df is not None else 0
        region_count = len(regions_df) if regions_df is not None else 0
        logger.info(f"Pulled {outage_count} outages, {region_count} region mappings")

        if outages_df is None or outages_df.empty:
            logger.warning("No planned outages found — nothing to analyze")
            run.success(rows_processed=0)
            return {"outages": 0, "regions": region_count, "impacts": 0}

        # Step 3: Compute impacts
        logger.section("Computing Impacts")
        impact_df = compute_impacts(outages_df, regions_df)
        logger.info(f"Computed {len(impact_df)} impact rows")

        # Log price impact distribution
        if not impact_df.empty:
            counts = impact_df["price_impact"].value_counts().to_dict()
            for impact_type, count in sorted(counts.items()):
                logger.info(f"  {impact_type}: {count}")

            # Log region distribution
            region_counts = impact_df["primary_region"].value_counts().to_dict()
            for region, count in sorted(region_counts.items()):
                if region:
                    logger.info(f"  region={region}: {count}")

        # Step 4: Upsert (unless dry run)
        if dry_run:
            logger.info("DRY RUN — skipping upsert")
            impact_count = 0
        else:
            logger.section("Upserting Impacts")
            impact_count = upsert_impacts(impact_df)
            logger.success(
                f"Upserted {impact_count} impact rows to "
                f"{IMPACT_SCHEMA}.{IMPACT_TABLE}"
            )

        run.success(rows_processed=impact_count)

        return {
            "outages": outage_count,
            "regions": region_count,
            "impacts": impact_count if not dry_run else len(impact_df),
        }

    except Exception as e:
        logger.exception(f"Impact analysis failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logger.close()


# ── CLI entrypoint ────────────────────────────────────────────────────────


def main():
    args = sys.argv[1:]

    if "--seed-only" in args:
        print("\n=== Seeding Pipeline Regions Reference Table ===\n")
        count = seed_reference_table()
        print(f"  Seeded {count} pipeline region mappings to {REFERENCE_SCHEMA}.{REFERENCE_TABLE}")
        print()
        return

    dry_run = "--dry-run" in args

    if dry_run:
        print("\n=== Gas EBB Impact Analysis (DRY RUN) ===\n")
    else:
        print("\n=== Gas EBB Impact Analysis ===\n")

    result = run_impact_analysis(dry_run=dry_run)

    print(f"\n  Outages analyzed:  {result['outages']}")
    print(f"  Region mappings:   {result['regions']}")
    print(f"  Impacts computed:  {result['impacts']}")
    if dry_run:
        print("  (dry run — no data written to DB)")
    print()


if __name__ == "__main__":
    main()
