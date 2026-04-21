"""
Runner for manually executing Meteologica API scrape scripts.

Scripts are organized by region: l48/, pjm/, ercot/, miso/, spp/, caiso/, nyiso/, isone/

Usage:
    python runs.py                    # interactive menu (all regions)
    python runs.py --list             # list all available scripts
    python runs.py all                # run all scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts sequentially
    python runs.py pjm               # run all PJM scripts
    python runs.py ercot miso        # run all ERCOT + MISO scripts
    python runs.py l48 --list        # list only L48 scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDE = {"__init__.py", "auth.py", "run.py", "runs.py", "flows.py"}
REGIONS = ["l48", "caiso", "ercot", "isone", "miso", "nyiso", "pjm", "spp"]


def discover_scripts(regions: list[str] | None = None) -> list[Path]:
    """Find all content .py scripts in region subdirectories."""
    target_regions = regions if regions else REGIONS
    scripts = []
    for region in target_regions:
        region_dir = SCRIPT_DIR / region
        if region_dir.is_dir():
            for p in sorted(region_dir.glob("*.py")):
                if p.name not in EXCLUDE:
                    scripts.append(p)
    return scripts


def _get_product(name: str) -> str:
    """Return product category for grouping."""
    if "price" in name or "da_power" in name:
        return "Day-Ahead Price"
    elif "wind" in name:
        return "Wind"
    elif "pv" in name or "photovoltaic" in name:
        return "PV (Solar)"
    elif "demand" in name:
        return "Demand"
    elif "hydro" in name:
        return "Hydro"
    else:
        return "Other"


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered list of available scripts, grouped by region and category."""
    print("\n=== Available Meteologica Scripts ===\n")
    current_region = None
    current_category = None
    for i, script in enumerate(scripts, 1):
        region = script.parent.name.upper()
        category = _get_product(script.stem)
        if region != current_region:
            current_region = region
            current_category = None
            print(f"\n  === {region} ===")
        if category != current_category:
            current_category = category
            print(f"  --- {category} ---")
        print(f"  [{i:>2}] {script.stem}")
    print()


def handle_cli_args(args: list[str]) -> dict:
    """Extract region names from CLI args."""
    region_args = [a for a in args if a.lower() in REGIONS]
    if not region_args:
        return {}
    return {
        "regions": [r.lower() for r in region_args],
        "_consumed_args": region_args,
    }


def main():
    config = RunnerConfig(
        name="Meteologica",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.stem,
        adapter=run_script_main_only,
        handle_cli_args=handle_cli_args,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
