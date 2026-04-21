"""
Runner for manually executing GridStatus Open Source Python scripts.

Scripts are organized by ISO: caiso/, ercot/, isone/, miso/, nyiso/, pjm/, spp/

Usage:
    python runs.py                    # interactive menu (all ISOs)
    python runs.py --list             # list all available scripts
    python runs.py all                # run all scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts sequentially
    python runs.py ercot              # run all ERCOT scripts
    python runs.py caiso pjm         # run all CAISO + PJM scripts
    python runs.py ercot --list       # list only ERCOT scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDE = {"__init__.py", "run.py", "runs.py", "flows.py"}
ISOS = ["caiso", "ercot", "isone", "miso", "nyiso", "pjm", "spp"]


def discover_scripts(isos: list[str] | None = None) -> list[Path]:
    """Find all script .py files in ISO subdirectories."""
    target_isos = isos if isos else ISOS
    scripts = []
    for iso in target_isos:
        iso_dir = SCRIPT_DIR / iso
        if iso_dir.is_dir():
            for p in sorted(iso_dir.glob("*.py")):
                if p.name not in EXCLUDE:
                    scripts.append(p)
    return scripts


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered list of available scripts, grouped by ISO."""
    print("\n=== Available GridStatus Open Source Scripts ===\n")
    current_iso = None
    for i, script in enumerate(scripts, 1):
        iso = script.parent.name.upper()
        if iso != current_iso:
            current_iso = iso
            print(f"\n  === {iso} ===")
        print(f"  [{i:>2}] {script.name}")
    print()


def handle_cli_args(args: list[str]) -> dict:
    """Extract ISO names from CLI args."""
    iso_args = [a for a in args if a.lower() in ISOS]
    if not iso_args:
        return {}
    return {
        "isos": [i.lower() for i in iso_args],
        "_consumed_args": iso_args,
    }


def main():
    config = RunnerConfig(
        name="GridStatus Open Source",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.name,
        adapter=run_script_main_only,
        handle_cli_args=handle_cli_args,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
