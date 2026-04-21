"""
Runner for manually executing Genscape API scrape scripts.

Usage:
    python runs.py                   # interactive menu
    python runs.py --list            # list all available scripts
    python runs.py all               # run all scripts
    python runs.py <number>          # run script by menu number
    python runs.py <number> <number> # run multiple scripts sequentially
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDE = {"__init__.py", "runs.py", "flows.py", "genscape_api_utils.py", "backfill.py"}


def discover_scripts() -> list[Path]:
    """Find all Genscape pipeline .py scripts (excluding helpers and orchestration)."""
    return sorted(
        p for p in SCRIPT_DIR.glob("*.py")
        if p.name not in EXCLUDE
    )


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered list of available Genscape scripts."""
    print("\n=== Available Genscape Scripts ===\n")
    for i, script in enumerate(scripts, 1):
        print(f"  [{i}] {script.stem}")
    print()


def main():
    config = RunnerConfig(
        name="Genscape",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.stem,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
