"""
Runner for manually executing ISONE Python scripts.

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list all available scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts sequentially
    python runs.py all                # run all scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only


def discover_scripts() -> list[Path]:
    """Find all .py scripts in the ISONE directory (excluding __init__, run, flows)."""
    return sorted(
        p for p in SCRIPT_DIR.glob("*.py")
        if p.name not in ("__init__.py", "run.py", "runs.py", "flows.py", "isone_api_utils.py")
    )


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered list of available ISONE scripts."""
    print("\n=== Available ISONE Scripts ===\n")
    for i, script in enumerate(scripts, 1):
        print(f"  [{i}] {script.name}")
    print()


def main():
    config = RunnerConfig(
        name="ISONE",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.name,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
