"""
Runner for manually executing WSI weather scripts.

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list available scripts
    python runs.py all                # run all scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts sequentially
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDE = {"__init__.py", "utils.py", "runs.py", "flows.py"}


def discover_scripts() -> list[Path]:
    """Find all runnable WSI scripts in this folder."""
    return sorted(
        p for p in SCRIPT_DIR.glob("*.py")
        if p.name not in EXCLUDE
    )


def display_menu(scripts: list[Path]) -> None:
    print("\n=== Available WSI Weather Scripts ===\n")
    for i, script in enumerate(scripts, 1):
        print(f"  [{i:>2}] {script.stem}")
    print()


def main() -> None:
    config = RunnerConfig(
        name="WSI Weather",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.stem,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
