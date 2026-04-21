"""
Runner for manually executing all WSI Python scripts across subfolders.

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
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDE = {"__init__.py", "utils.py", "runs.py", "flows.py"}


def discover_scripts() -> list[Path]:
    """Find all runnable WSI scripts across subfolders."""
    scripts: list[Path] = []
    for path in sorted(SCRIPT_DIR.rglob("*.py")):
        if path.name in EXCLUDE:
            continue
        # Skip files directly in wsi/ (only utils.py lives here)
        if path.parent == SCRIPT_DIR:
            continue
        scripts.append(path)
    return scripts


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered menu of available scripts, grouped by subfolder."""
    print("\n=== Available WSI Scripts ===\n")
    current_folder = None
    for index, script in enumerate(scripts, 1):
        folder = script.parent.name
        if folder != current_folder:
            current_folder = folder
            print(f"  --- {folder} ---")
        print(f"  [{index:>2}] {script.stem}")
    print()


def main() -> None:
    config = RunnerConfig(
        name="WSI",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda path: f"{path.parent.name}/{path.stem}",
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
