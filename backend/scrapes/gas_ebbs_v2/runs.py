"""
Runner for manually executing gas EBB v2 scraper pipelines (Azure SQL target).

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list all configured pipelines
    python runs.py <number>           # run pipeline by menu number
    python runs.py <number> <number>  # run multiple pipelines sequentially
    python runs.py all                # run all pipelines (parallel by family)
    python runs.py all --sequential   # run all pipelines sequentially
"""

import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.scrapes.gas_ebbs_v2.base_scraper import (
    create_scraper,
    discover_all_pipelines,
)

# Thread-safe counter and print lock
_print_lock = threading.Lock()
_counter_lock = threading.Lock()
_completed_count = 0


def display_menu(pipelines: list[tuple[str, str, Path]]) -> None:
    """Print a numbered list of available gas EBB pipelines."""
    print("\n=== Available Gas EBB v2 Pipelines (Azure SQL) ===\n")
    for i, (name, family, _) in enumerate(pipelines, 1):
        print(f"  [{i:2d}] {name:<25s} ({family})")
    print()


def run_pipeline(
    pipeline_name: str,
    source_family: str,
    total: int,
) -> tuple[str, str, bool, int, float]:
    """Instantiate and run a single pipeline scraper.

    Returns (pipeline_name, source_family, success, notice_count, elapsed).
    """
    global _completed_count

    start = time.time()
    try:
        scraper = create_scraper(source_family, pipeline_name)
        result = scraper.main()
        elapsed = time.time() - start
        count = len(result) if result else 0
        success = True
        msg = f"PASS  ({count} notices) [{elapsed:.1f}s]"
    except Exception as e:
        elapsed = time.time() - start
        count = 0
        success = False
        msg = f"FAIL  ({type(e).__name__}: {e}) [{elapsed:.1f}s]"

    with _counter_lock:
        _completed_count += 1
        seq = _completed_count

    with _print_lock:
        print(f"  [{seq}/{total}] {pipeline_name} ({source_family}) ... {msg}")

    return (pipeline_name, source_family, success, count, elapsed)


def _run_family_batch(
    family: str,
    pipelines: list[tuple[str, str, Path]],
    total: int,
) -> list[tuple[str, str, bool, int, float]]:
    """Run all pipelines in a source family sequentially."""
    results = []
    for name, fam, _ in pipelines:
        results.append(run_pipeline(name, fam, total))
    return results


def run_parallel(
    selected: list[tuple[str, str, Path]],
    max_workers: int = 10,
) -> tuple[int, int]:
    """Run pipelines in parallel, grouped by source family."""
    global _completed_count
    _completed_count = 0

    families: dict[str, list] = defaultdict(list)
    for name, family, config_path in selected:
        families[family].append((name, family, config_path))

    total = len(selected)
    print(
        f"\n=== Running {total} Gas EBB v2 pipeline(s) "
        f"across {len(families)} families (max {max_workers} concurrent) ===\n"
    )

    passed = 0
    failed = 0
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_family_batch, family, pipes, total): family
            for family, pipes in families.items()
        }
        for future in as_completed(futures):
            for _, _, success, _, _ in future.result():
                if success:
                    passed += 1
                else:
                    failed += 1

    elapsed_all = time.time() - start_all
    print(
        f"\n=== Done: {passed} passed, {failed} failed "
        f"(out of {total}) in {elapsed_all:.1f}s ===\n"
    )
    return passed, failed


def run_sequential(selected: list[tuple[str, str, Path]]) -> tuple[int, int]:
    """Run pipelines sequentially."""
    global _completed_count
    _completed_count = 0

    total = len(selected)
    passed = 0
    failed = 0
    print(f"\n=== Running {total} Gas EBB v2 pipeline(s) sequentially ===\n")

    start_all = time.time()
    for name, family, _ in selected:
        _, _, success, _, _ = run_pipeline(name, family, total)
        if success:
            passed += 1
        else:
            failed += 1

    elapsed_all = time.time() - start_all
    print(
        f"\n=== Done: {passed} passed, {failed} failed "
        f"(out of {total}) in {elapsed_all:.1f}s ===\n"
    )
    return passed, failed


def main():
    pipelines = discover_all_pipelines()
    if not pipelines:
        print("No gas EBB v2 pipelines configured.")
        sys.exit(1)

    # Parse CLI arguments
    flags = [a for a in sys.argv[1:] if a.startswith("-")]
    args = [a for a in sys.argv[1:] if not a.startswith("-")]

    # --list flag
    if "--list" in flags:
        display_menu(pipelines)
        sys.exit(0)

    sequential = "--sequential" in flags

    # Determine which pipelines to run
    run_all = any(a.lower() == "all" for a in args)
    number_args = [a for a in args if a.isdigit()]

    if run_all:
        indices = list(range(len(pipelines)))
    elif number_args:
        indices = [int(a) - 1 for a in number_args]
    else:
        # Interactive mode
        display_menu(pipelines)
        selection = input(
            "Enter pipeline number(s) to run (comma-separated, or 'all'): "
        ).strip()
        if selection.lower() == "all":
            indices = list(range(len(pipelines)))
        else:
            try:
                indices = [int(s.strip()) - 1 for s in selection.split(",")]
            except ValueError:
                print("Invalid input. Enter numbers separated by commas.")
                sys.exit(1)

    # Validate indices
    selected = [pipelines[i] for i in indices if 0 <= i < len(pipelines)]
    invalid = [i + 1 for i in indices if not (0 <= i < len(pipelines))]
    for num in invalid:
        print(f"  Invalid selection: {num} (choose 1-{len(pipelines)})")

    if not selected:
        sys.exit(1)

    # Run: parallel by default for bulk runs, sequential for small selections
    if len(selected) > 1 and not sequential:
        passed, failed = run_parallel(selected)
    else:
        passed, failed = run_sequential(selected)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
