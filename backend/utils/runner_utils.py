"""
Shared runner utility for manually executing backend scrape scripts.

Provides the CLI loop, output suppression, module loading, and adapter-based
script execution used by all three runners (meteologica, power, positions & trades).
"""

import importlib
import inspect
import io
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


# ---------------------------------------------------------------------------
# Output suppression
# ---------------------------------------------------------------------------

@contextmanager
def suppress_output():
    """Suppress all stdout, stderr, and logging output from scripts."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    old_level = logging.root.level
    old_handlers = logging.root.handlers[:]

    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    logging.root.setLevel(logging.CRITICAL + 1)
    for handler in old_handlers:
        logging.root.removeHandler(handler)

    try:
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        logging.root.setLevel(old_level)
        for handler in old_handlers:
            logging.root.addHandler(handler)


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------

def script_to_module(script_path: Path, project_root: Path) -> str:
    """Convert a script path to a dotted module name relative to *project_root*."""
    relative = script_path.relative_to(project_root).with_suffix("")
    return ".".join(relative.parts)


# ---------------------------------------------------------------------------
# Adapter functions — each one knows how to call a script's entry-points
# ---------------------------------------------------------------------------

def run_script_pull_format_upsert(module) -> tuple[bool, str]:
    """Meteologica pattern: _pull() -> (df, metadata), _format(df, metadata), _upsert(df)."""
    with suppress_output():
        df, metadata = module._pull()

    if df.empty:
        update_id = metadata.get("update_id", "?")
        return True, f"SKIP  (0 rows, update_id={update_id})"

    with suppress_output():
        df = module._format(df, metadata)
        module._upsert(df)

    rows = len(df)
    update_id = metadata.get("update_id", "?")
    return True, f"PASS  ({rows} rows, update_id={update_id})"


def run_script_pull_upsert(module) -> tuple[bool, str]:
    """Simpler pattern: _pull() -> df, optional _format(df), _upsert(df)."""
    with suppress_output():
        df = module._pull()

    if df.empty:
        return True, "SKIP  (0 rows)"

    if hasattr(module, "_format"):
        with suppress_output():
            df = module._format(df)

    with suppress_output():
        module._upsert(df)

    return True, f"PASS  ({len(df)} rows)"


def run_script_main_only(module) -> tuple[bool, str]:
    """Fallback: calls module.main(), bypassing Prefect @flow if present."""
    main_func = module.main
    # Bypass Prefect @flow decorator — call the raw function directly
    if hasattr(main_func, "fn"):
        main_func = main_func.fn
    with suppress_output():
        result = main_func()
    if result is not None:
        if isinstance(result, int):
            return True, f"PASS  ({result} rows)"
        if hasattr(result, "__len__"):
            return True, f"PASS  ({len(result)} rows)"
    return True, "PASS"


# ---------------------------------------------------------------------------
# Adapter auto-detection
# ---------------------------------------------------------------------------

def _count_required_params(func) -> int:
    """Return the number of parameters without defaults."""
    sig = inspect.signature(func)
    return sum(
        1 for p in sig.parameters.values()
        if p.default is inspect.Parameter.empty
        and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    )


def detect_adapter(module) -> Callable:
    """Pick the right adapter by inspecting a module's functions."""
    has_pull = hasattr(module, "_pull") and callable(module._pull)
    has_format = hasattr(module, "_format") and callable(module._format)
    has_main = hasattr(module, "main") and callable(module.main)

    if has_pull:
        pull_required = _count_required_params(module._pull)
        if pull_required > 0:
            # _pull needs args we can't supply → fall back to main()
            if has_main:
                return run_script_main_only
        else:
            if has_format and _count_required_params(module._format) >= 2:
                return run_script_pull_format_upsert
            return run_script_pull_upsert

    if has_main:
        return run_script_main_only

    # Last resort — try pull_format_upsert (original behaviour)
    return run_script_pull_format_upsert


# ---------------------------------------------------------------------------
# Script execution
# ---------------------------------------------------------------------------

def run_script(
    script_path: Path,
    project_root: Path,
    index: int,
    total: int,
    display_name: str,
    adapter: Callable | None = None,
) -> bool:
    """Import a module and execute it via the chosen (or auto-detected) adapter.

    Returns True on success, False on failure.
    """
    module_name = script_to_module(script_path, project_root)
    print(f"  [{index}/{total}] {display_name} ... ", end="", flush=True)

    start = time.time()
    try:
        # Redirect stdout/stderr during import to hide noisy library init
        # messages, but do NOT touch logging.root level — raising it to
        # CRITICAL+1 breaks Prefect's first-time module initialisation.
        _old_out, _old_err = sys.stdout, sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        try:
            module = importlib.import_module(module_name)
        finally:
            sys.stdout, sys.stderr = _old_out, _old_err

        chosen = adapter or detect_adapter(module)
        success, detail = chosen(module)

        elapsed = time.time() - start
        print(f"{detail} [{elapsed:.1f}s]")
        return success
    except Exception as e:
        elapsed = time.time() - start
        print(f"FAIL  ({type(e).__name__}: {e}) [{elapsed:.1f}s]")
        return False


# ---------------------------------------------------------------------------
# RunnerConfig + CLI loop
# ---------------------------------------------------------------------------

@dataclass
class RunnerConfig:
    """Configuration for a runner instance."""
    name: str                                           # e.g. "Meteologica", "Power"
    project_root: Path                                  # absolute path to repo root
    discover: Callable[..., list[Path]]                 # returns list of script paths
    display: Callable[[list[Path]], None]                # prints the menu
    display_name: Callable[[Path], str]                  # per-script display name
    adapter: Callable | None = None                     # None → auto-detect per script
    handle_cli_args: Callable[[list[str]], dict] | None = None  # custom CLI parsing


def runner_main(config: RunnerConfig) -> None:
    """Full CLI entry-point: --list, all, number selection, interactive mode."""
    # Parse flags and positional args
    flags = [a for a in sys.argv[1:] if a.startswith("-")]
    args = [a for a in sys.argv[1:] if not a.startswith("-")]

    # Let the runner do custom CLI parsing (e.g. region filtering)
    discover_kwargs = {}
    if config.handle_cli_args:
        discover_kwargs = config.handle_cli_args(args)
        # Remove consumed args (region names) so they don't confuse number parsing
        consumed = discover_kwargs.pop("_consumed_args", [])
        args = [a for a in args if a not in consumed]

    # Discover scripts
    scripts = config.discover(**discover_kwargs)
    if not scripts:
        print(f"No {config.name} scripts found.")
        sys.exit(1)

    # --list flag
    if "--list" in flags:
        config.display(scripts)
        sys.exit(0)

    # Determine which scripts to run
    run_all = any(a.lower() == "all" for a in args)
    number_args = [a for a in args if a.isdigit()]

    # If custom CLI consumed region args, treat that as "run all discovered"
    run_region = bool(discover_kwargs)

    if run_all or run_region:
        indices = list(range(len(scripts)))
    elif number_args:
        indices = [int(a) - 1 for a in number_args]
    else:
        # Interactive mode
        config.display(scripts)
        selection = input("Enter script number(s) to run (comma-separated, or 'all'): ").strip()
        if selection.lower() == "all":
            indices = list(range(len(scripts)))
        else:
            try:
                indices = [int(s.strip()) - 1 for s in selection.split(",")]
            except ValueError:
                print("Invalid input. Enter numbers separated by commas.")
                sys.exit(1)

    # Validate
    valid = [(i, scripts[i]) for i in indices if 0 <= i < len(scripts)]
    invalid = [i + 1 for i in indices if not (0 <= i < len(scripts))]
    for num in invalid:
        print(f"  Invalid selection: {num} (choose 1-{len(scripts)})")

    if not valid:
        sys.exit(1)

    # Run
    total = len(valid)
    passed = 0
    failed = 0
    print(f"\n=== Running {total} {config.name} script(s) ===\n")

    start_all = time.time()
    for seq, (_, script) in enumerate(valid, 1):
        if run_script(
            script,
            config.project_root,
            seq,
            total,
            config.display_name(script),
            config.adapter,
        ):
            passed += 1
        else:
            failed += 1

    elapsed_all = time.time() - start_all
    print(f"\n=== Done: {passed} passed, {failed} failed (out of {total}) in {elapsed_all:.1f}s ===\n")
    sys.exit(1 if failed else 0)
