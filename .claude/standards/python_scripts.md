# Python script conventions

Standards for runnable Python scripts in this repo (e.g.
`modelling/da_models/common/data/verify_data_loader.py`). Apply when
writing or substantially modifying any module with a `__main__` block
or any script meant to be run directly.

## Entry point and arguments

- **No `argparse` for routine scripts.** Define tunable defaults as
  module-level constants near the top of the file:
  ```python
  REGION: str = "RTO"
  CACHE_DIR: Path | None = None
  ```
  Change them by editing the file or by calling `run(...)` from a
  notebook. Reserve `argparse` only for scripts that genuinely take
  user input on every run.
- Single entry point named `run(...)`. Helper functions accept the
  same defaults so they're independently callable from notebooks.
- The `__main__` block is a one-liner:
  ```python
  if __name__ == "__main__":
      run()
  ```

## Imports

- `from __future__ import annotations` first.
- Order: stdlib → third-party → local (`from da_models...`).
- Local imports go AFTER the `sys.path` bootstrap (annotate them
  `# noqa: E402`) when the script lives deep in the package tree.

## Path bootstrap

Scripts that import sibling packages need explicit `sys.path` adjustment
so they run both as `python -m pkg.mod` and `python path/to/mod.py`:

```python
_MODELLING_ROOT = Path(__file__).resolve().parents[N]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))
```

`parents[N]` depth depends on where the file lives. Verify by running
both forms once.

## Console output

- Reconfigure stdout / stderr to UTF-8 at the top of `run()`:
  ```python
  for stream in (sys.stdout, sys.stderr):
      reconfigure = getattr(stream, "reconfigure", None)
      if callable(reconfigure):
          reconfigure(encoding="utf-8", errors="replace")
  ```
- **ASCII-only output.** No emojis, no Unicode box-drawing characters
  in printed strings — they raise `UnicodeEncodeError` on the Windows
  console (cp1252) without `PYTHONIOENCODING=utf-8` set. Use `===` /
  `---` / `|` style separators.
- For tables, default to `pd.DataFrame.to_string(index=False, formatters=...)`
  with `f"{value:>10,.1f}"`-style format specs. Zero dependencies, fits
  inline with `print(...)`, fine for the common case (a few columns,
  uniform numeric scale).
- Use `tabulate` when the table is dense enough that visual column
  separators materially help scan-ability — typically: many columns
  (≥10) where the eye gets lost between cells, multiple side-by-side
  blocks within one row (e.g. dual-target metrics like
  `vs lmp_total | vs SEP`), or heterogeneous numeric scales requiring
  per-column unit-aware formatting. Don't reach for tabulate for
  3-column metric summaries — `to_string` is fine there.
- Pin tabulate to ASCII-safe formats (`tablefmt="github"`, `"grid"`,
  `"psql"`, `"simple"`). Avoid `fancy_grid` and other Unicode-box
  formats — they break the cp1252 console rule above. Pick one
  `tablefmt` per script and stay with it; don't mix styles within one
  diagnostic.

## Type hints

All public functions get type hints. Use modern syntax (`Path | None`,
not `Optional[Path]`); Python 3.10+ is the floor here.

## Logging vs print

- Ad-hoc verification / one-shot diagnostic scripts: plain `print()`.
- Pipeline / orchestration / production code: `logging` module.

## Reference template

```python
"""One-line summary.

Longer doc explaining what the script does, where its defaults live,
and how to invoke it (notebook + CLI).

Usage::

    python -m da_models.<pkg>.<mod>
"""
from __future__ import annotations

import sys
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[N]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────
DEFAULT_X: str = "..."
CACHE_DIR: Path | None = None


def helper(x: str = DEFAULT_X, cache_dir: Path | None = CACHE_DIR) -> ...:
    ...


def run(x: str = DEFAULT_X, cache_dir: Path | None = CACHE_DIR) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    result = helper(x=x, cache_dir=cache_dir)
    # report ...


if __name__ == "__main__":
    run()
```

## Output artefact naming (backtest / diagnostic scripts)

Scripts that produce persisted artefacts (sweeps, ablations, diagnostics)
write to `<package>/backtest/output/` (or the script's local
`output/`) using:

```
{run_id}.parquet                 # primary tabular output
{run_id}_meta.json               # config / scenarios / summary sidecar
```

Where `run_id = {utc_timestamp}_{uuid_hex[:6]}` — e.g.
`2026-05-05T11-02-40_b88914`. The timestamp gives chronological
sortability; the uuid suffix prevents collisions when two runs start in
the same second. Helper:

```python
def _run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    return f"{stamp}_{uuid.uuid4().hex[:6]}"
```

The metadata JSON should carry enough to reproduce the run: `run_id`,
`kind` (script identifier), input args (target dates, scenarios, config
overrides), and a top-level summary block (e.g. leaderboard) when
applicable. Reference: `pjm_rto_hourly/backtest/param_sweep.py`.

Diagnostic scripts that don't persist (one-shot scans, exploratory
checks) skip this layer — print to stdout and return the artefact dict
from `run()` for notebook use.

## quiet flag for harness-friendly scripts

Any script that prints a multi-section report should accept
`quiet: bool = False` on its `run()`. When `True`, suppress all
`print_*` calls but still return the raw artefacts in the result dict
so callers can re-aggregate.

Used by harnesses like `param_sweep.py` that iterate over many
(date × scenario) cells calling `forecast_single_day.run(..., quiet=True)`,
capturing each result dict, then printing one cross-cell summary at the
end. Without `quiet`, every cell would dump four sections and the sweep
summary would be unreadable.

Result-dict contract: any artefact a harness needs (output_table,
metrics, dataframes, scenario name, etc.) goes into the return dict by
key. The dict is the API; the printed report is the UI. New keys are
fine; renaming or removing existing keys is a breaking change for the
harness consumers — grep callers first.

## Working example in repo

`modelling/da_models/common/data/verify_data_loader.py` follows this
standard end-to-end. Use it as the canonical example when scaffolding a
new script.
