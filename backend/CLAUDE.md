# backend/

dbt project, the MCP view server, Prefect orchestration, the shared
parquet cache, and the scheduled forecaster tree — this file maps the
sub-areas and carries the dbt conventions that have no other home.

## Sub-areas

- `backend/dbt/dbt_azure_postgresql/` — the dbt project (staging +
  marts on Azure Postgres). Conventions below.
- `backend/mcp_server/` — FastAPI + MCP server exposing PJM view
  models (outages, constraints, LMPs, hub impact, …) to agents and the
  frontend. Endpoint list + layout in `backend/mcp_server/README.md`;
  the brief-workflow output dir in `backend/mcp_server/runs/README.md`.
- `backend/schedulers/` — Prefect deployments. Forecast publishing is
  one yaml per family under
  `backend/schedulers/prefect/modelling/da_models/`.
- `backend/modelling/` — the scheduled copy of the DA-price
  forecasters; **sole writer** of `pjm_model_outputs.forecast_runs`.
  Import root, family-import rule, and the data-validation preflight
  contract live in `backend/modelling/README.md` — not duplicated here.

## dbt conventions

- **One folder per data source.** A source gets one folder (e.g.
  `models/power/pjm/`) with `staging/` + `marts/` subfolders; new
  derivations drop into that source's existing `marts/` or `staging/`.
  Why: minimize folder count and co-locate a source's transforms —
  don't split one ISO across sibling `*_cleaned/` / `*_modelling/` /
  `*_features/` folders.
- **Staging model names ≤51 chars.** dbt prefixes ephemeral CTEs with
  `__dbt__cte__` (12 chars) and Postgres caps identifiers at 63, so a
  staging name over 51 chars fails with a truncated-identifier error.
  Why: hit on `transmission_outages` —
  `staging_v1_pjm_transmission_outages_changes_24h_{simple,snapshot}`
  were 54/56 → over the cap with the prefix. Fix: shorten the variant
  suffix, or skip the staging layer and inline the CTEs into the mart
  (don't reach for a non-ephemeral materialization just to keep the
  file).

## Python scripts

Runnable Python here (a `__main__` block, or a module meant to be run
directly) follows the `python-scripts` skill.
