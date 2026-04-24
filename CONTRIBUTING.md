# Contributing

This repo is maintained by Aidan Keaveny (@akeavenyhelioscta). Sunny Bajwa (@sunnybajwa91) contributes within `modelling/`.

## Directory ownership

| Path           | Primary owner        | Notes                                                    |
| -------------- | -------------------- | -------------------------------------------------------- |
| `backend/`     | @akeavenyhelioscta   | Data ingestion, dbt, schedulers, Azure blob export.      |
| `modelling/`   | @sunnybajwa91        | Forecasting models, features, html reports.              |
| `azure-infra/` | @akeavenyhelioscta   | Infra.                                                   |

Data from `backend/` lands in Azure blob storage — `modelling/` reads from there, so the two sides shouldn't need to coordinate on schema changes often.

## Branching

- `main` is protected. No direct pushes; all changes land via PR.
- Branch names: `<scope>/<short-topic>`, e.g.
  - `modelling/add-lstm-features`
  - `backend/fix-pjm-holiday-seed`
  - `infra/bump-prefect-version`
- Keep branches short-lived. Rebase on `main` before opening a PR if it's more than a day or two old.

## Pull requests

1. Open a PR against `main`.
2. CODEOWNERS auto-requests Aidan as reviewer.
3. Wait for approval before merging. Squash-merge is the default.
4. Delete the branch after merge.

PR descriptions should cover **what changed** and **why**, plus a brief test plan if the change is runtime-affecting.

## Local setup

See `README.md` for the conda/pip environment and Prefect setup.
