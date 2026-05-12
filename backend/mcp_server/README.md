# mcp_server

FastAPI + MCP entry point for serving structured PJM view models to
agents and frontends. It now exposes ~14 view endpoints — transmission
outages (active / window_7d / changes_24h / network / for_constraints),
DA + RT/DART network constraints, historical outages for constraints,
DA hub-summary / outage-overlap LMPs, daily / hourly LMP summaries,
DART realization, and hub bus / hub-impact (shift-factor) views. The
authoritative live list (with the matching `mcp__pjm-views__*` tool
names and the brief-workflow output dirs) is in
`backend/mcp_server/runs/README.md`.

## Run

From the repo root:

```bash
uvicorn backend.mcp_server.main:app --reload
```

Each view is `GET /views/<name>?format=md|json`. MCP transport is
mounted via `FastApiMCP(app).mount_http()` at `/mcp`.

## Required environment variables

Loaded from `backend/.env` by `backend/credentials.py`:

- `AZURE_POSTGRESQL_DB_HOST`
- `AZURE_POSTGRESQL_DB_USER`
- `AZURE_POSTGRESQL_DB_PASSWORD`
- `AZURE_POSTGRESQL_DB_PORT`
- `AZURE_POSTGRESQL_DB_NAME`

Non-secret config comes from `backend/settings.py` (no extra vars required for this endpoint).

## Layout

The package follows one shape per view: `data/<view>.py` runs the pull
(`backend.utils.azure_postgresql_utils.pull_from_db` + a template under
`data/sql/`), `views/<view>.py` builds the view model, and a markdown
formatter renders the `format=md` response. `main.py` wires every route
and mounts MCP. `data/shift_factors.py` is the local DC-PTDF
computation behind the hub-impact view (cache under `data/network/`).
Browse `backend/mcp_server/views/` for the current set rather than
trusting a tree here — it grows every time an endpoint lands.

## Reused infra (not re-ported)

- `backend.utils.azure_postgresql_utils.pull_from_db` — Postgres pull
- `backend.settings` / `backend.credentials` — env loading
