---
name: vercel-project-setup
description: Vercel project configuration for the `frontend/` deployment. Use when configuring environment variables, setting Root Directory, linking the Vercel project, or troubleshooting why the Python tree is being built. Covers Root Directory = `frontend`, where cron/region/timeout config lives (`frontend/vercel.json`), and env-var scoping (dashboard per environment + local mirror via `frontend/.env.local`).
---

# Vercel project setup

- **Root Directory:** `frontend` (set in the Vercel dashboard so the
  Python tree is excluded from builds).
- **Cron jobs / function regions / max durations:** declared in
  `frontend/vercel.json` once endpoints exist. No `vercel.json` is
  required for the blank-page deploy. See the `frontend-cron-caching`
  skill for the schedule + function config conventions.
- **Environment variables:** managed in the Vercel dashboard, scoped
  per environment. Mirror locally via `frontend/.env.local`
  (gitignored).
- Server-only env vars must not be `NEXT_PUBLIC_*` prefixed. DB
  connection strings, API keys, blob SAS tokens — all consumed inside
  `frontend/lib/server/`.
