# PJM DA Frontend

Next.js 15 (App Router) + React 19 + TypeScript + Tailwind 3. Skeleton
only — no pages, data integrations, or cron jobs yet. This directory
will host the dashboards (fundies + model outputs) backed eventually
by Postgres and Azure Blob storage.

## Local development

```bash
cd frontend
npm install
npm run dev      # http://localhost:3000
npm run build    # production build
npm run lint
```

## Vercel deploy

This frontend lives inside the parent Python repo
(`helioscta-pjm-da-data-scrapes`). When linking the Vercel project,
configure:

- **Root Directory:** `frontend`
- **Framework preset:** Next.js (auto-detected once root is set)
- **Build / Install / Output:** leave at the Next.js defaults

Setting the root directory is what tells Vercel to ignore the rest of
the repo (Python modelling code, dbt, fundies notes, etc.).
