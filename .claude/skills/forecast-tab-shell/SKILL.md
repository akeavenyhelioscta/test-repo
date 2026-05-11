---
name: forecast-tab-shell
description: Standard page-shell pattern for any tab under `frontend/app/forecast/*`. Use when scaffolding a new forecast tab (a new model, a new comparison view) or refactoring an existing tab — covers PageHeader / RunMetadata / ActualsStatusBanner / EmptyStatePanel components, the canonical layout order, the URL-driven date+run picker convention, and the rule "every tab takes a payload (or two) from `pjm_model_outputs.forecast_runs` via `lib/server/forecastRuns.ts`."
---

# Forecast tab shell

Every tab under `frontend/app/forecast/*` follows the same skeleton. The
shell components live in `app/forecast/_components/`. The rule:
**don't hand-roll the header / status banner / empty state**. Importing
the components keeps tabs visually consistent and means a styling
change happens in one file.

## When to use

- Scaffolding a new forecast tab (new model, new comparison view).
- Refactoring an existing tab.
- Anything where you'd be tempted to copy-paste a `<header>` block,
  the "Actuals released / pending" banner, or a "no run found"
  panel from another tab.

## The components

All in `frontend/app/forecast/_components/`:

| Component             | Purpose                                                    |
|-----------------------|------------------------------------------------------------|
| `PageHeader`          | H1 + optional subline + optional right metadata block      |
| `RunMetadata`         | Convenience renderer for "Generated {ts} ET · run {id}"    |
| `ActualsStatusBanner` | Released (emerald) / pending (gray) hand-off banner        |
| `EmptyStatePanel`     | "No forecast run found" panel with optional hint           |
| `DatePicker`          | URL-driven date dropdown (`?target_date=`)                 |
| `RunPicker`           | URL-driven run dropdown (`?run_id=` or custom `paramKey`)  |
| `CollapsibleCard`     | Top-level section wrapper                                  |
| `SubCard`             | Nested section inside a CollapsibleCard                    |

The data layer is `lib/server/forecastRuns.ts`:
`listForecastRuns`, `readForecastRun`, `readLatestForecastRun`,
`listAvailableTargetDates`. Each tab fetches in parallel via
`Promise.all` — payload + run list + available dates.

## Canonical layout

```tsx
export default async function MyTabPage({ searchParams }: ...) {
  const params = await searchParams;
  const targetDate = (typeof params.target_date === "string" && params.target_date)
    || defaultTargetDate();
  const runId = (typeof params.run_id === "string" && params.run_id) || null;

  const [payload, runs, dates] = await Promise.all([
    runId
      ? readForecastRun(MODEL_NAME, targetDate, runId)
      : readLatestForecastRun(MODEL_NAME, targetDate),
    listForecastRuns(MODEL_NAME, targetDate),
    listAvailableTargetDates(MODEL_NAME),
  ]);

  if (!payload) return (
    <main className="px-8 py-8">
      <PageHeader title="My Tab" />
      <div className="mb-6 flex flex-wrap items-center gap-4">
        <DatePicker dates={dates} activeDate={targetDate} />
      </div>
      <EmptyStatePanel
        message="No forecast run found for this date."
        hint={<>Run the publisher with <code>MY_PUBLISH=1</code> ...</>}
      />
    </main>
  );

  // Family guard — pages render one model_family. Treat mismatch as wrong-tab.
  if (!isMyPayload(payload)) return (
    <main className="px-8 py-8">
      <PageHeader title="My Tab" />
      <EmptyStatePanel message={<>Unexpected family <code>{payload.model_family}</code>.</>} />
    </main>
  );

  const actualsReleased = payload.hourly.some((h) => h.actual_lmp != null);

  return (
    <main className="px-8 py-8">
      <PageHeader
        title={<>My Tab — {payload.hub}</>}
        subline={<>...one-line context (day_type, lead_days, model name, etc.)...</>}
        rightMetadata={<RunMetadata createdAtUtc={payload.created_at_utc} runId={payload.run_id} />}
      />

      <div className="mb-6 flex flex-wrap items-center gap-4">
        <DatePicker dates={dates} activeDate={targetDate} />
        <RunPicker runs={runs} activeRunId={runId} />
      </div>

      <ActualsStatusBanner released={actualsReleased} targetDate={payload.target_date} />

      {/* Body: CollapsibleCard > (chart + SubCard tables) > CollapsibleCard ... */}
    </main>
  );
}
```

The order matters: **header → pickers → status banner → body cards**.
Don't reorder for one tab — consistency is the point.

## Multi-model tabs (compare-style)

When a tab spans two models (e.g. `/forecast/compare`), the shell
still applies but with two adjustments:

1. **Two run pickers** — give each a distinct `paramKey` and `label`:
   ```tsx
   <RunPicker runs={knnRuns} activeRunId={knnRunId} paramKey="knn_run_id" label="KNN run" />
   <RunPicker runs={iceRuns} activeRunId={iceRunId} paramKey="ice_run_id" label="ICE run" />
   ```
2. **Stack RunMetadata** — wrap multiple `<RunMetadata>` blocks in a
   `<div className="space-y-1">` and pass the `label` prop:
   ```tsx
   rightMetadata={
     <div className="space-y-1">
       <RunMetadata label="KNN" createdAtUtc={knnPayload.created_at_utc} runId={knnPayload.run_id} />
       <RunMetadata label="ICE" createdAtUtc={icePayload.created_at_utc} runId={icePayload.run_id} />
     </div>
   }
   ```

The date picker is shared (one date governs both models). Use
`unionDates(knnDates, iceDates)` so the dropdown shows any date with
a run from either model.

## Tab registry

Tabs are listed once in `frontend/app/forecast/_lib/tabs.ts`:

```ts
export const TAB_DEFS: readonly TabDef[] = [
  { key: "like-day", label: "Like-Day KNN", href: "/forecast/like-day", modelName: MODEL_NAMES.knn },
  ...
];
```

Add a new tab by:
1. Adding an entry to `TAB_DEFS` (and a model_name to `MODEL_NAMES` if new).
2. Creating `app/forecast/<key>/page.tsx` following the canonical layout above.
3. Building any tab-specific components under `app/forecast/<key>/_components/`.

The `<TabBar/>` in `app/forecast/layout.tsx` picks up the new tab automatically.

## What NOT to do

- Don't hardcode the timestamp formatter — use `RunMetadata`. It already
  pins to `America/New_York` so all tabs render times the same way.
- Don't write a custom "no data" panel inline — use `EmptyStatePanel`.
- Don't write the actuals banner colors inline — use `ActualsStatusBanner`.
  Color tokens belong in `lib/chartConstants.ts` per the
  `frontend-styling` skill, not in tab pages.
- Don't reach into payload shape for hub/run_id in the header — pass
  them to `PageHeader`/`RunMetadata` props. Keeps the header
  payload-agnostic.
- Don't add `"use client"` to a tab page. Server Components SSR the
  whole page; the only client components in the shell are the pickers
  and recharts wrappers.

## Closing check

Per the `frontend-verify` skill: after editing any tab, fetch the
rendered page and grep for:
- The model name in the page (`pjm_rto_hourly`, etc.)
- The run-id short prefix in the right metadata
- "Actuals released" or "Actuals pending" depending on payload state
- No "No forecast run found" when a run exists for the date
