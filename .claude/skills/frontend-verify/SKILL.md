---
name: frontend-verify
description: Validation contract for any `frontend/` change that affects what the browser renders. Use whenever you edit a Server/Client Component, route handler, data type, or anything in `frontend/app/`, `frontend/lib/`, or `frontend/types/` that flows into a page. Codifies the rule "API OK ≠ rendered OK" — the closing check must fetch the rendered page (not the API) and grep for the expected user-visible content. Spell out the exact PowerShell probe and what counts as evidence the change works.
---

# Frontend rendering verification

Apply to any change in `frontend/` that affects what the browser
shows: components, route handlers, type changes, blob/DB readers,
chart series, table rows, filters, error states. The same rule
applies whether the work was directly requested or a side effect of
another task.

## The rule

**Don't claim a frontend change works until you've fetched the
rendered page and confirmed the expected user-visible content is in
the HTML.** API-level evidence (curl returns the data) is necessary
but not sufficient — components can ignore fields, gate them behind
feature flags, or simply not have a row/column for them.

The user is not the feedback loop. If a regression is going to be
caught by "I don't see the actuals in the table," your verification
already missed it.

## The four-step probe

After every frontend change that affects rendering, do all four —
in order — before reporting done.

1. **Identify the user-visible signal.** What text/label/value
   should now appear that wasn't there before, or should now be
   absent? Pick something concrete: a row label (`Actual`), a
   column header, a numeric value matching an upstream source
   (e.g. terminal output), an error string, a class name.

2. **Confirm the data path produces it.** Hit the underlying API
   route or DB query directly. PowerShell:

   ```powershell
   $r = Invoke-RestMethod "http://localhost:3000/api/<route>?<params>"
   $r.<field> | Format-Table -AutoSize
   ```

   If this is empty/null, fix the data path first — no point
   checking the page.

3. **Fetch the rendered page and grep for the signal.** This is
   the step that catches "API has it, table doesn't render it"
   bugs:

   ```powershell
   $d = Invoke-WebRequest "http://localhost:3000/<route>" -UseBasicParsing
   foreach ($p in 'Actual','P10','Forecast','<your signals>') {
       $c = ([regex]::Matches($d.Content, ">$p<")).Count
       "{0,-12} = {1}" -f $p, $c
   }
   ```

   For values, anchor on a label and dump the surrounding chunk:

   ```powershell
   $idx = $d.Content.IndexOf('>Actual<')
   if ($idx -ge 0) {
       $snippet = $d.Content.Substring($idx, 2200)
       ([regex]::Matches($snippet, '>(\d+\.\d)<')).Value -join ' '
   }
   ```

4. **Cross-check at least one value against the upstream source.**
   When the page should mirror something authoritative (a Python
   terminal report, a SQL result, a parquet column), pick 2–3
   cells and verify they match. "Page renders 43.2 at HE1, terminal
   shows 43.158 — matches" is the kind of evidence that ends the
   loop.

If any step fails, fix and re-run all four. Don't report success
between fixes.

## What to look for

- **Server Components** SSR everything — table cells, labels, and
  text appear directly in the page HTML and are greppable.
- **Client Components** (anything with `"use client"`, including
  recharts/`ForecastChart`-style SVG output) hydrate after JS
  loads. The SVG paths are NOT in the SSR HTML. For these:
  - The series name (`>Actual<` in a legend) and any
    server-rendered scaffolding ARE in the HTML.
  - The bound data prop IS in the HTML (Next.js serializes props
    into the `__NEXT_DATA__` script tag) — grep for a known value.
  - The recharts `<path>` SVG is NOT — don't try to verify SVG
    rendering via `Invoke-WebRequest`. Verify the data prop
    instead, and trust that recharts will plot what's in it.

- **Empty states** are the silent failure mode. The page renders
  fine, just without the thing you added. Always grep for both the
  presence of your new signal AND the absence of an empty-state
  marker (`No forecast snapshot`, `No data`, etc).

- **Default route params.** If the page has a default
  (`defaultTargetDate()` etc), check both `/<route>` and
  `/<route>?<explicit-params>`. UTC vs local-time defaults bite
  here, especially around midnight ET.

## Worked example (from the bug this skill came out of)

Change: published a forecast with actuals, expected the
`/forecast` page to show them.

- API check passed: `Invoke-RestMethod /api/da-forecast?...` returned
  `actual_lmp` for every hour.
- Rendered page check: `>Actual<` count in the HTML was **0** —
  `ForecastTable` only iterated `QUANTILE_ROWS = ["P10","P25",
  "P50","Forecast","P75","P90"]`, no Actual row.
- Fix: add an Actual row to the table that pulls
  `row.actual_lmp` per HE and computes block aggregates locally.
- Re-verify: `>Actual<` count = 1, HE1=43.2 in the HTML matches
  43.158 in the terminal report. Done.

The API check alone would have shipped the bug.

## Pre-flight: dev server must be running

The probe needs `localhost:3000` to be up. If `Invoke-WebRequest`
returns connection-refused, start it first:

```powershell
Set-Location frontend
npm run dev
```

Wait for `Ready in <ms>` in the output before probing. If multiple
dev servers are stuck, kill all `node.exe` processes whose
`CommandLine` contains `frontend\node_modules` or `npm-cli.js run
dev` before relaunching.
