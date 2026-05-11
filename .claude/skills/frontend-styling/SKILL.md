---
name: frontend-styling
description: Dark-theme styling standards for the `frontend/` Next.js app. Use when modifying colors, fonts, theme tokens, Tailwind config, or composing UI components — covers the single-dark-theme rule (no light mode), the `--background` / `--foreground` tokens in `globals.css`, allowed surface ramps (slate/gray only — no zinc/neutral parallel scales), accent-color codification in `lib/chartConstants.ts`, chart tick/gridline values, the no-`bg-white`/`text-black` rule, and the `suppressHydrationWarning` body attribute.
---

# Frontend styling standards

The frontend ships a single dark theme. Light mode is not on the
roadmap — operator-facing dashboards run all day next to terminal
windows and chart tools, so the dark surface is the design intent,
not a preference toggle.

## Theme tokens

Defined once in `frontend/app/globals.css` and exposed to Tailwind in
`frontend/tailwind.config.ts`:

- `--background: #0f1117` — page background. Tailwind: `bg-background`.
- `--foreground: #e5e7eb` — primary text. Tailwind: `text-foreground`.
- Body font stack: system (`-apple-system, BlinkMacSystemFont,
  "Segoe UI", Roboto, sans-serif`) — no custom web fonts.

## Extending the palette

- Prefer Tailwind's built-in slate / gray scale for surfaces
  (`bg-gray-900/60`, `border-gray-800`, `text-slate-400`) — that's
  what the reference app uses and what reads correctly against
  `#0f1117`. Don't introduce a parallel "neutral-X" or "zinc-X"
  scale; pick one ramp and stay on it.
- Accent colors (current-period highlight, error, baseline-fit, etc.)
  are codified in chart-token files, not redefined per component.
  When charts arrive, add `frontend/lib/chartConstants.ts` mirroring
  the spark-spread-viz file (slate gridlines, cyan current-period,
  red error, amber baseline-fit) and import from there — no
  per-chart hex literals.
- Avoid `bg-white` / `text-black` anywhere in the app. If a surface
  needs to pop against the page background, layer a darker
  translucent panel (`bg-gray-900/60`, `border-gray-800`) rather
  than inverting to a light surface.
- Charts and tables: ticks `#94a3b8`, gridlines `#1f2937`, zero/axis
  baseline `#475569`. Keep these in `lib/chartConstants.ts` once
  it exists.

## Hydration

The body sets `suppressHydrationWarning` because browser extensions
inject attributes that trip Next.js's mismatch warning on the dark
background. Don't remove it.
