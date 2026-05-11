---
name: improve-prompt
description: Turn a rough natural-language ask into a structured, one-shot-ready prompt using Anthropic's prompt-engineering best practices, AND archive the result to `.claude/prompts/<DDD_MMM_DD>_<slug>.md` so the user can look back at what they prompted. Use when the user says "improve this prompt", "make this a better prompt", "draft a CODEX/Claude prompt for…", "write me a prompt to…", or invokes `/improve-prompt`. Skip when the user is already inside an implementation task — don't reformat their working instructions into a prompt unless they ask.
---

# improve-prompt

Convert a rough ask into a structured prompt suitable for one-shot implementation by Claude Code, CODEX, or another capable agent. Always archive the final prompt under `.claude/prompts/` with a date prefix.

## When to use

Trigger phrases:
- "improve this prompt"
- "make this a better prompt"
- "draft a prompt for X"
- "write a Claude / CODEX prompt to do X"
- `/improve-prompt`

Skip when the user is mid-implementation and just wants the work done — they're not asking for a prompt artifact.

## What this skill produces

Two outputs, every time:
1. The improved prompt rendered inline in the conversation so the user can review.
2. The same prompt written verbatim to `.claude/prompts/<date-prefix>_<slug>.md`.

The archive write is non-optional. If the user asks for an improved prompt, the file gets written — that's what gives them a paper trail.

## File-naming contract

Path: `.claude/prompts/<DDD_MMM_DD>_<slug>.md`

- `<DDD_MMM_DD>` — short weekday + month + zero-padded day, e.g. `Fri_May_08`. Use the `currentDate` from system context (today is the source of truth, not the user's machine clock).
- `<slug>` — 3–6 lowercase tokens, snake_case, descriptive of the task. Examples: `extend_loaders_max_horizon`, `meteo_rto_hourly_scaffold`, `frontend_actuals_banner_redesign`.
- `.md` extension.

If `.claude/prompts/` doesn't exist, create it. If a file with the same name already exists, append `_v2`, `_v3`, … — never overwrite.

## The prompt structure

Use these XML tags in this order. Omit a tag when its content would be empty rather than leaving a placeholder.

```
<role>          1–3 sentences. Who Claude is and what kind of task this is.
<context>       Repo, files, conventions, constraints, the *why* behind each rule.
<source_files>  Read-these-first list (canonical templates, contracts, related code).
<template_file> When mirroring an existing file: paste it verbatim near the top. (Long-context tip: longform reference goes at the top, the task at the bottom.)
<task>          One paragraph. The headline ask.
<deliverables>  Numbered list of concrete artifacts (files, parameters, doc updates).
<implementation_rules>  Hard rules. Each rule states the rule + a one-line *why*.
<open_questions>        Ambiguities surfaced up-front, each with a sensible default-and-document fallback.
<success_criteria>      Checkable conditions. Runnable commands and grep checks beat prose.
<process>       Numbered step-by-step plan. Read first, edit second, verify last.
```

## Prompting rules to apply

These come from Anthropic's prompting guide (`docs.anthropic.com → prompt-engineering`) and from what's worked in this codebase.

1. **Be clear and direct.** Write what good output looks like, not "do this well." Replace adjectives with measurements.
2. **Rule + why.** Every "must / must not" line gets a `Why:` clause inline. Claude generalizes from reasons, not from prohibitions.
3. **Positive over negative.** Frame instructions as "do X" not "don't do Y." Reserve negatives for hard prohibitions, then make them loud (`MUST NOT`).
4. **XML tags.** Use the structure above. Tag names are part of the contract — keep them consistent across prompts.
5. **Long-context layout.** When a prompt mirrors an existing file, paste the file verbatim near the top inside `<template_file>`. The task goes at the bottom. Documented gain: up to ~30% on complex multi-document inputs.
6. **Implement-don't-suggest framing.** Open with "You are scaffolding…" / "You are extending…" not "How would we…". Claude 4.x defaults to implementing, so make that the contract.
7. **Concrete success criteria.** A runnable command + a `grep` check beats "make sure it works." Aim for criteria the user could verify without reading code.
8. **Surface ambiguities up-front.** Add `<open_questions>` with defaults so the agent commits to a position rather than guessing silently.
9. **Scope guardrails.** Explicitly mark out-of-scope work. Stops the agent from threading a parameter through 10 files when the ask is one layer.
10. **Centralization rule when applicable.** "ONE helper, every caller goes through it" heads off duplication-by-copy.

## How to do it

Three steps. Keep them visible to the user.

### Step 1 — Read enough to ground the prompt

Skim the files the prompt will reference. The point is to write specific paths and function names, not generic placeholders. Spend ~1–3 minutes here, not 15 — the goal is grounding, not full understanding.

If the rough ask is too vague to ground (e.g. "make a prompt about the frontend"), ask one clarifying question before drafting.

### Step 2 — Draft the prompt

Apply the XML structure and the 10 rules above. Inline the prompt in the conversation so the user can see what's about to be archived. Bias toward including:
- Specific file paths the prompt-runner should read.
- The actual function / parameter / column names involved (not "the relevant function").
- A rule with a `Why:` for every constraint.
- Success criteria that include at least one `grep` check OR one runnable command line.

### Step 3 — Archive

Write the same prompt verbatim to `.claude/prompts/<DDD_MMM_DD>_<slug>.md`. Confirm the path back to the user in one sentence — no need to dump the full body twice.

## Examples

### Example 1 — vague ask, ask one clarifier first

User: "Make me a prompt for the data loaders."

Response: "Quick — which loaders, and what's the change? E.g. 'extend the Meteologica coalesced loaders to surface multi-day forecast horizons' vs 'add a new loader for X parquet'."

(Don't draft until grounded.)

### Example 2 — concrete ask, draft + archive

User: "Improve this prompt: I want a script that publishes the latest forecast run to Slack."

Response:
1. Skim `frontend/lib/server/forecastRuns.ts` and any existing Slack helper.
2. Draft a prompt with `<role>` (publishing helper), `<context>` (forecast_runs schema), `<task>`, `<deliverables>` (one new script + tests), `<success_criteria>` (a runnable `python -m … --dry-run` that prints the message body).
3. Archive to `.claude/prompts/Fri_May_08_publish_forecast_to_slack.md`.

### Example 3 — mirror-this-file ask, use `<template_file>`

User: "Write a prompt for scaffolding a sibling of `pjm_rto_hourly` that uses Meteologica forecasts."

Response: prompt opens with `<template_file>` containing the entire pjm `forecast_single_day.py`, then the structural sections below it (so the long context lives at the top).

## Anti-patterns

- **Inline summaries instead of file paths.** "The relevant config" is useless; the prompt-runner has to grep for it. Always cite `path/to/file.py`.
- **Adjective constraints.** "Make it elegant / robust / clean" gives the agent nothing. "Each loader takes a `latest_only: bool = False` keyword-only parameter" gives it everything.
- **Bare prohibitions with no `Why:`.** "Don't import from sibling packages" → the agent doesn't know whether a one-line shim is also off-limits. With `Why:` it can judge edge cases.
- **Skipping the archive.** If the user asked for a prompt artifact, the file gets written. Don't ask "want me to save it?" — just save it and confirm the path.
- **Multiple files at once.** One archive file per prompt. If a request bundles two distinct prompts, write two files.
