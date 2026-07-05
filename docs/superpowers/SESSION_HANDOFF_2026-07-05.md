# Session handoff: intelligibility pass (2026-07-05)

Written so any fresh Claude Code session (including on Evan's Anthropic
internal account after a /login switch) can pick this up without the original
session's context.

## What carries over vs what does not

- Carries over (on this machine / in git): the repo and all commits, the
  pushed branch + PR #9, Claude's memory files at
  `~/.claude/projects/-Users-evanbarley-greenfield/memory/`, this doc, and the
  workflow script at `docs/superpowers/workflows/intelligible-surfaces.js`.
- Does NOT carry over: the old session's conversation context and the
  Workflow resume cache (resumeFromRunId is same-session only). A fresh
  session re-runs the script from the top; completed phases are guarded by
  commits, so re-running is safe but re-spends tokens on Prep unless you trim
  the script to the remaining phases.

## State of the work

Branch `feat/intelligible-overview` (PR #9 open to main; merge = GitHub Pages
deploy). Done and committed:

- `50505dc` Jul-5 data refresh (Redfin actives+sold, KC Jul-1 vintage)
- `3fa20c0` merge of staging into a main-based branch
- `1c5b325` design spec (docs/superpowers/specs/2026-07-05-answer-first-overview-design.md)
- `cb382c5` + `0947b52` answer-first Overview + review fixes (SHIPPED IN PR)
- `52e9cfc` all six remaining tab views extracted to src/views/*.mjs
- `9187e55` glossary entries for all six tabs' metrics

## What remains (phase 2, was mid-flight when the usage limit hit)

Per-tab enhancement using the extracted modules + new glossary entries:

1. Six agents (or sequential passes), one per `src/views/{pulse,bids,afford,records,geo,data}.mjs`:
   universe captions + explain popovers (via `src/ui/explain.mjs` + glossary),
   plain-language subtitles, verdict lines where the tab answers a question.
   The Data tab additionally must fix its stale report-key reads
   (report.validation.* nesting; see how src/views/overview.mjs reads it).
2. Gate: wire any needed main.mjs explain-entry context (mirror
   getExplainEntry in overview.mjs), fix the 9 deferred low-severity findings
   listed inside the workflow script's LOWSEV constant, run `npm run check`
   to green, commit.
3. Verify: correctness (extraction fidelity + report keys + event wiring),
   trust audit (captions vs code), copy review. Fix confirmed findings.

## How to resume

- Same session: `Workflow({scriptPath: <script>, resumeFromRunId: "wf_4dd065aa-aa2", args: {today: "..."}})`.
- Fresh session: run `docs/superpowers/workflows/intelligible-surfaces.js`
  via the Workflow tool; delete the Prep phase or keep it (extraction agent
  will find the views already extracted and should no-op/commit nothing).
- Model economy (Evan's ask): enhance/verify agents on sonnet; gate on the
  session model; keep only the hardest judgment stages on the big model.

## Standing constraints

- Never push or merge without Evan's explicit go (PR #9 merge deploys publicly).
- Data tab bug and deploy-source cleanup (retire deploy-new-style-preview.yml)
  are tracked here and in memory, not yet done.
- The dev server for this checkout should own port 4173; a stale server from
  the seattle-tracker-views worktree squatted it earlier (kill it if back).
