# Answer-first Overview — design spec

Date: 2026-07-05
Status: approved by Evan (approach B), implementation via multi-agent workflow

## Problem

The dashboard is hard to trust and doesn't answer the questions Evan actually
brings to it. Confirmed pain points (his words, via Q&A):

1. **Jargon** — metrics like over-ask ratio, fast-sale share, bid-up need
   decoding; unclear whether up is good or bad for a buyer.
2. **No clear answer** — he opens the dash asking "is now a good time?",
   "what does winning cost?", "what changed lately?" and gets data, not answers.
3. **Trust gaps** — all four: freshness confusion (red STALE badge), conflicting
   counts (21,074 rows vs 2,179 comps vs 846 matches vs 220 active), opaque
   sources (KC assessor vs Redfin vs manual MLS exports), opaque math.
   Plus his own observation: the wall of 1.00x sale/list medians "doesn't feel
   like it can be correct."

Density was explicitly NOT a complaint. Do not de-densify for its own sake.

### The 1.00x finding (verified 2026-07-05)

Of rows with a real list price in the enriched dataset, only ~1% are exactly
1.00; the true distribution straddles list (~49% over, ~50% under). The median
of a symmetric-around-1.00 distribution collapses to 1.00x and reads as "no
signal." The number is honest; the statistic is wrong for a split market.
Also: ~86% of rows carry no list price at all (only MLS-enriched rows do), so
provenance labeling is load-bearing.

## Design (approach B: answer-first Overview + trust layer)

### 1. Three answer blocks replace the Overview stack

Each block leads with a plain-English verdict sentence; numbers demote to proof.

- **"Is now a good time?"** — reuse existing market-direction logic, phrased
  for a buyer ("Patient is fine right now: 3 of 5 pressure signals eased vs
  May"). The six KPI tiles sit under it, each with a one-line plain-language
  subtitle and consistent better/worse-for-you phrasing.
- **"What does winning cost?"** — replaces the 1.00x median. Spread statistics
  computed ONLY on comps with a real list price, labeled as such: share sold
  over / at / under ask; typical premium (median $ and %) among over-ask
  winners; p75/p90 premium. Small over/at/under bar chart instead of the
  flatlined median sparkline.
- **"What changed since you last looked?"** — promote the existing
  since-you-last-looked stub into a real delta feed for the current slice:
  new actives, price cuts, went-pending-fast, new closed comps; rows link to
  Records/Redfin.

### 2. Trust layer (site-wide component, Overview-first)

- Universe caption under every headline stat: "of 2,179 closed comps in your
  slice (last 12 mo)".
- Click-to-open "how is this computed" popover per stat: definition, formula
  in words, source (KC assessor / Redfin / manual MLS exports), freshness with
  the source's normal cadence, caveats (e.g. only ~14% of rows carry a real
  list price; this metric uses that subset).
- Single glossary module feeds all definitions so they cannot drift.

### 3. Freshness rewrite

Cause-language instead of alarm: "KC assessor publishes ~biweekly; recorded
sales lag ~2 weeks — normal. Next expected update ~<date>." Red is reserved
for actual pipeline failures, not sources on their known cadence.

### 4. Code shape

- New tested domain modules: `src/domain/costToWin.mjs`,
  `src/domain/changesSince.mjs`, `src/domain/glossary.mjs`.
- New shared UI: `src/ui/explain.mjs` (caption + popover).
- Rebuilt Overview extracted to `src/views/overview.mjs`; `src/main.mjs`
  (2,798 lines) must not grow.
- Other tabs (Pulse, Bids, Afford, Geo, Records, Data) untouched this pass.

### 5. Testing / gates

- `node --test` coverage for costToWin + changesSince (pure functions).
- `npm run check` (lint, typecheck, test, build) green before done.
- Visual verification of the new Overview in the browser.

## Prerequisite (step 0): branch convergence

This checkout (`~/repos/seattle-tracker`) is on
`codex/staging-buyer-profile-toggle`, 9 commits ahead of its origin branch;
`main` carries the views instance's work. Per the June plan of record, converge
first: commit today's data refresh on staging, branch `feat/intelligible-overview`
off `origin/main`, merge staging into it, build there. Data-file conflicts
resolve to the freshest (2026-07-05) snapshot. No pushes without Evan's
explicit go.

## Out of scope

- Retiring `deploy-new-style-preview.yml` / deploy-source cleanup (separate task).
- Pulse/Bids/Afford/Geo/Records/Data redesigns.
- Reinstalling the daily LaunchAgent.
