# Two-agent coordination: UI refresh + data refresh (2026-09-19)

Two agents are working this repo at once. This file is the shared plan: who
can do what, who owns which files, the order things run in, and the contracts
between the two halves. Read it top to bottom before starting; append to the
**Status log** at the bottom when you finish or hand something over.

- **Frontend agent** — sandboxed cloud session. Owns the UI. Works on branch
  `claude/ui-refresh`. Cannot reach Redfin, King County, or map-tile hosts, and
  has none of the gitignored local inputs.
- **Local agent** — runs on Evan's laptop in `~/repos/seattle-tracker`. Has
  network access (Redfin, King County) and every local input. Owns the data
  pipeline and anything that must be run against real inputs.

Neither agent merges to `main` or opens/merges a PR without Evan's explicit go.
A push to `main` deploys the public site.

## 1. Who can do what

| Capability | Frontend (cloud) | Local (laptop) |
|---|---|---|
| Edit `src/`, styles, views, run `npm run check` | yes | yes |
| Headless-browser screenshots of every tab | yes | yes |
| Fetch Redfin actives / sold / property history | **no** (blocked) | yes |
| Download King County assessor extracts | **no** (blocked) | yes |
| Read `realtor_exports/`, `EXTR_*.csv`, `redfin_*` caches | **no** (gitignored, not in clone) | yes |
| Run `build_*`, `merge_*`, `backfill_*`, `refresh_*` scripts for real | **no** (no inputs) | yes |
| See real map tiles in Geo | **no** (tile host blocked) | yes |
| See the Afford tab configured (`public/affordability.config.json`) | **no** (private, gitignored) | yes |
| See real macOS font rendering | no (Linux fallback fonts) | yes |
| Push a feature branch | yes | yes |
| Commit refreshed data to `main` via the existing daily flow | no | yes |

Rule of thumb: if it needs the network or a file that is not in git, it is the
local agent's. If it is how the app looks or reads, it is the frontend agent's.

## 2. File ownership (so we never edit the same file)

**Frontend agent owns**

- `src/**` (all of `views/`, `ui/`, `domain/`, `workers/`, `main.mjs`, `styles.css`)
- `index.html`, `assets/`, `favicon.*`
- `tests/*.test.js` that cover `src/` (view, domain, glossary, contract tests)

**Local agent owns**

- `scripts/**` (every pipeline, fetch, merge, backfill, validate script)
- All data outputs: `public_sales_proxy_*.csv`, `public/*.csv`, `public/*.json`,
  `data_refresh_report.json`, `tmp/sold_2026_pending_context*`
- `tests/*.test.js` that cover `scripts/` (`build_mls_enriched_dataset`,
  `fetch_redfin_sold`, `merge_redfin_sold`)
- `redfin_searches.json`, `.github/workflows/*`, `netlify.toml`

**Shared, edit only by appending:** this file's Status log, `DATA_SCHEMA.md`.
If you need a change in the other agent's area, write it up in the Status log
as a request instead of making it.

## 3. Branches and merge protocol

- Data refresh commits keep landing on `main` exactly as the daily job already
  does (`Daily data refresh YYYY-MM-DD`). They touch data files only.
- UI work lives on `claude/ui-refresh`. It touches no data files, so it will
  not conflict with data commits. Before it is proposed for merge the frontend
  agent merges `main` into it; any data-file conflict resolves to `main`.
- Pipeline script fixes (section 5) go on their own branch off `main`, e.g.
  `fix/pipeline-sold-remerge`, so they can be reviewed apart from data.
- Nobody force-pushes a shared branch.

## 4. What is stale right now (measured from the committed CSV, as of Sep 19)

| Source | Newest row | Age | Refreshed by |
|---|---|---|---|
| Redfin actives | Sep 19 | fresh | daily launchd job |
| Redfin sold | Aug 18 | ~1 month | `npm run refresh:sold` (not in the daily job) |
| KC recorded sales | Jun 24 | ~3 months | `npm run refresh:kc-fresh` |
| Redfin history (pending date, list at pending) | Jun 12 | ~3 months | `npm run backfill:history` |
| Realtor MLS exports | Mar 19–20 | ~6 months | manual re-export from the realtor |

Consequences visible in the app today:

- **Bids tab is dead.** All 278 active listings show "n/a, insufficient comps".
  A bid comp needs a sold row with a real list price in the last 90 days; the
  newest one is Jun 12. Restoring it needs fresh sold rows *and* their history
  backfill (the sold feed itself carries no list price).
- **~480 zombie actives.** Rows from the March realtor export are still flagged
  Active/Pending with list dates on or before Mar 20. The report says 1,833
  active; only 1,457 come from today's Redfin feed.
- The "what does winning cost" and sale-vs-ask stats rest on rows whose newest
  member is about 3 months old.

## 5. Local agent: task list (in priority order)

**L1. Refresh the data. Order matters.**

```bash
npm run refresh:kc-fresh      # KC download -> rebuild -> re-merge actives -> history backfill (cached) -> sync
npm run refresh:sold          # fetch + merge Redfin sold. MUST run after the KC rebuild (see L2)
npm run build:url-index       # pick up URLs for the newly sold homes
npm run backfill:history -- --min-price 700000 --max-price 2000000   # recover list-at-pending for them
npm run sync:public
npm run validate:data
```

Done when: newest `saleDate` is within about 3 weeks of today, there are sold
rows with `saleToListRatio > 0` dated inside the last 90 days, and the Bids tab
scores at least some active listings. Commit the data files to `main` through
the normal flow.

**L2. Fix: a KC rebuild silently drops Redfin sold rows.**
`scripts/refresh_kc_data.sh` rebuilds the enriched CSV from county + realtor
inputs, then re-merges Redfin actives (step 3) and history (step 4), but never
re-merges Redfin sold. Running it alone wipes the ~1,069 `REDFIN_SOLD` rows
(all of Jul–Aug). Add a step that runs `node scripts/merge_redfin_sold.js` when
`redfin_sold_listings.csv` exists, before the history backfill so the backfill
can enrich the restored rows. Consider adding `fetch:sold` + `merge:sold` to
`refresh_actives_daily.sh` too, so sold data stops going a month stale.

**L3. Fix: expire stale open rows from old realtor exports.**
In `build_mls_enriched_dataset.js`, rows with join method `MLS_STATUS_OPEN` or
`APN_LISTING_STUB` and an open status keep that status forever. Proposal: drop
an open-status realtor row when its list date is older than N days (suggest 45)
*and* no row in today's Redfin actives feed matches it, since the Redfin feed is
the fresher source of truth for what is live. Needs real inputs to validate the
match rate, which is why it is yours. Add a test alongside
`tests/build_mls_enriched_dataset.test.js`.

**L4. Optional contract: per-source freshness in the report.** See section 7.

**L5. Verify the things the frontend agent cannot see**, once
`claude/ui-refresh` has UI commits on it: Geo with real map tiles (marker
contrast, popup styling on the basemap), the Afford tab with the private config
loaded, and how the type looks in Safari/Chrome on macOS. Report problems in
the Status log with a screenshot path or a one-line description.

**Needs Evan, not an agent:** a fresh set of realtor MLS exports
(`realtor_exports/*.csv`). Everything that depends on true MLS pending dates and
list-at-pending for non-Redfin-matched homes stays 6 months old until then.

## 6. Frontend agent: task list

- F1 (done, `031549f`): missing days-on-market / list price now reads as
  unknown instead of zero. This removes the fake "0.0% fast-sale", "0d", "$0
  bid-up" readings and stops the Pulse verdict calling a trend from absent
  data. It also corrects the fast-sale share, which was understated because
  rows with no DOM data were being counted as slow.
- F2: new design system and shell. One sticky header (brand, tabs, actions) and
  one lens bar (filter chips, filters, freshness) in place of four stacked
  bands. Rebuilt stylesheet on tokens, working dark mode, real mobile layout.
- F3: Overview. Fix the KPI tiles (label collisions, clipped deltas and
  sparkline labels), make "what does winning cost" a compact grid, remove
  filler sections.
- F4: per-tab fixes. Bids form layout and a single honest empty state instead
  of 278 "n/a" cards; Data tab 3,700px horizontal overflow; Geo side panel
  overflow; mobile three-column card bug; Records density.
- F5: honest freshness in the header, per source, derived from the rows
  themselves (and from the section 7 block when present).

## 7. Contracts between the two halves

**CSV schema.** The app reads the columns in `DATA_SCHEMA.md` by name. Local
agent: additive changes only; do not rename or drop a column without a note in
the Status log first. Frontend agent: will not start depending on a new column
without asking for it here.

**`data_refresh_report.json`.** The app reads `generatedAt`, `counts.*`,
`source.realtorFiles`, and everything under `validation.*`. Keep those stable.

**Proposed addition (L4), optional and backward compatible.** A top-level
`sources` block written by the pipeline, so the header can state freshness from
the fetch itself rather than inferring it from row dates:

```json
"sources": {
  "redfinActives": { "fetchedAt": "2026-09-19T13:52:00Z", "rows": 1457 },
  "redfinSold":    { "fetchedAt": "2026-08-18T14:10:00Z", "rows": 1069, "newestSaleDate": "2026-08-18" },
  "redfinHistory": { "backfilledAt": "2026-06-15T09:00:00Z", "rowsEnriched": 2543 },
  "kingCounty":    { "extractDate": "2026-07-01", "newestSaleDate": "2026-06-24" },
  "realtorExports":{ "newestFileMtime": "2026-03-20T18:00:00Z", "files": 8 }
}
```

The frontend will render this block when it exists and fall back to dates
derived from the rows when it does not, so there is no ordering dependency
between us. If you implement it, say so in the Status log.

**Semantics change to be aware of (F1).** Any downstream consumer of
`fast-sale share` should know its denominator is now "rows with a
days-on-market signal", not "all closed rows".

## 8. Status log (append only, newest last)

- 2026-09-19 frontend: audited all 7 tabs at desktop and mobile, light and
  dark. Baseline `npm run check` equivalents green (146 tests). Landed F1
  (`031549f`, 156 tests green). Wrote this plan. Starting F2.
- 2026-09-19 local (worktree `~/repos/seattle-tracker-views`, branch
  `data/sold-refresh-2026-09-19`, pushed): L1 sold half done. Ran
  `fetch_redfin_sold.js --sold-within-days 180 --bands` (26 regions, 2,874
  SF/TH rows, Mar 24 - Sep 18, no caps) + `merge_redfin_sold.js`: 1,810
  `REDFIN_SOLD` rows now (was 1,069), newest `saleDate` Sep 18 (was Aug 18),
  dataset 19,671 -> 20,412 rows, `npm run check` green. Commit `5ef35da`.
  Ingested the sold URLs into `redfin_url_index.json` (+525) and started
  `backfill_redfin_history.js --since 2026-06-01 --min-price 700000
  --max-price 2000000` (1,059 resolvable of 1,167 candidates, ~1h at the
  3s throttle); it lands as a second commit on the same branch. NOT pushed
  to `main` (waiting for Evan's go). Not run: `refresh:kc-fresh` (needs
  `realtor_exports/` + county download, and L2 says it would wipe the sold
  rows) — deferred until L2 is fixed. Hazard for whoever lands data on
  `main`: the 06:00 launchd job commits from `~/repos/seattle-tracker`; that
  checkout must `git pull` after any merge to `main` or its next morning
  commit will conflict and stall. Confirmed from the data that the DOM-zero
  artifact is real (`REDFIN_SOLD` rows carry blank `domDays`; blank
  list/pending dates made `daysToPending` read 0) — F1 covers it, no
  pipeline change needed. No requests for the frontend agent.
- 2026-09-19 local (later): the L1 history backfill is BLOCKED from Node —
  Redfin now serves an AWS WAF challenge to server-side fetches of property
  pages and of the belowTheFold/mainHouseInfoPanelInfo detail JSON
  (`propertyParcelInfo` and the `gis` sold/active feeds still work).
  `backfill_redfin_history.js` parsed 0 of 75 pages and was stopped.
  Replacement, no network: new `scripts/backfill_list_from_active_snapshots.js`
  reads the daily REDFIN_ACTIVE snapshots from git history and gives each
  REDFIN_SOLD row the asking price / list date / DOM from the last day its
  MLS# was seen active, with pendingDate = that day. Applied to 848/1,810
  (Jun 43/329, Jul 246/458, Aug 357/414, Sep 202/222). Wired into
  `refresh:sold` and `npm run backfill:snapshots`; 6 tests; check green
  (152). Commit `8621df7` on `data/sold-refresh-2026-09-19` (branch now has
  data + this pipeline script; split if you want them reviewed apart).
  FYI frontend: `addressSource` now also takes the value
  `REDFIN_SOLD+ACTIVE_SNAPSHOT` on those rows (display-only, additive;
  `mlsJoinMethod` unchanged). Still waiting on Evan's go before anything
  lands on `main`.
