export const meta = {
  name: 'intelligible-surfaces',
  description: 'Extend the trust layer + plain-language treatment to Pulse, Bids, Afford, Geo, Records, Data',
  phases: [
    { title: 'Prep', detail: 'extract 6 views into modules; expand glossary (parallel)' },
    { title: 'Enhance', detail: 'one agent per tab, disjoint files' },
    { title: 'Gate', detail: 'full check green, deferred low-sev fixes, commits' },
    { title: 'Verify', detail: 'correctness + trust + copy review' },
    { title: 'Fix', detail: 'apply confirmed findings' },
  ],
}

const REPO = '/Users/evanbarley-greenfield/repos/seattle-tracker'
const PRE = `You are a subagent in ${REPO} on branch feat/intelligible-overview (today ${args.today}). Vite vanilla-JS dashboard; UI is HTML template strings; domain in src/domain/*.mjs; views in src/views/*.mjs; tests are node --test in tests/. The Overview was already rebuilt (spec: docs/superpowers/specs/2026-07-05-answer-first-overview-design.md; reference implementation: src/views/overview.mjs with deps injection via overviewDeps() in src/main.mjs, universe captions + explain popovers from src/ui/explain.mjs fed by src/domain/glossary.mjs, global initExplainLayer event delegation already wired in main.mjs). Hard rules: never git push; never start/kill dev servers; never run "npm run build" or "npm run check" (the vite build races other agents; a later gate agent runs it); validate with "node scripts/run_checks.js lint" and "npm run test" only; never edit files outside your assigned ownership; all user-facing copy plain language a non-coder gets, no em dashes, no emojis; do NOT commit unless your task says to. Your final message is consumed by an orchestrator.`

const OUT_SCHEMA = { type: 'object', properties: { summary: { type: 'string' }, files: { type: 'array', items: { type: 'string' } }, notes: { type: 'string', description: 'API notes, deviations, anything the next agents must know' } }, required: ['summary', 'files', 'notes'] }

const LOWSEV = `- [src/views/overview.mjs] Pruned-baseline slices never re-persist mid-session (shouldCapture only true on first in-session diff) so that slice's memory is lost for next session.
- [src/views/overview.mjs] Good-time banner unconditionally claims "the partial current month is excluded" but tileDelta only drops the trailing month when its sample is under 50% of the prior month's; make the claim conditional or accurate.
- [src/domain/costToWin.mjs] Trusts CSV saleToListRatio over dollar columns (rows with rounded/contradictory ratios misclassified; premium dollars use impliedList=close/ratio not actual list). Prefer dollar columns when both exist and they disagree.
- [src/domain/glossary.mjs] fastSaleShare formulaWords says "went pending in 10 days or less" but membership is pipeline hot-tag OR CDOM<=10 with ~6.7% exceptions; make the words honest.
- [src/domain/glossary.mjs] medianClose popover states the 2-week county lag twice and runs ~150 words; delete the duplicate caveat. Same for freshness entry; shrink its second caveat to "A red warning means the last data refresh failed its checks, not that the data is a few days old."
- [src/views/overview.mjs] Labels "Typical premium when over" / "Typical discount when under" clipped; append "ask".
- [src/views/overview.mjs] "Saved-home cohorting is paused for this browser." -> "Saved-home matching is paused in this browser."
- [src/views/overview.mjs] Freshness fallback heading "No sale dates" -> "No closed sales in this slice yet".
- [src/views/overview.mjs] Tile label "Median DOM" acronym never expanded; retitle "Median days on market" or tie the subtitle to the letters DOM.`

phase('Prep')
const [extraction, glossaryX] = await parallel([
  () => agent(`${PRE}
TASK (mechanical refactor, zero behavior/copy change): Extract the six remaining tab views from src/main.mjs into modules, exactly mirroring how src/views/overview.mjs + overviewDeps() work. Create:
- src/views/pulse.mjs (renderPulseView, ~lines 740-1354 and its private helpers)
- src/views/bids.mjs (renderBidsView + renderManualBidPanel)
- src/views/afford.mjs (renderAffordView)
- src/views/records.mjs (renderRecordsView)
- src/views/geo.mjs (async renderGeoView + renderGeoSelectedRows)
- src/views/data.mjs (renderDataView)
Shared helpers used by several views (renderDataSourcePill, formatters, esc, icon, sparklineSvg etc.) STAY in main.mjs or their existing modules and are passed via a deps object; a private helper used by exactly one view moves with it. Add the new files to the scanned source list in tests/index_contract.test.js (overview.mjs was added the same way). You own src/main.mjs, the six new files, and that test file. Gate: node scripts/run_checks.js lint && npm run test green. Then commit ONLY this refactor: "refactor: extract remaining tab views into src/views modules" with trailers "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" and "Claude-Session: https://claude.ai/code/session_01A3gbg6WEGR8FH1K429n4kw". Report each view's exported signature and deps shape in notes.`, { label: 'prep:extract-views', phase: 'Prep', schema: OUT_SCHEMA, effort: 'high' }),
  () => agent(`${PRE}
TASK: Expand src/domain/glossary.mjs (you own it + tests/glossary.test.js ONLY) with grounded entries for every metric the other six tabs display. READ the actual computations first (src/main.mjs view functions before/while they get extracted, src/domain/selectors.mjs, pulseMetrics.mjs, affordability.mjs, buyerProfile.mjs, scripts where needed): Pulse (watchlist pressure metrics, weekly grain, whatever tiles/series it shows), Bids/Offer Lab (suggested bid, bid range, confidence score, suggested sale/list, comps basis), Afford (the affordability decision, rate/taxes/insurance/HOA assumptions, payment breakdown), Geo (the map pressure metric, pocket definitions), Records (column meanings incl. source/lineage tags: county vs MLS vs Redfin rows), Data (validation status, per-stage row counts, source cadences). Same entry shape as existing ones (plain, formulaWords, source, cadence, caveats, buyerDirection, universeId); keep popovers under ~80 words. ALSO fix these two known glossary findings:
${LOWSEV.split('\n').filter(l => l.includes('glossary')).join('\n')}
Extend tests/glossary.test.js to cover the new ids. Gate: node scripts/run_checks.js lint && npm run test green. Do not commit. Report the new metric ids in notes.`, { label: 'prep:glossary', phase: 'Prep', schema: OUT_SCHEMA }),
])
if (!extraction) throw new Error('Extraction failed; aborting (everything depends on it)')
const GLOSSARY_IDS = glossaryX ? glossaryX.notes : 'glossary expansion failed; use existing entries only'

phase('Enhance')
const TABS = [
  { key: 'pulse', file: 'src/views/pulse.mjs', brief: `Pulse answers "Is my watchlist heating up?". Add a one-sentence plain-English verdict at the top derived from what the view already computes (like the Overview's good-time banner), plain subtitles on each metric/series, universe captions, explain triggers. Keep the weekly-grain mechanics untouched.` },
  { key: 'bids', file: 'src/views/bids.mjs', brief: `Bids/Offer Lab answers "what should I bid?". Every suggested bid card: explain trigger on the suggested bid and on the confidence score; expand jargon (SUGG S/LIST -> plain words); caption stating the comps basis ("based on 28 nearby single-family comps"). The manual bid panel gets the same treatment. Align premium language with the Overview's cost-to-win phrasing.` },
  { key: 'afford', file: 'src/views/afford.mjs', brief: `Afford answers "what can I actually afford?". Surface the assumptions honestly: a visible plain-words line listing rate, taxes, insurance, HOA and any other inputs baked into the decision, each with an explain trigger; universe caption on any comp-derived number; make the decision label's meaning inspectable.` },
  { key: 'records', file: 'src/views/records.mjs', brief: `Records is the comps table. Give column headers explain triggers (or a compact legend if triggers per-header fight the layout); label row lineage plainly (county record vs MLS-enriched vs Redfin-only) using existing source fields; universe caption above the table tying its count to the slice.` },
  { key: 'geo', file: 'src/views/geo.mjs', brief: `Geo answers "where is pressure located?". Define "pressure" in a plain sentence under the heading with an explain trigger; universe captions on pocket stats; make sure captions/popovers do not break the async leaflet render path.` },
  { key: 'data', file: 'src/views/data.mjs', brief: `Data is pipeline health. KNOWN BUG to fix: it still reads wrong top-level keys from data_refresh_report.json; the report nests under report.validation.* etc. (the Overview freshness card was already fixed this way, see src/views/overview.mjs). Rewrite the health readout in cause language (which source, its cadence, what happened last run), red only on real failure; per-stage row counts each get a plain explanation of the stage.` },
]
const enhanced = await parallel(TABS.map(t => () =>
  agent(`${PRE}
Extraction notes (view signatures + deps): ${extraction.notes}
New glossary ids available: ${GLOSSARY_IDS}
TASK: You own EXACTLY ${t.file} (plus, if truly needed, append-only CSS at the END of src/styles.css in a /* ${t.key} explain */ block, and tests/${t.key}_view.test.js if you add pure helpers worth testing). Apply the Overview's intelligibility treatment to this tab. ${t.brief}
Use renderUniverseCaption/renderExplainButton from src/ui/explain.mjs and glossary ids (add NO new glossary entries yourself; if an id you need is missing, note it and write the one-line subtitle inline instead). The global initExplainLayer delegation in main.mjs already handles clicks on explain triggers anywhere; if your popovers need per-view data context, follow how overview.mjs maps entries via its getExplainEntry and note what main.mjs wiring you need (do NOT edit main.mjs; report the needed one-liner instead). Preserve all existing behavior, ids, and event contracts. Gate: node scripts/run_checks.js lint && npm run test green. Do not commit.`, { label: `enhance:${t.key}`, phase: 'Enhance', schema: OUT_SCHEMA, model: 'sonnet' })
))
const enhancedOk = enhanced.filter(Boolean)
log(`${enhancedOk.length}/6 tab agents returned`)

phase('Gate')
const gate = await agent(`${PRE}
Six tab agents enhanced src/views/{pulse,bids,afford,records,geo,data}.mjs (uncommitted). The glossary expansion is ALREADY COMMITTED (9187e55). Their notes:
${enhancedOk.map((e, i) => `--- ${TABS[i] ? TABS[i].key : i} ---\n${e.summary}\n${e.notes}`).join('\n')}
YOU are the sole integrator now and may edit anything in src/ and tests/. TASK:
1. Apply any main.mjs wiring the notes request (e.g. per-view explain-entry context mappings), resolving conflicts consistently with how overviewDeps/getExplainEntry work.
2. Fix these deferred low-severity findings from the previous review (skip the two glossary ones if already fixed, verify):
${LOWSEV}
3. Run the FULL gate: npm run check (lint, typecheck, all node --test, vite build) and fix whatever breaks until fully green.
4. Commit in two commits: "feat: trust layer and plain-language pass across Pulse, Bids, Afford, Geo, Records, Data" (all view/wiring work) and "fix: deferred low-severity findings from Overview review" - both with trailers "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" and "Claude-Session: https://claude.ai/code/session_01A3gbg6WEGR8FH1K429n4kw". Never push.
Report commit hashes, npm run check tail, and anything you had to change in the tab agents' work.`, { label: 'gate:integrate', phase: 'Gate', effort: 'high' })
if (!gate) throw new Error('Gate agent died')

const FINDINGS_SCHEMA = { type: 'object', properties: { findings: { type: 'array', items: { type: 'object', properties: { file: { type: 'string' }, line: { type: 'number' }, severity: { type: 'string', enum: ['high', 'medium', 'low'] }, summary: { type: 'string' }, failure_scenario: { type: 'string' } }, required: ['file', 'severity', 'summary', 'failure_scenario'] } } }, required: ['findings'] }

phase('Verify')
const reviews = await parallel([
  { key: 'correctness', prompt: `Review the diff of the last three commits (git log -3, git diff HEAD~3..HEAD) for CORRECTNESS only: the view extraction must be behavior-identical (diff each extracted function against its pre-extraction body via git show; flag dropped branches, lost event wiring, deps not passed, async geo/leaflet regressions); the Data tab report-key fix must match the actual data_refresh_report.json structure; explain triggers inside interactive elements must not double-fire tab switches (see the guards overview.mjs needed); XSS via unescaped interpolation in new captions.` },
  { key: 'trust-audit', prompt: `Adversarial trust audit of the six enhanced tabs: for a sample of at least 12 new captions/popovers across all six, verify the claim against the code (universe counts match rendered data, formulaWords match real computation, source/cadence claims true: KC ~biweekly, Redfin daily, manual MLS exports frozen at Mar 2026, lineage labels match row source fields). Flag every claim that is wrong or unverifiable.` },
  { key: 'copy-ux', prompt: `Review ONLY new user-facing copy across the six tabs + the low-severity fixes: leftover jargon without explanation, em dashes or emojis (banned), popovers over ~80 words, inconsistent better/worse-for-buyer phrasing vs the Overview's conventions, clipped or ambiguous labels. Quote each offender and give exact replacement copy.` },
].map(l => () => agent(`${PRE}\nGate agent report:\n${gate}\n\n${l.prompt}`, { label: `verify:${l.key}`, phase: 'Verify', schema: FINDINGS_SCHEMA, effort: 'high', model: 'sonnet' })))
const findings = reviews.filter(Boolean).flatMap(r => r.findings)
const actionable = findings.filter(f => f.severity !== 'low')
log(`${findings.length} findings, ${actionable.length} actionable`)

phase('Fix')
let fixReport = 'No actionable findings.'
if (actionable.length > 0) {
  fixReport = await agent(`${PRE}
Fix ALL of these confirmed findings (or reject with proof). Re-run npm run check until green, commit "fix: review findings on surface pass" with the same trailers. Never push.
${JSON.stringify(actionable, null, 2)}`, { label: 'fix:findings', phase: 'Fix', effort: 'high', model: 'opus' })
}

return { extraction: extraction.summary, gate, findingsTotal: findings.length, actionable, fixReport, lowSeverity: findings.filter(f => f.severity === 'low') }