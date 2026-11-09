"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

test("compScopeLabel maps comp tiers to plain scope words", async () => {
  const { compScopeLabel } = await importModule("src/views/bids.mjs");
  assert.equal(compScopeLabel("T1_NEIGHBORHOOD_TYPE"), "mostly nearby");
  assert.equal(compScopeLabel("T3_CITY_TYPE"), "mostly citywide");
  assert.equal(compScopeLabel("NONE"), "");
  assert.equal(compScopeLabel(undefined), "");
});

test("compBasisCaption states the comp basis using the shared universe-caption component", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  const html = compBasisCaption(28, "T1_NEIGHBORHOOD_TYPE", "Single Family");
  assert.match(html, /class="universe-caption"/);
  assert.match(html, /of 28 mostly nearby single family comps/);
});

test("compBasisCaption reads the citywide tier in plain words", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  assert.match(compBasisCaption(12, "T3_CITY_TYPE", "Condo"), /of 12 mostly citywide condo comps/);
});

test("compBasisCaption is empty with no comp count, so an unscored card renders no caption", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  assert.equal(compBasisCaption(0, "NONE", "Single Family"), "");
  assert.equal(compBasisCaption(undefined, "NONE", "Single Family"), "");
});

test("overAskPhrase mirrors the Overview's cost-to-win over/at/under-ask wording", async () => {
  const { overAskPhrase } = await importModule("src/views/bids.mjs");
  assert.equal(overAskPhrase(12), "12% over ask");
  assert.equal(overAskPhrase(-5), "5% under ask");
  assert.equal(overAskPhrase(0), "at ask");
  assert.equal(overAskPhrase(null), "");
  assert.equal(overAskPhrase(undefined), "");
});

test("the comp caption says which stretch of the market the comps came from", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  assert.match(compBasisCaption(40, "T1_NEIGHBORHOOD_TYPE", "Single Family", "fresh", 3), /comps that went pending in 0-7 days/, "week one is pooled, so the caption names the week, not the day");
  assert.match(compBasisCaption(14, "T3_CITY_TYPE", "Single Family", "stale", 62), /of 14 mostly citywide single family comps that went pending around day 62/);
  assert.match(compBasisCaption(14, "T3_CITY_TYPE", "Single Family", "stale"), /that went pending in 46\+ days/, "no day to name: fall back to the stage's range");
  assert.doesNotMatch(compBasisCaption(14, "T3_CITY_TYPE", "Single Family", ""), /pending/);
  assert.match(compBasisCaption(9, "T3_CITY_TYPE", "Single Family", "stale", 158, { domMatch: "side" }), /comps that went pending after their first week/);
  assert.doesNotMatch(compBasisCaption(9, "T3_CITY_TYPE", "Single Family", "stale", 158, { domMatch: "none" }), /pending/);
  assert.match(compBasisCaption(30, "T1_NEIGHBORHOOD_TYPE", "Single Family", "fresh", 3, { newerBuild: true }), /of 30 mostly nearby newer-build single family comps/);
});

test("the stage pill is toned by who holds the leverage and admits an unknown stage", async () => {
  const { stagePillHtml } = await importModule("src/views/bids.mjs");
  assert.match(stagePillHtml({ bidStage: "fresh" }), /stage-pill hot[^>]*>First week</);
  assert.match(stagePillHtml({ bidStage: "stale" }), /stage-pill cool[^>]*>Stale</);
  assert.match(stagePillHtml({ bidStage: "" }), /stage-pill unknown[^>]*>Stage unknown</);
});

test("listing history reads the cut from the first ask, the relist and the earlier sale", async () => {
  const { listingHistoryFacts, daysListedText } = await importModule("src/views/bids.mjs");
  const relisted = listingHistoryFacts({
    dataMode: "MLS_ENRICHED", pendingListPrice: 999000, originalListPrice: 1200000,
    hasMlsDomValue: true, mlsDOM: 17, hasMlsCdomValue: true, mlsCDOM: 89, priorSaleDate: "2026-04-23", priorSalePrice: 855000,
  });
  assert.equal(relisted.cutUsd, 201000);
  assert.equal(relisted.cutPct.toFixed(4), "0.1675");
  assert.equal(relisted.relisted, true);
  assert.equal(daysListedText(relisted), "89 (17 since relist)");
  assert.equal(relisted.priorSalePrice, 855000);
  assert.equal(relisted.askVsPriorPct.toFixed(3), "0.168");

  const plain = listingHistoryFacts({ dataMode: "MLS_ENRICHED", pendingListPrice: 1200000, originalListPrice: 1200000, hasMlsDomValue: true, mlsDOM: 5, hasMlsCdomValue: true, mlsCDOM: 5 });
  assert.equal(plain.cutUsd, 0);
  assert.equal(plain.relisted, false);
  assert.equal(plain.askVsPriorPct, null);
  assert.equal(daysListedText(plain), "5");

  const unknown = listingHistoryFacts({ dataMode: "MLS_ENRICHED", pendingListPrice: 1200000, originalListPrice: 0, hasMlsDomValue: false, hasMlsCdomValue: false, daysToPending: null });
  assert.equal(daysListedText(unknown), "n/a", "no reading is n/a, never 0");
});

test("the status says why the comps could not be matched to the listing's days on market", async () => {
  const { unstagedReason } = await importModule("src/views/bids.mjs");
  assert.equal(unstagedReason({ bidStage: "stale", bidStageLabel: "Stale" }), "any stage: too few stale comps", "DOM was given; do not ask for it again");
  assert.equal(unstagedReason({ bidStage: "" }), "any stage: enter DOM for a sharper read");
});

test("the win-chance ladder prices each step against the ask", async () => {
  const { bidLadderHtml } = await importModule("src/views/bids.mjs");
  const html = bidLadderHtml({ pendingListPrice: 1_000_000, bidLadder: [{ chance: 0.5, bid: 980_000 }, { chance: 0.6, bid: 1_000_000 }, { chance: 0.9, bid: 1_080_000 }] });
  assert.match(html, /50%<\/span><strong>\$980,000<\/strong><span class="ladder-vs-ask">2\.0% under ask/);
  assert.match(html, /60%<\/span><strong>\$1,000,000<\/strong><span class="ladder-vs-ask">at ask/);
  assert.match(html, /90%<\/span><strong>\$1,080,000<\/strong><span class="ladder-vs-ask">8\.0% over ask/);
  assert.equal(bidLadderHtml({ pendingListPrice: 1_000_000, bidLadder: [] }), "");
  assert.equal(bidLadderHtml({ pendingListPrice: 0, bidLadder: [{ chance: 0.5, bid: 1 }] }), "");
});

test("a comp's share of the weight is shown to a tenth of a percent", async () => {
  const { weightShareText } = await importModule("src/views/bids.mjs");
  assert.equal(weightShareText(0.0834), "8.3%");
  assert.equal(weightShareText(0.0004), "<0.1%");
  assert.equal(weightShareText(0), "n/a");
  assert.equal(weightShareText(undefined), "n/a");
});

test("the outlook line leads with which way the odds lean, and is empty without a read", async () => {
  const { outlookNoteHtml } = await importModule("src/views/bids.mjs");
  const html = outlookNoteHtml({ cutChance: 0.312, saleChance: 0.078, lean: { tone: "cool", label: "Time is on your side" } });
  assert.match(html, /outlook-chip cool">Time is on your side</);
  assert.match(html, /Next 14 days, listings like this: 31% cut the price · 8% go under contract/);
  assert.match(html, /data-explain-trigger="listingOutlook"/);
  const noSales = outlookNoteHtml({ cutChance: 0.3, saleChance: null, lean: null });
  assert.match(noSales, /30% cut the price </);
  assert.doesNotMatch(noSales, /under contract|outlook-chip/);
  assert.equal(outlookNoteHtml(null), "");
  assert.equal(outlookNoteHtml(undefined), "");
});

test("the queue narrows to a stage or to a signal", async () => {
  const { matchesBidFocus } = await importModule("src/views/bids.mjs");
  const row = { bidStage: "sitting", signals: ["justCut"] };
  assert.equal(matchesBidFocus(row, ""), true);
  assert.equal(matchesBidFocus(row, "sitting"), true);
  assert.equal(matchesBidFocus(row, "stale"), false);
  assert.equal(matchesBidFocus(row, "justCut"), true);
  assert.equal(matchesBidFocus(row, "relisted"), false);
  assert.equal(matchesBidFocus({ bidStage: "" }, "unknown"), true);
  assert.equal(matchesBidFocus(row, "unknown"), false);
  assert.equal(matchesBidFocus({ bidStage: "fresh" }, "justCut"), false, "no ledger read, no signals");
});

test("the latest cut is dated, and sized only when it differs from the total", async () => {
  const { lastCutWords } = await importModule("src/views/bids.mjs");
  assert.equal(lastCutWords({ usd: 50_000, daysAgo: 3, count: 1 }, 50_000), ", 3 days ago");
  assert.equal(lastCutWords({ usd: 25_000, daysAgo: 1, count: 2 }, 75_000), ", most recently $25K yesterday");
  assert.equal(lastCutWords({ usd: 25_000, daysAgo: 0, count: 1 }, 96_000), ", most recently $25K today", "the rest came off before tracking, or between listings");
  assert.equal(lastCutWords(null, 50_000), "");
  assert.equal(lastCutWords({ usd: 0, daysAgo: 3, count: 0 }), "");
});

test("an empty queue blames the filters, unless its own narrowing emptied it", async () => {
  const { emptyQueueMessage, bidFocusLabel } = await importModule("src/views/bids.mjs");
  assert.equal(emptyQueueMessage(0, ""), "No active listings match the current filters.");
  assert.equal(emptyQueueMessage(0, "justCut"), "No active listings match the current filters.");
  assert.equal(emptyQueueMessage(4, "justCut"), 'None of the 4 listings here is under "Just cut" right now. Click it again to see them all.');
  assert.equal(bidFocusLabel("stale"), "Stale");
  assert.equal(bidFocusLabel("holdingOut"), "No cut yet");
  assert.equal(bidFocusLabel("unknown"), "Stage unknown");
  assert.equal(bidFocusLabel("nonsense"), "");
});
