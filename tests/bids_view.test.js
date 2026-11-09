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
  assert.equal(compScopeLabel("T1_NEIGHBORHOOD_TYPE"), "nearby");
  assert.equal(compScopeLabel("T2_ZIP_TYPE"), "same-zip");
  assert.equal(compScopeLabel("T3_CITY_TYPE"), "citywide");
  assert.equal(compScopeLabel("NONE"), "");
  assert.equal(compScopeLabel(undefined), "");
});

test("compBasisCaption states the comp basis using the shared universe-caption component", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  const html = compBasisCaption(28, "T1_NEIGHBORHOOD_TYPE", "Single Family");
  assert.match(html, /class="universe-caption"/);
  assert.match(html, /of 28 nearby single family comps/);
});

test("compBasisCaption reads citywide and same-zip tiers in plain words", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  assert.match(compBasisCaption(12, "T3_CITY_TYPE", "Condo"), /of 12 citywide condo comps/);
  assert.match(compBasisCaption(9, "T2_ZIP_TYPE", "Townhouse"), /of 9 same-zip townhouse comps/);
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

test("the comp caption names the stage the comps were drawn from", async () => {
  const { compBasisCaption } = await importModule("src/views/bids.mjs");
  assert.match(compBasisCaption(14, "T3_CITY_TYPE", "Single Family", "stale"), /of 14 citywide single family comps that went pending in 46\+ days/);
  assert.doesNotMatch(compBasisCaption(14, "T3_CITY_TYPE", "Single Family", ""), /pending in/);
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

test("the caption and status say when the search reached past 90 days or could not match the stage", async () => {
  const { compBasisCaption, unstagedReason } = await importModule("src/views/bids.mjs");
  assert.match(compBasisCaption(9, "T3_CITY_TYPE", "Townhouse", "stale", 180), /\(last 180 days\)/);
  assert.doesNotMatch(compBasisCaption(9, "T3_CITY_TYPE", "Townhouse", "stale", 90), /last \d+ days/);
  assert.equal(unstagedReason({ bidStage: "stale", bidStageLabel: "Stale" }), "any stage: too few stale comps", "DOM was given; do not ask for it again");
  assert.equal(unstagedReason({ bidStage: "" }), "any stage: enter DOM for a sharper read");
});
