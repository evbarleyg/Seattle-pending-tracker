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
