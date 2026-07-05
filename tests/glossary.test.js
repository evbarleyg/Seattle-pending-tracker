"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

const REQUIRED_METRIC_IDS = [
  "medianClose",
  "medianPsf",
  "overAskRatio",
  "shareOverAsk",
  "typicalPremiumWhenOver",
  "p75Premium",
  "medianDom",
  "fastSaleShare",
  "activePending",
  "savedMatches",
  "bidQueue",
  "freshness",
];

const REQUIRED_UNIVERSE_IDS = ["allRows", "closedSlice", "savedMatches", "actives"];

const VALID_SOURCE_IDS = ["kc-assessor", "redfin", "mls-manual", "derived"];
const VALID_BUYER_DIRECTIONS = ["higherIsWorse", "higherIsBetter", "neutral"];

test("glossary exposes every required metric and universe id", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  for (const id of [...REQUIRED_METRIC_IDS, ...REQUIRED_UNIVERSE_IDS]) {
    assert.ok(glossary.METRICS[id], `missing glossary entry: ${id}`);
  }
  assert.deepEqual(glossary.UNIVERSE_IDS, REQUIRED_UNIVERSE_IDS);
});

test("every glossary entry carries every required field", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  for (const [key, entry] of Object.entries(glossary.METRICS)) {
    assert.equal(entry.id, key, `entry id must match its key: ${key}`);
    for (const field of ["label", "plain", "formulaWords", "cadence"]) {
      assert.ok(
        typeof entry[field] === "string" && entry[field].trim().length > 0,
        `${key}.${field} must be a non-empty string`
      );
    }
    assert.ok(entry.source && typeof entry.source === "object", `${key}.source must be an object`);
    assert.ok(VALID_SOURCE_IDS.includes(entry.source.id), `${key}.source.id invalid: ${entry.source.id}`);
    assert.ok(
      typeof entry.source.label === "string" && entry.source.label.trim().length > 0,
      `${key}.source.label must be a non-empty string`
    );
    assert.ok(Array.isArray(entry.caveats) && entry.caveats.length > 0, `${key}.caveats must be a non-empty array`);
    for (const caveat of entry.caveats) {
      assert.ok(typeof caveat === "string" && caveat.trim().length > 0, `${key} has an empty caveat`);
    }
    assert.ok(
      glossary.UNIVERSE_IDS.includes(entry.universeId),
      `${key}.universeId must be a known universe id, got: ${entry.universeId}`
    );
    assert.ok(glossary.METRICS[entry.universeId], `${key}.universeId must resolve to a glossary entry`);
  }
});

test("buyerDirection values are valid and match buyer semantics", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  for (const [key, entry] of Object.entries(glossary.METRICS)) {
    assert.ok(
      VALID_BUYER_DIRECTIONS.includes(entry.buyerDirection),
      `${key}.buyerDirection invalid: ${entry.buyerDirection}`
    );
  }
  // Pin the sign conventions that mirror competitiveDelta in pulseMetrics.mjs:
  // rising prices/ratios/heat hurt a buyer; rising DOM and inventory help.
  assert.equal(glossary.METRICS.medianClose.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.overAskRatio.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.fastSaleShare.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.medianDom.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.activePending.buyerDirection, "higherIsBetter");
});

test("copy stays grounded and plain: 10-day fast-sale rule, no em dashes, no emojis", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  assert.match(glossary.METRICS.fastSaleShare.formulaWords, /10 days/);
  assert.match(glossary.METRICS.bidQueue.formulaWords, /6 /);
  assert.match(glossary.METRICS.savedMatches.formulaWords, /60/);
  for (const entry of Object.values(glossary.METRICS)) {
    const texts = [entry.label, entry.plain, entry.formulaWords, entry.cadence, entry.source.label, ...entry.caveats];
    for (const text of texts) {
      assert.ok(!text.includes("—"), `em dash found in ${entry.id}: ${text}`);
      assert.ok(!/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(text), `emoji found in ${entry.id}: ${text}`);
    }
  }
});

test("getMetric returns entries and throws on unknown ids", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  assert.equal(glossary.getMetric("medianClose").label, "Median close");
  assert.equal(glossary.getMetric("closedSlice").id, "closedSlice");
  assert.throws(() => glossary.getMetric("notARealMetric"), /Unknown glossary metric id/);
  assert.throws(() => glossary.getMetric(""), /Unknown glossary metric id/);
});

test("formatCadenceNote builds a freshness sentence with a next-update estimate", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  // Synthetic dates, no PII. Fixed `now` keeps the assertion deterministic.
  const note = glossary.formatCadenceNote("freshness", {
    latestSaleDate: "2026-06-29",
    generatedAt: "2026-07-05T15:06:58.634Z",
    now: "2026-07-05T00:00:00",
  });
  assert.match(note, /about every 2 weeks/);
  assert.match(note, /6 days ago \(6\/29\/26\)/);
  assert.match(note, /Next update expected around 7\/19\/26\./);

  // Accepts an entry object as well as an id.
  const sameNote = glossary.formatCadenceNote(glossary.getMetric("freshness"), {
    latestSaleDate: "2026-06-29",
    generatedAt: "2026-07-05T15:06:58.634Z",
    now: "2026-07-05T00:00:00",
  });
  assert.equal(sameNote, note);
});

test("formatCadenceNote handles overdue updates, missing dates, and manual sources", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  // Overdue: pipeline ran more than 14 days before `now`.
  const overdue = glossary.formatCadenceNote("medianClose", {
    generatedAt: "2026-06-01",
    now: "2026-07-01T12:00:00",
  });
  assert.match(overdue, /The next update is due about now\./);

  // No dates at all: cadence sentence only, no crash.
  const bare = glossary.formatCadenceNote("freshness", {});
  assert.equal(bare, glossary.getMetric("freshness").cadence);

  // Manual sources get the pipeline sentence instead of a date estimate.
  const manual = glossary.formatCadenceNote("overAskRatio", { latestSaleDate: "2026-06-29", now: "2026-07-05" });
  assert.match(manual, /It updates the next time the data pipeline runs\./);
  assert.ok(!/Next update expected around/.test(manual));

  // Derived metrics recompute live: no pipeline sentence appended.
  const derived = glossary.formatCadenceNote("savedMatches", {});
  assert.equal(derived, glossary.getMetric("savedMatches").cadence);

  assert.throws(() => glossary.formatCadenceNote("nope", {}), /Unknown glossary metric id/);
});
