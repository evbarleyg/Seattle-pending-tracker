"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

const REQUIRED_METRIC_IDS = [
  // Overview
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
  // Pulse
  "pulseSalesCount",
  "pulseFastSaleShare",
  "pulseMedianDom",
  "pulseMedianSaleToList",
  "pulseMedianBidUp",
  "pulseMedianClose",
  "pulseWeeklyGrain",
  "competitionPockets",
  "watchlistPockets",
  // Bids / Offer Lab
  "suggestedBid",
  "bidRange",
  "bidConfidence",
  "bidCompBasis",
  "suggestedSaleList",
  "bidMedianOverAsk",
  // Afford
  "affordDecision",
  "affordMaxComfortable",
  "affordMaxStretch",
  "affordCarry",
  "affordFreeCashFlow",
  "affordPostCloseLiquidity",
  "affordDeployable",
  "affordTier",
  // Geo
  "geoPressure",
  // Records
  "recordSource",
  "recordAsk",
  "recordDom",
  "recordSaleToList",
  "recordBidUp",
  "recordHot",
  "projectedClose",
  // Data
  "dataValidation",
  "dataRowCounts",
  "dataSourceCadence",
];

const REQUIRED_UNIVERSE_IDS = [
  "allRows",
  "closedSlice",
  "savedMatches",
  "actives",
  "pulseWatchlist",
  "recordRows",
  "affordScenario",
];

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
  // Same conventions on the per-tab entries.
  assert.equal(glossary.METRICS.pulseMedianDom.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.pulseMedianSaleToList.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.pulseMedianBidUp.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.suggestedSaleList.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.bidConfidence.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.affordCarry.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.affordMaxComfortable.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.affordFreeCashFlow.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.recordDom.buyerDirection, "higherIsBetter");
  assert.equal(glossary.METRICS.recordSaleToList.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.recordBidUp.buyerDirection, "higherIsWorse");
  assert.equal(glossary.METRICS.geoPressure.buyerDirection, "higherIsWorse");
});

test("tab entries stay grounded in the real thresholds and universes", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  const M = glossary.METRICS;

  // Pulse: 90-day compare windows over MLS-enriched watchlist rows.
  for (const id of ["pulseSalesCount", "pulseFastSaleShare", "pulseMedianDom", "pulseMedianSaleToList", "pulseMedianBidUp", "pulseMedianClose"]) {
    assert.equal(M[id].universeId, "pulseWatchlist", `${id} must compute over the watchlist universe`);
    assert.match(M[id].formulaWords, /90/, `${id} must state the 90-day window`);
  }
  assert.match(M.pulseWeeklyGrain.formulaWords, /7-day week/);
  assert.match(M.pulseWeeklyGrain.formulaWords, /fewer than 5 sales/);
  assert.match(M.watchlistPockets.plain, /Ballard/);
  assert.match(M.watchlistPockets.plain, /Magnolia/);

  // Bids: BID_MIN_COMPS=6, 90-day comp window, strategy percentiles,
  // clamp 0.90x-1.25x, confidence bands at 75/55 (selectors.mjs).
  assert.match(M.suggestedBid.formulaWords, /50th/);
  assert.match(M.suggestedBid.formulaWords, /60th/);
  assert.match(M.suggestedBid.formulaWords, /70th/);
  assert.match(M.suggestedBid.caveats.join(" "), /0\.90x/);
  assert.match(M.suggestedBid.caveats.join(" "), /1\.25x/);
  assert.match(M.bidCompBasis.formulaWords, /6 /);
  assert.match(M.bidCompBasis.formulaWords, /90 days/);
  assert.match(M.bidConfidence.formulaWords, /75/);
  assert.match(M.bidConfidence.formulaWords, /55/);

  // Records: projection needs 6 comps with 25th/75th band; hot tags at 5/10 days.
  assert.match(M.projectedClose.formulaWords, /6 /);
  assert.match(M.projectedClose.formulaWords, /25th/);
  assert.match(M.projectedClose.formulaWords, /75th/);
  assert.match(M.recordHot.formulaWords, /5 days/);
  assert.match(M.recordHot.formulaWords, /10 days/);
  assert.match(M.recordAsk.formulaWords, /tax-assessed/);

  // Geo: legend buckets from ratioColor in main.mjs.
  assert.match(M.geoPressure.formulaWords, /0\.90x/);
  assert.match(M.geoPressure.formulaWords, /1\.10x/);

  // Afford entries never claim a market source: they are scenario math.
  for (const id of ["affordDecision", "affordMaxComfortable", "affordMaxStretch", "affordCarry", "affordFreeCashFlow", "affordPostCloseLiquidity", "affordDeployable", "affordTier"]) {
    assert.equal(M[id].source.id, "derived", `${id} must be derived, not a market source`);
    assert.equal(M[id].universeId, "affordScenario", `${id} must compute over the scenario universe`);
  }

  // Data: validation checks from scripts/validate_data_refresh.js.
  assert.match(M.dataValidation.formulaWords, /asking price/);
  assert.match(M.dataValidation.formulaWords, /columns/);
});

test("known copy fixes hold: honest fast-sale rule, no duplicated lag caveat", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  const M = glossary.METRICS;

  // fastSaleShare membership is DOM<=10 OR the pipeline hot tag, not a pure
  // pending-in-10-days rule; the copy must admit the tag-only exceptions.
  assert.match(M.fastSaleShare.formulaWords, /hot-sale tag/);
  assert.ok(
    !/went pending in 10 days or less\.$/.test(M.fastSaleShare.formulaWords.trim()),
    "fastSaleShare must not claim a pure 10-day-pending rule"
  );

  // medianClose: the 2-week county lag lives in the cadence sentence only, so
  // the popover states it once instead of twice.
  assert.match(M.medianClose.cadence, /2 weeks/);
  assert.ok(
    !M.medianClose.caveats.some((caveat) => /2 weeks/.test(caveat)),
    "medianClose caveats must not repeat the county-lag cadence note"
  );

  // freshness: the red-warning caveat is the short single-sentence version.
  assert.ok(
    M.freshness.caveats.includes(
      "A red warning means the last data refresh failed its checks, not that the data is a few days old."
    ),
    "freshness must carry the shortened red-warning caveat"
  );
  assert.ok(
    !M.freshness.caveats.some((caveat) => /1 to 3 weeks/.test(caveat)),
    "freshness caveats must not repeat the county-rhythm cadence note"
  );
});

test("popover copy stays short: plain + formula + caveats under 150 words", async () => {
  const glossary = await importModule("src/domain/glossary.mjs");
  for (const [key, entry] of Object.entries(glossary.METRICS)) {
    const words = [entry.plain, entry.formulaWords, ...entry.caveats]
      .join(" ")
      .split(/\s+/)
      .filter(Boolean).length;
    assert.ok(words <= 150, `${key} popover copy is ${words} words; keep it under 150`);
  }
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
