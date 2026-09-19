"use strict";

// Regression: a field the source never reported must read as "unknown", not as
// a real zero. Redfin sold rows carry no days-on-market and no list price, so
// before this fix a window made only of those rows rendered "0.0% fast-sale",
// "0d" median DOM and "$0" bid-up instead of "no data".

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

// Pulse-summary shape (see pulseRowSummary in selectors.mjs).
const soldNoListingFields = (overrides = {}) => ({
  saleDate: "2026-08-01",
  isHotMarket: false,
  isUltraHot: false,
  domValue: null,
  saleToList: 0,
  delta: 0,
  hasMarketListPrice: false,
  closePrice: 1250000,
  pricePerSqft: 600,
  ...overrides,
});

test("summarizeRows reports unknown DOM, heat and bid-up as null, not zero", async () => {
  const { summarizeRows } = await importModule("src/domain/pulseMetrics.mjs");
  const summary = summarizeRows([soldNoListingFields(), soldNoListingFields(), soldNoListingFields()]);

  assert.equal(summary.salesCount, 3);
  assert.equal(summary.medianDom, null, "null domValue must not become a 0-day median");
  assert.equal(summary.domSampleSize, 0);
  assert.equal(summary.hotShare, null, "no row has a DOM signal, so fast-sale share is unknown");
  assert.equal(summary.heatSampleSize, 0);
  assert.equal(summary.medianBidUp, null, "delta=0 on a row with no list price is a placeholder, not a bid-up");
  assert.equal(summary.bidUpSampleSize, 0);
  // Fields the rows do carry still summarize normally.
  assert.equal(summary.medianClosePrice, 1250000);
});

test("summarizeRows fast-sale share uses only rows with a days-on-market signal", async () => {
  const { summarizeRows } = await importModule("src/domain/pulseMetrics.mjs");
  const rows = [
    soldNoListingFields({ domValue: 4, isHotMarket: true, isUltraHot: true, hasMarketListPrice: true, delta: 60000, saleToList: 1.05 }),
    soldNoListingFields({ domValue: 30, isHotMarket: false, hasMarketListPrice: true, delta: -20000, saleToList: 0.98 }),
    soldNoListingFields(), // unknown DOM: must not dilute the share
    soldNoListingFields(),
  ];
  const summary = summarizeRows(rows);

  assert.equal(summary.heatSampleSize, 2);
  assert.equal(summary.hotShare, 0.5, "1 fast sale of 2 rows with a DOM signal, not 1 of 4");
  assert.equal(summary.medianDom, 17);
  assert.equal(summary.bidUpSampleSize, 2);
  assert.equal(summary.medianBidUp, 20000);
});

test("summarizeRows keeps a genuine zero-day DOM and a genuine $0 bid-up", async () => {
  const { summarizeRows } = await importModule("src/domain/pulseMetrics.mjs");
  const summary = summarizeRows([
    soldNoListingFields({ domValue: 0, isHotMarket: true, isUltraHot: true, hasMarketListPrice: true, delta: 0, saleToList: 1 }),
  ]);
  assert.equal(summary.medianDom, 0);
  assert.equal(summary.hotShare, 1);
  assert.equal(summary.medianBidUp, 0, "sold exactly at a real list price is a true $0");
});

test("summarizeRows still counts bid-up on legacy rows that carry no hasMarketListPrice flag", async () => {
  const { summarizeRows } = await importModule("src/domain/pulseMetrics.mjs");
  const legacy = { saleDate: "2026-03-01", domValue: 6, isHotMarket: true, saleToList: 1.04, delta: 50000, closePrice: 1300000 };
  const summary = summarizeRows([legacy]);
  assert.equal(summary.medianBidUp, 50000);
  assert.equal(summary.bidUpSampleSize, 1);
});

test("computeBaseStats fast-sale share ignores rows with unknown DOM", async () => {
  const { computeBaseStats } = await importModule("src/domain/selectors.mjs");
  const known = (dom, hot) => ({
    closePrice: 1200000, saleToList: 1.02, delta: 20000, hasMarketListPrice: true, pricePerSqft: 600,
    dataMode: "MLS_ENRICHED", hasMlsDomValue: true, mlsDOM: dom, isHotMarket: hot,
  });
  const unknown = {
    closePrice: 1200000, saleToList: 0, delta: 0, hasMarketListPrice: false, pricePerSqft: 600,
    dataMode: "PUBLIC_PROXY", hasMlsDomValue: false, mlsDOM: 0, daysToPending: null, isHotMarket: false,
  };
  const stats = computeBaseStats([known(3, true), known(5, true), known(40, false), unknown, unknown, unknown]);

  assert.equal(stats.sampleSize, 6);
  assert.equal(stats.heatSampleSize, 3);
  assert.equal(stats.hotShare.toFixed(3), (2 / 3).toFixed(3), "2 of the 3 rows with DOM data, not 2 of 6");
  assert.equal(stats.medianBidUp, 20000, "placeholder delta=0 rows are excluded from the bid-up median");
});

test("computeBaseStats returns null fast-sale share when no row has DOM data", async () => {
  const { computeBaseStats } = await importModule("src/domain/selectors.mjs");
  const unknown = { closePrice: 1200000, saleToList: 0, delta: 0, hasMarketListPrice: false, dataMode: "PUBLIC_PROXY", daysToPending: null, isHotMarket: false };
  const stats = computeBaseStats([unknown, unknown]);
  assert.equal(stats.hotShare, null);
  assert.equal(stats.medianBidUp, null);
});

test("summarizeCompetition does not read missing CDOM/DOM as zero days", async () => {
  const { summarizeCompetition } = await importModule("src/domain/buyerProfile.mjs");
  // Normalized rows store a missing CDOM/DOM as numeric 0 alongside has*Value=false.
  const missing = { closePrice: 1250000, saleToList: 0, delta: 0, hasMarketListPrice: false, isHotMarket: false, hasMlsCdomValue: false, mlsCDOM: 0, hasMlsDomValue: false, mlsDOM: 0, daysToPending: null, domValue: null };
  const summary = summarizeCompetition([missing, missing]);
  assert.equal(summary.medianDom, null);
  assert.equal(summary.hotShare, null);
  assert.equal(summary.medianBidUp, null);

  const real = { ...missing, hasMlsDomValue: true, mlsDOM: 8, domValue: 8, isHotMarket: true, hasMarketListPrice: true, delta: 30000, saleToList: 1.03 };
  const mixed = summarizeCompetition([real, missing]);
  assert.equal(mixed.medianDom, 8);
  assert.equal(mixed.hotShare, 1, "the one row with a DOM signal was a fast sale");
  assert.equal(mixed.medianBidUp, 30000);
});

test("competitiveDelta and metricDirection read a missing side as no comparison", async () => {
  const { competitiveDelta, metricDirection } = await importModule("src/domain/pulseMetrics.mjs");
  // Unknown current vs a real prior used to compute 0 - 0.786 = "-78.6 pts" and
  // tip the watchlist verdict to "cooling off" on nothing but absent data.
  assert.equal(competitiveDelta("hotShare", null, 0.786), null);
  assert.equal(competitiveDelta("hotShare", 0.5, undefined), null);
  assert.equal(competitiveDelta("medianDom", "", 12), null);
  assert.equal(metricDirection("hotShare", null, 0.786), 0);
  assert.equal(metricDirection("medianBidUp", null, 51000), 0);
  // Real readings, including a genuine zero, still compare.
  assert.equal(competitiveDelta("hotShare", 0, 0.5), -0.5);
  assert.equal(competitiveDelta("medianDom", 5, 12), 7, "DOM is sign-flipped: fewer days is hotter");
  assert.equal(metricDirection("medianDom", 5, 12), 1);
});

test("watchlist verdict does not call a trend from windows with no readable signals", async () => {
  const { watchlistVerdict } = await importModule("src/views/pulse.mjs");
  const { summarizeRows } = await importModule("src/domain/pulseMetrics.mjs");
  const current = summarizeRows([soldNoListingFields(), soldNoListingFields(), soldNoListingFields()]);
  const previous = summarizeRows([
    soldNoListingFields({ domValue: 4, isHotMarket: true, hasMarketListPrice: true, delta: 60000, saleToList: 1.05 }),
    soldNoListingFields({ domValue: 6, isHotMarket: true, hasMarketListPrice: true, delta: 40000, saleToList: 1.03 }),
    soldNoListingFields({ domValue: 9, isHotMarket: true, hasMarketListPrice: true, delta: 20000, saleToList: 1.02 }),
  ]);
  const verdict = watchlistVerdict({ windowDays: 90, current, previous });
  assert.equal(verdict.tone, "flat", "missing DOM/list data must not read as the market cooling");
  assert.doesNotMatch(verdict.answer, /cooling off|heating up/i);
});

test("hasHeatSignal treats hot rows and rows with any DOM reading as known", async () => {
  const { hasHeatSignal } = await importModule("src/domain/data.mjs");
  assert.equal(hasHeatSignal({ isHotMarket: true }), true, "tagged hot with no DOM number is still a signal");
  assert.equal(hasHeatSignal({ isHotMarket: false, domValue: 25 }), true);
  assert.equal(hasHeatSignal({ isHotMarket: false, domValue: 0 }), true);
  assert.equal(hasHeatSignal({ isHotMarket: false, domValue: null }), false);
  assert.equal(hasHeatSignal({ isHotMarket: false, dataMode: "MLS_ENRICHED", hasMlsDomValue: true, mlsDOM: 12 }), true);
  assert.equal(hasHeatSignal({ isHotMarket: false, dataMode: "PUBLIC_PROXY", daysToPending: null }), false);
  assert.equal(hasHeatSignal(null), false);
});
