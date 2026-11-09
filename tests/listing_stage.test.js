"use strict";

// Synthetic fixtures only.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

function sale(dom, ask, close, overrides = {}) {
  return {
    dataMode: "MLS_ENRICHED", hasDollarListPrice: true, hasMarketListPrice: true, listPriceAtPending: ask, pendingListPrice: ask,
    closePrice: close, saleToList: close / ask, hasMlsCdomValue: dom !== null, mlsCDOM: dom ?? 0, hasMlsDomValue: false, daysToPending: null,
    ...overrides,
  };
}

test("stageForDom cuts at 7, 14 and 45 days, and an unknown reading has no stage", async () => {
  const { stageForDom } = await importModule("src/domain/listingStage.mjs");
  assert.deepEqual([0, 7, 8, 14, 15, 45, 46, 400].map(stageForDom), ["fresh", "fresh", "past", "past", "sitting", "sitting", "stale", "stale"]);
  for (const unknown of [null, undefined, "", Number.NaN, -1, "n/a"]) {
    assert.equal(stageForDom(unknown), null, `${String(unknown)} must not default to a stage`);
  }
});

test("stageForRow prefers cumulative days, so a relisted home is not fresh", async () => {
  const { stageForRow } = await importModule("src/domain/listingStage.mjs");
  const relisted = { dataMode: "MLS_ENRICHED", hasMlsDomValue: true, mlsDOM: 3, hasMlsCdomValue: true, mlsCDOM: 61 };
  assert.equal(stageForRow(relisted), "stale");
  assert.equal(stageForRow({ dataMode: "MLS_ENRICHED", hasMlsDomValue: false, hasMlsCdomValue: false, daysToPending: null }), null);
});

test("live listings that nearly all read 0 days are treated as a broken feed", async () => {
  const { hasPlausibleLiveDom } = await importModule("src/domain/listingStage.mjs");
  const live = (dom) => ({ dataMode: "MLS_ENRICHED", hasMlsCdomValue: true, mlsCDOM: dom });
  assert.equal(hasPlausibleLiveDom(Array.from({ length: 40 }, () => live(0))), false);
  assert.equal(hasPlausibleLiveDom([0, 3, 9, 14, 20, 33, 41, 58, 70, 95, 120].map(live)), true);
  assert.equal(hasPlausibleLiveDom([live(0), live(0)]), true, "too few listings to judge");
  assert.equal(hasPlausibleLiveDom([]), true);
});

test("computeStageLeverage splits over / at / under ask by stage and ignores rows it cannot place", async () => {
  const { computeStageLeverage } = await importModule("src/domain/listingStage.mjs");
  const rows = [
    sale(3, 1_000_000, 1_100_000), sale(5, 1_000_000, 1_050_000), sale(6, 1_000_000, 1_000_000), sale(7, 1_000_000, 980_000), sale(2, 1_000_000, 1_020_000),
    sale(30, 1_000_000, 960_000), sale(20, 1_000_000, 970_000), sale(40, 1_000_000, 1_000_000), sale(25, 1_000_000, 950_000), sale(18, 1_000_000, 990_000),
    sale(90, 1_000_000, 900_000),
    sale(null, 1_000_000, 1_200_000), // no days on market
    sale(4, 1_000_000, 1_300_000, { hasDollarListPrice: false }), // assessed-value fallback, not an ask
    sale(4, 1_000_000, 0), // never closed
  ];
  const leverage = computeStageLeverage(rows);
  assert.equal(leverage.eligibleCount, 11);
  assert.equal(leverage.totalCount, 14);
  const by = Object.fromEntries(leverage.stages.map((stage) => [stage.id, stage]));
  assert.equal(by.fresh.count, 5);
  assert.equal(by.fresh.overShare, 0.6);
  assert.equal(by.fresh.atShare, 0.2);
  assert.equal(by.fresh.underShare, 0.2);
  assert.equal(by.fresh.medianDiffUsd, 20_000);
  assert.equal(by.fresh.enough, true);
  assert.equal(by.sitting.overShare, 0);
  assert.equal(by.sitting.underShare, 0.8);
  assert.equal(by.sitting.medianRatio, 0.97);
  assert.equal(by.past.count, 0);
  assert.equal(by.past.overShare, null, "an empty stage is unknown, not 0%");
  assert.equal(by.stale.enough, false, "one sale is not a reading");
  assert.ok(Math.abs(by.fresh.shareOfSales - 5 / 11) < 1e-9);
});

test("the verdict contrasts week one with everything after, and declines on thin data", async () => {
  const { computeStageLeverage, buildStageVerdict } = await importModule("src/domain/listingStage.mjs");
  const fresh = [1.1, 1.05, 1.02, 1.0, 0.98].map((ratio) => sale(4, 1_000_000, Math.round(1_000_000 * ratio)));
  const later = [0.96, 0.97, 0.95, 0.99, 1.0, 0.94, 0.98, 0.97, 0.96, 1.01].map((ratio) => sale(30, 1_000_000, Math.round(1_000_000 * ratio)));
  const verdict = buildStageVerdict(computeStageLeverage([...fresh, ...later]));
  assert.match(verdict, /60% of homes that sold by day 7 went over ask/);
  assert.match(verdict, /only 1 in 10 did/);
  assert.match(verdict, /80% sold for less than ask/);
  assert.match(buildStageVerdict(computeStageLeverage(fresh)), /Not enough sales/);
  assert.match(buildStageVerdict(computeStageLeverage([])), /Not enough sales/);
});

test("plain-language helpers", async () => {
  const { ratioVsAskPhrase, stageTypicalResult } = await importModule("src/domain/listingStage.mjs");
  assert.equal(ratioVsAskPhrase(1.032), "3.2% over ask");
  assert.equal(ratioVsAskPhrase(0.961), "3.9% under ask");
  assert.equal(ratioVsAskPhrase(1), "at ask");
  assert.equal(ratioVsAskPhrase(0), "n/a");
  assert.equal(stageTypicalResult({ enough: true, medianDiffUsd: -35_000 }), "$35K under");
  assert.equal(stageTypicalResult({ enough: true, medianDiffUsd: 41_000 }), "$41K over");
  assert.equal(stageTypicalResult({ enough: true, medianDiffUsd: 0 }), "at ask");
  assert.equal(stageTypicalResult({ enough: false, medianDiffUsd: 41_000 }), "n/a");
});

// --- the bid model built on stages -----------------------------------------

const NOW = "2026-11-08";

function comp(index, dom, ratio, overrides = {}) {
  return {
    ...sale(dom, 1_200_000, Math.round(1_200_000 * ratio)),
    mapPropertyKey: `comp-${index}`, typeLabel: "Single Family", neighborhoodLabel: "Ballard", zip: "98117", mlsStatusNorm: "SOLD",
    saleDate: "2026-10-15", effectiveDate: "2026-10-15", hasActualClose: true, isLikelyPresoldNewBuild: false, ...overrides,
  };
}

function listing(dom, overrides = {}) {
  return {
    mapPropertyKey: "active-1", typeLabel: "Single Family", neighborhoodLabel: "Ballard", zip: "98117", dataMode: "MLS_ENRICHED", mlsStatusNorm: "ACTIVE",
    pendingListPrice: 1_250_000, hasActualClose: false, hasMlsCdomValue: dom !== null, mlsCDOM: dom ?? 0, hasMlsDomValue: false, daysToPending: null, ...overrides,
  };
}

const WEEK_ONE = Array.from({ length: 8 }, (_, i) => comp(i, 5, 1.06));
const SITTING = Array.from({ length: 8 }, (_, i) => comp(100 + i, 30, 0.96));

test("a listing is priced against sales from its own stage", async () => {
  const { scoreBidForRow } = await importModule("src/domain/selectors.mjs");
  const pool = [...WEEK_ONE, ...SITTING];
  const fresh = scoreBidForRow(listing(3), pool, "balanced", false, { nowValue: NOW });
  const sitting = scoreBidForRow(listing(33), pool, "balanced", false, { nowValue: NOW });
  assert.equal(fresh.bidStage, "fresh");
  assert.equal(fresh.bidCompCount, 8);
  assert.equal(fresh.bidRatio.toFixed(2), "1.06");
  assert.equal(sitting.bidStage, "sitting");
  assert.equal(sitting.bidStageLabel, "Sitting");
  assert.equal(sitting.bidCompCount, 8);
  assert.equal(sitting.bidRatio.toFixed(2), "0.96", "five weeks in, the comps say under ask");
  assert.ok(sitting.bidSuggested < sitting.pendingListPrice);
  assert.equal(sitting.bidStageMatched, true);
});

test("an unknown days-on-market reading uses every recent comp, says so, and costs confidence", async () => {
  const { scoreBidForRow, BID_UNSTAGED_CONFIDENCE_PENALTY } = await importModule("src/domain/selectors.mjs");
  const pool = [...WEEK_ONE, ...SITTING];
  const unknown = scoreBidForRow(listing(null), pool, "balanced", false, { nowValue: NOW });
  assert.equal(unknown.bidStatus, "SCored");
  assert.equal(unknown.bidStage, "");
  assert.equal(unknown.bidStageMatched, false);
  assert.equal(unknown.bidCompCount, 16);
  const staged = scoreBidForRow(listing(3), [...WEEK_ONE, ...WEEK_ONE.map((c, i) => ({ ...c, mapPropertyKey: `x-${i}` }))], "balanced", false, { nowValue: NOW });
  assert.equal(staged.bidCompCount, 16);
  assert.equal(staged.bidConfidence - unknown.bidConfidence, BID_UNSTAGED_CONFIDENCE_PENALTY);
});

test("same-stage comps widen from neighborhood to ZIP to city, then to 180 days, before giving up on the stage", async () => {
  const { selectBidComps, BID_COMP_WINDOW_DAYS, BID_COMP_WINDOW_EXTENDED_DAYS } = await importModule("src/domain/selectors.mjs");
  const elsewhere = (c) => ({ ...c, neighborhoodLabel: "Magnolia", zip: "98199" });
  const target = listing(60);
  const stale = (count, overrides = {}) => Array.from({ length: count }, (_, i) => comp(200 + i, 70, 0.95, overrides));

  assert.equal(selectBidComps(target, [...WEEK_ONE, ...stale(6)], { nowValue: NOW }).tier, "T1_NEIGHBORHOOD_TYPE");
  assert.equal(selectBidComps(target, [...WEEK_ONE, ...stale(6, { neighborhoodLabel: "Crown Hill" })], { nowValue: NOW }).tier, "T2_ZIP_TYPE");

  const city = selectBidComps(target, [...WEEK_ONE, ...stale(6).map(elsewhere)], { nowValue: NOW });
  assert.equal(city.tier, "T3_CITY_TYPE");
  assert.equal(city.windowDays, BID_COMP_WINDOW_DAYS);
  assert.equal(city.stageMatched, true);

  const old = selectBidComps(target, [...WEEK_ONE, ...stale(6, { saleDate: "2026-06-20" })], { nowValue: NOW });
  assert.equal(old.windowDays, BID_COMP_WINDOW_EXTENDED_DAYS);
  assert.equal(old.rows.length, 6);
  assert.equal(old.stageMatched, true);

  const none = selectBidComps(target, [...WEEK_ONE, ...stale(3)], { nowValue: NOW });
  assert.equal(none.stageMatched, false, "three stale sales anywhere is not a basis");
  assert.equal(none.rows.length, 11, "falls back to all recent comps in the neighborhood");
});

test("comps older than 90 days never leak into a 90-day basis", async () => {
  const { selectBidComps } = await importModule("src/domain/selectors.mjs");
  const oldWeekOne = Array.from({ length: 5 }, (_, i) => comp(300 + i, 4, 1.2, { saleDate: "2026-06-01" }));
  const picked = selectBidComps(listing(2), [...WEEK_ONE, ...oldWeekOne], { nowValue: NOW });
  assert.equal(picked.rows.length, 8);
  assert.ok(picked.rows.every((row) => row.saleDate === "2026-10-15"));
});

test("the comp pool bands on asking price and reaches back 180 days", async () => {
  const { buildBidCompPool, DEFAULT_FILTERS } = await importModule("src/domain/selectors.mjs");
  const filters = { ...DEFAULT_FILTERS, minClose: 1_100_000, maxClose: 1_600_000 };
  const rows = [
    comp(1, 5, 1.0),
    comp(2, 5, 1.15, { pendingListPrice: 1_000_000, listPriceAtPending: 1_000_000, closePrice: 1_150_000 }), // bid up INTO the band: asked below it
    comp(3, 40, 0.95, { pendingListPrice: 1_120_000, listPriceAtPending: 1_120_000, closePrice: 1_064_000 }), // sold down OUT of the band: asked inside it
    comp(4, 5, 1.0, { saleDate: "2026-06-01" }), // 160 days
    comp(5, 5, 1.0, { saleDate: "2026-04-01" }), // 221 days
  ];
  assert.deepEqual(buildBidCompPool(filters, rows, NOW).map((row) => row.mapPropertyKey), ["comp-1", "comp-3", "comp-4"]);
});

test("strategies stay ordered and the range brackets the suggestion", async () => {
  const { scoreBidForRow } = await importModule("src/domain/selectors.mjs");
  const spread = [0.98, 1.0, 1.0, 1.01, 1.02, 1.03, 1.05, 1.07, 1.1, 1.14].map((ratio, i) => comp(i, 5, ratio));
  const [low, mid, high] = ["conservative", "balanced", "aggressive"].map((s) => scoreBidForRow(listing(3), spread, s, false, { nowValue: NOW }));
  assert.ok(low.bidSuggested <= mid.bidSuggested && mid.bidSuggested <= high.bidSuggested);
  for (const bid of [low, mid, high]) assert.ok(bid.bidLow <= bid.bidSuggested && bid.bidSuggested <= bid.bidHigh);
});

test("a queue whose listings all read 0 days is scored as stage-unknown, not as week one", async () => {
  const { computeActiveBidSuggestions, DEFAULT_FILTERS } = await importModule("src/domain/selectors.mjs");
  const filters = { ...DEFAULT_FILTERS, minClose: 1_000_000, maxClose: 1_600_000 };
  const broken = Array.from({ length: 12 }, (_, i) => listing(0, { mapPropertyKey: `a-${i}`, effectiveDate: NOW }));
  const result = computeActiveBidSuggestions(filters, [...broken, ...WEEK_ONE, ...SITTING], "balanced", { nowValue: NOW });
  assert.equal(result.rows.length, 12);
  assert.ok(result.rows.every((row) => row.bidStage === "" && row.bidStageMatched === false));
  const healthy = [0, 2, 5, 9, 12, 20, 28, 35, 44, 60, 75, 90].map((dom, i) => listing(dom, { mapPropertyKey: `b-${i}`, effectiveDate: NOW }));
  const ok = computeActiveBidSuggestions(filters, [...healthy, ...WEEK_ONE, ...SITTING], "balanced", { nowValue: NOW });
  assert.equal(ok.rows[0].bidStage, "fresh", "a genuine day-zero listing in a healthy feed keeps its stage");
});
