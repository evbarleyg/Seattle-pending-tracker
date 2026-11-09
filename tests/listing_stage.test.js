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

const WEEK_ONE = Array.from({ length: 10 }, (_, i) => comp(i, 5, 1.06));
const SITTING = Array.from({ length: 10 }, (_, i) => comp(100 + i, 30, 0.96));

test("a listing is priced against sales from its own stage", async () => {
  const { scoreBidForRow } = await importModule("src/domain/selectors.mjs");
  const pool = [...WEEK_ONE, ...SITTING];
  const fresh = scoreBidForRow(listing(3), pool, "balanced", false, { nowValue: NOW });
  const sitting = scoreBidForRow(listing(33), pool, "balanced", false, { nowValue: NOW });
  assert.equal(fresh.bidStage, "fresh");
  assert.equal(fresh.bidCompCount, 10);
  assert.equal(fresh.bidRatio.toFixed(2), "1.06");
  assert.equal(sitting.bidStage, "sitting");
  assert.equal(sitting.bidStageLabel, "Sitting");
  assert.equal(sitting.bidCompCount, 10);
  assert.equal(sitting.bidRatio.toFixed(2), "0.96", "five weeks in, the comps say under ask");
  assert.ok(sitting.bidSuggested < sitting.pendingListPrice);
  assert.equal(sitting.bidStageMatched, true);
});

test("an unknown days-on-market reading uses comps from every stage, says so, and costs confidence", async () => {
  const { scoreBidForRow, BID_UNSTAGED_CONFIDENCE_PENALTY } = await importModule("src/domain/selectors.mjs");
  const unknown = scoreBidForRow(listing(null), [...WEEK_ONE, ...SITTING], "balanced", false, { nowValue: NOW });
  assert.equal(unknown.bidStatus, "SCored");
  assert.equal(unknown.bidStage, "");
  assert.equal(unknown.bidStageMatched, false);
  assert.equal(unknown.bidCompCount, 20);
  // Same comps either way, so the only difference is the penalty.
  const twenty = [...WEEK_ONE, ...WEEK_ONE.map((c, i) => ({ ...c, mapPropertyKey: `x-${i}` }))];
  const staged = scoreBidForRow(listing(3), twenty, "balanced", false, { nowValue: NOW });
  const unstaged = scoreBidForRow(listing(null), twenty, "balanced", false, { nowValue: NOW });
  assert.equal(staged.bidCompCount, 20);
  assert.equal(staged.bidConfidence - unstaged.bidConfidence, BID_UNSTAGED_CONFIDENCE_PENALTY);
});

test("the comp pool ignores the price band and home type, reaches back 12 months and keeps the other filters", async () => {
  const { buildBidCompPool, DEFAULT_FILTERS } = await importModule("src/domain/selectors.mjs");
  const filters = { ...DEFAULT_FILTERS, minClose: 1_100_000, maxClose: 1_600_000 };
  const rows = [
    comp(1, 5, 1.0),
    comp(2, 5, 1.15, { pendingListPrice: 1_000_000, listPriceAtPending: 1_000_000, closePrice: 1_150_000 }), // asked below the band
    comp(3, 5, 1.0, { pendingListPrice: 1_900_000, listPriceAtPending: 1_900_000, closePrice: 1_900_000 }), // asked above it
    comp(4, 5, 1.0, { saleDate: "2025-12-01" }), // 342 days
    comp(5, 5, 1.0, { saleDate: "2025-10-01" }), // 403 days
    comp(6, 5, 1.0, { typeLabel: "Condo" }),
    comp(7, 5, 1.0, { isLikelyPresoldNewBuild: true }),
  ];
  assert.deepEqual(buildBidCompPool(filters, rows, NOW).map((row) => row.mapPropertyKey), ["comp-1", "comp-2", "comp-3", "comp-4", "comp-6"], "the condo stays: the comp finder can price any type");
  assert.deepEqual(buildBidCompPool({ ...filters, neighborhoods: ["Magnolia"] }, rows, NOW), []);
  assert.equal(buildBidCompPool({ ...filters, affordability: "in_budget" }, rows, NOW).length, 5, "a budget ceiling is a price band by another name");
});

test("confidence falls as the comps disagree, thin out, age, or cannot be matched", async () => {
  const { bidConfidenceScore, bidConfidenceLabel } = await importModule("src/domain/selectors.mjs");
  const solid = { spread: 0.04, effectiveCount: 40, newestAge: 10, stageMatched: true };
  assert.equal(bidConfidenceScore(solid), 80);
  assert.equal(bidConfidenceLabel(bidConfidenceScore(solid)), "High");
  assert.equal(bidConfidenceScore({ ...solid, spread: 0.07 }), 65, "5 points per point of spread");
  assert.equal(bidConfidenceLabel(bidConfidenceScore({ ...solid, spread: 0.11 })), "Low", "a typical week-one spread");
  assert.equal(bidConfidenceScore({ ...solid, effectiveCount: 10 }), 65, "1.5 points per comp short of 20");
  assert.equal(bidConfidenceScore({ ...solid, newestAge: 75 }), 70, "a point per 3 days past 45");
  assert.equal(bidConfidenceScore({ ...solid, newestAge: 300 }), 55, "capped at 25");
  assert.equal(bidConfidenceScore({ ...solid, newestAge: null }), 55);
  assert.equal(bidConfidenceScore({ ...solid, stageMatched: false }), 55);
  const worst = bidConfidenceScore({ spread: 0.3, effectiveCount: 6, newestAge: 200, stageMatched: false });
  assert.equal(worst, 1, "a scored bid never reaches 0, which is the score of no suggestion at all");
  assert.equal(bidConfidenceLabel(worst), "Low");
  assert.equal(bidConfidenceLabel(0), "N/A");
});

test("tightly clustered comps score higher than a bidding-war spread, however many or local", async () => {
  const { scoreBidForRow } = await importModule("src/domain/selectors.mjs");
  const wide = [0.97, 1.0, 1.0, 1.02, 1.04, 1.06, 1.08, 1.1, 1.13, 1.18].flatMap((ratio, i) => [comp(i, 5, ratio), comp(50 + i, 4, ratio), comp(80 + i, 6, ratio)]);
  const tight = [0.96, 0.965, 0.97, 0.97, 0.975, 0.98, 0.98, 0.985, 0.99, 1.0].map((ratio, i) => comp(200 + i, 30, ratio, { neighborhoodLabel: "Magnolia", zip: "98199" }));
  const weekOne = scoreBidForRow(listing(3), [...wide, ...tight], "balanced", false, { nowValue: NOW });
  const sitting = scoreBidForRow(listing(30), [...wide, ...tight], "balanced", false, { nowValue: NOW });
  assert.equal(weekOne.bidCompCount, 30);
  assert.equal(weekOne.bidCompTier, "T1_NEIGHBORHOOD_TYPE");
  assert.equal(sitting.bidCompTier, "T3_CITY_TYPE");
  assert.ok(sitting.bidConfidence > weekOne.bidConfidence, `${sitting.bidConfidence} vs ${weekOne.bidConfidence}`);
  assert.equal(weekOne.bidConfidenceLabel, "Low");
});

test("the outcome range brackets the bid range, and the ladder climbs with the odds", async () => {
  const { scoreBidForRow, BID_LADDER_CHANCES } = await importModule("src/domain/selectors.mjs");
  const spread = [0.95, 0.98, 1.0, 1.0, 1.01, 1.02, 1.03, 1.05, 1.07, 1.1, 1.14, 1.2].map((ratio, i) => comp(i, 5, ratio));
  const bid = scoreBidForRow(listing(3), spread, "balanced", false, { nowValue: NOW });
  assert.ok(bid.bidOutcomeLow <= bid.bidLow && bid.bidHigh <= bid.bidOutcomeHigh);
  assert.ok(bid.bidOutcomeLow < bid.pendingListPrice && bid.bidOutcomeHigh > bid.pendingListPrice);
  assert.deepEqual(bid.bidLadder.map((step) => step.chance), BID_LADDER_CHANCES);
  for (let i = 1; i < bid.bidLadder.length; i += 1) assert.ok(bid.bidLadder[i].bid >= bid.bidLadder[i - 1].bid);
  assert.equal(bid.bidLadder.find((step) => step.chance === 0.6).bid, bid.bidSuggested, "Balanced is the 60% step");
  const wild = [1.0, 1.05, 1.1, 1.15, 1.2, 1.22, 1.24, 1.3, 1.35, 1.4].map((ratio, i) => comp(i, 5, ratio));
  const capped = scoreBidForRow(listing(3), wild, "balanced", false, { nowValue: NOW });
  assert.deepEqual(capped.bidLadder.map((step) => step.chance), [0.5, 0.6, 0.7], "a step the 1.25x clamp would move is dropped, not mislabelled");
  const none = scoreBidForRow(listing(3), spread.slice(0, 3), "balanced", false, { nowValue: NOW });
  assert.equal(none.bidStatus, "InsufficientComps");
  assert.deepEqual([none.bidSuggested, none.bidOutcomeLow, none.bidOutcomeHigh, none.bidLadder.length], [0, 0, 0, 0]);
  // Five comps plus one 79 days older, which counts 0.4: 5.65 effective.
  const nearly = scoreBidForRow(listing(3), [...spread.slice(0, 5), { ...spread[5], saleDate: "2026-07-28" }], "balanced", false, { nowValue: NOW });
  assert.equal(nearly.bidStatus, "InsufficientComps");
  assert.equal(nearly.bidCompCount, 5, "never shown as 6 of the 6 needed");
  for (const saleDate of ["2026-10-15", "2026-10-01", "2026-09-09"]) {
    const six = scoreBidForRow(listing(3), spread.slice(0, 6).map((c) => ({ ...c, saleDate })), "balanced", false, { nowValue: NOW });
    assert.equal(six.bidStatus, "SCored", `six equal comps are enough whatever rounding makes of them (${saleDate})`);
  }
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
