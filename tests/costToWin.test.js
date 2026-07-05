"use strict";

// Synthetic fixtures only, no PII. Every expected value below is hand-computed
// from the fixture rows; float tolerances cover IEEE-754 noise from ratio math.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

function assertClose(actual, expected, eps = 1e-9, label = "") {
  assert.ok(Number.isFinite(actual), `${label} expected a finite number, got ${actual}`);
  assert.ok(
    Math.abs(actual - expected) <= eps,
    `${label} expected ${actual} to be within ${eps} of ${expected}`
  );
}

// Eligible comp: closed sale with a real list price and a usable ratio.
function comp(saleToList, closePrice) {
  return { hasMarketListPrice: true, saleToList, closePrice };
}

test("percentile uses linear interpolation and returns null on empty input", async () => {
  const { percentile } = await importModule("src/domain/costToWin.mjs");

  // Even count: median interpolates halfway between the middle pair.
  assert.equal(percentile([1, 2, 3, 4], 0.5), 2.5);
  // 3 values, q=0.75: pos = 2 * 0.75 = 1.5, so 0.05 + 0.5 * (0.10 - 0.05) = 0.075.
  assertClose(percentile([0.02, 0.05, 0.1], 0.75), 0.075, 1e-12, "p75");
  // Unsorted input is sorted internally; non-finite values are dropped.
  assert.equal(percentile([9, 1, 5, NaN, null], 0.5), 5);
  // Single value: every percentile is that value.
  assert.equal(percentile([7], 0.9), 7);
  // Endpoints.
  assert.equal(percentile([10, 20, 30], 0), 10);
  assert.equal(percentile([10, 20, 30], 1), 30);
  // Empty or all-invalid input yields null, not 0.
  assert.equal(percentile([], 0.5), null);
  assert.equal(percentile([NaN, undefined], 0.5), null);
  assert.equal(percentile(null, 0.5), null);
});

test("computeCostToWin splits a mixed slice and prices the premium and discount", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const rows = [
    // Over ask: premium pct = ratio - 1; implied list = close / ratio.
    comp(1.05, 1_050_000), // +5.0%, +$50,000 over an implied $1,000,000 list
    comp(1.02, 1_020_000), // +2.0%, +$20,000
    comp(1.1, 1_100_000), // +10.0%, +$100,000
    // Exactly at ask.
    comp(1.0, 800_000),
    // Under ask: discount pct = 1 - ratio.
    comp(0.95, 950_000), // -5.0%, -$50,000 under an implied $1,000,000 list
    comp(0.98, 980_000), // -2.0%, -$20,000
    // Ineligible: county row, no list price ever (assessed-value fallback).
    { hasMarketListPrice: false, saleToList: 0, closePrice: 900_000, dataMode: "PUBLIC_PROXY" },
    // Ineligible: flagged row whose ratio was blanked (0 = unavailable sentinel).
    { hasMarketListPrice: true, saleToList: 0, closePrice: 1_200_000 },
    // Ineligible: no closed price, so it is not a winner yet.
    { hasMarketListPrice: true, saleToList: 1.04, closePrice: 0 },
  ];

  const stats = computeCostToWin(rows);
  assert.equal(stats.totalCount, 9);
  assert.equal(stats.eligibleCount, 6);
  assert.equal(stats.overCount, 3);
  assert.equal(stats.atCount, 1);
  assert.equal(stats.underCount, 2);
  assertClose(stats.overShare, 3 / 6, 1e-12, "overShare");
  assertClose(stats.atShare, 1 / 6, 1e-12, "atShare");
  assertClose(stats.underShare, 2 / 6, 1e-12, "underShare");
  // Over-ask premiums sorted: [0.02, 0.05, 0.10].
  assertClose(stats.medianPremiumPctWhenOver, 0.05, 1e-9, "medianPremiumPct");
  assertClose(stats.medianPremiumUsdWhenOver, 50_000, 1e-3, "medianPremiumUsd");
  // p75: 0.05 + 0.5 * (0.10 - 0.05) = 0.075; p90: 0.05 + 0.8 * (0.10 - 0.05) = 0.09.
  assertClose(stats.p75PremiumPct, 0.075, 1e-9, "p75PremiumPct");
  assertClose(stats.p90PremiumPct, 0.09, 1e-9, "p90PremiumPct");
  // Under-ask discounts sorted: [0.02, 0.05] -> median 0.035; USD [20000, 50000] -> 35000.
  assertClose(stats.medianDiscountPctWhenUnder, 0.035, 1e-9, "medianDiscountPct");
  assertClose(stats.medianDiscountUsdWhenUnder, 35_000, 1e-3, "medianDiscountUsd");
});

test("computeCostToWin handles empty and missing input", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  for (const input of [[], null, undefined]) {
    const stats = computeCostToWin(input);
    assert.equal(stats.totalCount, 0);
    assert.equal(stats.eligibleCount, 0);
    assert.equal(stats.overCount, 0);
    assert.equal(stats.atCount, 0);
    assert.equal(stats.underCount, 0);
    assert.equal(stats.overShare, null);
    assert.equal(stats.atShare, null);
    assert.equal(stats.underShare, null);
    assert.equal(stats.medianPremiumPctWhenOver, null);
    assert.equal(stats.medianPremiumUsdWhenOver, null);
    assert.equal(stats.p75PremiumPct, null);
    assert.equal(stats.p90PremiumPct, null);
    assert.equal(stats.medianDiscountPctWhenUnder, null);
    assert.equal(stats.medianDiscountUsdWhenUnder, null);
  }
});

test("computeCostToWin with zero eligible rows reports counts but no shares", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const rows = [
    { hasMarketListPrice: false, saleToList: 0, closePrice: 850_000 },
    { hasMarketListPrice: false, saleToList: 0, closePrice: 1_150_000 },
    { hasMarketListPrice: false, saleToList: 0, closePrice: 990_000 },
  ];
  const stats = computeCostToWin(rows);
  assert.equal(stats.totalCount, 3);
  assert.equal(stats.eligibleCount, 0);
  assert.equal(stats.overShare, null);
  assert.equal(stats.atShare, null);
  assert.equal(stats.underShare, null);
  assert.equal(stats.medianPremiumPctWhenOver, null);
  assert.equal(stats.medianDiscountPctWhenUnder, null);
});

test("computeCostToWin when every winner paid over asking", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  // Premiums: 1.04 on $1,040,000 -> +$40,000 (+4%); 1.08 on $540,000 -> +$40,000 (+8%).
  const stats = computeCostToWin([comp(1.04, 1_040_000), comp(1.08, 540_000)]);
  assert.equal(stats.eligibleCount, 2);
  assert.equal(stats.overCount, 2);
  assert.equal(stats.atCount, 0);
  assert.equal(stats.underCount, 0);
  assert.equal(stats.overShare, 1);
  assert.equal(stats.atShare, 0);
  assert.equal(stats.underShare, 0);
  assertClose(stats.medianPremiumPctWhenOver, 0.06, 1e-9, "medianPremiumPct");
  assertClose(stats.medianPremiumUsdWhenOver, 40_000, 1e-3, "medianPremiumUsd");
  // Sorted premiums [0.04, 0.08]: p75 = 0.04 + 0.75 * 0.04 = 0.07; p90 = 0.076.
  assertClose(stats.p75PremiumPct, 0.07, 1e-9, "p75PremiumPct");
  assertClose(stats.p90PremiumPct, 0.076, 1e-9, "p90PremiumPct");
  assert.equal(stats.medianDiscountPctWhenUnder, null);
  assert.equal(stats.medianDiscountUsdWhenUnder, null);
});

test("computeCostToWin when every winner paid under asking", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  // Discounts: 0.90 on $900,000 -> -$100,000 (-10%); 0.96 on $960,000 -> -$40,000 (-4%).
  const stats = computeCostToWin([comp(0.9, 900_000), comp(0.96, 960_000)]);
  assert.equal(stats.overCount, 0);
  assert.equal(stats.atCount, 0);
  assert.equal(stats.underCount, 2);
  assert.equal(stats.underShare, 1);
  assert.equal(stats.medianPremiumPctWhenOver, null);
  assert.equal(stats.medianPremiumUsdWhenOver, null);
  assert.equal(stats.p75PremiumPct, null);
  assert.equal(stats.p90PremiumPct, null);
  assertClose(stats.medianDiscountPctWhenUnder, 0.07, 1e-9, "medianDiscountPct");
  assertClose(stats.medianDiscountUsdWhenUnder, 70_000, 1e-3, "medianDiscountUsd");
});

test("computeCostToWin counts exact-1.0 ties as at-list, tolerating float noise", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const stats = computeCostToWin([
    comp(1.0, 800_000),
    comp(800_000 / 800_000, 750_000), // genuine sold-at-list division, exactly 1
    comp(1 + 1e-12, 900_000), // float noise inside the at-list epsilon
  ]);
  assert.equal(stats.eligibleCount, 3);
  assert.equal(stats.overCount, 0);
  assert.equal(stats.atCount, 3);
  assert.equal(stats.underCount, 0);
  assert.equal(stats.atShare, 1);
  assert.equal(stats.medianPremiumPctWhenOver, null);
  assert.equal(stats.medianDiscountPctWhenUnder, null);
});

test("computeCostToWin with a single over-ask winner pins every percentile to it", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const stats = computeCostToWin([comp(1.03, 1_030_000)]);
  assertClose(stats.medianPremiumPctWhenOver, 0.03, 1e-9, "medianPremiumPct");
  assertClose(stats.p75PremiumPct, 0.03, 1e-9, "p75PremiumPct");
  assertClose(stats.p90PremiumPct, 0.03, 1e-9, "p90PremiumPct");
  assertClose(stats.medianPremiumUsdWhenOver, 30_000, 1e-3, "medianPremiumUsd");
});

test("buildCostToWinVerdict phrases a split market in plain English", async () => {
  const { computeCostToWin, buildCostToWinVerdict } = await importModule("src/domain/costToWin.mjs");
  // 3 over / 1 at / 2 under: half paid list or less; typical premium $50K (5.0%).
  const stats = computeCostToWin([
    comp(1.05, 1_050_000),
    comp(1.02, 1_020_000),
    comp(1.1, 1_100_000),
    comp(1.0, 800_000),
    comp(0.95, 950_000),
    comp(0.98, 980_000),
  ]);
  assert.equal(
    buildCostToWinVerdict(stats),
    "About half of winners paid list price or less; when buyers went over, the typical premium was $50K (5.0%)."
  );
});

test("buildCostToWinVerdict covers the degenerate slices", async () => {
  const { computeCostToWin, buildCostToWinVerdict } = await importModule("src/domain/costToWin.mjs");

  assert.equal(
    buildCostToWinVerdict(computeCostToWin([])),
    "No closed sales in this slice yet, so cost to win cannot be measured."
  );
  assert.equal(
    buildCostToWinVerdict(null),
    "No closed sales in this slice yet, so cost to win cannot be measured."
  );

  const noListStats = computeCostToWin([
    { hasMarketListPrice: false, saleToList: 0, closePrice: 850_000 },
    { hasMarketListPrice: false, saleToList: 0, closePrice: 1_150_000 },
    { hasMarketListPrice: false, saleToList: 0, closePrice: 990_000 },
  ]);
  assert.equal(
    buildCostToWinVerdict(noListStats),
    "None of the 3 closed sales here carry a real list price, so cost to win cannot be measured."
  );

  const allOver = computeCostToWin([comp(1.04, 1_040_000), comp(1.08, 540_000)]);
  assert.equal(
    buildCostToWinVerdict(allOver),
    "Every winner in this slice paid over asking; the typical premium was $40K (6.0%)."
  );

  const allUnder = computeCostToWin([comp(0.9, 900_000), comp(0.96, 960_000)]);
  assert.equal(
    buildCostToWinVerdict(allUnder),
    "Every winner in this slice paid list price or less; the typical discount was $70K (7.0%)."
  );

  const allAt = computeCostToWin([comp(1.0, 800_000), comp(1.0, 750_000)]);
  assert.equal(buildCostToWinVerdict(allAt), "Every winner in this slice paid exactly list price.");
});

test("buildCostToWinVerdict scales its share wording with the list-or-less share", async () => {
  const { buildCostToWinVerdict } = await importModule("src/domain/costToWin.mjs");

  function statsWithListOrLessShare(listOrLessCount, total = 100) {
    return {
      totalCount: total,
      eligibleCount: total,
      overCount: total - listOrLessCount,
      atCount: 0,
      underCount: listOrLessCount,
      medianPremiumUsdWhenOver: 31_000,
      medianPremiumPctWhenOver: 0.024,
    };
  }

  const expectations = [
    [90, "Nearly all winners"],
    [70, "Most winners"],
    [57, "More than half of winners"],
    [50, "About half of winners"],
    [40, "Just under half of winners"],
    [20, "A minority of winners"],
    [10, "Only a few winners"],
  ];
  for (const [count, phrase] of expectations) {
    assert.equal(
      buildCostToWinVerdict(statsWithListOrLessShare(count)),
      `${phrase} paid list price or less; when buyers went over, the typical premium was $31K (2.4%).`
    );
  }
});

test("buildCostToWinVerdict accepts injected formatters", async () => {
  const { computeCostToWin, buildCostToWinVerdict } = await importModule("src/domain/costToWin.mjs");
  const stats = computeCostToWin([
    comp(1.05, 1_050_000),
    comp(1.02, 1_020_000),
    comp(0.95, 950_000),
    comp(0.98, 980_000),
  ]);
  const verdict = buildCostToWinVerdict(stats, {
    formatMoneyCompact: (value) => `US$${Math.round(value).toLocaleString("en-US")}`,
    formatPct: (value) => `${(value * 100).toFixed(0)} percent`,
  });
  assert.equal(
    verdict,
    "About half of winners paid list price or less; when buyers went over, the typical premium was US$35,000 (4 percent)."
  );
});

// Dollar-backed comp: normalizeRow confirmed listPriceAtPending came straight
// from a listing column (hasDollarListPrice), so dollars outrank the ratio.
function dollarComp(listPriceAtPending, closePrice, saleToList) {
  return { hasMarketListPrice: true, hasDollarListPrice: true, listPriceAtPending, closePrice, saleToList };
}

test("computeCostToWin prefers dollar columns over a rounded or contradictory CSV ratio", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const rows = [
    // CSV ratio rounded to 1.00, but the dollars show a $4,000 over-ask win.
    dollarComp(1_000_000, 1_004_000, 1.0),
    // CSV ratio says over ask, dollars say $10,000 under: dollars win.
    dollarComp(1_000_000, 990_000, 1.01),
    // Dollars equal (ratio noise notwithstanding): counts as at ask.
    dollarComp(800_000, 800_000, 1.0000001),
    // Ratio-only row still classified via the ratio path.
    comp(1.05, 1_050_000),
  ];

  const stats = computeCostToWin(rows);
  assert.equal(stats.eligibleCount, 4);
  assert.equal(stats.overCount, 2);
  assert.equal(stats.atCount, 1);
  assert.equal(stats.underCount, 1);
  // Premium dollars come from the actual list price, not close / ratio.
  // Over-ask USD values sorted: [4000, 50000] -> median 27000.
  assertClose(stats.medianPremiumUsdWhenOver, 27_000, 1e-3, "medianPremiumUsd");
  // Premium pcts sorted: [0.004, 0.05] -> median 0.027.
  assertClose(stats.medianPremiumPctWhenOver, 0.027, 1e-9, "medianPremiumPct");
  assertClose(stats.medianDiscountUsdWhenUnder, 10_000, 1e-3, "medianDiscountUsd");
  assertClose(stats.medianDiscountPctWhenUnder, 0.01, 1e-9, "medianDiscountPct");
});

test("computeCostToWin ignores the dollar path when the flagged list price is unusable", async () => {
  const { computeCostToWin } = await importModule("src/domain/costToWin.mjs");
  const rows = [
    // Flagged dollar-backed but the list price is 0: falls back to the ratio.
    { hasMarketListPrice: true, hasDollarListPrice: true, listPriceAtPending: 0, closePrice: 1_020_000, saleToList: 1.02 },
    // hasDollarListPrice false: listPriceAtPending may be an assessed-value
    // fallback and must NOT be trusted even though it is present.
    { hasMarketListPrice: true, hasDollarListPrice: false, listPriceAtPending: 700_000, closePrice: 980_000, saleToList: 0.98 },
  ];

  const stats = computeCostToWin(rows);
  assert.equal(stats.eligibleCount, 2);
  assert.equal(stats.overCount, 1);
  assert.equal(stats.underCount, 1);
  // Both derived from the ratio path: implied lists of $1,000,000 each.
  assertClose(stats.medianPremiumUsdWhenOver, 20_000, 1e-3, "medianPremiumUsd");
  assertClose(stats.medianDiscountUsdWhenUnder, 20_000, 1e-3, "medianDiscountUsd");
});
