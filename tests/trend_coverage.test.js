"use strict";

// A trend may only be drawn from months whose subset metric (sale versus ask,
// days on market, fast-sale share) rests on enough of that month's sales. In
// 2026 the June and July list-price rows were the short-escrow survivors of the
// snapshot backfill (34% and 49% coverage), which would have faked a trend.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function overview() {
  return import(pathToFileURL(path.resolve(__dirname, "..", "src/views/overview.mjs")).href);
}

// The default-lens months measured on 2026-09-19 (closed, with a list price).
const month = (m, salesCount, ratioSampleSize, overAskShare) => ({ month: m, salesCount, sampleSize: salesCount, ratioSampleSize, overAskShare, medianClosePrice: 1300000 });
const SERIES = [
  month("2026-03", 140, 112, 0.54),
  month("2026-04", 138, 98, 0.54),
  month("2026-05", 152, 97, 0.56),
  month("2026-06", 145, 49, 0.55),
  month("2026-07", 115, 56, 0.38),
  month("2026-08", 86, 78, 0.46),
  month("2026-09", 46, 45, 0.33),
];

test("monthCoverage is the subset's share of the month, and 1 for whole-month metrics", async () => {
  const { monthCoverage } = await overview();
  assert.equal(monthCoverage(SERIES[3], "ratioSampleSize").toFixed(2), "0.34");
  assert.equal(monthCoverage(SERIES[5], "ratioSampleSize").toFixed(2), "0.91");
  assert.equal(monthCoverage(SERIES[3], undefined), 1, "median close is computed on every sale");
  assert.equal(monthCoverage({ salesCount: 0, ratioSampleSize: 0 }, "ratioSampleSize"), 1, "an empty month is not a coverage failure");
  assert.equal(monthCoverage({ salesCount: 10, ratioSampleSize: 14 }, "ratioSampleSize"), 1, "never above 1");
  assert.equal(monthCoverage({ salesCount: 10 }, "ratioSampleSize"), 0, "a missing subset count is zero coverage, not full");
  assert.equal(monthCoverage(null, "ratioSampleSize"), 1);
});

test("the threshold keeps August and September and excludes June and July", async () => {
  const { monthCoverage, MIN_TREND_COVERAGE } = await overview();
  const covered = (m) => monthCoverage(SERIES.find((e) => e.month === m), "ratioSampleSize") >= MIN_TREND_COVERAGE;
  assert.equal(covered("2026-06"), false);
  assert.equal(covered("2026-07"), false);
  assert.equal(covered("2026-08"), true);
  assert.equal(covered("2026-09"), true);
  assert.equal(covered("2026-03"), true);
});

test("trendMonths keeps readable months in order and never skips one for coverage", async () => {
  const { trendMonths } = await overview();
  const months = trendMonths(SERIES, "overAskShare", { sampleField: "ratioSampleSize", minComps: 5 });
  // June and July stay in the list: coverage is judged on the pair compared, so
  // an uncovered month blocks its comparison instead of being silently skipped
  // in favor of an older month.
  assert.deepEqual(months.map((e) => e.month), SERIES.map((e) => e.month));
});

test("trendMonths drops a thin trailing month and months under the comp floor", async () => {
  const { trendMonths } = await overview();
  const withThinTail = [...SERIES, month("2026-10", 9, 8, 0.25)];
  const months = trendMonths(withThinTail, "overAskShare", { sampleField: "ratioSampleSize", minComps: 5 });
  assert.equal(months[months.length - 1].month, "2026-09", "8 priced sales is under half of September's 45");

  const tiny = trendMonths([month("2026-08", 86, 78, 0.46), month("2026-09", 46, 3, 0.33)], "overAskShare", { sampleField: "ratioSampleSize", minComps: 5 });
  assert.deepEqual(tiny.map((e) => e.month), ["2026-08"], "3 priced sales is under the 5-comp floor");

  const nullValue = trendMonths([month("2026-08", 86, 78, null), month("2026-09", 46, 45, 0.33)], "overAskShare", { sampleField: "ratioSampleSize", minComps: 5 });
  assert.deepEqual(nullValue.map((e) => e.month), ["2026-09"], "a null metric (Number(null) is 0) is not a reading");
});

test("whole-month metrics are gated on sales count only", async () => {
  const { trendMonths, monthCoverage } = await overview();
  const months = trendMonths(SERIES, "medianClosePrice", { sampleField: undefined, minComps: 5 });
  assert.equal(months.length, SERIES.length);
  months.forEach((entry) => assert.equal(monthCoverage(entry, undefined), 1));
});
