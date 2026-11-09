"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

const NOW = "2026-11-08";

function comp(key, dom, ratio, overrides = {}) {
  const ask = overrides.pendingListPrice || 1_250_000;
  return {
    mapPropertyKey: key, dataMode: "MLS_ENRICHED", typeLabel: "Single Family", neighborhoodLabel: "Ballard", zip: "98117",
    pendingListPrice: ask, closePrice: Math.round(ask * ratio), saleToList: ratio, saleDate: "2026-10-15", yearBuilt: 1948,
    hasMlsCdomValue: dom !== null, mlsCDOM: dom ?? 0, hasMlsDomValue: false, daysToPending: null, ...overrides,
  };
}

function listing(dom, overrides = {}) {
  return {
    mapPropertyKey: "active", dataMode: "MLS_ENRICHED", typeLabel: "Single Family", neighborhoodLabel: "Ballard", zip: "98117",
    pendingListPrice: 1_250_000, yearBuilt: 1948, hasMlsCdomValue: dom !== null, mlsCDOM: dom ?? 0, hasMlsDomValue: false, daysToPending: null, ...overrides,
  };
}

const many = (count, prefix, dom, ratio, overrides = {}) => Array.from({ length: count }, (_, i) => comp(`${prefix}-${i}`, dom, ratio, overrides));
const ELSEWHERE = { neighborhoodLabel: "Magnolia", zip: "98199" };

async function medianRatio(target, pool) {
  const { selectBidComps, compRatioQuantiles } = await importModule("src/domain/bidComps.mjs");
  return compRatioQuantiles(selectBidComps(target, pool, { nowValue: NOW }), [0.5])[0];
}

test("identical comps count one each, so the effective count is the plain count", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const picked = selectBidComps(listing(3), many(10, "a", 5, 1.05), { nowValue: NOW });
  assert.equal(picked.effectiveCount.toFixed(6), "10.000000");
  assert.equal(picked.localShare, 1);
  assert.equal(picked.newestAge, 24);
  assert.equal(picked.stageMatched, true);
});

test("comps that barely count add little to the effective count", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const picked = selectBidComps(listing(3), [...many(10, "near", 5, 1.05), ...many(10, "far", 5, 1.0, ELSEWHERE)], { nowValue: NOW });
  assert.ok(picked.effectiveCount > 10 && picked.effectiveCount < 13, String(picked.effectiveCount));
});

test("week one is walled off: a first-week listing sees only first-week sales, a later one never does", async () => {
  const pool = [...many(10, "w1", 7, 1.08), ...many(10, "w2", 8, 0.99)];
  assert.equal(await medianRatio(listing(0), pool), 1.08);
  assert.equal(await medianRatio(listing(7), pool), 1.08);
  assert.equal(await medianRatio(listing(8), pool), 0.99);
  assert.equal(await medianRatio(listing(200), pool), 0.99);
});

test("inside week one the exact day does not matter", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(6, "d1", 1, 1.02), ...many(6, "d5", 5, 1.09)];
  const [day1, day6] = [1, 6].map((dom) => selectBidComps(listing(dom), pool, { nowValue: NOW }));
  assert.deepEqual(Array.from(day1.weights), Array.from(day6.weights));
});

test("after week one a sale counts by how close its days to pending were", async () => {
  const pool = [...many(10, "d25", 25, 0.98), ...many(10, "d150", 150, 0.93)];
  assert.equal(await medianRatio(listing(20), pool), 0.98);
  assert.equal(await medianRatio(listing(180), pool), 0.93);
});

test("there is no cliff at a stage boundary: day 45 and day 46 get nearly the same weights", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(8, "d30", 30, 0.98), ...many(8, "d60", 60, 0.96)];
  const [a, b] = [45, 46].map((dom) => selectBidComps(listing(dom), pool, { nowValue: NOW }));
  a.weights.forEach((w, i) => assert.ok(Math.abs(w - b.weights[i]) / w < 0.05));
});

test("nearby sales outweigh a larger number elsewhere, by neighborhood or by ZIP", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(8, "near", 5, 1.09), ...many(30, "far", 5, 1.01, ELSEWHERE)];
  assert.equal(await medianRatio(listing(3), pool), 1.09);
  assert.equal(selectBidComps(listing(3), pool, { nowValue: NOW }).tier, "T1_NEIGHBORHOOD_TYPE");
  assert.equal(await medianRatio(listing(3, { neighborhoodLabel: "Seattle" }), pool), 1.09, "no neighborhood given: the ZIP still finds them");
  assert.equal(await medianRatio(listing(3, { zip: "" }), pool), 1.09);
  assert.equal(await medianRatio(listing(3, ELSEWHERE), pool), 1.01);
});

test("a couple of nearby sales inform the bid without deciding it", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(2, "near", 5, 1.2), ...many(60, "far", 5, 1.02, ELSEWHERE)];
  assert.equal(await medianRatio(listing(3), pool), 1.02);
  const picked = selectBidComps(listing(3), pool, { nowValue: NOW });
  assert.equal(picked.tier, "T3_CITY_TYPE");
  assert.equal(picked.localShare.toFixed(2), "0.14", "two nearby sales earn a 4.8x boost, not the full 20x");
});

test("the local boost is earned: it grows with the number of nearby sales", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const boostWith = (count) => {
    const picked = selectBidComps(listing(3), [...many(count, "near", 5, 1.05), ...many(20, "far", 5, 1.0, ELSEWHERE)], { nowValue: NOW });
    const at = (prefix) => picked.weights[picked.typed.comps.findIndex((c) => c.mapPropertyKey.startsWith(prefix))];
    return at("near") / at("far");
  };
  assert.equal(boostWith(1).toFixed(2), "3.11");
  assert.equal(boostWith(8).toFixed(2), "10.50");
  assert.equal(boostWith(50).toFixed(2), "17.38");
});

test("finding one more comparable sale nearby never costs a listing its basis", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(15, "late", 60, 0.96, ELSEWHERE), ...many(40, "w1", 5, 1.08)];
  const before = selectBidComps(listing(60), pool, { nowValue: NOW });
  const after = selectBidComps(listing(60), [...pool, comp("near", 60, 0.96)], { nowValue: NOW });
  assert.equal(before.domMatch, "near");
  assert.equal(after.domMatch, "near", "it used to drop to every comp, bidding wars included");
  assert.ok(after.comparableCount > before.comparableCount);
  assert.equal(await medianRatio(listing(60), [...pool, comp("near", 60, 0.96)]), 0.96);
  const thin = many(8, "late", 60, 0.96, ELSEWHERE);
  assert.notEqual(selectBidComps(listing(60), [...thin, comp("near", 60, 0.96)], { nowValue: NOW }).tier, "NONE");
});

test("a catch-all label is not a neighborhood", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const scattered = { neighborhoodLabel: "Seattle (Other)" };
  const pool = [...many(10, "north", 5, 1.0, { ...scattered, zip: "98155" }), ...many(10, "south", 5, 1.08, { ...scattered, zip: "98178" })];
  const picked = selectBidComps(listing(3, { ...scattered, zip: "98155" }), pool, { nowValue: NOW });
  assert.equal(picked.localShare > 0.5 && picked.localShare < 1, true, "its own ZIP is local; the far one sharing the label is not");
  assert.equal(await medianRatio(listing(3, { ...scattered, zip: "98155" }), pool), 1.0);
});

test("a sale that barely counts does not make the basis look current", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const old = many(12, "old", 5, 1.03, { saleDate: "2026-06-01" });
  const offTarget = comp("recent-but-far", 5, 1.0, { saleDate: "2026-11-05", pendingListPrice: 2_950_000, yearBuilt: 2022, ...ELSEWHERE });
  const unrelated = many(400, "late", 90, 0.95); // same type, zero weight for a week-one listing
  assert.equal(selectBidComps(listing(3), [...old, offTarget, ...unrelated], { nowValue: NOW }).newestAge, 160);
});

test("newer builds are priced off newer builds, and older homes off older ones", async () => {
  const { isNewerBuild } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(8, "new", 5, 1.0, { yearBuilt: 2019 }), ...many(40, "old", 5, 1.06)];
  assert.equal(await medianRatio(listing(3, { yearBuilt: 2022 }), pool), 1.0);
  assert.equal(await medianRatio(listing(3, { yearBuilt: 1926 }), pool), 1.06);
  assert.equal(await medianRatio(listing(3, { yearBuilt: 0 }), pool), 1.06, "unknown year: no era weighting, so the many win");
  assert.equal(isNewerBuild({ yearBuilt: 2011 }, NOW), true);
  assert.equal(isNewerBuild({ yearBuilt: 2010 }, NOW), false);
  assert.equal(isNewerBuild({ yearBuilt: 0 }, NOW), false);
  assert.equal(isNewerBuild({ yearBuilt: 2027 }, NOW), true, "a presale");
  assert.equal(isNewerBuild({ yearBuilt: 19481948 }, NOW), false, "a typo is unknown, not brand new");
  assert.equal(await medianRatio(listing(3, { yearBuilt: 19481948 }), pool), 1.06);
});

test("a comp's era is judged when it sold, not today", async () => {
  // Built 2010, sold 2025: 15 years old on the market, so it informs newer listings.
  const pool = [...many(8, "was-new", 5, 1.0, { yearBuilt: 2010, saleDate: "2025-12-20" }), ...many(8, "old", 5, 1.06, { saleDate: "2025-12-20" })];
  assert.equal(await medianRatio(listing(3, { yearBuilt: 2020 }), pool), 1.0);
});

test("comps asked near the listing's price count most", async () => {
  const pool = [...many(10, "cheap", 5, 1.1, { pendingListPrice: 800_000 }), ...many(10, "close", 5, 1.03, { pendingListPrice: 1_300_000 })];
  assert.equal(await medianRatio(listing(3), pool), 1.03);
  assert.equal(await medianRatio(listing(3, { pendingListPrice: 780_000 }), pool), 1.1);
  assert.ok([1.03, 1.1].includes(await medianRatio(listing(3, { pendingListPrice: 0 }), pool)), "no ask yet (comp finder): still returns comps");
});

test("recent sales outweigh old ones, and nothing past 12 months counts", async () => {
  const { selectBidComps, BID_HALFLIFE_DAYS } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(10, "spring", 5, 1.1, { saleDate: "2026-04-01" }), ...many(10, "autumn", 5, 1.01, { saleDate: "2026-10-20" })];
  assert.equal(await medianRatio(listing(3), pool), 1.01);
  const halved = selectBidComps(listing(3), [comp("now", 5, 1.0, { saleDate: NOW }), comp("then", 5, 1.0, { saleDate: "2026-09-09" }), ...many(8, "pad", 5, 1.0)], { nowValue: NOW });
  const byKey = new Map(halved.typed.comps.map((c, i) => [c.mapPropertyKey, halved.weights[i]]));
  assert.equal(BID_HALFLIFE_DAYS, 60);
  assert.equal((byKey.get("then") / byKey.get("now")).toFixed(4), "0.5000");
  const expired = selectBidComps(listing(3), many(10, "old", 5, 1.0, { saleDate: "2025-11-01" }), { nowValue: NOW });
  assert.equal(expired.tier, "NONE");
  assert.equal(expired.effectiveCount, 0);
});

test("other home types never count", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const picked = selectBidComps(listing(3), many(20, "condo", 5, 1.0, { typeLabel: "Condo" }), { nowValue: NOW });
  assert.equal(picked.tier, "NONE");
  assert.equal(picked.newestAge, null);
});

test("scarce comps near a late listing's age widen to every sale past week one, never to week one", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(40, "w1", 5, 1.08), ...many(8, "d10", 10, 0.99), ...many(2, "d300", 300, 0.93)];
  const picked = selectBidComps(listing(400), pool, { nowValue: NOW });
  assert.equal(picked.domMatch, "side");
  assert.equal(picked.stageMatched, true);
  assert.equal(Math.round(picked.effectiveCount), 10);
  picked.typed.comps.forEach((c, i) => { if (c.mlsCDOM <= 7) assert.equal(picked.weights[i], 0, "no bidding-war sale carries weight"); });
  assert.equal(await medianRatio(listing(400), pool), 0.99);
  assert.equal(selectBidComps(listing(12), pool, { nowValue: NOW }).domMatch, "near");
  assert.equal(selectBidComps(listing(3), pool, { nowValue: NOW }).domMatch, "near");
});

test("a stretch of the market with too few sales falls back to every comp and says so", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(12, "w1", 5, 1.05), ...many(3, "late", 70, 0.95)];
  const picked = selectBidComps(listing(60), pool, { nowValue: NOW });
  assert.equal(picked.stage, "stale");
  assert.equal(picked.stageMatched, false);
  assert.equal(picked.domMatch, "none");
  assert.equal(Math.round(picked.effectiveCount), 15);
  const unknown = selectBidComps(listing(null), [...pool, comp("no-dom", null, 1.0)], { nowValue: NOW });
  assert.equal(unknown.stage, null);
  assert.equal(Math.round(unknown.effectiveCount), 16, "with no reading to match, comps without one count too");
  const known = selectBidComps(listing(3), [...pool, comp("no-dom", null, 1.0)], { nowValue: NOW });
  assert.equal(Math.round(known.effectiveCount), 12, "a comp with no days on market cannot be placed, so it sits out");
});

test("quantiles come back in order, in one pass, and null when there is nothing to weigh", async () => {
  const { selectBidComps, compRatioQuantiles } = await importModule("src/domain/bidComps.mjs");
  const pool = [0.95, 0.98, 1.0, 1.0, 1.02, 1.04, 1.06, 1.08, 1.1, 1.15].map((ratio, i) => comp(`c-${i}`, 5, ratio));
  const picked = selectBidComps(listing(3), pool, { nowValue: NOW });
  assert.deepEqual(compRatioQuantiles(picked, [0.1, 0.5, 0.6, 0.9, 1]), [0.95, 1.02, 1.04, 1.1, 1.15]);
  assert.deepEqual(compRatioQuantiles(selectBidComps(listing(3), [], { nowValue: NOW }), [0.1, 0.9]), [null, null]);
});

test("the comps shown are the ones that count most, with their share of the weight", async () => {
  const { selectBidComps, topBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = [...many(3, "near", 5, 1.05), ...many(20, "far", 5, 1.0, ELSEWHERE), comp("newest-near", 5, 1.07, { saleDate: "2026-11-05" })];
  const top = topBidComps(selectBidComps(listing(3), pool, { nowValue: NOW }), 5);
  assert.equal(top.length, 5);
  assert.equal(top[0].mapPropertyKey, "newest-near");
  assert.deepEqual(top.slice(1, 4).map((c) => c.neighborhoodLabel), ["Ballard", "Ballard", "Ballard"]);
  assert.equal(top[4].neighborhoodLabel, "Magnolia");
  for (let i = 1; i < top.length; i += 1) assert.ok(top[i].bidCompWeightShare <= top[i - 1].bidCompWeightShare);
  assert.ok(top.reduce((sum, c) => sum + c.bidCompWeightShare, 0) < 1);
});

test("the prepared pool is reused for the same pool and day, and rebuilt when the day changes", async () => {
  const { selectBidComps } = await importModule("src/domain/bidComps.mjs");
  const pool = many(10, "a", 5, 1.05);
  const first = selectBidComps(listing(3), pool, { nowValue: NOW });
  assert.equal(selectBidComps(listing(5), pool, { nowValue: NOW }).typed, first.typed);
  const later = selectBidComps(listing(3), pool, { nowValue: "2026-12-08" });
  assert.notEqual(later.typed, first.typed);
  assert.equal(later.newestAge, 54);
});
