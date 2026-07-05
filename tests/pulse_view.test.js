"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

// watchlistVerdict is the one-sentence "is my watchlist heating up?" answer
// at the top of the Pulse tab. It reuses the same six recent-window signals
// (salesCount, hotShare, medianDom, medianSaleToList, medianBidUp,
// medianClosePrice) and the same competitiveDelta/metricDirection sign
// conventions the recent-window tile grid already renders with, so these
// tests pin the counting rule rather than re-deriving the domain math.
async function loadWatchlistVerdict() {
  const mod = await import("../src/views/pulse.mjs");
  return mod.watchlistVerdict;
}

test("watchlistVerdict: no recent sales reads as not-enough-data", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  const v = watchlistVerdict({ current: { salesCount: 0 }, previous: { salesCount: 0 } });
  assert.equal(v.tone, "flat");
  assert.match(v.answer, /Not enough recent watchlist sales/);
});

test("watchlistVerdict: missing recent90 entirely reads as not-enough-data", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  const v = watchlistVerdict(undefined);
  assert.equal(v.tone, "flat");
  assert.match(v.answer, /Not enough recent watchlist sales/);
});

test("watchlistVerdict: fewer than 2 readable signals reads as hard-to-say", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  // Only salesCount moved; every other metric is flat (equal current/previous),
  // so only 1 of the 6 signals has a direction.
  const v = watchlistVerdict({
    current: { salesCount: 12, hotShare: 0.4, medianDom: 8, medianSaleToList: 1.02, medianBidUp: 6000, medianClosePrice: 700000 },
    previous: { salesCount: 6, hotShare: 0.4, medianDom: 8, medianSaleToList: 1.02, medianBidUp: 6000, medianClosePrice: 700000 },
  });
  assert.equal(v.tone, "flat");
  assert.match(v.answer, /Hard to say/);
});

test("watchlistVerdict: all six signals tightening reads hotter", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  const v = watchlistVerdict({
    current: { salesCount: 20, hotShare: 0.5, medianDom: 5, medianSaleToList: 1.05, medianBidUp: 15000, medianClosePrice: 800000 },
    previous: { salesCount: 10, hotShare: 0.3, medianDom: 10, medianSaleToList: 1.0, medianBidUp: 5000, medianClosePrice: 750000 },
  });
  assert.equal(v.tone, "hotter");
  assert.match(v.answer, /heating up/);
  assert.match(v.detail, /6 of 6 tracked signals/);
});

test("watchlistVerdict: all six signals easing reads cooler", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  const v = watchlistVerdict({
    current: { salesCount: 10, hotShare: 0.3, medianDom: 10, medianSaleToList: 1.0, medianBidUp: 5000, medianClosePrice: 750000 },
    previous: { salesCount: 20, hotShare: 0.5, medianDom: 5, medianSaleToList: 1.05, medianBidUp: 15000, medianClosePrice: 800000 },
  });
  assert.equal(v.tone, "cooler");
  assert.match(v.answer, /cooling off/);
  assert.match(v.detail, /6 of 6 tracked signals/);
});

test("watchlistVerdict: an even split reads as no clear shift", async () => {
  const watchlistVerdict = await loadWatchlistVerdict();
  // 3 signals hotter (salesCount, hotShare, medianBidUp), 3 cooler
  // (medianDom sitting longer, medianSaleToList easing, medianClosePrice down).
  const v = watchlistVerdict({
    current: { salesCount: 20, hotShare: 0.5, medianDom: 12, medianSaleToList: 0.98, medianBidUp: 15000, medianClosePrice: 700000 },
    previous: { salesCount: 10, hotShare: 0.3, medianDom: 8, medianSaleToList: 1.02, medianBidUp: 5000, medianClosePrice: 750000 },
  });
  assert.equal(v.tone, "flat");
  assert.match(v.answer, /No clear shift/);
  assert.match(v.detail, /3 signals eased and 3 tightened/);
});
