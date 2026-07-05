"use strict";

// Synthetic fixtures only — no real addresses, parcels, or PII.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

function loadChangesSince() {
  return importModule("src/domain/changesSince.mjs");
}

// Baseline visit anchor used across tests. Plain date strings keep every
// comparison in local time, matching how the app's toDate parses row dates.
const BASELINE_AT = "2026-07-01";
const NOW = "2026-07-04";

function activeRow(overrides = {}) {
  return {
    dataMode: "MLS_ENRICHED",
    id: "mls-open-2400001-1111111111-3",
    parcelNbr: "1111111111",
    address: "101 NW Sample St, Seattle WA 98117",
    neighborhoodLabel: "Ballard / Crown Hill",
    zip: "98117",
    mlsStatusNorm: "ACTIVE",
    mlsStatusLabel: "Active",
    listDate: "2026-06-20",
    pendingDate: "",
    saleDate: "",
    closePrice: 0,
    pendingListPrice: 1_000_000,
    listPriceAtPending: 1_000_000,
    hasMarketListPrice: true,
    hasActualClose: false,
    daysToPending: null,
    ...overrides,
  };
}

function pendingRow(overrides = {}) {
  return activeRow({
    id: "mls-open-2400002-2222222222-4",
    parcelNbr: "2222222222",
    address: "202 SW Example Ave, Seattle WA 98126",
    neighborhoodLabel: "West Seattle",
    zip: "98126",
    mlsStatusNorm: "PENDING",
    mlsStatusLabel: "Pending",
    listDate: "2026-06-28",
    pendingDate: "2026-07-03",
    daysToPending: 5,
    ...overrides,
  });
}

function closedRow(overrides = {}) {
  return activeRow({
    id: "mls-only-2400003-3333333333-9",
    parcelNbr: "3333333333",
    address: "303 NE Fixture Pl, Seattle WA 98115",
    neighborhoodLabel: "Ravenna / Wedgwood",
    zip: "98115",
    mlsStatusNorm: "SOLD",
    mlsStatusLabel: "Sold",
    listDate: "2026-06-01",
    pendingDate: "2026-06-10",
    saleDate: "2026-07-02",
    closePrice: 1_250_000,
    hasActualClose: true,
    daysToPending: 9,
    ...overrides,
  });
}

function countyClosedRow(overrides = {}) {
  return {
    dataMode: "PUBLIC_PROXY",
    id: "3350001",
    parcelNbr: "4444444444",
    address: "404 S County Ct, Seattle 98108",
    neighborhoodLabel: "Beacon Hill",
    zip: "98108",
    mlsStatusNorm: "",
    mlsStatusLabel: "",
    listDate: "2026-06-15",
    pendingDate: "2026-06-15",
    saleDate: "2026-06-15",
    closePrice: 900_000,
    pendingListPrice: 850_000, // assessed-value fallback, NOT a list price
    listPriceAtPending: 850_000,
    hasMarketListPrice: false,
    hasActualClose: true,
    daysToPending: null,
    ...overrides,
  };
}

test("changeKey uses APN + list date for MLS rows and survives status transitions", async () => {
  const { changeKey } = await loadChangesSince();

  const active = activeRow();
  const key = changeKey(active);
  assert.equal(key, "apn:1111111111|2026-06-20");

  // Same listing later: pipeline rebuilt (new synthetic id suffix), row went
  // pending and then sold. The key must not move.
  const pending = activeRow({
    id: "mls-open-2400001-1111111111-77",
    mlsStatusNorm: "PENDING",
    pendingDate: "2026-07-02",
  });
  const sold = activeRow({
    id: "mls-only-2400001-1111111111-5",
    mlsStatusNorm: "SOLD",
    pendingDate: "2026-07-02",
    saleDate: "2026-07-20",
    closePrice: 1_050_000,
    hasActualClose: true,
  });
  assert.equal(changeKey(pending), key);
  assert.equal(changeKey(sold), key);
});

test("changeKey builds APN from major + minor when parcelNbr is missing", async () => {
  const { changeKey } = await loadChangesSince();
  const row = activeRow({ parcelNbr: "", major: "111111", minor: "1111" });
  assert.equal(changeKey(row), "apn:1111111111|2026-06-20");
});

test("changeKey keeps county records distinct on shared excise ids", async () => {
  const { changeKey } = await loadChangesSince();
  const first = countyClosedRow();
  const second = countyClosedRow({ parcelNbr: "5555555555", address: "Parcel 5555555555" });
  assert.equal(changeKey(first), "id:3350001|4444444444|2026-06-15");
  assert.notEqual(changeKey(first), changeKey(second));
});

test("changeKey falls back to the stable id for parcel-less Redfin actives", async () => {
  const { changeKey } = await loadChangesSince();
  const row = activeRow({ id: "redfin-active-220397186", parcelNbr: "" });
  assert.equal(changeKey(row), "id:redfin-active-220397186||");
});

test("changeKey returns empty string for untrackable input instead of throwing", async () => {
  const { changeKey } = await loadChangesSince();
  assert.equal(changeKey(null), "");
  assert.equal(changeKey(undefined), "");
  assert.equal(changeKey({}), "");
  assert.equal(changeKey({ dataMode: "MLS_ENRICHED" }), "");
});

test("captureBaseline stores a compact, JSON-serializable snapshot", async () => {
  const { captureBaseline, changeKey, CHANGES_BASELINE_VERSION } = await loadChangesSince();
  const rows = [activeRow(), closedRow(), countyClosedRow()];
  const baseline = captureBaseline(rows, BASELINE_AT);

  assert.equal(baseline.v, CHANGES_BASELINE_VERSION);
  assert.ok(!Number.isNaN(new Date(baseline.capturedAt).getTime()));
  assert.equal(Object.keys(baseline.rows).length, 3);

  // Open listing with a real list price: status + price.
  assert.deepEqual(baseline.rows[changeKey(rows[0])], { s: "A", p: 1_000_000 });
  // Closed rows only need membership; no price stored.
  assert.deepEqual(baseline.rows[changeKey(rows[1])], { s: "C" });
  assert.deepEqual(baseline.rows[changeKey(rows[2])], { s: "C" });

  // Round-trips through JSON exactly (what localStorage will do).
  assert.deepEqual(JSON.parse(JSON.stringify(baseline)), baseline);
});

test("captureBaseline never stores an assessed-value fallback as a price", async () => {
  const { captureBaseline, changeKey } = await loadChangesSince();
  const noListPrice = activeRow({ hasMarketListPrice: false });
  const baseline = captureBaseline([noListPrice], BASELINE_AT);
  assert.deepEqual(baseline.rows[changeKey(noListPrice)], { s: "A" });
});

test("captureBaseline skips keyless rows and tolerates junk input", async () => {
  const { captureBaseline } = await loadChangesSince();
  const baseline = captureBaseline([null, undefined, {}, "not a row", activeRow()], BASELINE_AT);
  assert.equal(Object.keys(baseline.rows).length, 1);

  const emptyBaseline = captureBaseline(null, BASELINE_AT);
  assert.deepEqual(emptyBaseline.rows, {});

  // Unparseable timestamp falls back without throwing.
  const fallback = captureBaseline([activeRow()], "not a date");
  assert.ok(!Number.isNaN(new Date(fallback.capturedAt).getTime()));
});

test("captureBaseline resolves key collisions by keeping the most advanced status", async () => {
  const { captureBaseline, changeKey } = await loadChangesSince();
  // Same listing represented twice (for example the MLS record and the
  // county record of one sale collapsing to the same key).
  const asActive = activeRow();
  const asSold = activeRow({
    mlsStatusNorm: "SOLD",
    saleDate: "2026-06-25",
    closePrice: 1_040_000,
    hasActualClose: true,
  });
  assert.equal(changeKey(asActive), changeKey(asSold));

  const baseline = captureBaseline([asActive, asSold], BASELINE_AT);
  assert.deepEqual(baseline.rows[changeKey(asActive)], { s: "C" });
});

test("isValidBaseline accepts a fresh capture and rejects malformed shapes", async () => {
  const { captureBaseline, isValidBaseline } = await loadChangesSince();
  assert.equal(isValidBaseline(captureBaseline([activeRow()], BASELINE_AT)), true);

  const bad = [
    null,
    undefined,
    "garbage",
    42,
    [],
    {},
    { v: 99, capturedAt: BASELINE_AT, rows: {} },
    { v: 1, capturedAt: "not a date", rows: {} },
    { v: 1, capturedAt: BASELINE_AT, rows: [] },
    { v: 1, capturedAt: BASELINE_AT },
  ];
  for (const value of bad) {
    assert.equal(isValidBaseline(value), false, `expected invalid: ${JSON.stringify(value)}`);
  }
});

test("diffSinceBaseline treats missing or malformed baselines as a first visit and never throws", async () => {
  const { diffSinceBaseline } = await loadChangesSince();
  const rows = [activeRow(), closedRow()];
  const malformed = [null, undefined, "garbage", 42, [], { v: 1 }, { v: 1, capturedAt: "nope", rows: {} }];
  for (const baseline of malformed) {
    const diff = diffSinceBaseline(baseline, rows, NOW);
    assert.equal(diff.isFirstVisit, true);
    assert.equal(diff.baselineAt, null);
    assert.equal(diff.minutesSinceBaseline, null);
    assert.deepEqual(diff.newActives, []);
    assert.deepEqual(diff.priceCuts, []);
    assert.deepEqual(diff.wentPendingFast, []);
    assert.deepEqual(diff.newClosed, []);
    assert.deepEqual(diff.totals, { newActives: 0, priceCuts: 0, wentPendingFast: 0, newClosed: 0 });
  }
});

test("diffSinceBaseline reports baseline age and tolerates junk rows and timestamps", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);

  const diff = diffSinceBaseline(baseline, [], NOW);
  assert.equal(diff.isFirstVisit, false);
  assert.equal(new Date(diff.baselineAt).getTime(), new Date(baseline.capturedAt).getTime());
  assert.equal(diff.minutesSinceBaseline, 3 * 24 * 60);

  // Non-array rows and an unparseable "now" must not throw.
  const tolerant = diffSinceBaseline(baseline, null, "not a date");
  assert.equal(tolerant.isFirstVisit, false);
  assert.deepEqual(tolerant.totals, { newActives: 0, priceCuts: 0, wentPendingFast: 0, newClosed: 0 });
});

test("new actives: fresh listings appear, already-seen and stale-backfill listings do not", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const seenActive = activeRow();
  const baseline = captureBaseline([seenActive], BASELINE_AT);

  const freshListing = activeRow({
    id: "redfin-active-900001",
    parcelNbr: "",
    address: "505 N Fresh Ln, Seattle WA 98103",
    neighborhoodLabel: "Green Lake / Wallingford",
    zip: "98103",
    listDate: "2026-07-02",
    pendingListPrice: 1_349_000,
    listPriceAtPending: 1_349_000,
  });
  // Listed 30 days before the baseline but only now entering the dataset:
  // still within the 45-day lag allowance, so it is news.
  const laggedListing = activeRow({
    id: "redfin-active-900002",
    parcelNbr: "",
    address: "606 N Lagged Ln, Seattle WA 98103",
    listDate: "2026-06-01",
  });
  // Listed months before the baseline: backfill, not news.
  const staleListing = activeRow({
    id: "redfin-active-900003",
    parcelNbr: "",
    address: "707 N Stale Ln, Seattle WA 98103",
    listDate: "2026-04-01",
  });

  const diff = diffSinceBaseline(baseline, [seenActive, freshListing, laggedListing, staleListing], NOW);
  assert.deepEqual(diff.newActives.map((r) => r.id), ["redfin-active-900001", "redfin-active-900002"]);
  assert.equal(diff.totals.newActives, 2);

  const entry = diff.newActives[0];
  assert.equal(entry.address, "505 N Fresh Ln, Seattle WA 98103");
  assert.equal(entry.neighborhood, "Green Lake / Wallingford");
  assert.equal(entry.price, 1_349_000);
  assert.equal(entry.status, "Active");
  assert.ok(entry.url.includes("zillow.com"));
  assert.ok(entry.url.includes("505-n-fresh-ln"));
});

test("price cuts: only real list-price drops on listings seen before count", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const cutCandidate = activeRow(); // 1,000,000 at baseline
  const riseCandidate = activeRow({
    id: "mls-open-2400010-6666666666-1",
    parcelNbr: "6666666666",
    address: "808 NE Riser St, Seattle WA 98115",
  });
  const flatCandidate = activeRow({
    id: "mls-open-2400011-7777777777-1",
    parcelNbr: "7777777777",
    address: "909 NE Flat St, Seattle WA 98115",
  });
  const proxyCandidate = activeRow({
    id: "mls-open-2400012-8888888888-1",
    parcelNbr: "8888888888",
    address: "111 NE Proxy St, Seattle WA 98115",
    hasMarketListPrice: false, // assessed fallback only; baseline stores no price
  });
  const baseline = captureBaseline([cutCandidate, riseCandidate, flatCandidate, proxyCandidate], BASELINE_AT);

  const current = [
    activeRow({ pendingListPrice: 950_000, listPriceAtPending: 950_000 }),
    activeRow({ ...riseCandidate, pendingListPrice: 1_100_000, listPriceAtPending: 1_100_000 }),
    activeRow({ ...flatCandidate }),
    activeRow({ ...proxyCandidate, pendingListPrice: 800_000, listPriceAtPending: 800_000 }),
  ];
  const diff = diffSinceBaseline(baseline, current, NOW);

  assert.equal(diff.priceCuts.length, 1);
  const cut = diff.priceCuts[0];
  assert.equal(cut.oldPrice, 1_000_000);
  assert.equal(cut.newPrice, 950_000);
  assert.equal(cut.cutAmount, 50_000);
  assert.equal(cut.cutPct, 0.05);
  assert.equal(cut.address, "101 NW Sample St, Seattle WA 98117");
  assert.equal(cut.neighborhood, "Ballard / Crown Hill");
  assert.ok(cut.url.includes("zillow.com"));
  assert.equal(diff.totals.priceCuts, 1);
});

test("price cuts: a re-listed home that dropped its real list price mid-flight is ignored", async () => {
  const { diffSinceBaseline } = await loadChangesSince();
  // Baseline knew this key only as a closed sale; a later active row reusing
  // the key must not read as a price cut (no trustworthy old price).
  const baseline = {
    v: 1,
    capturedAt: BASELINE_AT,
    rows: { "apn:1111111111|2026-06-20": { s: "C" } },
  };
  const diff = diffSinceBaseline(baseline, [activeRow({ pendingListPrice: 900_000, listPriceAtPending: 900_000 })], NOW);
  assert.deepEqual(diff.priceCuts, []);
  assert.deepEqual(diff.newActives, []);
});

test("went pending fast: contract after the last visit and within 10 days of listing", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);

  const fast = pendingRow(); // listed 06-28, pending 07-03 -> 5 days
  const slow = pendingRow({
    id: "mls-open-2400020-9999999999-1",
    parcelNbr: "9999999999",
    address: "222 SW Slow Ave, Seattle WA 98126",
    listDate: "2026-05-20",
    pendingDate: "2026-07-03",
    daysToPending: 44,
  });
  const beforeVisit = pendingRow({
    id: "mls-open-2400021-1010101010-1",
    parcelNbr: "1010101010",
    address: "333 SW Early Ave, Seattle WA 98126",
    listDate: "2026-06-20",
    pendingDate: "2026-06-24",
    daysToPending: 4,
  });

  const diff = diffSinceBaseline(baseline, [fast, slow, beforeVisit], NOW);
  assert.equal(diff.wentPendingFast.length, 1);
  assert.equal(diff.wentPendingFast[0].address, "202 SW Example Ave, Seattle WA 98126");
  assert.equal(diff.wentPendingFast[0].price, 1_000_000);
  assert.equal(diff.totals.wentPendingFast, 1);
});

test("went pending fast: falls back to list-to-pending day math and honors the boundary", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);

  const fastNoField = pendingRow({ daysToPending: null }); // 06-28 -> 07-03 = 5 days
  const exactlyTen = pendingRow({
    id: "mls-open-2400022-1212121212-1",
    parcelNbr: "1212121212",
    address: "444 SW Boundary Ave, Seattle WA 98126",
    listDate: "2026-06-23",
    pendingDate: "2026-07-03",
    daysToPending: null, // 10 days: still fast
  });
  const eleven = pendingRow({
    id: "mls-open-2400023-1313131313-1",
    parcelNbr: "1313131313",
    address: "555 SW Over Ave, Seattle WA 98126",
    listDate: "2026-06-22",
    pendingDate: "2026-07-03",
    daysToPending: null, // 11 days: not fast
  });
  const noDates = pendingRow({
    id: "mls-open-2400024-1414141414-1",
    parcelNbr: "1414141414",
    address: "666 SW Unknown Ave, Seattle WA 98126",
    listDate: "",
    pendingDate: "2026-07-03",
    daysToPending: null, // cannot prove it was fast
  });

  const diff = diffSinceBaseline(baseline, [fastNoField, exactlyTen, eleven, noDates], NOW);
  // Both went pending on 07-03; the tie breaks on key ascending.
  assert.deepEqual(
    diff.wentPendingFast.map((r) => r.address),
    ["444 SW Boundary Ave, Seattle WA 98126", "202 SW Example Ave, Seattle WA 98126"]
  );
});

test("went pending fast: active rows carrying an artifact pending date stay out", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);
  // Real dataset quirk: hundreds of ACTIVE rows carry pendingDate === listDate
  // as a builder fill. Status, not the date, decides.
  const artifact = activeRow({ listDate: "2026-07-02", pendingDate: "2026-07-02" });
  const diff = diffSinceBaseline(baseline, [artifact], NOW);
  assert.deepEqual(diff.wentPendingFast, []);
  assert.equal(diff.newActives.length, 1);
});

test("new closed: watched listings that sold always count; unseen sales respect the lag window", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const watchedActive = activeRow();
  const oldSale = countyClosedRow({ id: "3350009", parcelNbr: "1515151515", saleDate: "2026-06-25" });
  const baseline = captureBaseline([watchedActive, oldSale], BASELINE_AT);

  const watchedNowSold = activeRow({
    mlsStatusNorm: "SOLD",
    mlsStatusLabel: "Sold",
    // Sale date even older than the lag window: still news because we watched
    // it as an open listing.
    saleDate: "2026-04-15",
    closePrice: 1_075_000,
    hasActualClose: true,
  });
  const unseenRecentSale = countyClosedRow({ id: "3350010", parcelNbr: "1616161616", saleDate: "2026-06-20" });
  const unseenAncientSale = countyClosedRow({ id: "3350011", parcelNbr: "1717171717", saleDate: "2026-03-01" });

  const diff = diffSinceBaseline(baseline, [watchedNowSold, oldSale, unseenRecentSale, unseenAncientSale], NOW);
  assert.equal(diff.newClosed.length, 2);
  const ids = diff.newClosed.map((r) => r.id).sort();
  assert.deepEqual(ids, ["3350010", watchedNowSold.id].sort());
  assert.equal(diff.totals.newClosed, 2);

  const sold = diff.newClosed.find((r) => r.id === watchedNowSold.id);
  assert.equal(sold.price, 1_075_000);
});

test("each row lands in at most one list and duplicates collapse to one entry", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);

  // Same sale represented twice (MLS + county record with the same key), and
  // it also went pending fast before closing: it must appear once, as closed.
  const soldFast = closedRow({ listDate: "2026-06-28", pendingDate: "2026-07-02", daysToPending: 4 });
  const soldFastDupe = closedRow({
    id: "mls-only-2400003-3333333333-42",
    listDate: "2026-06-28",
    pendingDate: "2026-07-02",
    daysToPending: 4,
  });
  const diff = diffSinceBaseline(baseline, [soldFast, soldFastDupe], NOW);

  assert.equal(diff.newClosed.length, 1);
  assert.deepEqual(diff.wentPendingFast, []);
  assert.deepEqual(diff.newActives, []);
});

test("rows that disappeared from the dataset are ignored without errors", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([activeRow(), pendingRow(), closedRow()], BASELINE_AT);
  const diff = diffSinceBaseline(baseline, [], NOW);
  assert.deepEqual(diff.totals, { newActives: 0, priceCuts: 0, wentPendingFast: 0, newClosed: 0 });
});

test("feed lists sort newest first with deterministic ties", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const baseline = captureBaseline([], BASELINE_AT);
  const older = activeRow({
    id: "redfin-active-900010",
    parcelNbr: "",
    address: "1 Old Way",
    listDate: "2026-07-01",
  });
  const newer = activeRow({
    id: "redfin-active-900011",
    parcelNbr: "",
    address: "2 New Way",
    listDate: "2026-07-03",
  });
  const diff = diffSinceBaseline(baseline, [older, newer], NOW);
  assert.deepEqual(diff.newActives.map((r) => r.address), ["2 New Way", "1 Old Way"]);

  // Price cuts sort by biggest cut first.
  const a = activeRow();
  const b = activeRow({
    id: "mls-open-2400030-1818181818-1",
    parcelNbr: "1818181818",
    address: "3 Deep Cut Rd",
    pendingListPrice: 2_000_000,
    listPriceAtPending: 2_000_000,
  });
  const cutBaseline = captureBaseline([a, b], BASELINE_AT);
  const cutDiff = diffSinceBaseline(cutBaseline, [
    activeRow({ pendingListPrice: 990_000, listPriceAtPending: 990_000 }),
    activeRow({ ...b, pendingListPrice: 1_800_000, listPriceAtPending: 1_800_000 }),
  ], NOW);
  assert.deepEqual(cutDiff.priceCuts.map((r) => r.cutAmount), [200_000, 10_000]);
});

test("full loop: capture, JSON round-trip through storage, then diff", async () => {
  const { captureBaseline, diffSinceBaseline } = await loadChangesSince();
  const visitOneRows = [activeRow(), countyClosedRow()];
  const stored = JSON.stringify(captureBaseline(visitOneRows, BASELINE_AT));

  const visitTwoRows = [
    activeRow({ pendingListPrice: 975_000, listPriceAtPending: 975_000 }), // cut
    countyClosedRow(), // unchanged
    pendingRow(), // went pending fast
    activeRow({ id: "redfin-active-900020", parcelNbr: "", address: "9 Brand New St", listDate: "2026-07-02" }),
    closedRow(), // new closed within lag window
  ];
  const diff = diffSinceBaseline(JSON.parse(stored), visitTwoRows, NOW);

  assert.equal(diff.isFirstVisit, false);
  assert.deepEqual(diff.totals, { newActives: 1, priceCuts: 1, wentPendingFast: 1, newClosed: 1 });
  assert.equal(diff.newActives[0].address, "9 Brand New St");
  assert.equal(diff.priceCuts[0].newPrice, 975_000);
  assert.equal(diff.wentPendingFast[0].neighborhood, "West Seattle");
  assert.equal(diff.newClosed[0].price, 1_250_000);
  for (const entry of [...diff.newActives, ...diff.priceCuts, ...diff.wentPendingFast, ...diff.newClosed]) {
    assert.ok(entry.key.length > 0);
    assert.ok(entry.address.length > 0);
    assert.ok(entry.url.includes("zillow.com"));
  }
});
