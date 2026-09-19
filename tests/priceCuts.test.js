"use strict";

// Price cuts on homes for sale right now, read from the published listing
// ledger (public/listing_ledger.csv): first asking price seen versus the latest.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function mod() {
  return import(pathToFileURL(path.resolve(__dirname, "..", "src/domain/priceCuts.mjs")).href);
}

const HEADER = "mlsNumber,address,zip,propertyType,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,listDate,lastDom,lastStatus";
const CSV = [
  HEADER,
  "100,1 Cut St,98103,Single Family,2026-07-01,2026-09-19,2026-09-19,1300000,1200000,2026-06-28,83,Active",
  "200,2 Flat Ave,98103,Single Family,2026-09-10,2026-09-19,2026-09-19,1250000,1250000,2026-09-09,10,Active",
  '300,"3 Raised Rd, Unit B",98117,Single Family,2026-08-01,2026-09-19,2026-09-19,1150000,1175000,2026-07-30,51,Active',
  "400,4 Big Cut Ln,98107,Single Family,2026-06-08,2026-09-19,2026-09-19,1600000,1400000,2026-05-20,122,Active",
  "500,5 Blank Way,98107,Single Family,2026-09-01,2026-09-19,2026-09-19,,1100000,2026-08-30,20,Active",
  "",
].join("\n");

const listing = (mls, extra = {}) => ({ mlsListingNumber: String(mls), address: `${mls} Test`, neighborhoodLabel: "Ballard", pendingListPrice: 1200000, ...extra });

test("parseLedgerCsv reads the header, quoted fields and numbers, and skips blank lines", async () => {
  const { parseLedgerCsv } = await mod();
  const rows = parseLedgerCsv(CSV);
  assert.equal(rows.length, 5);
  assert.equal(rows[0].mlsNumber, "100");
  assert.equal(rows[0].firstAsk, 1300000);
  assert.equal(rows[0].lastAsk, 1200000);
  assert.equal(rows[0].lastDom, 83);
  assert.equal(rows[2].address, "3 Raised Rd, Unit B", "a comma inside quotes stays in the field");
  assert.equal(rows[4].firstAsk, null, "a blank ask is unknown, not zero");
  assert.deepEqual(parseLedgerCsv(""), []);
  assert.deepEqual(parseLedgerCsv(HEADER), []);
  assert.deepEqual(parseLedgerCsv("no,ledger,columns\n1,2,3"), [], "a file without the ledger's key column yields nothing");
});

test("priceCutFor reports a cut only when the latest ask is below the first", async () => {
  const { parseLedgerCsv, buildLedgerIndex, priceCutFor } = await mod();
  const index = buildLedgerIndex(parseLedgerCsv(CSV));

  const cut = priceCutFor(listing(100), index);
  assert.equal(cut.firstAsk, 1300000);
  assert.equal(cut.lastAsk, 1200000);
  assert.equal(cut.cutAmount, 100000);
  assert.equal(cut.cutPct.toFixed(4), (100000 / 1300000).toFixed(4));
  assert.equal(cut.daysListed, 83);

  assert.equal(priceCutFor(listing(200), index), null, "same ask is not a cut");
  assert.equal(priceCutFor(listing(300), index), null, "a raise is not a cut");
  assert.equal(priceCutFor(listing(500), index), null, "no first ask on record means we cannot say");
  assert.equal(priceCutFor(listing(999), index), null, "not in the ledger");
  assert.equal(priceCutFor({ address: "no mls number" }, index), null);
  assert.equal(priceCutFor(listing(100), null), null, "ledger not loaded");
});

test("summarizePriceCuts counts only listings the ledger tracks and ranks the biggest cuts", async () => {
  const { parseLedgerCsv, buildLedgerIndex, summarizePriceCuts } = await mod();
  const index = buildLedgerIndex(parseLedgerCsv(CSV));
  const listings = [listing(100), listing(200), listing(300), listing(400), listing(999)];
  const s = summarizePriceCuts(listings, index);

  assert.equal(s.trackedCount, 4, "999 is not in the ledger, so it is not in the denominator");
  assert.equal(s.cutCount, 2);
  assert.equal(s.cutShare, 0.5);
  assert.equal(s.medianCutAmount, 150000);
  assert.equal(s.medianCutPct.toFixed(4), ((100000 / 1300000 + 200000 / 1600000) / 2).toFixed(4));
  assert.equal(s.medianDaysListedCut, 102.5);
  assert.equal(s.medianDaysListedUncut, 30.5, "the raised and the flat listing: 51 and 10 days");
  assert.deepEqual(s.biggest.map((entry) => entry.row.mlsListingNumber), ["400", "100"], "largest percent cut first");
  assert.equal(s.biggest[0].cut.cutAmount, 200000);
});

test("summarizePriceCuts is null-safe when nothing is tracked", async () => {
  const { summarizePriceCuts, buildLedgerIndex } = await mod();
  const empty = summarizePriceCuts([listing(1)], buildLedgerIndex([]));
  assert.equal(empty.trackedCount, 0);
  assert.equal(empty.cutShare, null, "no tracked listings means unknown, not 0%");
  assert.equal(empty.medianCutPct, null);
  assert.deepEqual(empty.biggest, []);
  assert.equal(summarizePriceCuts(null, null).trackedCount, 0);
});

test("days to first cut counts only listings tracked from the day they listed", async () => {
  const { parseLedgerCsv, buildLedgerIndex, priceCutFor, summarizePriceCuts } = await mod();
  const header = `${HEADER},lastAskChangeDate,firstAskChangeDate,askChangeCount`;
  const csv = [
    header,
    // Seen the day after listing, first cut 24 days in, cut twice.
    "10,10 Fresh St,98103,Single Family,2026-07-02,2026-09-19,2026-09-19,1300000,1200000,2026-07-01,80,Active,2026-08-20,2026-07-25,2",
    // On the market since 2025, first SEEN when tracking began: its wait is not knowable.
    "20,20 Old Rd,98103,Single Family,2026-06-08,2026-09-19,2026-09-19,1500000,1400000,2025-07-11,430,Active,2026-07-13,2026-07-13,1",
    // Tracked from listing, first cut 10 days in, once.
    "30,30 Quick Ave,98107,Single Family,2026-08-01,2026-09-19,2026-09-19,1250000,1200000,2026-08-01,49,Active,2026-08-11,2026-08-11,1",
    // No cut at all.
    "40,40 Firm Ln,98107,Single Family,2026-09-10,2026-09-19,2026-09-19,1200000,1200000,2026-09-09,10,Active,,,0",
  ].join("\n");
  const index = buildLedgerIndex(parseLedgerCsv(csv));

  const fresh = priceCutFor(listing(10), index);
  assert.equal(fresh.daysToFirstCut, 24);
  assert.equal(fresh.changeCount, 2);
  assert.equal(priceCutFor(listing(20), index).daysToFirstCut, null, "first seen 11 months after listing: an earlier cut could have been missed");
  assert.equal(priceCutFor(listing(30), index).daysToFirstCut, 10);

  const s = summarizePriceCuts([listing(10), listing(20), listing(30), listing(40)], index);
  assert.equal(s.cutCount, 3);
  assert.equal(s.firstCutSampleCount, 2, "the old listing is a cut but is not in the wait sample");
  assert.equal(s.medianDaysToFirstCut, 17);
  assert.equal(s.multiCutCount, 1);
});

test("a ledger without the ask-change columns still reports cuts, with no wait", async () => {
  const { parseLedgerCsv, buildLedgerIndex, priceCutFor, summarizePriceCuts } = await mod();
  const index = buildLedgerIndex(parseLedgerCsv(CSV));
  const cut = priceCutFor(listing(100), index);
  assert.equal(cut.cutAmount, 100000);
  assert.equal(cut.daysToFirstCut, null);
  assert.equal(cut.changeCount, null);
  const s = summarizePriceCuts([listing(100), listing(400)], index);
  assert.equal(s.medianDaysToFirstCut, null);
  assert.equal(s.firstCutSampleCount, 0);
  assert.equal(s.multiCutCount, 0);
});

test("buildPriceCutVerdict says it in plain words and scales the wording to the share", async () => {
  const { buildPriceCutVerdict } = await mod();
  const base = { trackedCount: 216, cutCount: 71, cutShare: 71 / 216, medianCutPct: 0.043, medianCutAmount: 55000, medianDaysListedCut: 61, medianDaysListedUncut: 9 };
  const third = buildPriceCutVerdict(base);
  assert.match(third, /About a third of the homes for sale in your filters have already cut their price/);
  assert.match(third, /4\.3%/);
  assert.match(third, /\$55K/);

  assert.match(buildPriceCutVerdict({ ...base, cutCount: 120, cutShare: 120 / 216 }), /More than half/);
  assert.match(buildPriceCutVerdict({ ...base, cutCount: 12, cutShare: 12 / 216 }), /Few sellers are cutting/);
  assert.match(buildPriceCutVerdict({ ...base, cutCount: 0, cutShare: 0, medianCutPct: null, medianCutAmount: null }), /None of the 216 homes for sale in your filters has cut its price/);
  assert.equal(buildPriceCutVerdict({ trackedCount: 0, cutCount: 0, cutShare: null }), "");
});
