"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  Ledger,
  ledgerEntryFromRow,
  seedFromSnapshotText,
  seedFromCache,
  applyLedger,
  applyEntryToRow,
  rejectReason,
  sourceFromJoinMethod,
} = require("../scripts/sold_enrichment_ledger.js");

function entry(overrides = {}) {
  return {
    mlsNumber: "2400001", address: "10038 13th Ave NW", zip: "98177", saleDate: "2026-04-19", closePrice: "1070000",
    listPriceAtPending: "1100000", originalListPrice: "1100000", listDate: "2026-03-20", pendingDate: "2026-03-30",
    dom: "10", cdom: "10", source: "REDFIN_HISTORY", sourceDate: "2026-06-19",
    ...overrides,
  };
}

function soldRow(overrides = {}) {
  return {
    mlsJoinMethod: "REDFIN_SOLD", mlsListingNumber: "2400001", address: "10038 13th Ave NW", zip: "98177",
    saleDate: "2026-04-19", closePrice: "1070000", listPriceAtPending: "", mlsListingPrice: "", mlsListPriceAtPending: "",
    mlsOriginalPrice: "", listDate: "", mlsListDate: "", pendingDate: "", mlsPendingDate: "", mlsContractualDate: "",
    mlsDOM: "", mlsCDOM: "", bidUpAmount: "", bidUpPct: "", saleToListRatio: "", saleToOriginalListRatio: "",
    mlsDaysToPending: "", mlsDaysPendingToSale: "", listPriceSource: "", addressSource: "REDFIN_SOLD",
    ...overrides,
  };
}

test("sourceFromJoinMethod maps join methods to lineage labels and ignores bare feed rows", () => {
  assert.strictEqual(sourceFromJoinMethod("REDFIN_HISTORY", ""), "REDFIN_HISTORY");
  assert.strictEqual(sourceFromJoinMethod("REDFIN_HISTORY_SUSPECT", ""), "REDFIN_HISTORY");
  assert.strictEqual(sourceFromJoinMethod("APN_PRICE_DATE_WINDOW", ""), "MLS_EXPORT");
  assert.strictEqual(sourceFromJoinMethod("REDFIN_SOLD", ""), "");
  assert.strictEqual(sourceFromJoinMethod("REDFIN_ACTIVE", ""), "");
  assert.strictEqual(sourceFromJoinMethod("REDFIN_SOLD", "ACTIVE_SNAPSHOT"), "ACTIVE_SNAPSHOT", "an explicit lineage column wins");
});

test("ledgerEntryFromRow harvests only closed rows with a genuine MLS list price", () => {
  const row = { mlsJoinMethod: "REDFIN_HISTORY", mlsListingNumber: "2400001", address: "10038 13th Ave NW", zip: "98177-1234", saleDate: "2026-04-19", closePrice: "1070000", mlsListingPrice: "1100000", mlsOriginalPrice: "1150000", mlsListDate: "2026-03-20", mlsPendingDate: "2026-03-30", mlsDOM: "10", mlsCDOM: "10" };
  const e = ledgerEntryFromRow(row, "2026-06-19");
  assert.strictEqual(e.zip, "98177");
  assert.strictEqual(e.listPriceAtPending, "1100000");
  assert.strictEqual(e.originalListPrice, "1150000");
  assert.strictEqual(e.pendingDate, "2026-03-30");
  assert.strictEqual(e.source, "REDFIN_HISTORY");
  assert.strictEqual(ledgerEntryFromRow({ ...row, closePrice: "0" }, "x"), null);
  assert.strictEqual(ledgerEntryFromRow({ ...row, mlsListingPrice: "", listPriceAtPending: "" }, "x"), null);
  assert.strictEqual(ledgerEntryFromRow({ ...row, mlsJoinMethod: "REDFIN_SOLD", listPriceSource: "" }, "x"), null, "a bare feed row never contributes");
  assert.strictEqual(ledgerEntryFromRow({ ...row, mlsListingPrice: "", mlsListPriceAtPending: "", listPriceAtPending: "950000" }, "x"), null,
    "the generic listPriceAtPending column alone is not evidence of an asking price (county rows put the assessed value there)");
  assert.strictEqual(ledgerEntryFromRow({ ...row, dataMode: "PUBLIC_PROXY" }, "x"), null, "county proxy rows never contribute");
});

test("replaying the same snapshot does not churn an entry (values equal -> incumbent kept)", () => {
  const ledger = new Ledger();
  assert.strictEqual(ledger.upsert(entry({ sourceDate: "2026-06-19" })), true);
  assert.strictEqual(ledger.upsert(entry({ sourceDate: "2026-09-19" })), false);
  assert.strictEqual(ledger.rows()[0].sourceDate, "2026-06-19");
  assert.strictEqual(ledger.upsert(entry({ sourceDate: "2026-09-19", listPriceAtPending: "1090000" })), true, "a changed value with equal richness and a later date does replace");
});

test("the richest version of a sale wins in the ledger, and it is idempotent", () => {
  const ledger = new Ledger();
  assert.strictEqual(ledger.upsert(entry({ pendingDate: "", listDate: "", dom: "", cdom: "", sourceDate: "2026-06-01" })), true);
  assert.strictEqual(ledger.upsert(entry({ sourceDate: "2026-05-10" })), true, "a version with pending + list dates beats a bare one even if older");
  assert.strictEqual(ledger.upsert(entry({ pendingDate: "", sourceDate: "2026-09-19" })), false, "a poorer later version does not replace it");
  assert.strictEqual(ledger.entries.size, 1);
  const rows = ledger.rows();
  assert.strictEqual(rows[0].pendingDate, "2026-03-30");
  const again = new Ledger(rows);
  assert.strictEqual(again.entries.size, 1);
});

test("find: exact MLS# + sale date, then same MLS# within 14 days, then address within 14 days", () => {
  const ledger = new Ledger([entry()]);
  assert.ok(ledger.find(soldRow()));
  assert.ok(ledger.find(soldRow({ saleDate: "2026-04-25" })), "county close date drifts a few days");
  assert.strictEqual(ledger.find(soldRow({ saleDate: "2026-06-01" })), null, "too far apart");
  assert.ok(ledger.find(soldRow({ mlsListingNumber: "", saleDate: "2026-04-21" })), "address fallback");
  assert.strictEqual(ledger.find(soldRow({ mlsListingNumber: "9999999", address: "1 Other St", zip: "98103" })), null);
});

test("applyEntryToRow fills list, dates, DOM, ratios and lineage but keeps mlsJoinMethod = REDFIN_SOLD", () => {
  const row = soldRow();
  assert.strictEqual(applyEntryToRow(row, entry()), true);
  assert.strictEqual(row.listPriceAtPending, "1100000");
  assert.strictEqual(row.mlsListingPrice, "1100000");
  assert.strictEqual(row.pendingDate, "2026-03-30");
  assert.strictEqual(row.listDate, "2026-03-20");
  assert.strictEqual(row.mlsDOM, "10");
  assert.strictEqual(row.bidUpAmount, "-30000");
  assert.strictEqual(row.mlsDaysToPending, "10");
  assert.strictEqual(row.mlsDaysPendingToSale, "20");
  assert.strictEqual(row.listPriceSource, "REDFIN_HISTORY");
  assert.strictEqual(row.mlsJoinMethod, "REDFIN_SOLD");
  assert.strictEqual(row.addressSource, "REDFIN_SOLD");
});

test("rejectReason guards: wrong join method, existing list, close mismatch, pending after sale", () => {
  const e = entry();
  assert.strictEqual(rejectReason(soldRow({ mlsJoinMethod: "APN_PRICE_DATE_WINDOW" }), e), "not_redfin_sold");
  assert.strictEqual(rejectReason(soldRow({ listPriceAtPending: "1" }), e), "already_has_list");
  assert.strictEqual(rejectReason(soldRow(), null), "no_entry");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "0" }), e), "no_close_price");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "1200000" }), e), "close_mismatch", "a different close means a different sale");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "1075000" }), e), null, "within 1% is the same sale");
  assert.strictEqual(rejectReason(soldRow({ saleDate: "2026-03-25" }), entry({ pendingDate: "2026-03-30" })), "pending_after_sale");
});

test("seedFromSnapshotText harvests enriched rows from a CSV snapshot", () => {
  const text = [
    "mlsJoinMethod,mlsListingNumber,address,zip,saleDate,closePrice,mlsListingPrice,mlsOriginalPrice,mlsListDate,mlsPendingDate,mlsDOM,mlsCDOM,listPriceAtPending",
    "REDFIN_HISTORY,2400001,10038 13th Ave NW,98177,2026-04-19,1070000,1100000,1100000,2026-03-20,2026-03-30,10,10,1100000",
    "REDFIN_SOLD,2400002,1 Bare St,98177,2026-04-20,900000,,,,,,,",
    "APN_PRICE_DATE_WINDOW,2400003,2 County St,98103,2026-04-21,800000,810000,810000,2026-03-01,2026-03-10,9,9,810000",
  ].join("\n");
  const ledger = new Ledger();
  assert.strictEqual(seedFromSnapshotText(ledger, text, "2026-06-19"), 2);
  assert.strictEqual(ledger.find(soldRow()).source, "REDFIN_HISTORY");
  assert.strictEqual(ledger.find(soldRow({ mlsListingNumber: "2400003", saleDate: "2026-04-21", closePrice: "800000" })).source, "MLS_EXPORT");
});

test("seedFromCache joins cached summaries to sales by URL and rejects date/price mismatches", () => {
  const cache = {
    "https://r/a": { fetchedAt: "2026-06-19T10:00:00Z", summary: { soldDate: "2026-04-19", soldPrice: 1070000, listPriceAtPending: 1100000, listDate: "2026-03-20", pendingDate: "2026-03-30", mlsNumber: "2400001" } },
    "https://r/b": { fetchedAt: "2026-06-19T10:00:00Z", summary: { soldDate: "2025-11-01", soldPrice: 700000, listPriceAtPending: 720000 } }, // prior-cycle sale: date mismatch
    "https://r/c": { fetchedAt: "2026-06-19T10:00:00Z", summary: null }, // parse-empty
    "https://r/d": { fetchedAt: "2026-06-19T10:00:00Z", summary: { soldDate: "2026-04-22", soldPrice: 900000, listPriceAtPending: 950000 } }, // no sold row with this URL
  };
  const sold = [
    { redfinUrl: "https://r/a", mlsListingNumber: "2400001", address: "10038 13th Ave NW", zip: "98177", soldDate: "2026-04-19", soldPrice: "1070000" },
    { redfinUrl: "https://r/b", mlsListingNumber: "2400002", address: "1 Relist St", zip: "98177", soldDate: "2026-08-01", soldPrice: "700000" },
  ];
  const ledger = new Ledger();
  const r = seedFromCache(ledger, cache, sold);
  assert.strictEqual(r.contributed, 1);
  assert.strictEqual(r.skipped, 2);
  const e = ledger.find(soldRow());
  assert.strictEqual(e.listPriceAtPending, "1100000");
  assert.strictEqual(e.dom, "10");
  assert.strictEqual(e.sourceDate, "2026-06-19");
});

test("applyLedger fills only bare REDFIN_SOLD rows and reports by month and source", () => {
  const ledger = new Ledger([entry(), entry({ mlsNumber: "2400003", address: "2 County St", zip: "98103", saleDate: "2026-05-02", closePrice: "800000", listPriceAtPending: "810000", source: "MLS_EXPORT" })]);
  const rows = [
    soldRow(),
    soldRow({ mlsListingNumber: "2400003", address: "2 County St", zip: "98103", saleDate: "2026-05-02", closePrice: "800000" }),
    soldRow({ mlsListingNumber: "2400099", address: "9 Unknown St", saleDate: "2026-05-03", closePrice: "600000" }),
    { mlsJoinMethod: "APN_PRICE_DATE_WINDOW", mlsListingNumber: "2400001", saleDate: "2026-04-19", closePrice: "1070000", listPriceAtPending: "" },
  ];
  const report = applyLedger(rows, ledger);
  assert.strictEqual(report.candidates, 3);
  assert.strictEqual(report.applied, 2);
  assert.deepStrictEqual(report.skipped, { no_entry: 1 });
  assert.deepStrictEqual(report.bySource, { REDFIN_HISTORY: 1, MLS_EXPORT: 1 });
  assert.deepStrictEqual(report.byMonth["2026-05"], { candidates: 2, applied: 1 });
  assert.strictEqual(rows[3].listPriceAtPending, "", "county rows are never touched");
});
