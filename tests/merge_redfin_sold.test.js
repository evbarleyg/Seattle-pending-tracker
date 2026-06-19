"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { mergeSold, buildSoldEnrichedRow, daysBetween } = require("../scripts/merge_redfin_sold.js");

const HEADERS = [
  "dataMode", "id", "address", "neighborhood", "type", "typeCode", "addressSource",
  "listDate", "pendingDate", "saleDate", "listPriceAtPending", "closePrice",
  "beds", "baths", "sqft", "yearBuilt", "zip", "districtName", "sqFtLot", "lat", "lon",
  "mlsClosePrice", "mlsListingNumber", "mlsStatus", "mlsRegion", "mlsSellingDate",
  "mlsListingPrice", "mlsSellingPrice", "mlsDOM", "mlsCDOM", "mlsJoinMethod",
  "saleToListRatio", "saleToOriginalListRatio", "bidUpAmount", "bidUpPct",
];

test("daysBetween measures absolute gap and rejects junk", () => {
  assert.strictEqual(daysBetween("2026-05-10", "2026-05-12"), 2);
  assert.strictEqual(daysBetween("2026-05-12", "2026-05-10"), 2);
  assert.strictEqual(daysBetween("2026-05-10", "2026-05-10"), 0);
  assert.strictEqual(daysBetween("", "2026-05-10"), Infinity);
});

test("buildSoldEnrichedRow maps sold fields and leaves list/ratio blank", () => {
  const r = {
    redfinListingId: "999", mlsListingNumber: "2200001", soldDate: "2026-05-02",
    soldPrice: "800000", domDays: "6", beds: "3", baths: "2", sqft: "1800",
    yearBuilt: "1990", zip: "98107", lotSize: "4000", lat: "47.6", lon: "-122.3",
    uiPropertyType: "1", neighborhood: "Ballard", address: "1 A St", queryLabel: "Ballard",
  };
  const row = buildSoldEnrichedRow(r, HEADERS);
  assert.strictEqual(row.dataMode, "MLS_ENRICHED");
  assert.strictEqual(row.mlsJoinMethod, "REDFIN_SOLD");
  assert.strictEqual(row.mlsStatus, "Sold");
  assert.strictEqual(row.addressSource, "REDFIN_SOLD");
  assert.strictEqual(row.saleDate, "2026-05-02");
  assert.strictEqual(row.mlsSellingDate, "2026-05-02");
  assert.strictEqual(row.closePrice, "800000");
  assert.strictEqual(row.mlsClosePrice, "800000");
  assert.strictEqual(row.type, "Single Family");
  assert.strictEqual(row.mlsDOM, "6");
  // Never fabricate a list price or sale/list ratio from a sold-only feed.
  assert.strictEqual(row.listPriceAtPending, "");
  assert.strictEqual(row.mlsListingPrice, "");
  assert.strictEqual(row.saleToListRatio, "");
  assert.strictEqual(row.bidUpAmount, "");
});

test("mergeSold skips dupes (MLS# + address/date), appends new, strips prior REDFIN_SOLD", () => {
  const existing = [
    { mlsListingNumber: "111", address: "1 A St", zip: "98107", saleDate: "2026-05-01", mlsSellingDate: "2026-05-01", mlsJoinMethod: "MLS_SOLD_NOT_IN_COUNTY" },
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", saleDate: "2026-05-10", mlsSellingDate: "", mlsJoinMethod: "" }, // county sold, no MLS#
    { mlsListingNumber: "", address: "Old Sold", zip: "98103", saleDate: "2026-04-01", mlsSellingDate: "", mlsJoinMethod: "REDFIN_SOLD" }, // prior run -> stripped
  ];
  const redfin = [
    { mlsListingNumber: "111", address: "1 A St", zip: "98107", soldDate: "2026-05-02", soldPrice: "800000", uiPropertyType: "1", queryLabel: "Ballard" }, // dup by MLS#
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2026-05-12", soldPrice: "900000", uiPropertyType: "1", queryLabel: "Wedgwood" }, // dup by addr (+2d)
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2025-01-01", soldPrice: "700000", uiPropertyType: "1", queryLabel: "Wedgwood" }, // same addr, prior-year sale -> appended
    { mlsListingNumber: "222", address: "9 Z Pl", zip: "98117", soldDate: "2026-06-01", soldPrice: "1200000", uiPropertyType: "3", queryLabel: "Ballard" }, // brand new -> appended
  ];
  const { finalRows, report } = mergeSold(HEADERS, existing, redfin, {});

  assert.strictEqual(report.priorRedfinSoldRemoved, 1);
  assert.strictEqual(report.matchedByMls, 1);
  assert.strictEqual(report.matchedByAddr, 1);
  assert.strictEqual(report.appended, 2);
  assert.strictEqual(report.rowsAfter, 4); // 2 curated kept + 2 appended
  assert.deepStrictEqual(report.appendedByMonth, { "2025-01": 1, "2026-06": 1 });
  // curated rows are preserved untouched; appended rows are all REDFIN_SOLD
  const appended = finalRows.filter((r) => r.mlsJoinMethod === "REDFIN_SOLD");
  assert.strictEqual(appended.length, 2);
  assert.ok(finalRows.some((r) => r.mlsListingNumber === "111" && r.mlsJoinMethod === "MLS_SOLD_NOT_IN_COUNTY"));
});

test("mergeSold honors a minSoldDate window (drops older sales)", () => {
  const redfin = [
    { mlsListingNumber: "", address: "A St", zip: "98107", soldDate: "2025-03-01", soldPrice: "500000", uiPropertyType: "1", queryLabel: "X" },
    { mlsListingNumber: "", address: "B St", zip: "98107", soldDate: "2024-08-01", soldPrice: "500000", uiPropertyType: "1", queryLabel: "X" }, // older -> dropped
  ];
  const { report } = mergeSold(HEADERS, [], redfin, { minSoldDate: "2024-12-18" });
  assert.strictEqual(report.appended, 1);
  assert.strictEqual(report.droppedOutOfWindow, 1);
});

test("mergeSold respects a custom match-day window", () => {
  const existing = [{ mlsListingNumber: "", address: "2 B Ave", zip: "98115", saleDate: "2026-05-10", mlsJoinMethod: "" }];
  const redfin = [{ mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2026-05-20", soldPrice: "900000", uiPropertyType: "1", queryLabel: "X" }]; // 10 days apart
  assert.strictEqual(mergeSold(HEADERS, existing, redfin, { matchDays: 14 }).report.appended, 0); // within window -> dup
  assert.strictEqual(mergeSold(HEADERS, existing, redfin, { matchDays: 5 }).report.appended, 1); // outside window -> appended
});
