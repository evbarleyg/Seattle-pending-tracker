"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  collectActiveSnapshots,
  applySnapshotToRow,
  backfillFromSnapshots,
  rejectReason,
  PROVENANCE,
} = require("../scripts/backfill_list_from_active_snapshots.js");

const SNAP_HEADERS = "mlsJoinMethod,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsOriginalPrice,mlsListDate,listDate,mlsDOM,mlsCDOM,zip";

function snapshot(date, lines) {
  return { date, text: [SNAP_HEADERS, ...lines].join("\n") };
}

function soldRow(overrides = {}) {
  return {
    mlsJoinMethod: "REDFIN_SOLD",
    mlsListingNumber: "2400001",
    saleDate: "2026-08-19",
    closePrice: "1070000",
    listPriceAtPending: "",
    mlsListingPrice: "",
    mlsOriginalPrice: "",
    pendingDate: "",
    mlsPendingDate: "",
    mlsContractualDate: "",
    listDate: "",
    mlsListDate: "",
    mlsDOM: "",
    mlsCDOM: "",
    bidUpAmount: "",
    bidUpPct: "",
    saleToListRatio: "",
    saleToOriginalListRatio: "",
    mlsDaysToPending: "",
    mlsDaysPendingToSale: "",
    addressSource: "REDFIN_SOLD",
    ...overrides,
  };
}

test("collectActiveSnapshots keeps the LAST day each MLS# was seen active", () => {
  const lastSeen = collectActiveSnapshots([
    snapshot("2026-07-17", ["REDFIN_ACTIVE,2400001,1150000,,1150000,2026-07-17,2026-07-17,0,0,98177"]),
    snapshot("2026-07-30", ["REDFIN_ACTIVE,2400001,1100000,,1150000,2026-07-17,2026-07-17,13,13,98177"]),
    snapshot("2026-08-01", ["REDFIN_ACTIVE,2400002,899000,,899000,2026-06-26,2026-06-26,36,36,98177"]),
  ]);
  assert.strictEqual(lastSeen.size, 2);
  const rec = lastSeen.get("2400001");
  assert.strictEqual(rec.date, "2026-07-30");
  assert.strictEqual(rec.list, 1100000); // the reduced ask, not the opening one
  assert.strictEqual(rec.originalList, 1150000);
  assert.strictEqual(rec.dom, "13");
  assert.strictEqual(rec.listDate, "2026-07-17");
});

test("collectActiveSnapshots ignores non-active rows and actives without a price", () => {
  const lastSeen = collectActiveSnapshots([
    snapshot("2026-08-01", [
      "REDFIN_SOLD,2400009,,,,,,,,98103",
      "REDFIN_ACTIVE,2400010,,,,,,,,98103",
      "APN_PRICE_DATE_WINDOW,2400011,999000,,999000,2026-05-01,2026-05-01,3,3,98103",
    ]),
  ]);
  assert.strictEqual(lastSeen.size, 0);
});

test("applySnapshotToRow fills list@pending, pending date, DOM and ratios", () => {
  const row = soldRow();
  const rec = { date: "2026-07-30", list: 1100000, originalList: 1150000, listDate: "2026-07-17", dom: "13", cdom: "13", zip: "98177" };
  assert.strictEqual(applySnapshotToRow(row, rec), true);
  assert.strictEqual(row.listPriceAtPending, "1100000");
  assert.strictEqual(row.mlsListingPrice, "1100000");
  assert.strictEqual(row.mlsOriginalPrice, "1150000");
  assert.strictEqual(row.pendingDate, "2026-07-30");
  assert.strictEqual(row.mlsPendingDate, "2026-07-30");
  assert.strictEqual(row.listDate, "2026-07-17");
  assert.strictEqual(row.mlsDOM, "13");
  assert.strictEqual(row.mlsCDOM, "13");
  assert.strictEqual(row.bidUpAmount, "-30000");
  assert.ok(Math.abs(Number(row.saleToListRatio) - 1070000 / 1100000) < 1e-9);
  assert.ok(Math.abs(Number(row.saleToOriginalListRatio) - 1070000 / 1150000) < 1e-9);
  assert.strictEqual(row.mlsDaysToPending, "13");
  assert.strictEqual(row.mlsDaysPendingToSale, "20");
  assert.strictEqual(row.addressSource, PROVENANCE);
  assert.strictEqual(row.mlsJoinMethod, "REDFIN_SOLD", "join method must stay REDFIN_SOLD so merge:sold reruns still strip it");
});

test("a genuine sold-at-asking yields sale/list = 1 (asking price came from the actives feed)", () => {
  const row = soldRow({ closePrice: "899000" });
  const rec = { date: "2026-07-15", list: 899000, originalList: 899000, listDate: "2026-06-26", dom: "19", cdom: "19", zip: "98177" };
  assert.strictEqual(applySnapshotToRow(row, rec), true);
  assert.strictEqual(row.saleToListRatio, "1");
  assert.strictEqual(row.bidUpAmount, "0");
});

test("rejectReason guards: non-sold rows, rows with a list price, missing/implausible/relisted snapshots", () => {
  const rec = { date: "2026-07-30", list: 1100000, originalList: 1100000, listDate: "2026-07-17", dom: "13", cdom: "13", zip: "98177" };
  assert.strictEqual(rejectReason(soldRow({ mlsJoinMethod: "APN_PRICE_DATE_WINDOW" }), rec), "not_redfin_sold");
  assert.strictEqual(rejectReason(soldRow({ listPriceAtPending: "1000000" }), rec), "already_has_list");
  assert.strictEqual(rejectReason(soldRow(), undefined), "no_snapshot");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "0" }), rec), "no_close_price");
  assert.strictEqual(rejectReason(soldRow({ saleDate: "2026-07-01" }), rec), "active_after_sale");
  assert.strictEqual(rejectReason(soldRow({ saleDate: "2027-03-01" }), rec), "stale_snapshot");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "2500000" }), rec), "implausible_ratio");
  assert.strictEqual(rejectReason(soldRow(), rec), null);
  const untouched = soldRow({ saleDate: "2026-07-01" });
  assert.strictEqual(applySnapshotToRow(untouched, rec), false);
  assert.strictEqual(untouched.listPriceAtPending, "");
});

test("backfillFromSnapshots only touches REDFIN_SOLD rows and reports per month", () => {
  const lastSeen = collectActiveSnapshots([
    snapshot("2026-07-30", ["REDFIN_ACTIVE,2400001,1100000,,1100000,2026-07-17,2026-07-17,13,13,98177"]),
    snapshot("2026-08-12", ["REDFIN_ACTIVE,2400003,1125000,,1125000,2026-08-07,2026-08-07,5,5,98177"]),
  ]);
  const county = { mlsJoinMethod: "APN_PRICE_DATE_WINDOW", mlsListingNumber: "2400001", saleDate: "2026-08-19", closePrice: "1070000", listPriceAtPending: "" };
  const rows = [
    soldRow(),
    soldRow({ mlsListingNumber: "2400003", saleDate: "2026-09-03", closePrice: "1125000" }),
    soldRow({ mlsListingNumber: "2400099", saleDate: "2026-09-10", closePrice: "800000" }),
    county,
  ];
  const report = backfillFromSnapshots(rows, lastSeen);
  assert.strictEqual(report.candidates, 3);
  assert.strictEqual(report.applied, 2);
  assert.deepStrictEqual(report.skipped, { no_snapshot: 1 });
  assert.deepStrictEqual(report.byMonth["2026-08"], { candidates: 1, applied: 1 });
  assert.deepStrictEqual(report.byMonth["2026-09"], { candidates: 2, applied: 1 });
  assert.strictEqual(county.listPriceAtPending, "", "county rows are never mutated");
  assert.strictEqual(rows[2].listPriceAtPending, "");
});
