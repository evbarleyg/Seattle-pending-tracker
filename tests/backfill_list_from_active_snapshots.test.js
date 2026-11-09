"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  collectActiveSnapshots,
  loadLedgerMap,
  applySnapshotToRow,
  backfillFromSnapshots,
  rejectReason,
  LIST_PRICE_SOURCE,
  LIST_PRICE_SOURCE_COLUMN,
  MAX_RATIO_DEVIATION,
} = require("../scripts/backfill_list_from_active_snapshots.js");

test("validateOverlap compares history-scraped rows with the ledger and reports match rates", () => {
  const { validateOverlap } = require("../scripts/backfill_list_from_active_snapshots.js");
  const lastSeen = new Map([
    ["1", { date: "2026-07-30", list: 1100000, originalList: 1100000, listDate: "2026-07-17", dom: "13", cdom: "13", zip: "98177" }],
    ["2", { date: "2026-08-10", list: 900000, originalList: 900000, listDate: "2026-08-01", dom: "9", cdom: "9", zip: "98103" }],
    ["3", { date: "2026-08-01", list: 700000, originalList: 700000, listDate: "2026-07-20", dom: "12", cdom: "12", zip: "98103" }],
  ]);
  const rows = [
    { mlsJoinMethod: "REDFIN_HISTORY", mlsListingNumber: "1", address: "A", mlsListingPrice: "1100000", mlsPendingDate: "2026-07-31", mlsDOM: "14" }, // exact ask, pending one day after last listed, DOM within 2
    { mlsJoinMethod: "REDFIN_SOLD", listPriceSource: "REDFIN_HISTORY", mlsListingNumber: "2", address: "B", mlsListingPrice: "905000", mlsPendingDate: "2026-08-10", mlsDOM: "9" }, // within 1%, same day
    { mlsJoinMethod: "REDFIN_HISTORY", mlsListingNumber: "3", address: "C", mlsListingPrice: "650000", mlsPendingDate: "2026-08-09", mlsDOM: "30" }, // differs: price off 7.7%, pending 8 days later, DOM off
    { mlsJoinMethod: "REDFIN_HISTORY", mlsListingNumber: "1", address: "old", mlsListingPrice: "1100000", mlsPendingDate: "2026-05-01", mlsDOM: "1" }, // before `since`: ignored
    { mlsJoinMethod: "REDFIN_SOLD", listPriceSource: "ACTIVE_SNAPSHOT", mlsListingNumber: "1", address: "snap", mlsListingPrice: "1100000", mlsPendingDate: "2026-07-30" }, // not a history row: ignored
  ];
  const v = validateOverlap(rows, lastSeen, { since: "2026-06-08" });
  assert.strictEqual(v.compared, 3);
  assert.deepStrictEqual(v.price, { exact: 1, within1pct: 1, differs: 1 });
  assert.strictEqual(v.pendingDate.same, 1);
  assert.strictEqual(v.pendingDate.ledgerEarlier1, 1);
  assert.strictEqual(v.pendingDate.ledgerEarlier4plus, 1);
  assert.deepStrictEqual(v.dom, { same: 1, within2: 1, differs: 1, missing: 0 });
  assert.strictEqual(v.rates.priceExactOrWithin1pct, 66.7);
  assert.strictEqual(v.rates.pendingSameOrOneDayEarly, 66.7);
  assert.strictEqual(v.samples.length, 1);
  assert.strictEqual(v.samples[0].address, "C");
});

test("loadLedgerMap uses the last GENUINELY active day and ask, and firstAsk as the original list", () => {
  const text = [
    "mlsNumber,redfinPropertyId,redfinListingId,url,address,zip,propertyType,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,lastActiveAsk,listDate,lastDom,lastCdom,lastStatus",
    "2400001,288413,223584238,https://r/x,10037 15th Ave NW,98177,Single Family,2026-07-17,2026-08-06,2026-08-01,1150000,1100000,1100000,2026-07-17,20,20,Active Under Contract",
    "2400002,,,,1 No Ask St,98103,Single Family,2026-07-17,2026-07-20,2026-07-20,,,,2026-07-17,3,3,Active",
    ",,,,No MLS St,98103,Single Family,2026-07-17,2026-07-20,2026-07-20,900000,900000,900000,,,,Active",
  ].join("\n");
  const map = loadLedgerMap(text);
  assert.strictEqual(map.size, 1, "rows without an MLS# or an ask are skipped");
  const rec = map.get("2400001");
  assert.strictEqual(rec.date, "2026-08-01", "pending = last genuinely active day, not the under-contract tail");
  assert.strictEqual(rec.list, 1100000);
  assert.strictEqual(rec.originalList, 1150000);
  assert.strictEqual(rec.listDate, "2026-07-17");
  assert.strictEqual(rec.dom, "20");
  assert.strictEqual(rec.zip, "98177");
});

const SNAP_HEADERS = "mlsJoinMethod,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsOriginalPrice,mlsListDate,listDate,mlsDOM,mlsCDOM,zip";

function snapshot(date, lines) {
  return { date, text: [SNAP_HEADERS, ...lines].join("\n") };
}

function soldRow(overrides = {}) {
  return {
    mlsJoinMethod: "REDFIN_SOLD",
    mlsListingNumber: "2400001",
    address: "10038 13th Ave NW",
    zip: "98177",
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
    listPriceSource: "",
    ...overrides,
  };
}

const REC = { date: "2026-07-30", list: 1100000, originalList: 1150000, listDate: "2026-07-17", dom: "13", cdom: "13", zip: "98177" };

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

test("applySnapshotToRow fills list@pending, pending date, DOM and ratios, and tags lineage in its own column", () => {
  const row = soldRow();
  assert.strictEqual(applySnapshotToRow(row, REC), true);
  assert.strictEqual(row.listPriceAtPending, "1100000");
  assert.strictEqual(row.mlsListingPrice, "1100000");
  assert.strictEqual(row.mlsListPriceAtPending, "1100000");
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
  assert.strictEqual(LIST_PRICE_SOURCE_COLUMN, "listPriceSource");
  assert.strictEqual(row.listPriceSource, LIST_PRICE_SOURCE);
  assert.strictEqual(row.listPriceSource, "ACTIVE_SNAPSHOT");
  assert.strictEqual(row.addressSource, "REDFIN_SOLD", "addressSource describes the address and must not be overloaded");
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
  assert.strictEqual(rejectReason(soldRow({ mlsJoinMethod: "APN_PRICE_DATE_WINDOW" }), REC), "not_redfin_sold");
  assert.strictEqual(rejectReason(soldRow({ listPriceAtPending: "1000000" }), REC), "already_has_list");
  assert.strictEqual(rejectReason(soldRow(), undefined), "no_snapshot");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "0" }), REC), "no_close_price");
  assert.strictEqual(rejectReason(soldRow({ saleDate: "2026-07-01" }), REC), "active_after_sale");
  assert.strictEqual(rejectReason(soldRow({ saleDate: "2027-03-01" }), REC), "stale_snapshot");
  assert.strictEqual(rejectReason(soldRow({ closePrice: "2500000" }), REC), "implausible_ratio");
  assert.strictEqual(rejectReason(soldRow(), REC), null);
  const untouched = soldRow({ saleDate: "2026-07-01" });
  assert.strictEqual(applySnapshotToRow(untouched, REC), false);
  assert.strictEqual(untouched.listPriceAtPending, "");
  assert.strictEqual(untouched.listPriceSource, "");
});

test("the ratio guard is 35%: 1.36x the ask is rejected, 1.27x is accepted", () => {
  assert.strictEqual(MAX_RATIO_DEVIATION, 0.35);
  assert.strictEqual(rejectReason(soldRow({ closePrice: "1500000" }), REC), "implausible_ratio"); // 1.364
  assert.strictEqual(rejectReason(soldRow({ closePrice: "1400000" }), REC), null); // 1.273
  assert.strictEqual(rejectReason(soldRow({ closePrice: "700000" }), REC), "implausible_ratio"); // 0.636
  assert.strictEqual(rejectReason(soldRow({ closePrice: "720000" }), REC), null); // 0.655
});

test("backfillFromSnapshots only touches REDFIN_SOLD rows, reports per month, and logs suspicious joins", () => {
  const lastSeen = collectActiveSnapshots([
    snapshot("2026-07-30", ["REDFIN_ACTIVE,2400001,1100000,,1100000,2026-07-17,2026-07-17,13,13,98177"]),
    snapshot("2026-08-12", ["REDFIN_ACTIVE,2400003,1125000,,1125000,2026-08-07,2026-08-07,5,5,98177"]),
    snapshot("2026-08-20", ["REDFIN_ACTIVE,2400004,600000,,600000,2026-08-01,2026-08-01,19,19,98103"]),
  ]);
  const county = { mlsJoinMethod: "APN_PRICE_DATE_WINDOW", mlsListingNumber: "2400001", saleDate: "2026-08-19", closePrice: "1070000", listPriceAtPending: "" };
  const collision = soldRow({ mlsListingNumber: "2400004", address: "1 Collision Ct", zip: "98103", saleDate: "2026-09-10", closePrice: "1300000" });
  const rows = [
    soldRow(),
    soldRow({ mlsListingNumber: "2400003", saleDate: "2026-09-03", closePrice: "1125000" }),
    soldRow({ mlsListingNumber: "2400099", saleDate: "2026-09-10", closePrice: "800000" }),
    collision,
    county,
  ];
  const report = backfillFromSnapshots(rows, lastSeen);
  assert.strictEqual(report.candidates, 4);
  assert.strictEqual(report.applied, 2);
  assert.deepStrictEqual(report.skipped, { no_snapshot: 1, implausible_ratio: 1 });
  assert.deepStrictEqual(report.byMonth["2026-08"], { candidates: 1, applied: 1 });
  assert.deepStrictEqual(report.byMonth["2026-09"], { candidates: 3, applied: 1 });
  assert.strictEqual(county.listPriceAtPending, "", "county rows are never mutated");
  assert.strictEqual(rows[2].listPriceAtPending, "");
  assert.strictEqual(collision.listPriceAtPending, "", "an implausible join leaves the row untouched");
  assert.strictEqual(report.rejected.length, 1);
  assert.deepStrictEqual(report.rejected[0], {
    reason: "implausible_ratio",
    address: "1 Collision Ct",
    zip: "98103",
    mlsListingNumber: "2400004",
    saleDate: "2026-09-10",
    closePrice: 1300000,
    lastSeenActive: "2026-08-20",
    lastAsk: 600000,
    ratio: 2.167,
  });
});

test("loadLedgerMap derives DOM from the ledger's dates when the feed reported none", () => {
  const text = [
    "mlsNumber,zip,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,lastActiveAsk,listDate,lastDom,lastCdom,lastStatus,listDateSource,chainId,chainPosition,chainListDate,chainFirstAsk",
    "2600001,98117,2026-10-02,2026-10-09,2026-10-07,1250000,1250000,1250000,2026-10-01,,,Active Under Contract,FIRST_SEEN,2600001,1,2026-10-01,1250000",
    "2600002,98117,2026-10-02,2026-10-09,2026-10-07,1250000,1250000,1250000,,,,Active,,2600002,1,,1250000",
  ].join("\n");
  const map = loadLedgerMap(text);
  assert.strictEqual(map.get("2600001").dom, "6", "Oct 1 list date to Oct 7, the last day seen for sale");
  assert.strictEqual(map.get("2600001").cdom, "6");
  assert.strictEqual(map.get("2600002").dom, "", "no list date and no feed DOM stays unknown, never zero");
  assert.strictEqual(map.get("2600002").cdom, "");
});

test("loadLedgerMap carries a relisted home's cumulative days and opening ask from its chain", () => {
  const text = [
    "mlsNumber,zip,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,lastActiveAsk,listDate,lastDom,lastCdom,lastStatus,listDateSource,chainId,chainPosition,chainListDate,chainFirstAsk,chainPriorDays",
    "2600010,98146,2026-09-18,2026-09-24,2026-09-24,1083000,1083000,1083000,2026-09-17,7,7,Active,FEED,2600008,3,2026-08-11,1200000,30",
  ].join("\n");
  const rec = loadLedgerMap(text).get("2600010");
  assert.strictEqual(rec.dom, "7", "this MLS#'s own clock");
  assert.strictEqual(rec.cdom, "37", "7 days on this listing plus 30 on the two before it; days off the market do not count");
  assert.strictEqual(rec.originalList, 1200000);
  const row = { mlsJoinMethod: "REDFIN_SOLD", mlsListingNumber: "2600010", closePrice: "1070000", saleDate: "2026-10-20", listPriceAtPending: "" };
  assert.strictEqual(applySnapshotToRow(row, rec), true);
  assert.strictEqual(row.mlsDOM, "7");
  assert.strictEqual(row.mlsCDOM, "37");
  assert.strictEqual(row.mlsOriginalPrice, "1200000");
  assert.strictEqual(Number(row.saleToOriginalListRatio).toFixed(3), "0.892");
});

test("a row filled earlier by the enrichment ledger still gets its relist chain's cumulative days and opening ask", () => {
  const rec = { date: "2026-08-20", list: 1140000, originalList: 1200000, listDate: "2026-08-10", dom: "10", cdom: "52", priorDays: 42, zip: "98146" };
  const filled = () => ({
    mlsJoinMethod: "REDFIN_SOLD", mlsListingNumber: "9", closePrice: "1120000", saleDate: "2026-09-15", listPriceAtPending: "1140000",
    mlsListingPrice: "1140000", mlsOriginalPrice: "1140000", mlsDOM: "10", mlsCDOM: "10", saleToListRatio: "0.982", listPriceSource: "ACTIVE_SNAPSHOT",
  });
  const rows = [filled()];
  const report = backfillFromSnapshots(rows, new Map([["9", rec]]));
  assert.strictEqual(report.applied, 0);
  assert.strictEqual(report.chainUpgraded, 1);
  assert.strictEqual(rows[0].mlsCDOM, "52");
  assert.strictEqual(rows[0].mlsDOM, "10");
  assert.strictEqual(rows[0].mlsOriginalPrice, "1200000");
  assert.strictEqual(Number(rows[0].saleToOriginalListRatio).toFixed(3), "0.933");
  assert.strictEqual(rows[0].mlsListingPrice, "1140000", "list-at-pending and its ratio are left alone");
  assert.strictEqual(rows[0].saleToListRatio, "0.982");
  const relistedAfterSale = [filled()];
  const later = backfillFromSnapshots(relistedAfterSale, new Map([["9", { ...rec, date: "2026-10-01" }]]));
  assert.strictEqual(later.chainUpgraded, 0, "seen for sale after this closing: a different marketing period");
  assert.strictEqual(relistedAfterSale[0].mlsCDOM, "10");
  const neverRelisted = [{ ...filled(), mlsOriginalPrice: "1200000" }];
  backfillFromSnapshots(neverRelisted, new Map([["9", { ...rec, cdom: "30", priorDays: 0 }]]));
  assert.strictEqual(neverRelisted[0].mlsCDOM, "10", "a larger ledger figure without a chain is the under-contract tail, not market time");
});

test("snapshots from the weeks DOM was misread as zero are not trusted for a timeline", () => {
  const header = "id,zip,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsOriginalPrice,mlsListDate,listDate,mlsDOM,mlsCDOM,mlsJoinMethod";
  const snapshots = [
    { date: "2026-10-10", text: [header, "a,98117,1,1200000,,1200000,2026-10-10,2026-10-10,0,0,REDFIN_ACTIVE", "b,98117,2,900000,,900000,2026-09-30,2026-09-30,10,10,REDFIN_ACTIVE"].join("\n") },
  ];
  const map = collectActiveSnapshots(snapshots);
  assert.strictEqual(map.get("1").dom, "", "0 days, listed the day of the snapshot: indistinguishable from the bug");
  assert.strictEqual(map.get("1").listDate, "");
  assert.strictEqual(map.get("1").list, 1200000, "the ask is still good");
  assert.strictEqual(map.get("2").dom, "10");
  assert.strictEqual(map.get("2").listDate, "2026-09-30");
});
