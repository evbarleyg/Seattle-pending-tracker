"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  buildEnrichedRowFromRedfin,
  resolveListingTimeline,
  refreshOpenRowAsk,
  refreshOpenRowTimeline,
  openRowAtAddress,
  indexPriorSales,
  applyPriorSale,
  zipNeighborhood,
} = require("../scripts/merge_redfin_actives.js");

const HEADERS = [
  "dataMode", "id", "address", "neighborhood", "listDate", "pendingDate", "saleDate", "listPriceAtPending", "closePrice", "zip",
  "mlsListDate", "mlsListingNumber", "mlsStatus", "mlsListingPrice", "mlsOriginalPrice", "mlsDOM", "mlsCDOM", "mlsJoinMethod",
];
const FETCHED_AT = "2026-11-08T14:30:56.849Z"; // 06:30 PST on Nov 8

function feedRow(overrides = {}) {
  return {
    fetchedAt: FETCHED_AT, mlsListingNumber: "2597226", redfinListingId: "555", mlsStatus: "Active", listPrice: "999000",
    domDays: "", address: "10030 37th Ave SW", zip: "98146", uiPropertyType: "1", ...overrides,
  };
}

test("a blank feed DOM is unknown, not zero: no ledger entry leaves list date and DOM blank", () => {
  const row = buildEnrichedRowFromRedfin(feedRow(), HEADERS, FETCHED_AT, null);
  assert.strictEqual(row.mlsDOM, "");
  assert.strictEqual(row.mlsCDOM, "");
  assert.strictEqual(row.listDate, "", "must not be stamped as listed today");
  assert.strictEqual(row.mlsListDate, "");
  assert.strictEqual(row.mlsListingPrice, "999000");
  assert.strictEqual(row.mlsOriginalPrice, "999000");
});

test("a DOM the feed does report still wins, including a genuine zero", () => {
  const week = buildEnrichedRowFromRedfin(feedRow({ domDays: "7" }), HEADERS, FETCHED_AT, { listDate: "2026-01-01" });
  assert.strictEqual(week.mlsDOM, "7");
  assert.strictEqual(week.listDate, "2026-11-01");
  const today = buildEnrichedRowFromRedfin(feedRow({ domDays: "0" }), HEADERS, FETCHED_AT, null);
  assert.strictEqual(today.mlsDOM, "0");
  assert.strictEqual(today.listDate, "2026-11-08", "dated in Seattle time, matching the ledger");
});

test("the ledger supplies list date and DOM when the feed has none", () => {
  const ledger = { listDate: "2026-10-22", firstAsk: "1028000", chainListDate: "2026-10-22", chainFirstAsk: "1028000" };
  const row = buildEnrichedRowFromRedfin(feedRow(), HEADERS, FETCHED_AT, ledger);
  assert.strictEqual(row.listDate, "2026-10-22");
  assert.strictEqual(row.mlsListDate, "2026-10-22");
  assert.strictEqual(row.mlsDOM, "17");
  assert.strictEqual(row.mlsCDOM, "17");
  assert.strictEqual(row.mlsOriginalPrice, "1028000", "the opening ask, so the price cut is visible");
});

test("a relisted home keeps its own DOM but reports cumulative days and the chain's opening ask", () => {
  const ledger = { listDate: "2026-10-22", firstAsk: "1028000", chainListDate: "2026-08-11", chainFirstAsk: "1200000", chainPriorDays: "66" };
  const t = resolveListingTimeline(feedRow(), ledger, "2026-11-08");
  assert.deepStrictEqual(t, { listDate: "2026-10-22", dom: "17", cdom: "83", originalPrice: "1200000" }, "17 days on this listing plus 66 on earlier ones; the days between listings do not count");
});

test("a ledger list date after the fetch date is ignored rather than producing a negative DOM", () => {
  const t = resolveListingTimeline(feedRow(), { listDate: "2026-11-20" }, "2026-11-08");
  assert.strictEqual(t.dom, "");
  assert.strictEqual(t.listDate, "");
});

test("an open row from an old export takes today's ask and keeps the old one as its original price", () => {
  const open = { closePrice: "", mlsListingPrice: "1400000", mlsListPriceAtPending: "1400000", listPriceAtPending: "1400000", mlsOriginalPrice: "" };
  assert.strictEqual(refreshOpenRowAsk(open, 1200000), true);
  assert.strictEqual(open.mlsListingPrice, "1200000");
  assert.strictEqual(open.listPriceAtPending, "1200000");
  assert.strictEqual(open.mlsOriginalPrice, "1400000");
  const sold = { closePrice: "1380000", mlsListingPrice: "1450000" };
  assert.strictEqual(refreshOpenRowAsk(sold, 1500000), false, "a closed sale's list-at-pending is history and never moves");
  assert.strictEqual(sold.mlsListingPrice, "1450000");
  assert.strictEqual(refreshOpenRowAsk({ closePrice: "", mlsListingPrice: "1200000" }, 1200000), false, "unchanged ask is not a change");
});

test("98107 is labelled Ballard, the same as the county build and the app", () => {
  assert.strictEqual(zipNeighborhood("98107"), "Ballard");
  assert.strictEqual(zipNeighborhood("98117"), "Ballard / Crown Hill");
});

test("a home's earlier sale at the same address is not today's listing", () => {
  const soldInApril = { address: "1142 N 77th St", closePrice: "855000", mlsStatus: "Sold" };
  assert.strictEqual(openRowAtAddress(soldInApril), null, "the resale must be appended as its own active row");
  const stillOpen = { address: "4309 Evanston Ave N", closePrice: "", mlsStatus: "Active" };
  assert.strictEqual(openRowAtAddress(stillOpen), stillOpen);
  assert.strictEqual(openRowAtAddress(undefined), null);
});

test("an active listing carries the home's latest earlier sale, matched across address spellings and by unit", () => {
  const priorSales = indexPriorSales([
    { address: "1142 N 77TH ST, Seattle 98103", zip: "98103", saleDate: "2024-02-01", closePrice: "700000" },
    { address: "1142 N 77th St", zip: "98103", saleDate: "2026-04-23", closePrice: "855000" },
    { address: "4412 A CORLISS AVE N, Seattle 98103", zip: "98103", saleDate: "2026-01-10", closePrice: "725000" },
    { address: "9 Open Listing Ln", zip: "98103", saleDate: "", closePrice: "" },
  ]);
  const flip = { address: "1142 N 77th St", zip: "98103", mlsListingPrice: "1275000" };
  assert.strictEqual(applyPriorSale(flip, priorSales), true);
  assert.strictEqual(flip.priorSaleDate, "2026-04-23", "the most recent closing wins");
  assert.strictEqual(flip.priorSalePrice, "855000");
  const sameUnit = { address: "4412 Corliss Ave N Unit A", zip: "98103", mlsListingPrice: "760000" };
  assert.strictEqual(applyPriorSale(sameUnit, priorSales), true);
  const otherUnit = { address: "4412 Corliss Ave N Unit B", zip: "98103", mlsListingPrice: "760000" };
  assert.strictEqual(applyPriorSale(otherUnit, priorSales), false);
  assert.strictEqual(otherUnit.priorSalePrice, "");
});

test("an earlier record priced nothing like the listing is left off", () => {
  const priorSales = indexPriorSales([{ address: "2708 Franklin Ave E", zip: "98102", saleDate: "2026-04-22", closePrice: "15500000" }]);
  const unit = { address: "2708 Franklin Ave E", zip: "98102", mlsListingPrice: "1249000" };
  assert.strictEqual(applyPriorSale(unit, priorSales), false, "a builder's whole-site purchase is not this home's price");
  assert.strictEqual(unit.priorSaleDate, "");
});

test("a deed recorded after the home was already for sale is not what the seller paid", () => {
  const priorSales = indexPriorSales([
    { address: "4011 SW Maryland Pl", zip: "98116", saleDate: "2024-03-01", closePrice: "640000" },
    { address: "4011 SW Maryland Pl", zip: "98116", saleDate: "2026-05-26", closePrice: "453000" },
  ]);
  const row = { address: "4011 SW Maryland Pl", zip: "98116", mlsListingPrice: "899000" };
  assert.strictEqual(applyPriorSale(row, priorSales, "2026-05-22"), true);
  assert.strictEqual(row.priorSaleDate, "2024-03-01", "the last closing BEFORE the marketing period began");
  assert.strictEqual(row.priorSalePrice, "640000");
  const onlyLater = indexPriorSales([{ address: "1 Later St", zip: "98116", saleDate: "2026-05-26", closePrice: "453000" }]);
  assert.strictEqual(applyPriorSale({ address: "1 Later St", zip: "98116", mlsListingPrice: "899000" }, onlyLater, "2026-05-22"), false);
});

test("an open row from an old export has its days on market recounted, not frozen at export day", () => {
  const open = { closePrice: "", mlsListDate: "2026-03-24", mlsDOM: "1", mlsCDOM: "1" };
  assert.strictEqual(refreshOpenRowTimeline(open, feedRow(), { listDate: "2026-09-18" }, "2026-11-08"), true);
  assert.strictEqual(open.mlsDOM, "229", "from the MLS list date the export recorded");
  assert.strictEqual(open.mlsCDOM, "229");
  const longCdom = { closePrice: "", mlsListDate: "2026-10-20", mlsDOM: "3", mlsCDOM: "178" };
  refreshOpenRowTimeline(longCdom, feedRow(), null, "2026-11-08");
  assert.strictEqual(longCdom.mlsDOM, "19");
  assert.strictEqual(longCdom.mlsCDOM, "194", "the 175 days an earlier listing accrued stay on top of the recount");
  refreshOpenRowTimeline(longCdom, feedRow(), null, "2026-11-09");
  assert.strictEqual(longCdom.mlsDOM, "20");
  assert.strictEqual(longCdom.mlsCDOM, "195", "and the offset survives the next morning's run");
  const noDates = { closePrice: "", mlsListDate: "", mlsDOM: "4", mlsCDOM: "4" };
  assert.strictEqual(refreshOpenRowTimeline(noDates, feedRow(), null, "2026-11-08"), false, "nothing to count from: left alone");
  const sold = { closePrice: "900000", mlsListDate: "2026-03-24", mlsDOM: "6", mlsCDOM: "6" };
  assert.strictEqual(refreshOpenRowTimeline(sold, feedRow(), null, "2026-11-08"), false);
  assert.strictEqual(sold.mlsDOM, "6");
});

test("a Coming Soon teaser reads zero days: it is not on the market yet", () => {
  const ledger = { listDate: "2026-10-28", chainListDate: "2026-10-28", firstAsk: "1100000" };
  const t = resolveListingTimeline(feedRow({ mlsStatus: "Coming Soon", listPrice: "1100000" }), ledger, "2026-11-08");
  assert.deepStrictEqual(t, { listDate: "", dom: "0", cdom: "0", originalPrice: "1100000" });
});

test("a listing under contract is not written as Active", () => {
  assert.strictEqual(buildEnrichedRowFromRedfin(feedRow({ mlsStatus: "Active Under Contract" }), HEADERS, FETCHED_AT, null).mlsStatus, "Active Under Contract");
  for (const status of ["Active", "First Look", "Coming Soon"]) {
    assert.strictEqual(buildEnrichedRowFromRedfin(feedRow({ mlsStatus: status }), HEADERS, FETCHED_AT, null).mlsStatus, "Active", status);
  }
});
