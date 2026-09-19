"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { sanitize, staleReason, hasFabricatedTimeline, isOpenStatus, loadLedgerLastSeen } = require("../scripts/sanitize_enriched_rows.js");

const OPTS = { today: "2026-09-19", maxOpenAgeDays: 45, ledgerGraceDays: 7 };

function openRow(overrides = {}) {
  return { mlsJoinMethod: "MLS_STATUS_OPEN", mlsListingNumber: "2300001", address: "1 Ghost St", zip: "98103", mlsStatus: "Active", mlsListDate: "2026-03-10", listDate: "2026-03-10", closePrice: "", ...overrides };
}

test("isOpenStatus covers the NWMLS pending flavors", () => {
  for (const s of ["Active", "Active Under Contract", "Pending", "Pending Inspection", "Pending BU Requested", "Contingent", "Coming Soon", "First Look"]) assert.strictEqual(isOpenStatus(s), true, s);
  for (const s of ["Sold", "Closed", "Expired", "", undefined]) assert.strictEqual(isOpenStatus(s), false, String(s));
});

test("staleReason: old non-feed open rows are stale unless the ledger saw the MLS# recently", () => {
  const ledger = new Map([["2300009", "2026-09-18"], ["2300010", "2026-08-01"]]);
  assert.strictEqual(staleReason(openRow(), ledger, OPTS), "list_date_stale_and_not_in_ledger");
  assert.strictEqual(staleReason(openRow({ mlsListDate: "", listDate: "" }), ledger, OPTS), "no_list_date_and_not_in_ledger");
  assert.strictEqual(staleReason(openRow({ mlsListingNumber: "2300009" }), ledger, OPTS), null, "seen yesterday in the feed: live");
  assert.strictEqual(staleReason(openRow({ mlsListingNumber: "2300010" }), ledger, OPTS), "list_date_stale_and_not_in_ledger", "last seen in August: gone");
  assert.strictEqual(staleReason(openRow({ mlsListDate: "2026-09-01", listDate: "2026-09-01" }), ledger, OPTS), null, "listed 18 days ago: give it time");
  assert.strictEqual(staleReason(openRow({ mlsJoinMethod: "REDFIN_ACTIVE" }), ledger, OPTS), null, "feed rows are never expired here");
  assert.strictEqual(staleReason(openRow({ closePrice: "900000" }), ledger, OPTS), null, "a closed row is not an open row");
  assert.strictEqual(staleReason(openRow({ mlsStatus: "Sold" }), ledger, OPTS), null);
  assert.strictEqual(staleReason(openRow({ mlsStatus: "Pending Inspection" }), ledger, OPTS), "list_date_stale_and_not_in_ledger");
});

test("hasFabricatedTimeline: list == pending with no MLS list price on a closed MLS-enriched row", () => {
  const base = { dataMode: "MLS_ENRICHED", closePrice: "1149000", mlsListingPrice: "", mlsListPriceAtPending: "", mlsListDate: "2025-08-16", mlsPendingDate: "2025-08-16" };
  assert.strictEqual(hasFabricatedTimeline(base), true);
  assert.strictEqual(hasFabricatedTimeline({ ...base, mlsListingPrice: "1100000" }), false, "a real list price means a real timeline");
  assert.strictEqual(hasFabricatedTimeline({ ...base, mlsPendingDate: "2025-08-30" }), false);
  assert.strictEqual(hasFabricatedTimeline({ ...base, closePrice: "" }), false);
  assert.strictEqual(hasFabricatedTimeline({ ...base, dataMode: "PUBLIC_PROXY" }), false, "county proxy rows carry list == pending by construction and are left alone");
});

test("sanitize drops stale rows, keeps ledger-proven ones, blanks fabricated timelines, and reports everything", () => {
  const ledger = loadLedgerLastSeen("mlsNumber,lastSeen\n2300009,2026-09-18\n");
  const fabricated = { dataMode: "MLS_ENRICHED", mlsJoinMethod: "REDFIN_HISTORY", mlsListingNumber: "2400001", address: "7424 2nd Ave NE", saleDate: "2026-01-22", closePrice: "1149000", mlsListingPrice: "", mlsListPriceAtPending: "", listDate: "2025-08-16", mlsListDate: "2025-08-16", pendingDate: "2025-08-16", mlsPendingDate: "2025-08-16", mlsContractualDate: "", mlsDOM: "0", mlsCDOM: "0", mlsDaysToPending: "0", mlsDaysPendingToSale: "159", mlsStatus: "Sold" };
  const rows = [
    openRow(),
    openRow({ mlsListingNumber: "2300009", address: "2 Live St" }),
    { mlsJoinMethod: "REDFIN_ACTIVE", mlsListingNumber: "2500001", address: "3 Feed St", mlsStatus: "Active", mlsListDate: "2026-09-17", closePrice: "" },
    fabricated,
    { mlsJoinMethod: "APN_PRICE_DATE_WINDOW", mlsListingNumber: "2200001", address: "4 Sold St", mlsStatus: "Sold", closePrice: "800000", mlsListingPrice: "790000", mlsListDate: "2026-04-01", mlsPendingDate: "2026-04-01", mlsDOM: "0" },
  ];
  const { rows: kept, report } = sanitize(rows, ledger, OPTS);
  assert.strictEqual(report.rowsBefore, 5);
  assert.strictEqual(report.rowsAfter, 4);
  assert.strictEqual(report.staleOpenDropped, 1);
  assert.strictEqual(report.staleOpenKeptViaLedger, 1);
  assert.deepStrictEqual(report.staleReasons, { list_date_stale_and_not_in_ledger: 1 });
  assert.strictEqual(report.dropped[0].address, "1 Ghost St");
  assert.strictEqual(report.fabricatedTimelinesBlanked, 1);
  assert.strictEqual(fabricated.pendingDate, "");
  assert.strictEqual(fabricated.mlsDOM, "");
  assert.strictEqual(fabricated.mlsDaysPendingToSale, "");
  assert.strictEqual(fabricated.listDate, "2025-08-16", "list date is kept");
  assert.strictEqual(fabricated.closePrice, "1149000");
  assert.strictEqual(rows[4].mlsDOM, "0", "a row with a real list price keeps its same-day timeline");
  assert.ok(kept.includes(rows[1]) && kept.includes(rows[2]) && kept.includes(fabricated) && kept.includes(rows[4]));
});
