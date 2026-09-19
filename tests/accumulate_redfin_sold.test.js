"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { accumulateSold, soldKey, unionHeaders } = require("../scripts/accumulate_redfin_sold.js");

function sold(overrides = {}) {
  return {
    fetchedAt: "2026-09-19T19:54:21.154Z", redfinPropertyId: "95780", mlsListingNumber: "2400001",
    address: "10038 13th Ave NW", zip: "98177", soldDate: "2026-08-19", soldPrice: "1070000",
    ...overrides,
  };
}

test("soldKey prefers property id, then MLS#, then address", () => {
  assert.strictEqual(soldKey(sold()), "pid:95780|2026-08-19");
  assert.strictEqual(soldKey(sold({ redfinPropertyId: "" })), "mls:2400001|2026-08-19");
  assert.strictEqual(soldKey(sold({ redfinPropertyId: "", mlsListingNumber: "" })), "addr:10038 13TH AVE NW|98177|2026-08-19");
});

test("a short new window is ADDED to the existing rows, never replacing them", () => {
  const existing = [
    sold({ redfinPropertyId: "1", soldDate: "2026-04-10", fetchedAt: "2026-09-19T00:00:00Z" }),
    sold({ redfinPropertyId: "2", soldDate: "2026-08-19", fetchedAt: "2026-09-19T00:00:00Z" }),
  ];
  const incoming = [
    sold({ redfinPropertyId: "2", soldDate: "2026-08-19", fetchedAt: "2026-09-20T00:00:00Z", soldPrice: "1070001" }), // same sale, newer fetch
    sold({ redfinPropertyId: "3", soldDate: "2026-09-19", fetchedAt: "2026-09-20T00:00:00Z" }),
  ];
  const { rows, report } = accumulateSold(existing, incoming, { retainDays: 400, today: "2026-09-20" });
  assert.strictEqual(report.added, 1);
  assert.strictEqual(report.replaced, 1);
  assert.strictEqual(report.keptOlder, 0);
  assert.strictEqual(report.pruned, 0);
  assert.strictEqual(report.total, 3);
  assert.deepStrictEqual(rows.map((r) => r.redfinPropertyId), ["3", "2", "1"], "newest sale first");
  assert.strictEqual(rows[1].soldPrice, "1070001", "the newer fetch of the same sale wins");
  assert.deepStrictEqual(report.soldDateRange, { min: "2026-04-10", max: "2026-09-19" });
});

test("an older fetch of the same sale does not overwrite a newer one", () => {
  const existing = [sold({ fetchedAt: "2026-09-20T00:00:00Z", soldPrice: "1070000" })];
  const incoming = [sold({ fetchedAt: "2026-08-19T00:00:00Z", soldPrice: "999" })];
  const { rows, report } = accumulateSold(existing, incoming, { retainDays: 0 });
  assert.strictEqual(report.keptOlder, 1);
  assert.strictEqual(rows[0].soldPrice, "1070000");
});

test("sales older than retain-days are pruned; retain-days 0 keeps everything", () => {
  const rows = [
    sold({ redfinPropertyId: "old", soldDate: "2025-01-01" }),
    sold({ redfinPropertyId: "new", soldDate: "2026-09-01" }),
  ];
  const pruned = accumulateSold(rows, [], { retainDays: 400, today: "2026-09-20" });
  assert.strictEqual(pruned.report.pruned, 1);
  assert.strictEqual(pruned.report.cutoff, "2025-08-16");
  assert.deepStrictEqual(pruned.rows.map((r) => r.redfinPropertyId), ["new"]);
  const kept = accumulateSold(rows, [], { retainDays: 0 });
  assert.strictEqual(kept.report.pruned, 0);
  assert.strictEqual(kept.rows.length, 2);
});

test("unionHeaders keeps the existing order and appends new columns", () => {
  assert.deepStrictEqual(unionHeaders(["a", "b"], ["b", "c", "a"]), ["a", "b", "c"]);
  assert.deepStrictEqual(unionHeaders([], ["x", "y"]), ["x", "y"]);
});
