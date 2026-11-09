"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  parseCsvLine,
  hasColumns,
  num,
  reportRealtorFiles,
  dataQualityWarnings,
} = require("../scripts/validate_data_refresh.js");

test("parseCsvLine handles quoted commas and escaped quotes", () => {
  const line = 'APN,"Address, Seattle",123,"Quoted ""Name"""';
  const cols = parseCsvLine(line);
  assert.deepEqual(cols, ["APN", "Address, Seattle", "123", 'Quoted "Name"']);
});

test("hasColumns returns only missing columns", () => {
  const headers = ["id", "address", "closePrice", "type"];
  assert.deepEqual(hasColumns(headers, ["id", "type", "saleDate"]), ["saleDate"]);
});

test("num parses currency and handles empty values safely", () => {
  assert.equal(num("$1,250,000"), 1250000);
  assert.equal(num(""), 0);
  assert.equal(num(undefined), 0);
  assert.equal(num("bad"), 0);
});

test("reportRealtorFiles reads committed source manifest for CI validation", () => {
  const report = {
    source: {
      realtorFiles: ["NE Seattle Sale Stats.csv", "", "QA_Magnolia Rich Snapshot.csv"],
    },
  };

  assert.deepEqual(reportRealtorFiles(report), [
    "NE Seattle Sale Stats.csv",
    "QA_Magnolia Rich Snapshot.csv",
  ]);
  assert.deepEqual(reportRealtorFiles({}), []);
});

const TODAY = "2026-11-08";

function active(dom, cdom = dom) {
  return { dataMode: "MLS_ENRICHED", mlsStatus: "Active", closePrice: "", mlsDOM: dom, mlsCDOM: cdom, mlsListingPrice: "1200000" };
}

function sale(index, overrides = {}) {
  return {
    dataMode: "MLS_ENRICHED", mlsStatus: "Sold", id: `sale-${index}`, address: `${1000 + index} Test Ave N`, zip: "98103", saleDate: "2026-10-20",
    closePrice: String(1000000 + index * 1000), mlsListingPrice: "1000000", mlsDOM: "6", mlsCDOM: "6", ...overrides,
  };
}

test("a healthy dataset raises no content warnings", () => {
  const rows = [
    ...["0", "3", "12", "40", "75", "9", "21", "5"].map((dom) => active(dom)),
    ...Array.from({ length: 30 }, (_, i) => sale(i)),
    { dataMode: "PUBLIC_PROXY", address: "5 County St", zip: "98103", saleDate: "2026-10-10", closePrice: "900000" },
  ];
  assert.deepEqual(dataQualityWarnings(rows, { today: TODAY }), []);
});

test("actives that all read 0 days on market are called out (the 2026-09-21 feed change)", () => {
  const rows = Array.from({ length: 40 }, () => active("0"));
  const warnings = dataQualityWarnings(rows, { today: TODAY });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /100% of active listings show 0 days on market/);
});

test("actives with no days-on-market reading are called out separately from zeros", () => {
  const rows = [...Array.from({ length: 30 }, () => active("")), ...Array.from({ length: 10 }, () => active("14"))];
  const warnings = dataQualityWarnings(rows, { today: TODAY });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /75% of active listings have no days-on-market reading/);
});

test("recent sales losing their asking price or days on market are called out; old sales are not judged", () => {
  const bare = { mlsListingPrice: "", mlsDOM: "", mlsCDOM: "" };
  const recent = Array.from({ length: 30 }, (_, i) => sale(i, bare));
  const warnings = dataQualityWarnings(recent, { today: TODAY });
  assert.equal(warnings.length, 2);
  assert.match(warnings[0], /only 0% of sales from the last 60 days carry an asking price/);
  assert.match(warnings[1], /only 0% of sales from the last 60 days carry days on market/);
  const old = recent.map((r) => ({ ...r, saleDate: "2026-03-01" }));
  assert.deepEqual(dataQualityWarnings(old, { today: TODAY }), []);
  const datedOnly = recent.map((r) => ({ ...r, mlsListingPrice: "1000000", mlsDaysToPending: "6" }));
  assert.deepEqual(dataQualityWarnings(datedOnly, { today: TODAY }), [], "a list-to-pending span counts as a days-on-market reading");
});

test("a county file months behind is called out with the command that fixes it", () => {
  const rows = [{ dataMode: "PUBLIC_PROXY", address: "5 County St", zip: "98103", saleDate: "2026-06-23", closePrice: "900000" }];
  const warnings = dataQualityWarnings(rows, { today: TODAY });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /newest King County sale is 2026-06-23 \(138 days old\)/);
  assert.match(warnings[0], /refresh:kc-fresh/);
});

test("the same sale present twice is counted; a resale months later is not", () => {
  const base = Array.from({ length: 20 }, (_, i) => sale(i, { saleDate: "2026-03-01" }));
  const twin = { ...base[0], id: "3380001", address: "1000 TEST AVENUE NORTH, SEATTLE WA 98103", saleDate: "2026-03-04", dataMode: "PUBLIC_PROXY" };
  const withTwin = dataQualityWarnings([...base, twin], { today: "2026-03-20" });
  assert.equal(withTwin.length, 1);
  assert.match(withTwin[0], /^1 closed sales look like duplicates/);
  const resale = { ...base[0], id: "earlier", saleDate: "2025-09-01" };
  assert.deepEqual(dataQualityWarnings([...base, resale], { today: "2026-03-20" }), []);
  const secondParcel = { ...base[0] }; // one county sale, one excise id, two parcel rows
  assert.deepEqual(dataQualityWarnings([...base, secondParcel], { today: "2026-03-20" }), []);
  const unitA = { ...base[0], id: "a", address: "1000 Test Ave N Unit A" };
  const unitB = { ...base[0], id: "b", address: "1000 Test Ave N Unit B" };
  assert.deepEqual(dataQualityWarnings([...base.slice(1), unitA, unitB], { today: "2026-03-20" }), [], "neighbouring units at one price");
});
