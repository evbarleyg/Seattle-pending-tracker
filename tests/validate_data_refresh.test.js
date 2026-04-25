"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  parseCsvLine,
  hasColumns,
  num,
  reportRealtorFiles,
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
