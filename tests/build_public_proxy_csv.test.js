"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  deriveNeighborhood,
  learnAreaSubNeighborhood,
} = require("../scripts/build_public_proxy_csv.js");

test("deriveNeighborhood ignores blank area/subarea mappings and falls back to ZIP", () => {
  const learned = new Map([["::", "Downtown"]]);
  assert.equal(
    deriveNeighborhood("", "", "98105", learned),
    "University District / Laurelhurst"
  );
});

test("learnAreaSubNeighborhood skips rows without meaningful parcel geography", () => {
  const learnedCounts = new Map();
  learnAreaSubNeighborhood(learnedCounts, { area: "", subArea: "" }, "98101");
  learnAreaSubNeighborhood(learnedCounts, { area: "", subArea: "705" }, "98103");
  assert.equal(learnedCounts.size, 0);
});

test("deriveNeighborhood preserves useful learned labels for numeric subareas", () => {
  const learned = new Map([["56::705", "Magnolia"]]);
  assert.equal(
    deriveNeighborhood("56", "705", "98103", learned),
    "Magnolia"
  );
});

test("deriveNeighborhood keeps explicit textual subareas", () => {
  const learned = new Map([["56::Ballard West", "Downtown"]]);
  assert.equal(
    deriveNeighborhood("56", "Ballard West", "98101", learned),
    "Ballard West"
  );
});
