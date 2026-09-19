"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { joinStreetAndUnit, homeToRow } = require("../scripts/fetch_redfin_actives.js");

test("joinStreetAndUnit does not double a unit the street line already carries", () => {
  assert.strictEqual(joinStreetAndUnit("2727 Fairview Ave E #4", "#4"), "2727 Fairview Ave E #4");
  assert.strictEqual(joinStreetAndUnit("9057 Greenwood Ave N Unit 306", "#306"), "9057 Greenwood Ave N Unit 306");
  assert.strictEqual(joinStreetAndUnit("708 N 102nd St #2", "2"), "708 N 102nd St #2");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW Unit A", "Unit A"), "4032 53rd Ave SW Unit A", "the whole unit phrase already present");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW Unit A", "A"), "4032 53rd Ave SW Unit A");
  assert.strictEqual(joinStreetAndUnit("1080 W Ewing Pl Unit 0-P", "Unit 0-P"), "1080 W Ewing Pl Unit 0-P");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW", "Unit A"), "4032 53rd Ave SW Unit A", "a missing unit phrase is appended once");
  assert.strictEqual(joinStreetAndUnit("2727 Fairview Ave E", "#4"), "2727 Fairview Ave E #4", "a missing unit is still appended");
  assert.strictEqual(joinStreetAndUnit("100 4th Ave", "#4"), "100 4th Ave #4", "a street word that happens to match the unit is not the unit");
  assert.strictEqual(joinStreetAndUnit("1 Main St", ""), "1 Main St");
  assert.strictEqual(joinStreetAndUnit("", "#4"), "#4");
});

test("homeToRow builds the address through joinStreetAndUnit", () => {
  const home = { streetLine: { value: "2727 Fairview Ave E #4" }, unitNumber: { value: "#4" }, propertyId: 1, listingId: 2, mlsId: { value: "2517214" }, mlsStatus: "Active", price: { value: 999000 } };
  const row = homeToRow(home, { label: "Eastlake" }, "2026-09-19T13:39:44.613Z");
  assert.strictEqual(row.address, "2727 Fairview Ave E #4");
  assert.strictEqual(row.unitNumber, "#4");
  assert.strictEqual(row.mlsListingNumber, "2517214");
});
