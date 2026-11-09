"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  streetKeyFromAddress,
  streetKeyFromRedfinPath,
  extractUnitFromAddress,
  houseNumber,
  unitFromRedfinPath,
} = require("../scripts/redfin_address_key.js");

test("county, mailing and Redfin spellings of one address share a key", () => {
  const want = "5179 42ND AVE S|98118";
  assert.strictEqual(streetKeyFromAddress("5179 42ND AVENUE SOUTH, SEATTLE WA 98118", "98118"), want);
  assert.strictEqual(streetKeyFromAddress("5179 42ND AVE S, Seattle 98118", ""), want);
  assert.strictEqual(streetKeyFromAddress("5179 42ND AVE S, Seattle", "98118"), want, "a bare city tail is stripped");
  assert.strictEqual(streetKeyFromAddress("5179 42nd Ave S", "98118"), want);
  assert.strictEqual(streetKeyFromRedfinPath("/WA/Seattle/5179-42nd-Ave-S-98118/home/12345"), want);
});

test("compound directionals are abbreviated whole, and names that merely contain one are left alone", () => {
  assert.strictEqual(streetKeyFromAddress("8518 11TH AVENUE NORTHWEST", "98117"), "8518 11TH AVE NW|98117");
  assert.strictEqual(streetKeyFromAddress("1234 West Viewmont Way West", "98199"), "1234 W VIEWMONT WAY W|98199");
  assert.strictEqual(streetKeyFromAddress("10700 Northgate Way", "98125"), "10700 NORTHGATE WAY|98125");
  assert.strictEqual(streetKeyFromAddress("3400 Eastlake Ave E", "98102"), "3400 EASTLAKE AVE E|98102");
});

test("a unit never reaches the street key, whichever way it is written", () => {
  const want = "4412 CORLISS AVE N|98103";
  for (const address of ["4412 Corliss Ave N Unit A", "4412 A CORLISS AVE N, Seattle 98103", "4412A CORLISS AVE N", "4412 Corliss Ave N #A"]) {
    assert.strictEqual(streetKeyFromAddress(address, "98103"), want, address);
    assert.strictEqual(extractUnitFromAddress(address), "UNIT-A", address);
  }
});

test("a spaced N, S, E or W after the house number is a directional, not a unit", () => {
  assert.strictEqual(streetKeyFromAddress("123 N 45th St", "98103"), "123 N 45TH ST|98103");
  assert.strictEqual(extractUnitFromAddress("123 N 45th St"), "");
  assert.strictEqual(extractUnitFromAddress("815 E Pike St"), "");
  assert.strictEqual(extractUnitFromAddress("1025 NE 130th St"), "");
});

test("no ZIP anywhere means no key; houseNumber reads the leading digits", () => {
  assert.strictEqual(streetKeyFromAddress("2832 FAIRVIEW AVE E, Seattle", ""), "");
  assert.strictEqual(houseNumber(" 2832 FAIRVIEW AVE E"), "2832");
  assert.strictEqual(houseNumber("4113C CHILBERG AVE SW"), "4113");
  assert.strictEqual(houseNumber("Parcel 276770-4695"), "");
});

test("a unit letter on the house number is handled the same in an address and in a Redfin URL", () => {
  const url = "/WA/Seattle/1014-B-NE-70th-St-98115/home/555";
  assert.strictEqual(streetKeyFromRedfinPath(url), "1014 NE 70TH ST|98115");
  assert.strictEqual(streetKeyFromAddress("1014 B NE 70th St", "98115"), "1014 NE 70TH ST|98115");
  assert.strictEqual(unitFromRedfinPath(url), "UNIT-B");
  assert.strictEqual(streetKeyFromRedfinPath("/WA/Seattle/123-N-45th-St-98103/home/9"), "123 N 45TH ST|98103", "a directional is not a unit");
  assert.strictEqual(unitFromRedfinPath("/WA/Seattle/123-N-45th-St-98103/home/9"), "");
});
