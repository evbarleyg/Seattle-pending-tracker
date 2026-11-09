"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { mergeSold, buildSoldEnrichedRow, daysBetween } = require("../scripts/merge_redfin_sold.js");

const HEADERS = [
  "dataMode", "id", "address", "neighborhood", "type", "typeCode", "addressSource", "major", "minor", "parcelNbr", "assessedValue",
  "listDate", "pendingDate", "saleDate", "listPriceAtPending", "closePrice",
  "beds", "baths", "sqft", "yearBuilt", "zip", "districtName", "sqFtLot", "lat", "lon",
  "mlsClosePrice", "mlsListingNumber", "mlsStatus", "mlsRegion", "mlsSellingDate",
  "mlsListingPrice", "mlsSellingPrice", "mlsDOM", "mlsCDOM", "mlsJoinMethod",
  "saleToListRatio", "saleToOriginalListRatio", "bidUpAmount", "bidUpPct",
];

test("daysBetween measures absolute gap and rejects junk", () => {
  assert.strictEqual(daysBetween("2026-05-10", "2026-05-12"), 2);
  assert.strictEqual(daysBetween("2026-05-12", "2026-05-10"), 2);
  assert.strictEqual(daysBetween("2026-05-10", "2026-05-10"), 0);
  assert.strictEqual(daysBetween("", "2026-05-10"), Infinity);
});

test("buildSoldEnrichedRow maps sold fields and leaves list/ratio blank", () => {
  const r = {
    redfinListingId: "999", mlsListingNumber: "2200001", soldDate: "2026-05-02",
    soldPrice: "800000", domDays: "6", beds: "3", baths: "2", sqft: "1800",
    yearBuilt: "1990", zip: "98107", lotSize: "4000", lat: "47.6", lon: "-122.3",
    uiPropertyType: "1", neighborhood: "Ballard", address: "1 A St", queryLabel: "Ballard",
  };
  const row = buildSoldEnrichedRow(r, HEADERS);
  assert.strictEqual(row.dataMode, "MLS_ENRICHED");
  assert.strictEqual(row.mlsJoinMethod, "REDFIN_SOLD");
  assert.strictEqual(row.mlsStatus, "Sold");
  assert.strictEqual(row.addressSource, "REDFIN_SOLD");
  assert.strictEqual(row.saleDate, "2026-05-02");
  assert.strictEqual(row.mlsSellingDate, "2026-05-02");
  assert.strictEqual(row.closePrice, "800000");
  assert.strictEqual(row.mlsClosePrice, "800000");
  assert.strictEqual(row.type, "Single Family");
  assert.strictEqual(row.mlsDOM, "6");
  // Never fabricate a list price or sale/list ratio from a sold-only feed.
  assert.strictEqual(row.listPriceAtPending, "");
  assert.strictEqual(row.mlsListingPrice, "");
  assert.strictEqual(row.saleToListRatio, "");
  assert.strictEqual(row.bidUpAmount, "");
});

test("mergeSold skips dupes (MLS# + address/date), appends new, strips prior REDFIN_SOLD", () => {
  const existing = [
    { mlsListingNumber: "111", address: "1 A St", zip: "98107", saleDate: "2026-05-01", mlsSellingDate: "2026-05-01", mlsJoinMethod: "MLS_SOLD_NOT_IN_COUNTY" },
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", saleDate: "2026-05-10", mlsSellingDate: "", closePrice: "900000", mlsJoinMethod: "" }, // county sold, no MLS#
    { mlsListingNumber: "", address: "Old Sold", zip: "98103", saleDate: "2026-04-01", mlsSellingDate: "", mlsJoinMethod: "REDFIN_SOLD" }, // prior run -> stripped
  ];
  const redfin = [
    { mlsListingNumber: "111", address: "1 A St", zip: "98107", soldDate: "2026-05-02", soldPrice: "800000", uiPropertyType: "1", queryLabel: "Ballard" }, // dup by MLS#
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2026-05-12", soldPrice: "900000", uiPropertyType: "1", queryLabel: "Wedgwood" }, // dup by addr (+2d)
    { mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2025-01-01", soldPrice: "700000", uiPropertyType: "1", queryLabel: "Wedgwood" }, // same addr, prior-year sale -> appended
    { mlsListingNumber: "222", address: "9 Z Pl", zip: "98117", soldDate: "2026-06-01", soldPrice: "1200000", uiPropertyType: "3", queryLabel: "Ballard" }, // brand new -> appended
  ];
  const { finalRows, report } = mergeSold(HEADERS, existing, redfin, {});

  assert.strictEqual(report.priorRedfinSoldRemoved, 1);
  assert.strictEqual(report.matchedByMls, 1);
  assert.strictEqual(report.matchedByAddr, 1);
  assert.strictEqual(report.appended, 2);
  assert.strictEqual(report.rowsAfter, 4); // 2 curated kept + 2 appended
  assert.deepStrictEqual(report.appendedByMonth, { "2025-01": 1, "2026-06": 1 });
  // curated rows are preserved untouched; appended rows are all REDFIN_SOLD
  const appended = finalRows.filter((r) => r.mlsJoinMethod === "REDFIN_SOLD");
  assert.strictEqual(appended.length, 2);
  assert.ok(finalRows.some((r) => r.mlsListingNumber === "111" && r.mlsJoinMethod === "MLS_SOLD_NOT_IN_COUNTY"));
});

test("mergeSold honors a minSoldDate window (drops older sales)", () => {
  const redfin = [
    { mlsListingNumber: "", address: "A St", zip: "98107", soldDate: "2025-03-01", soldPrice: "500000", uiPropertyType: "1", queryLabel: "X" },
    { mlsListingNumber: "", address: "B St", zip: "98107", soldDate: "2024-08-01", soldPrice: "500000", uiPropertyType: "1", queryLabel: "X" }, // older -> dropped
  ];
  const { report } = mergeSold(HEADERS, [], redfin, { minSoldDate: "2024-12-18" });
  assert.strictEqual(report.appended, 1);
  assert.strictEqual(report.droppedOutOfWindow, 1);
});

test("mergeSold respects a custom match-day window", () => {
  const existing = [{ mlsListingNumber: "", address: "2 B Ave", zip: "98115", saleDate: "2026-05-10", closePrice: "905000", mlsJoinMethod: "" }];
  const redfin = [{ mlsListingNumber: "", address: "2 B Ave", zip: "98115", soldDate: "2026-05-20", soldPrice: "900000", uiPropertyType: "1", queryLabel: "X" }]; // 10 days apart
  assert.strictEqual(mergeSold(HEADERS, existing, redfin, { matchDays: 14 }).report.appended, 0); // within window -> dup
  assert.strictEqual(mergeSold(HEADERS, existing, redfin, { matchDays: 5 }).report.appended, 1); // outside window -> appended
});

// County address spellings that used to slip past the street key and put the
// same sale in the dataset twice (about 400 of 2,937 REDFIN_SOLD rows on
// 2026-11-08). `county` here is a county sale an MLS export already enriched.
function county(address, zip, saleDate, closePrice) {
  return { dataMode: "MLS_ENRICHED", mlsListingNumber: "", mlsListingPrice: String(closePrice), address, zip, saleDate, closePrice: String(closePrice), mlsJoinMethod: "APN_PRICE_DATE_WINDOW" };
}
function bareCounty(address, zip, saleDate, closePrice, parcelNbr = "1234567890") {
  return { dataMode: "PUBLIC_PROXY", id: `excise-${parcelNbr}`, mlsListingNumber: "", mlsListingPrice: "", address, zip, saleDate, closePrice: String(closePrice), major: parcelNbr.slice(0, 6), minor: parcelNbr.slice(6), parcelNbr, assessedValue: "700000", mlsJoinMethod: "" };
}
function sold(address, zip, soldDate, soldPrice) {
  return { mlsListingNumber: "", address, zip, soldDate, soldPrice: String(soldPrice), uiPropertyType: "1", queryLabel: "X" };
}

test("mergeSold recognises the county spelling of a sale it already holds", () => {
  const cases = [
    [county("1025 NE 130TH ST, Seattle", "98125", "2026-06-16", 1300000), sold("1025 NE 130th St", "98125", "2026-06-16", 1300000), "bare city tail"],
    [county("5179 42ND AVENUE SOUTH, SEATTLE WA 98118", "98118", "2026-05-18", 821000), sold("5179 42nd Ave S", "98118", "2026-05-20", 821000), "spelled-out directional"],
    [county("4412 A CORLISS AVE N, Seattle 98103", "98103", "2026-06-18", 725000), sold("4412 Corliss Ave N Unit A", "98103", "2026-06-24", 725000), "unit letter on the house number"],
    [county("4113C CHILBERG AVE SW, SEATTLE WA 98116", "98116", "2026-06-09", 1160000), sold("4113 Chilberg Ave SW Unit C", "98116", "2026-06-10", 1160000), "unit letter attached"],
  ];
  for (const [existing, redfin, label] of cases) {
    const { report } = mergeSold(HEADERS, [existing], [redfin], {});
    assert.strictEqual(report.appended, 0, label);
    assert.strictEqual(report.matchedByAddr, 1, label);
  }
});

test("mergeSold falls back to house number + exact price when the street text cannot be reconciled", () => {
  const cases = [
    [county("3028 61ST AVE SW, Seattle", "", "2026-06-22", 795000), sold("3028 61st Ave SW", "98116", "2026-06-24", 795000), "county row has no ZIP"],
    [county("1611 10TH W UNIT 1, SEATTLE WA 98119", "98119", "2026-06-05", 1479000), sold("1611 10th Ave W #1", "98119", "2026-06-11", 1479000), "county dropped the street type"],
  ];
  for (const [existing, redfin, label] of cases) {
    const { report } = mergeSold(HEADERS, [existing], [redfin], {});
    assert.strictEqual(report.appended, 0, label);
    assert.strictEqual(report.matchedByPrice, 1, label);
  }
  const otherZip = mergeSold(HEADERS, [county("3028 MAIN ST, Seattle 98104", "98104", "2026-06-22", 795000)], [sold("3028 61st Ave SW", "98116", "2026-06-24", 795000)], {});
  assert.strictEqual(otherZip.report.appended, 1, "same number and price in another ZIP is a different home");
  const otherMonth = mergeSold(HEADERS, [county("3028 61ST AVE SW, Seattle", "", "2026-03-01", 795000)], [sold("3028 61st Ave SW", "98116", "2026-06-24", 795000)], {});
  assert.strictEqual(otherMonth.report.appended, 1, "outside the date window");
  const lateDeed = mergeSold(HEADERS, [county("6542 43RD AVE NORTHEAST", "98115", "2026-05-31", 1060000)], [sold("6542 43rd Ave NE", "98115", "2026-06-24", 1060000)], {});
  assert.strictEqual(lateDeed.report.appended, 0, "24 days apart at the exact price is still one sale");
  assert.strictEqual(lateDeed.report.matchedByPrice, 1);
});

test("mergeSold keeps townhouse neighbours apart and ignores a county row recorded at $0", () => {
  const neighbours = mergeSold(
    HEADERS,
    [county("2851B NW 70TH ST, SEATTLE WA 98117", "98117", "2026-05-13", 895000)],
    [sold("2851 NW 70th St Unit C", "98117", "2026-05-14", 915000), sold("2851 NW 70th St Unit D", "98117", "2026-05-15", 895000)],
    {},
  );
  assert.strictEqual(neighbours.report.appended, 2, "units C and D are not unit B, even at the same price");
  const unpriced = mergeSold(HEADERS, [county("9022 36TH AVE SW, Seattle 98126", "98126", "2026-06-08", 0)], [sold("9022 36th Ave SW", "98126", "2026-06-09", 499000)], {});
  assert.strictEqual(unpriced.report.appended, 1, "the app hides $0 rows, so the priced sale must come in");
});

test("a bare county record of the same sale gives way to the Redfin row, which keeps its parcel", () => {
  const twin = bareCounty("1025 NE 130TH ST, Seattle", "98125", "2026-06-16", 1300000, "6411000123");
  const other = bareCounty("77 Unrelated Ave, Seattle 98125", "98125", "2026-06-16", 800000, "9999990001");
  const redfin = { ...sold("1025 NE 130th St", "98125", "2026-06-18", 1300000), mlsListingNumber: "2555001", redfinListingId: "42" };
  const { finalRows, report } = mergeSold(HEADERS, [twin, other], [redfin], {});
  assert.strictEqual(report.appended, 1);
  assert.strictEqual(report.replacedCounty, 1);
  assert.strictEqual(report.countyRowsSuperseded, 1);
  assert.strictEqual(report.matchedTotal, 0);
  assert.strictEqual(finalRows.length, 2, "one row per sale");
  const row = finalRows.find((r) => r.mlsJoinMethod === "REDFIN_SOLD");
  assert.strictEqual(row.mlsListingNumber, "2555001", "the MLS# survives, so list price and DOM can be filled later");
  assert.strictEqual(row.parcelNbr, "6411000123");
  assert.strictEqual(row.major, "641100");
  assert.strictEqual(row.assessedValue, "700000");
  assert.ok(finalRows.includes(other), "unrelated county sales are untouched");

  // Next morning: the county twin is gone from the file and the Redfin row is rebuilt.
  const rerun = mergeSold(HEADERS, finalRows, [redfin], {});
  assert.strictEqual(rerun.finalRows.length, 2);
  assert.strictEqual(rerun.report.replacedCounty, 0);
  assert.strictEqual(rerun.finalRows.find((r) => r.mlsJoinMethod === "REDFIN_SOLD").parcelNbr, "6411000123", "parcel identity is carried across reruns");
});

test("every parcel row of one county sale is replaced once, and a second unit is not swallowed by the same twin", () => {
  const parcels = [bareCounty("7520 57TH PL NE, Seattle 98115", "98115", "2026-05-02", 700000, "1111110001"), bareCounty("7520 57TH PL NE, Seattle 98115", "98115", "2026-05-02", 700000, "1111110002")];
  const one = mergeSold(HEADERS, parcels, [sold("7520 57th Pl NE", "98115", "2026-05-04", 700000)], {});
  assert.strictEqual(one.finalRows.length, 1);
  assert.strictEqual(one.report.countyRowsSuperseded, 2);

  const noUnit = [bareCounty("901 28TH AVE S, Seattle 98144", "98144", "2026-04-29", 739000)];
  const two = mergeSold(HEADERS, noUnit, [sold("901 28th Ave S Unit H", "98144", "2026-04-30", 739000), sold("901 28th Ave S Unit G", "98144", "2026-04-30", 679000)], {});
  assert.strictEqual(two.report.appended, 2);
  assert.strictEqual(two.finalRows.length, 2);
});

test("a replacement row outlives the feed that supplied it, because the county row it replaced is gone", () => {
  const twin = bareCounty("1025 NE 130TH ST, Seattle", "98125", "2025-10-04", 1300000, "6411000123");
  const redfin = { ...sold("1025 NE 130th St", "98125", "2025-10-04", 1300000), mlsListingNumber: "2555001", redfinListingId: "42" };
  const first = mergeSold(HEADERS, [twin], [redfin], {});
  first.finalRows[0].mlsListingPrice = "1250000"; // enriched later in the same run
  const afterPrune = mergeSold(HEADERS, first.finalRows, [], {}); // the 400-day prune dropped it from the cumulative file
  assert.strictEqual(afterPrune.finalRows.length, 1, "the sale is still in the dataset");
  assert.strictEqual(afterPrune.report.replacementsCarriedOver, 1);
  assert.strictEqual(afterPrune.finalRows[0].mlsListingPrice, "1250000", "carried over as it stood");
  const plain = mergeSold(HEADERS, [{ id: "redfin-sold-9", mlsJoinMethod: "REDFIN_SOLD", address: "9 Z Pl", zip: "98117", saleDate: "2025-10-04", closePrice: "900000", parcelNbr: "" }], [], {});
  assert.strictEqual(plain.finalRows.length, 0, "a row that replaced nothing is still stripped with the feed");
});

test("one closing listed twice in the feed is appended once", () => {
  const a = { ...sold("3826 35th Ave W", "98199", "2026-09-24", 1100000), mlsListingNumber: "2591996", redfinListingId: "224197600" };
  const b = { ...a, soldDate: "2026-09-26" };
  const { finalRows, report } = mergeSold(HEADERS, [], [a, b], {});
  assert.strictEqual(finalRows.length, 1);
  assert.strictEqual(report.appended, 1);

  // The same closing under a second listing id, with and without the unit, once
  // the county twin that used to absorb both has been replaced.
  const twin = bareCounty("6206 43RD AVE NE, Seattle 98115", "98115", "2026-10-06", 785000);
  const first = { ...sold("6206 43rd Ave NE", "98115", "2026-10-08", 785000), redfinListingId: "100" };
  const second = { ...sold("6206 43rd Ave NE Unit B", "98115", "2026-10-08", 785000), redfinListingId: "101" };
  const pair = mergeSold(HEADERS, [twin], [first, second], {});
  assert.strictEqual(pair.finalRows.length, 1);
  assert.strictEqual(pair.report.replacedCounty, 1);
});

test("only a county record at the same price is replaced; other deeds at the address stand", () => {
  const house = bareCounty("2410 EVERETT AVE E, Seattle 98102", "98102", "2026-03-10", 2500000, "0000000352");
  const lot = bareCounty("2410 EVERETT AVE E, Seattle 98102", "98102", "2026-03-10", 350000, "0000000353");
  const { finalRows, report } = mergeSold(HEADERS, [lot, house], [sold("2410 Everett Ave E", "98102", "2026-03-12", 2500000)], {});
  assert.strictEqual(report.countyRowsSuperseded, 1);
  assert.ok(finalRows.includes(lot), "the $350K parcel next door is its own sale");
  assert.strictEqual(finalRows.find((r) => r.mlsJoinMethod === "REDFIN_SOLD").parcelNbr, "0000000352", "parcel comes from the record that matches on price");
  const rerun = mergeSold(HEADERS, finalRows, [sold("2410 Everett Ave E", "98102", "2026-03-12", 2500000)], {});
  assert.strictEqual(rerun.report.appended, 1, "next morning the lot's deed does not block the house's row from being rebuilt");
  assert.strictEqual(rerun.report.replacementsCarriedOver, 0);
  assert.strictEqual(rerun.finalRows.length, 2);

  const disputed = bareCounty("12233 12TH AVE NW, Seattle 98177", "98177", "2025-12-26", 3770000);
  const kept = mergeSold(HEADERS, [disputed], [sold("12233 12th Ave NW", "98177", "2025-12-30", 2970000)], {});
  assert.deepStrictEqual(kept.finalRows, [disputed], "same home, same week, different price: the county row stands and nothing is added");
  assert.strictEqual(kept.report.matchedByAddr, 1);
});
