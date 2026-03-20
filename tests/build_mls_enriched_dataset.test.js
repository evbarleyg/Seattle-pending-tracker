"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  MLS_ENRICHMENT_COLUMNS,
  canonicalAddressKey,
  canonicalMlsStreet,
  canonicalMlsStreetNoUnit,
  dedupeRealtorRows,
  findHeaderIndex,
  normalizeBooleanText,
  regionFromFilename,
  normalizeThirdPartyApproval,
  resolveRealtorApns,
  stripTrailingUnit,
} = require("../scripts/build_mls_enriched_dataset.js");

test("findHeaderIndex accepts incremental exports without APN", () => {
  const lines = [
    "Sold And Stats (9)",
    "Listing Number,Street Number,Status,Listing Date,Selling Date,Pending Date,Contractual Date,Listing Price,Original Price,Selling Price,DOM,CDOM",
    "2494511,1521,Active,3/19/2026 12:00:00 AM,,,,800000,800000,,1,1",
  ];
  assert.equal(findHeaderIndex(lines), 1);
});

test("regionFromFilename strips stats/date suffixes and fixes known typo", () => {
  assert.equal(regionFromFilename("Cental Seattle Stats 2_9 to 3_20.csv"), "Central Seattle");
  assert.equal(regionFromFilename("QA_Magnolia Sale Stats.csv"), "QA Magnolia");
  assert.equal(regionFromFilename("Sold And Stats - QA.csv"), "QA Magnolia");
  assert.equal(regionFromFilename("Sold And Stats - NW.csv"), "NW Seattle");
});

test("canonical street helpers normalize matching keys and unitless fallbacks", () => {
  const row = {
    "Street Number": "717",
    Unit: "B",
    "Street Direction": "",
    "Street Name": "Martin Luther King Jr.",
    "Street Suffix": "Wy",
    "Street Post Direction": "S",
    "Zip Code": "98144",
  };
  assert.equal(canonicalMlsStreet(row), "717 B MARTIN LUTHER KING JR WAY S");
  assert.equal(canonicalMlsStreetNoUnit(row), "717 MARTIN LUTHER KING JR WAY S");
  assert.equal(stripTrailingUnit("1220 E Columbia St #105"), "1220 E COLUMBIA ST");
  assert.equal(
    canonicalAddressKey("717 B Martin Luther King Jr Way S", "98144"),
    "717 B MARTIN LUTHER KING JR WAY S|98144"
  );
});

test("resolveRealtorApns prefers direct APN then backfills from listing and county address maps", async () => {
  const rows = [
    {
      apn: "1234567890",
      listingNumber: "direct",
      addressKeyExact: "DIRECT|98101",
      addressKeyNoUnit: "DIRECT|98101",
    },
    {
      apn: "",
      listingNumber: "L1",
      addressKeyExact: "LISTING|98101",
      addressKeyNoUnit: "LISTING|98101",
    },
    {
      apn: "",
      listingNumber: "L2",
      addressKeyExact: "RES-EXACT|98101",
      addressKeyNoUnit: "RES-EXACT|98101",
    },
    {
      apn: "",
      listingNumber: "L3",
      addressKeyExact: "ACCOUNT-EXACT|98101",
      addressKeyNoUnit: "ACCOUNT-EXACT|98101",
    },
    {
      apn: "",
      listingNumber: "L4",
      addressKeyExact: "NO-UNIT-AMBIGUOUS|98101",
      addressKeyNoUnit: "RES-NO-UNIT|98101",
    },
    {
      apn: "",
      listingNumber: "L5",
      addressKeyExact: "NO-UNIT-ACCOUNT|98101",
      addressKeyNoUnit: "ACCOUNT-NO-UNIT|98101",
    },
    {
      apn: "",
      listingNumber: "L6",
      addressKeyExact: "AMBIGUOUS|98101",
      addressKeyNoUnit: "AMBIGUOUS|98101",
    },
  ];

  const countyAddressMaps = {
    resBldgExactByAddress: new Map([
      ["RES-EXACT|98101", new Set(["2222222222"])],
      ["AMBIGUOUS|98101", new Set(["7777777777", "8888888888"])],
    ]),
    resBldgNoUnitByAddress: new Map([
      ["RES-NO-UNIT|98101", new Set(["4444444444"])],
    ]),
    accountExactByAddress: new Map([
      ["ACCOUNT-EXACT|98101", new Set(["3333333333"])],
    ]),
    accountNoUnitByAddress: new Map([
      ["ACCOUNT-NO-UNIT|98101", new Set(["5555555555"])],
    ]),
  };

  const counts = await resolveRealtorApns(rows, {
    listingApnByListingNumber: new Map([["L1", "1111111111"]]),
    countyAddressMaps,
  });

  assert.deepEqual(counts, {
    directApn: 1,
    previousListingNumber: 1,
    resBldgExact: 1,
    accountExact: 1,
    resBldgNoUnit: 1,
    accountNoUnit: 1,
    unresolved: 1,
  });
  assert.equal(rows[1].apn, "1111111111");
  assert.equal(rows[1].apnResolutionMethod, "PREVIOUS_ENRICHED_LISTING_NUMBER");
  assert.equal(rows[2].apn, "2222222222");
  assert.equal(rows[3].apn, "3333333333");
  assert.equal(rows[4].apn, "4444444444");
  assert.equal(rows[5].apn, "5555555555");
  assert.equal(rows[6].apn, "");
  assert.equal(rows[6].apnResolutionMethod, "");
});

test("dedupeRealtorRows keeps the latest duplicate status while inheriting APN from older rows", () => {
  const result = dedupeRealtorRows([
    {
      uid: "old-active",
      listingNumber: "2470141",
      status: "Active",
      isClosed: false,
      apn: "7234600794",
      listingDate: "2026-01-27",
      pendingDate: "",
      contractualDate: "",
      sellingDate: "",
      listingPrice: 899000,
      sellingPrice: 0,
      originalPrice: 899000,
      domRaw: "13",
      cdomRaw: "13",
      dom: 13,
      cdom: 13,
      mlsAddress: "3222 Example St, Seattle WA 98144",
      addressKeyExact: "3222 EXAMPLE ST|98144",
      addressKeyNoUnit: "3222 EXAMPLE ST|98144",
      beds: 3,
      baths: 2.5,
      sqft: 1700,
      yearBuilt: 2018,
      styleCode: "32 - Townhouse",
      subdivision: "Central Area",
      city: "Seattle",
      state: "WA",
      zip: "98144",
    },
    {
      uid: "new-sold",
      listingNumber: "2470141",
      status: "Sold",
      isClosed: true,
      apn: "",
      listingDate: "2026-01-27",
      pendingDate: "2026-02-25",
      contractualDate: "2026-02-25",
      sellingDate: "2026-03-19",
      listingPrice: 899000,
      sellingPrice: 865000,
      originalPrice: 899000,
      domRaw: "29",
      cdomRaw: "29",
      dom: 29,
      cdom: 29,
      mlsAddress: "3222 Example St, Seattle WA 98144",
      addressKeyExact: "3222 EXAMPLE ST|98144",
      addressKeyNoUnit: "3222 EXAMPLE ST|98144",
      beds: 0,
      baths: 0,
      sqft: 0,
      yearBuilt: 0,
      styleCode: "",
      subdivision: "",
      city: "Seattle",
      state: "WA",
      zip: "98144",
    },
  ]);

  assert.equal(result.rows.length, 1);
  assert.deepEqual(result.counts, {
    duplicateListingCount: 1,
    duplicateRowsCollapsed: 1,
    apnConflictListingCount: 0,
    apnConflictRowCount: 0,
  });
  assert.equal(result.rows[0].status, "Sold");
  assert.equal(result.rows[0].sellingDate, "2026-03-19");
  assert.equal(result.rows[0].sellingPrice, 865000);
  assert.equal(result.rows[0].apn, "7234600794");
  assert.equal(result.rows[0].beds, 3);
  assert.equal(result.rows[0].styleCode, "32 - Townhouse");
});

test("dedupeRealtorRows prefers higher DOM snapshot when status is unchanged", () => {
  const result = dedupeRealtorRows([
    {
      uid: "earlier-active",
      listingNumber: "2467242",
      status: "Active",
      isClosed: false,
      apn: "5157700007",
      listingDate: "2026-01-16",
      pendingDate: "",
      contractualDate: "",
      sellingDate: "",
      listingPrice: 859995,
      sellingPrice: 0,
      originalPrice: 859995,
      domRaw: "24",
      cdomRaw: "24",
      dom: 24,
      cdom: 24,
      mlsAddress: "1117 34th Ave B, Seattle WA 98122",
      addressKeyExact: "1117 34TH AVE B|98122",
      addressKeyNoUnit: "1117 34TH AVE|98122",
      beds: 2,
      baths: 1.25,
      sqft: 1130,
      yearBuilt: 2019,
      styleCode: "32 - Townhouse",
      subdivision: "Madrona",
      city: "Seattle",
      state: "WA",
      zip: "98122",
    },
    {
      uid: "later-active",
      listingNumber: "2467242",
      status: "Active",
      isClosed: false,
      apn: "",
      listingDate: "2026-01-16",
      pendingDate: "",
      contractualDate: "",
      sellingDate: "",
      listingPrice: 845000,
      sellingPrice: 0,
      originalPrice: 859995,
      domRaw: "63",
      cdomRaw: "63",
      dom: 63,
      cdom: 63,
      mlsAddress: "1117 34th Ave B, Seattle WA 98122",
      addressKeyExact: "1117 34TH AVE B|98122",
      addressKeyNoUnit: "1117 34TH AVE|98122",
      beds: 0,
      baths: 0,
      sqft: 0,
      yearBuilt: 0,
      styleCode: "",
      subdivision: "",
      city: "Seattle",
      state: "WA",
      zip: "98122",
    },
  ]);

  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].listingPrice, 845000);
  assert.equal(result.rows[0].dom, 63);
  assert.equal(result.rows[0].cdom, 63);
  assert.equal(result.rows[0].apn, "5157700007");
});

test("dedupeRealtorRows leaves conflicting APN duplicates unresolved and reports them", () => {
  const result = dedupeRealtorRows([
    {
      uid: "a",
      listingNumber: "2499999",
      status: "Active",
      apn: "1111111111",
      listingDate: "2026-03-10",
    },
    {
      uid: "b",
      listingNumber: "2499999",
      status: "Sold",
      apn: "2222222222",
      listingDate: "2026-03-10",
      sellingDate: "2026-03-19",
      sellingPrice: 900000,
    },
  ]);

  assert.equal(result.rows.length, 2);
  assert.deepEqual(result.counts, {
    duplicateListingCount: 0,
    duplicateRowsCollapsed: 0,
    apnConflictListingCount: 1,
    apnConflictRowCount: 2,
  });
});

test("MLS enrichment output schema includes rich neighborhood snapshot fields", () => {
  [
    "mlsParkingType",
    "mlsParkingCoveredTotal",
    "mlsTaxesAnnual",
    "mlsBuildingCondition",
    "mlsView",
    "mlsBankOwned",
    "mlsThirdPartyApprovalRequired",
    "mlsNewConstructionState",
    "mlsSquareFootageSource",
  ].forEach((field) => {
    assert.ok(MLS_ENRICHMENT_COLUMNS.includes(field), `${field} should be emitted`);
  });
});

test("rich MLS field normalizers keep explicit false values distinct from blanks", () => {
  assert.equal(normalizeBooleanText("TRUE"), "true");
  assert.equal(normalizeBooleanText("False"), "false");
  assert.equal(normalizeBooleanText(""), "");
  assert.equal(normalizeThirdPartyApproval("None"), "None");
  assert.equal(normalizeThirdPartyApproval("Short Sale"), "Short Sale");
  assert.equal(normalizeThirdPartyApproval(""), "");
});
