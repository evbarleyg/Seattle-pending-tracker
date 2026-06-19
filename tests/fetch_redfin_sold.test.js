"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  snapSoldWithinDays,
  buildSoldParams,
  isSoldHome,
  epochMsToIso,
  homeToRow,
  passesFilters,
  dedupeRows,
  monthOf,
} = require("../scripts/fetch_redfin_sold.js");

test("buildSoldParams sends the verified sold filter and never sends sf", () => {
  const p = buildSoldParams(
    { regionId: 121, regionType: 1, market: "seattle" },
    { maxHomes: 350, soldWithinDays: 365 },
  );
  assert.strictEqual(p.get("sold_within_days"), "365");
  assert.strictEqual(p.get("status"), "9");
  assert.strictEqual(p.get("region_id"), "121");
  assert.strictEqual(p.get("region_type"), "1");
  assert.strictEqual(p.get("num_homes"), "350");
  // `sf` pollutes a sold search with active listings — it must NOT be present.
  assert.strictEqual(p.get("sf"), null);
  // omitted to keep region attribution clean.
  assert.strictEqual(p.get("include_nearby_homes"), null);
});

test("buildSoldParams honors a custom sold window", () => {
  const p = buildSoldParams({ regionId: 1, regionType: 1 }, { maxHomes: 350, soldWithinDays: 90 });
  assert.strictEqual(p.get("sold_within_days"), "90");
});

test("epochMsToIso converts Redfin epoch-ms sold dates and rejects junk", () => {
  assert.strictEqual(epochMsToIso(1766044800000), "2025-12-18");
  assert.strictEqual(epochMsToIso(0), "");
  assert.strictEqual(epochMsToIso(null), "");
  assert.strictEqual(epochMsToIso(undefined), "");
  assert.strictEqual(epochMsToIso("not-a-number"), "");
});

test("isSoldHome keeps Closed/searchStatus-4 rows with a sold date, drops the rest", () => {
  assert.strictEqual(isSoldHome({ mlsStatus: "Closed", soldDate: 1766044800000 }), true);
  assert.strictEqual(isSoldHome({ searchStatus: 4, soldDate: 1766044800000 }), true);
  assert.strictEqual(isSoldHome({ mlsStatus: "Active", searchStatus: 1, soldDate: 1766044800000 }), false);
  assert.strictEqual(isSoldHome({ mlsStatus: "Closed" }), false); // no sold date
  assert.strictEqual(isSoldHome(null), false);
});

test("homeToRow maps the sold gis payload shape", () => {
  const home = {
    propertyId: 18383,
    listingId: 999,
    mlsId: { value: "2200001" },
    mlsStatus: "Closed",
    searchStatus: 4,
    soldDate: 1766044800000,
    price: { value: 660000 },
    pricePerSqFt: { value: 700 },
    dom: { value: 6 },
    sqFt: { value: 942 },
    lotSize: { value: 0 },
    yearBuilt: { value: 2019 },
    beds: 2,
    baths: 2,
    fullBaths: 2,
    stories: 1,
    hoa: { value: 450 },
    streetLine: { value: "1762 NW 57th St" },
    unitNumber: { value: "#501" },
    city: "Seattle",
    state: "WA",
    zip: "98107",
    location: { value: "Ballard" },
    uiPropertyType: 2,
    latLong: { value: { latitude: 47.6, longitude: -122.3 } },
    listingAgent: { name: "A Agent" },
    listingBroker: { name: "L Broker" },
    sellingBroker: { name: "S Broker" },
    url: "/WA/Seattle/1762-NW-57th-St-98107/unit-501/home/18383",
  };
  const row = homeToRow(home, { label: "Ballard" }, "2026-06-19T00:00:00.000Z");
  assert.strictEqual(row.soldDate, "2025-12-18");
  assert.strictEqual(row.soldPrice, 660000);
  assert.strictEqual(row.mlsStatus, "Closed");
  assert.strictEqual(row.address, "1762 NW 57th St #501");
  assert.strictEqual(row.neighborhood, "Ballard");
  assert.strictEqual(row.propertyType, "Condo");
  assert.strictEqual(row.sellingBrokerName, "S Broker");
  assert.strictEqual(row.mlsListingNumber, "2200001");
  assert.strictEqual(row.domDays, 6);
  assert.strictEqual(row.redfinUrl, "https://www.redfin.com/WA/Seattle/1762-NW-57th-St-98107/unit-501/home/18383");
  assert.strictEqual(row.queryLabel, "Ballard");
});

test("passesFilters honors the price window and property-type allowlist", () => {
  const f = { minPrice: 500000, maxPrice: 2000000, uiPropertyTypes: [1, 3] };
  assert.strictEqual(passesFilters({ soldPrice: 700000, uiPropertyType: 1 }, f), true);
  assert.strictEqual(passesFilters({ soldPrice: 400000, uiPropertyType: 1 }, f), false); // below min
  assert.strictEqual(passesFilters({ soldPrice: 700000, uiPropertyType: 2 }, f), false); // type excluded
  assert.strictEqual(passesFilters({ soldPrice: 700000, uiPropertyType: 1 }, {}), true); // no filters
});

test("dedupeRows collapses by listing id and falls back to address+zip+soldDate", () => {
  const rows = [
    { redfinListingId: "1", address: "A", zip: "1", soldDate: "2026-01-01" },
    { redfinListingId: "1", address: "A", zip: "1", soldDate: "2026-01-01" }, // dup by id
    { redfinListingId: "", redfinPropertyId: "", mlsListingNumber: "", address: "B", zip: "2", soldDate: "2026-02-02" },
    { redfinListingId: "", redfinPropertyId: "", mlsListingNumber: "", address: "B", zip: "2", soldDate: "2026-02-02" }, // dup by fallback
    { redfinListingId: "", redfinPropertyId: "", mlsListingNumber: "", address: "B", zip: "2", soldDate: "2026-03-03" }, // later resale kept
  ];
  assert.strictEqual(dedupeRows(rows).length, 3);
});

test("snapSoldWithinDays rounds to the canonical Redfin buckets it honors", () => {
  assert.strictEqual(snapSoldWithinDays(120), 90); // verified live: 120 behaves exactly as 90
  assert.strictEqual(snapSoldWithinDays(90), 90);
  assert.strictEqual(snapSoldWithinDays(200), 180);
  assert.strictEqual(snapSoldWithinDays(365), 365);
  assert.strictEqual(snapSoldWithinDays(5), 7); // clamp up to the smallest bucket
  assert.strictEqual(snapSoldWithinDays(99999), 1825);
  assert.strictEqual(snapSoldWithinDays(0), 365); // falls back to default
});

test("monthOf extracts YYYY-MM for cohorting", () => {
  assert.strictEqual(monthOf("2026-03-14"), "2026-03");
  assert.strictEqual(monthOf(""), "");
  assert.strictEqual(monthOf(null), "");
});
