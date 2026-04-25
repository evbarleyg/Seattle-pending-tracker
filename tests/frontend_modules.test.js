"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

test("Vite CSV parser and normalizer preserve buyer defaults", async () => {
  const data = await importModule("src/domain/data.mjs");
  const csv = [
    "id,address,type,closePrice,pendingListPrice,saleDate,zip,neighborhood,mlsStatus,mlsDOM,mlsCDOM,mlsClosePrice,mlsListingPrice",
    "1,\"123 NW Test St, Seattle, WA 98117\",Single Family,1250000,1180000,2026-03-10,98117,,SOLD,6,7,1250000,1180000",
  ].join("\n");
  const rows = data.normalizeRows(data.parseCsv(csv));

  assert.equal(rows.length, 1);
  assert.equal(rows[0].typeLabel, "Single Family");
  assert.equal(rows[0].neighborhoodLabel, "Ballard / Crown Hill");
  assert.equal(rows[0].pulseWatchlistGroup, "Ballard");
  assert.equal(rows[0].saleToList.toFixed(3), "1.059");
});

test("county record links preserve parcel on the live King County endpoint", async () => {
  const data = await importModule("src/domain/data.mjs");
  assert.equal(
    data.countyRecordUrl({ parcelNbr: "0861000050" }),
    "https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr=0861000050"
  );
  assert.equal(
    data.countyRecordUrl({ major: "086100", minor: "0050" }),
    "https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr=0861000050"
  );
});

test("selectors compute filtered slices with default price and type", async () => {
  const { computeSlices, createDefaultFilters, buildOptions } = await importModule("src/domain/selectors.mjs");
  const rows = [
    {
      mapPropertyKey: "a",
      typeLabel: "Single Family",
      neighborhoodLabel: "Ballard",
      dataMode: "MLS_ENRICHED",
      mlsStatusNorm: "SOLD",
      closePrice: 1200000,
      pendingListPrice: 1150000,
      saleDate: "2026-03-10",
      effectiveDate: "2026-03-10",
      saleToList: 1.04,
      delta: 50000,
      pricePerSqft: 600,
      isHotMarket: true,
      isUltraHot: false,
      hasActualClose: true,
    },
    {
      mapPropertyKey: "b",
      typeLabel: "Condo",
      neighborhoodLabel: "Ballard",
      dataMode: "MLS_ENRICHED",
      mlsStatusNorm: "SOLD",
      closePrice: 900000,
      pendingListPrice: 900000,
      saleDate: "2026-03-11",
      effectiveDate: "2026-03-11",
      saleToList: 1,
      delta: 0,
      pricePerSqft: 700,
      isHotMarket: false,
      isUltraHot: false,
      hasActualClose: true,
    },
  ];
  const filters = createDefaultFilters(buildOptions(rows).types);
  const slices = computeSlices(filters, rows, {});

  assert.equal(filters.type, "Single Family");
  assert.equal(filters.minClose, 1100000);
  assert.equal(filters.maxClose, 1400000);
  assert.equal(slices.closedSlice.length, 1);
  assert.equal(slices.stats.medianClose, 1200000);
});

test("bid selector reuses one comp pool and scores active rows", async () => {
  const { buildBidCompPool, computeActiveBidSuggestions, DEFAULT_FILTERS } = await importModule("src/domain/selectors.mjs");
  const comps = Array.from({ length: 7 }, (_, index) => ({
    mapPropertyKey: `comp-${index}`,
    typeLabel: "Single Family",
    neighborhoodLabel: "Ballard",
    zip: "98117",
    dataMode: "MLS_ENRICHED",
    mlsStatusNorm: "SOLD",
    closePrice: 1200000 + index * 1000,
    pendingListPrice: 1100000,
    saleDate: "2026-04-01",
    effectiveDate: "2026-04-01",
    saleToList: 1.06,
    isLikelyPresoldNewBuild: false,
    hasActualClose: true,
  }));
  const active = {
    mapPropertyKey: "active-1",
    typeLabel: "Single Family",
    neighborhoodLabel: "Ballard",
    zip: "98117",
    dataMode: "MLS_ENRICHED",
    mlsStatusNorm: "ACTIVE",
    pendingListPrice: 1125000,
    effectiveDate: "2026-04-12",
    isHotMarket: true,
    isUltraHot: false,
    hasActualClose: false,
  };
  const filters = { ...DEFAULT_FILTERS, type: "Single Family", minClose: 1000000, maxClose: 1400000 };
  const rows = [...comps, active];
  const compPool = buildBidCompPool(filters, rows, "2026-04-20");
  const result = computeActiveBidSuggestions(filters, rows, "balanced", { compPool, nowValue: "2026-04-20" });

  assert.equal(compPool.length, 7);
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].bidStatus, "SCored");
  assert.equal(result.rows[0].bidCompTier, "T1_NEIGHBORHOOD_TYPE");
  assert.ok(result.rows[0].bidSuggested > active.pendingListPrice);
});
