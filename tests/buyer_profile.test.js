"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  DEFAULT_PROFILE_MEMORY,
  buildMicromarketProfiles,
  buildProfileCohort,
  normalizeProfileMemory,
  profileScore,
  rowMatchesProfile,
} = require("../buyer_profile.js");

test("normalizeProfileMemory falls back to the saved north Seattle profile", () => {
  const profile = normalizeProfileMemory();
  assert.equal(profile.name, DEFAULT_PROFILE_MEMORY.name);
  assert.equal(profile.homesCount, 12);
  assert.ok(profile.watchlistGroups.includes("Ballard"));
});

test("profileScore and rowMatchesProfile favor north Seattle character homes", () => {
  const strongMatch = {
    pulseWatchlistGroup: "Ballard",
    neighborhoodLabel: "Ballard / Crown Hill",
    typeLabel: "Single Family",
    beds: 4,
    baths: 2.5,
    sqft: 2350,
    yearBuilt: 1918,
    lotSize: 4800,
    mlsParkingType: "Garage-Detached",
    mlsView: "Territorial",
    closePrice: 1460000,
  };
  const weakMatch = {
    pulseWatchlistGroup: "",
    neighborhoodLabel: "Downtown",
    typeLabel: "Condo",
    beds: 1,
    baths: 1,
    sqft: 760,
    yearBuilt: 2008,
    lotSize: 0,
    closePrice: 615000,
  };

  assert.ok(profileScore(strongMatch, DEFAULT_PROFILE_MEMORY) >= 70);
  assert.equal(rowMatchesProfile(strongMatch, DEFAULT_PROFILE_MEMORY), true);
  assert.ok(profileScore(weakMatch, DEFAULT_PROFILE_MEMORY) < 30);
  assert.equal(rowMatchesProfile(weakMatch, DEFAULT_PROFILE_MEMORY), false);
});

test("buildProfileCohort summarizes matching homes in the current slice", () => {
  const rows = [
    {
      pulseWatchlistGroup: "Ballard",
      neighborhoodLabel: "Ballard / Crown Hill",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Single Family",
      beds: 4,
      baths: 2.5,
      sqft: 2280,
      yearBuilt: 1915,
      lotSize: 5000,
      closePrice: 1480000,
      saleToList: 1.08,
      delta: 120000,
      isHotMarket: true,
      saleDate: "2026-03-12",
      mlsDOM: 6,
    },
    {
      pulseWatchlistGroup: "Fremont / Green Lake / Woodland Park",
      neighborhoodLabel: "Fremont / Green Lake / Wallingford",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Single Family",
      beds: 3,
      baths: 1.75,
      sqft: 1710,
      yearBuilt: 1908,
      lotSize: 4200,
      closePrice: 1310000,
      saleToList: 1.05,
      delta: 70000,
      isHotMarket: true,
      saleDate: "2026-02-28",
      mlsCDOM: 7,
    },
    {
      pulseWatchlistGroup: "Queen Anne",
      neighborhoodLabel: "Queen Anne / Magnolia",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Condo",
      beds: 2,
      baths: 2,
      sqft: 1200,
      yearBuilt: 2005,
      lotSize: 0,
      closePrice: 990000,
      saleToList: 0.99,
      delta: -10000,
      isHotMarket: false,
      saleDate: "2026-02-20",
      mlsDOM: 19,
    },
  ];

  const cohort = buildProfileCohort(rows, DEFAULT_PROFILE_MEMORY);
  assert.equal(cohort.rows.length, 2);
  assert.equal(cohort.summary.topMicromarket, "Ballard");
  assert.equal(cohort.summary.medianClosePrice, 1395000);
  assert.equal(cohort.summary.medianBidUp, 95000);
  assert.equal(cohort.summary.hotShare, 1);
});

test("buildMicromarketProfiles produces recent competitive summaries and fit labels", () => {
  const rows = [
    {
      pulseWatchlistGroup: "Ballard",
      neighborhoodLabel: "Ballard / Crown Hill",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Single Family",
      beds: 4,
      baths: 2.5,
      sqft: 2400,
      yearBuilt: 1912,
      lotSize: 5100,
      closePrice: 1500000,
      saleToList: 1.09,
      delta: 145000,
      isHotMarket: true,
      saleDate: "2026-04-12",
      mlsDOM: 5,
    },
    {
      pulseWatchlistGroup: "Ballard",
      neighborhoodLabel: "Ballard",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Single Family",
      beds: 3,
      baths: 2,
      sqft: 1880,
      yearBuilt: 1924,
      lotSize: 4200,
      closePrice: 1325000,
      saleToList: 1.05,
      delta: 82000,
      isHotMarket: true,
      saleDate: "2026-03-30",
      mlsCDOM: 7,
    },
    {
      pulseWatchlistGroup: "Magnolia",
      neighborhoodLabel: "Magnolia",
      dataMode: "MLS_ENRICHED",
      typeLabel: "Single Family",
      beds: 2,
      baths: 1,
      sqft: 1120,
      yearBuilt: 1978,
      lotSize: 6200,
      closePrice: 1410000,
      saleToList: 1.01,
      delta: 25000,
      isHotMarket: false,
      saleDate: "2026-04-05",
      mlsDOM: 14,
    },
  ];

  const profiles = buildMicromarketProfiles(rows, DEFAULT_PROFILE_MEMORY, "2026-04-19");
  const ballard = profiles.find((entry) => entry.group === "Ballard");
  const magnolia = profiles.find((entry) => entry.group === "Magnolia");

  assert.ok(ballard);
  assert.equal(ballard.summary.salesCount, 2);
  assert.equal(ballard.fitLabel, "Very Strong Fit");
  assert.match(ballard.descriptor, /pocket/i);

  assert.ok(magnolia);
  assert.equal(magnolia.summary.salesCount, 1);
  assert.equal(magnolia.fitLabel, "Peripheral Fit");
});
