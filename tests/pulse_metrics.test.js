"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  computeRecentComparisons,
  computeMonthlyGroupSeries,
  computeWindowSummary,
  metricDirection,
  pulseWatchlistGroup,
} = require("../pulse_metrics.js");

test("pulseWatchlistGroup maps watchlist neighborhoods as expected", () => {
  assert.equal(pulseWatchlistGroup("Ballard / Crown Hill"), "Ballard");
  assert.equal(pulseWatchlistGroup("Queen Anne / Magnolia"), "Queen Anne");
  assert.equal(pulseWatchlistGroup("Magnolia"), "Magnolia");
  assert.equal(pulseWatchlistGroup("Fremont / Green Lake / Wallingford"), "Fremont / Green Lake / Woodland Park");
  assert.equal(pulseWatchlistGroup("Ravenna / Wedgwood"), "");
});

test("computeWindowSummary compares current and prior windows with equal day spans", () => {
  const rows = [
    {
      saleDate: "2026-04-14",
      isHotMarket: true,
      isUltraHot: true,
      domValue: 4,
      saleToList: 1.08,
      delta: 80000,
      closePrice: 1280000,
    },
    {
      saleDate: "2026-04-05",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 16,
      saleToList: 1.01,
      delta: 15000,
      closePrice: 1180000,
    },
    {
      saleDate: "2026-03-18",
      isHotMarket: true,
      isUltraHot: false,
      domValue: 6,
      saleToList: 1.03,
      delta: 42000,
      closePrice: 1210000,
    },
    {
      saleDate: "2026-03-12",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 18,
      saleToList: 0.99,
      delta: -10000,
      closePrice: 1100000,
    },
  ];

  const summary = computeWindowSummary(rows, "2026-04-18", 30);
  assert.equal(summary.current.salesCount, 2);
  assert.equal(summary.previous.salesCount, 2);
  assert.equal(summary.current.hotShare, 0.5);
  assert.equal(summary.previous.hotShare, 0.5);
  assert.equal(summary.current.medianDom, 10);
  assert.equal(summary.previous.medianDom, 12);
  assert.equal(summary.current.medianSaleToList, 1.045);
  assert.equal(summary.previous.medianSaleToList, 1.01);
  assert.equal(summary.current.medianBidUp, 47500);
  assert.equal(summary.previous.medianBidUp, 16000);
  assert.equal(summary.current.medianClosePrice, 1230000);
  assert.equal(summary.previous.medianClosePrice, 1155000);
});

test("computeRecentComparisons supports 90-day pulse windows", () => {
  const rows = [
    {
      saleDate: "2026-04-10",
      isHotMarket: true,
      isUltraHot: false,
      domValue: 5,
      saleToList: 1.07,
      delta: 65000,
      closePrice: 1290000,
    },
    {
      saleDate: "2026-02-03",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 15,
      saleToList: 1.01,
      delta: 12000,
      closePrice: 1160000,
    },
    {
      saleDate: "2025-12-18",
      isHotMarket: true,
      isUltraHot: true,
      domValue: 4,
      saleToList: 1.08,
      delta: 72000,
      closePrice: 1315000,
    },
    {
      saleDate: "2025-11-29",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 17,
      saleToList: 0.99,
      delta: -8000,
      closePrice: 1110000,
    },
  ];

  const comparison = computeRecentComparisons(rows, "2026-04-18", [90])[0];
  assert.equal(comparison.windowDays, 90);
  assert.equal(comparison.current.salesCount, 2);
  assert.equal(comparison.previous.salesCount, 2);
  assert.equal(comparison.current.medianDom, 10);
  assert.equal(comparison.previous.medianDom, 10.5);
  assert.equal(comparison.current.medianClosePrice, 1225000);
  assert.equal(comparison.previous.medianClosePrice, 1212500);
});

test("computeMonthlyGroupSeries rolls rows into watchlist buckets by month", () => {
  const rows = [
    {
      neighborhoodLabel: "Ballard / Crown Hill",
      saleDate: "2026-03-22",
      isHotMarket: true,
      isUltraHot: true,
      domValue: 5,
      saleToList: 1.07,
      delta: 65000,
      closePrice: 1225000,
    },
    {
      neighborhoodLabel: "Ballard",
      saleDate: "2026-03-04",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 14,
      saleToList: 1.01,
      delta: 12000,
      closePrice: 1140000,
    },
    {
      neighborhoodLabel: "Queen Anne / Magnolia",
      saleDate: "2026-04-08",
      isHotMarket: true,
      isUltraHot: false,
      domValue: 7,
      saleToList: 1.02,
      delta: 25000,
      closePrice: 1330000,
    },
    {
      neighborhoodLabel: "Magnolia",
      saleDate: "2026-04-04",
      isHotMarket: false,
      isUltraHot: false,
      domValue: 19,
      saleToList: 0.98,
      delta: -9000,
      closePrice: 1275000,
    },
  ];

  const series = computeMonthlyGroupSeries(rows, "2026-04-18", 3);

  assert.equal(series.Ballard.length, 3);
  assert.equal(series.Ballard[1].month, "2026-03");
  assert.equal(series.Ballard[1].salesCount, 2);
  assert.equal(series.Ballard[1].hotShare, 0.5);
  assert.equal(series.Ballard[1].medianDom, 9.5);

  assert.equal(series["Queen Anne"][2].month, "2026-04");
  assert.equal(series["Queen Anne"][2].salesCount, 1);
  assert.equal(series["Queen Anne"][2].medianSaleToList, 1.02);

  assert.equal(series.Magnolia[2].salesCount, 1);
  assert.equal(series.Magnolia[2].medianBidUp, -9000);
});

test("metricDirection treats lower DOM as hotter and higher ratios as hotter", () => {
  assert.equal(metricDirection("medianDom", 7, 12), 1);
  assert.equal(metricDirection("medianDom", 18, 10), -1);
  assert.equal(metricDirection("medianSaleToList", 1.05, 1.01), 1);
  assert.equal(metricDirection("medianBidUp", 25000, 40000), -1);
});
