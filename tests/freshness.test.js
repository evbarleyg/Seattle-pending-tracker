"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

const NOW = "2026-09-19";

const active = (listDate) => ({ dataMode: "MLS_ENRICHED", mlsStatusNorm: "ACTIVE", hasActualClose: false, listDate });
const sold = (saleDate, extra = {}) => ({ dataMode: "MLS_ENRICHED", mlsStatusNorm: "SOLD", hasActualClose: true, saleDate, hasMarketListPrice: false, ...extra });
const county = (saleDate) => ({ dataMode: "PUBLIC_PROXY", mlsStatusNorm: "", hasActualClose: true, saleDate, hasMarketListPrice: false });

function byId(result) {
  return Object.fromEntries(result.items.map((item) => [item.id, item]));
}

test("each source reports the newest date its own kind of row carries", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  const rows = [
    active("2026-09-19"),
    active("2026-03-20"),
    sold("2026-08-18"), // Redfin sold: recent, but no asking price
    sold("2026-06-12", { hasMarketListPrice: true }),
    sold("2026-02-01", { hasMarketListPrice: true }),
    county("2026-06-24"),
    county("2026-01-05"),
  ];
  const s = byId(computeSourceFreshness(rows, { now: NOW }));

  assert.equal(s.listings.date, "2026-09-19");
  assert.equal(s.listings.ageDays, 0);
  assert.equal(s.sales.date, "2026-08-18");
  assert.equal(s.sales.ageDays, 32);
  assert.equal(s.pricedSales.date, "2026-06-12", "a newer sale without an asking price must not count here");
  assert.equal(s.pricedSales.ageDays, 99);
  assert.equal(s.county.date, "2026-06-24");
});

test("tone follows each source's own cadence, and there is no red state", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  const stale = computeSourceFreshness(
    [active("2026-09-19"), sold("2026-08-18"), sold("2026-06-12", { hasMarketListPrice: true }), county("2026-06-24")],
    { now: NOW }
  );
  const s = byId(stale);
  assert.equal(s.listings.tone, "ok");
  assert.equal(s.sales.tone, "behind", "32 days is past the 28-day allowance");
  assert.equal(s.pricedSales.tone, "behind");
  assert.equal(s.county.tone, "behind");
  stale.items.forEach((item) => assert.ok(["ok", "behind", "unknown"].includes(item.tone)));

  // County resting on its normal 3 to 5 week lag is fine, not behind.
  const normal = computeSourceFreshness(
    [active("2026-09-18"), sold("2026-09-02", { hasMarketListPrice: true }), county("2026-08-20")],
    { now: NOW }
  );
  assert.equal(byId(normal).county.tone, "ok");
});

test("a source with no rows is unknown rather than zero days old", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  const onlyCounty = computeSourceFreshness([county("2026-09-01")], { now: NOW });
  const s = byId(onlyCounty);
  assert.equal(s.listings.date, "");
  assert.equal(s.listings.ageDays, null);
  assert.equal(s.listings.tone, "unknown");
  assert.equal(s.pricedSales.tone, "unknown");
  assert.equal(s.county.tone, "ok");

  computeSourceFreshness([], { now: NOW }).items.forEach((item) => assert.equal(item.tone, "unknown"));
  assert.equal(computeSourceFreshness(null, { now: NOW }).items.length, 4);
  assert.equal(computeSourceFreshness([null, undefined], { now: NOW }).items.length, 4, "sparse arrays do not throw");
});

test("pending and sold rows are not mistaken for active listings", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  const rows = [
    { dataMode: "MLS_ENRICHED", mlsStatusNorm: "PENDING", hasActualClose: false, listDate: "2026-09-19" },
    { dataMode: "MLS_ENRICHED", mlsStatusNorm: "ACTIVE", hasActualClose: true, saleDate: "2026-09-10", listDate: "2026-09-18" },
    active("2026-09-01"),
  ];
  assert.equal(byId(computeSourceFreshness(rows, { now: NOW })).listings.date, "2026-09-01");
});

test("a reported fetch time beats the newest list date for the listings feed", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  // Feed pulled today, but the newest listing in it was listed two days ago.
  // Built from local parts so its local date is Sep 19 in every timezone.
  const fetchedToday = new Date(2026, 8, 19, 6, 52, 0).toISOString();
  const report = { sources: { redfinActives: { fetchedAt: fetchedToday } } };
  const withReport = byId(computeSourceFreshness([active("2026-09-17")], { now: NOW, report }));
  assert.equal(withReport.listings.date, "2026-09-19");
  assert.equal(withReport.listings.ageDays, 0);
  // An older fetch time never drags a fresher row date backwards.
  const oldReport = { sources: { redfinActives: { fetchedAt: new Date(2026, 6, 5, 7, 56, 0).toISOString() } } };
  assert.equal(byId(computeSourceFreshness([active("2026-09-17")], { now: NOW, report: oldReport })).listings.date, "2026-09-17");
  // A report without the block changes nothing.
  assert.equal(byId(computeSourceFreshness([active("2026-09-17")], { now: NOW, report: { generatedAt: "x" } })).listings.date, "2026-09-17");
});

test("a late-evening clock does not age today's rows by a day", async () => {
  const { computeSourceFreshness } = await importModule("src/domain/freshness.mjs");
  const evening = new Date(2026, 8, 19, 22, 30, 0);
  const s = byId(computeSourceFreshness([active("2026-09-19"), sold("2026-09-18", { hasMarketListPrice: true })], { now: evening }));
  assert.equal(s.listings.ageDays, 0);
  assert.equal(s.sales.ageDays, 1);
});

test("describeSalesLag judges one date against the sales threshold it quotes", async () => {
  const { describeSalesLag, FRESHNESS_SOURCES } = await importModule("src/domain/freshness.mjs");
  const okDays = FRESHNESS_SOURCES.find((source) => source.id === "sales").okDays;

  const recent = describeSalesLag("2026-09-05", NOW);
  assert.equal(recent.ageDays, 14);
  assert.equal(recent.tone, "ok");
  assert.match(recent.sentence, /14 days ago, within the usual 28-day lag/);

  const old = describeSalesLag("2026-08-18", NOW);
  assert.equal(old.ageDays, 32);
  assert.equal(old.tone, "behind");
  assert.match(old.sentence, /32 days ago, past the usual 28-day lag/);

  // Right at and just past the limit: the sentence never contradicts its tone
  // (rounding to weeks used to give "4 weeks old, past the usual four weeks").
  const atLimit = describeSalesLag("2026-08-22", NOW);
  assert.equal(atLimit.ageDays, okDays);
  assert.equal(atLimit.tone, "ok");
  const justPast = describeSalesLag("2026-08-21", NOW);
  assert.equal(justPast.tone, "behind");
  assert.match(justPast.sentence, /29 days ago, past the usual 28-day lag/);

  assert.match(describeSalesLag("2026-09-19", NOW).sentence, /That is today, within/);
  assert.match(describeSalesLag("2026-09-18", NOW).sentence, /1 day ago/);
  const none = describeSalesLag("", NOW);
  assert.equal(none.tone, "unknown");
  assert.equal(none.sentence, "");
});

test("formatDuration gives a bare duration for composing sentences", async () => {
  const { formatDuration } = await importModule("src/domain/freshness.mjs");
  assert.equal(formatDuration(1), "1 day");
  assert.equal(formatDuration(9), "9 days");
  assert.equal(formatDuration(32), "5 weeks");
  assert.equal(formatDuration(99), "3 months");
});

test("formatAge is coarse and never says zero for missing data", async () => {
  const { formatAge } = await importModule("src/domain/freshness.mjs");
  assert.equal(formatAge(0), "today");
  assert.equal(formatAge(1), "1 day ago");
  assert.equal(formatAge(9), "9 days ago");
  assert.equal(formatAge(32), "5 weeks ago");
  assert.equal(formatAge(99), "3 months ago");
  assert.equal(formatAge(183), "6 months ago");
  assert.equal(formatAge(null), "no data");
  assert.equal(formatAge(undefined), "no data");
});
