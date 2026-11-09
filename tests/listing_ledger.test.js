"use strict";

// Synthetic fixtures only.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function load() {
  return import(pathToFileURL(path.resolve(__dirname, "..", "src/domain/listingLedger.mjs")).href);
}

const HEADER = "mlsNumber,address,zip,propertyType,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,listDate,lastDom,lastStatus,lastAskChangeDate,firstAskChangeDate,askChangeCount,trackedFromListing,listDateSource,chainId,chainPosition,chainListDate,chainFirstAsk,chainPriorDays";
const AS_OF = "2026-11-08";

function line({ mls, address = `${mls} Test St`, zip = "98117", type = "Single Family", firstSeen, lastSeen = AS_OF, firstAsk = 1200000, lastAsk = firstAsk, listDate = "", chainId = mls, position = 1, chainListDate = listDate, chainFirstAsk = firstAsk, status = "Active", priorDays = 0, lastSeenActive = lastSeen }) {
  return [mls, address, zip, type, firstSeen, lastSeen, lastSeenActive, firstAsk, lastAsk, listDate, "", status, "", "", 0, "true", "FEED", chainId, position, chainListDate, chainFirstAsk, priorDays].join(",");
}

function ledgerText(entries) {
  return [HEADER, ...entries.map(line)].join("\n");
}

test("parseListingLedger types the columns, maps numeric property types and labels the ZIP", async () => {
  const { parseListingLedger, ledgerAsOf } = await load();
  const rows = parseListingLedger(ledgerText([
    { mls: "1", type: "1", zip: "98107", firstSeen: "2026-11-01", listDate: "2026-10-31" },
    { mls: "2", type: "Townhouse", firstSeen: "2026-10-01", lastSeen: "2026-10-20" },
  ]));
  assert.equal(rows.length, 2);
  assert.equal(rows[0].typeLabel, "Single Family");
  assert.equal(rows[0].neighborhoodLabel, "Ballard");
  assert.equal(rows[0].firstAsk, 1200000);
  assert.equal(rows[1].typeLabel, "Townhouse");
  assert.equal(ledgerAsOf(rows), AS_OF);
  assert.deepEqual(parseListingLedger(""), []);
  assert.deepEqual(parseListingLedger("some,other,file\n1,2,3"), [], "a file without the ledger's columns is not a ledger");
});

test("relisted MLS numbers collapse into one marketing period with the first clock and opening ask", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const periods = buildMarketingPeriods(parseListingLedger(ledgerText([
    { mls: "A3", address: "10 Relist Ave", firstSeen: "2026-10-23", firstAsk: 1028000, lastAsk: 999000, listDate: "2026-10-22", chainId: "A1", position: 3, chainListDate: "2026-08-11", chainFirstAsk: 1200000 },
    { mls: "A1", address: "10 Relist Ave", firstSeen: "2026-08-12", lastSeen: "2026-08-27", firstAsk: 1200000, listDate: "2026-08-11", chainId: "A1", position: 1 },
    { mls: "A2", address: "10 Relist Ave", firstSeen: "2026-08-28", lastSeen: "2026-10-20", firstAsk: 1140000, listDate: "2026-08-27", chainId: "A1", position: 2, chainListDate: "2026-08-11", chainFirstAsk: 1200000 },
    { mls: "B1", firstSeen: "2026-11-05", listDate: "2026-11-04" },
  ])));
  assert.equal(periods.length, 2);
  const relisted = periods.find((period) => period.id === "A1");
  assert.deepEqual(relisted.mlsNumbers, ["A1", "A2", "A3"]);
  assert.equal(relisted.startDate, "2026-08-11");
  assert.equal(relisted.firstSeen, "2026-08-12");
  assert.equal(relisted.lastSeen, AS_OF);
  assert.equal(relisted.openingAsk, 1200000);
  assert.equal(relisted.currentAsk, 999000);
  assert.equal(relisted.relistCount, 2);
  assert.ok(Math.abs(relisted.cutPct - 0.1675) < 1e-9, "cuts taken between listings count");
});

test("a Coming Soon teaser that was replaced is dropped; one still showing counts as supply", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const periods = buildMarketingPeriods(parseListingLedger(ledgerText([
    { mls: "teaser", address: "5 Launch Ln", firstSeen: "2026-11-01", lastSeen: "2026-11-03", status: "Coming Soon" },
    { mls: "real", address: "5 Launch Ln", firstSeen: "2026-11-04", listDate: "2026-11-03" },
    { mls: "upcoming", firstSeen: "2026-11-07", status: "Coming Soon" },
    { mls: "2212548128419413457", address: "6 Seeded St", firstSeen: "2026-09-05", lastSeen: "2026-09-14" }, // seeded teaser, status lost
  ])));
  assert.deepEqual(periods.map((period) => period.id).sort(), ["real", "upcoming"]);
  const { computeSupplySnapshot } = await load();
  const snap = computeSupplySnapshot(periods, { asOf: AS_OF });
  assert.equal(snap.agedCount, 2);
  assert.equal(snap.stageCounts.fresh, 2, "a teaser has been on the market 0 days");
});

test("a ledger written before relist chains still works, one period per MLS number", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const text = [
    "mlsNumber,address,zip,propertyType,firstSeen,lastSeen,firstAsk,lastAsk,listDate",
    "1,1 Old St,98117,Single Family,2026-10-01,2026-11-08,1300000,1250000,2026-09-30",
    "2,2 Old St,98117,Single Family,2026-11-02,2026-11-08,1200000,1200000,",
  ].join("\n");
  const periods = buildMarketingPeriods(parseListingLedger(text));
  assert.equal(periods.length, 2);
  assert.equal(periods[0].startDate, "2026-09-30");
  assert.equal(periods[0].relistCount, 0);
  assert.equal(periods[1].startDate, "", "no list date stays unknown");
  const { computeSupplySnapshot } = await load();
  const snap = computeSupplySnapshot(periods, { asOf: AS_OF, soldMlsNumbers: new Set(["x"]) });
  assert.equal(snap.chainsKnown, false);
  assert.equal(snap.relistShare, null, "unknown, not 0%");
  assert.equal(snap.exits, null, "a relist would be miscounted as a withdrawal");
});

test("filterPeriods applies type, neighborhood and the price band on the current ask", async () => {
  const { parseListingLedger, buildMarketingPeriods, filterPeriods } = await load();
  const periods = buildMarketingPeriods(parseListingLedger(ledgerText([
    { mls: "in", firstSeen: "2026-11-01", firstAsk: 1700000, lastAsk: 1550000 }, // cut into the band
    { mls: "cheap", firstSeen: "2026-11-01", firstAsk: 900000 },
    { mls: "th", type: "3", firstSeen: "2026-11-01" },
    { mls: "mag", zip: "98199", firstSeen: "2026-11-01" },
  ])));
  const base = { type: "Single Family", minClose: 1100000, maxClose: 1600000, neighborhoods: [], excludeTypes: [] };
  assert.deepEqual(filterPeriods(periods, base).map((p) => p.id).sort(), ["in", "mag"]);
  assert.deepEqual(filterPeriods(periods, { ...base, neighborhoods: ["Magnolia"] }).map((p) => p.id), ["mag"]);
  assert.deepEqual(filterPeriods(periods, { ...base, type: "All", maxClose: null, minClose: 0 }).length, 4);
  assert.deepEqual(filterPeriods(periods, { ...base, type: "All", excludeTypes: ["Townhouse"] }).map((p) => p.id).sort(), ["in", "mag"]);
});

function fixturePeriods(mod) {
  return mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    // For sale now
    { mls: "fresh", firstSeen: "2026-11-05", listDate: "2026-11-04" }, // 4 days
    { mls: "sit", firstSeen: "2026-10-10", listDate: "2026-10-09", firstAsk: 1300000, lastAsk: 1235000 }, // 30 days, cut 5%
    { mls: "stale2", address: "9 Chain Rd", firstSeen: "2026-10-01", listDate: "2026-09-30", firstAsk: 1260000, lastAsk: 1260000, chainId: "stale1", position: 2, chainListDate: "2026-07-31", chainFirstAsk: 1400000, priorDays: 59 }, // 39 + 59 = 98 days on the market, cut 10%
    { mls: "stale1", address: "9 Chain Rd", firstSeen: "2026-08-01", lastSeen: "2026-09-28", listDate: "2026-07-31", firstAsk: 1400000, chainId: "stale1", position: 1 },
    { mls: "past", firstSeen: "2026-10-30", listDate: "2026-10-29" }, // 10 days
    { mls: "undated", firstSeen: "2026-10-20" },
    // Left the market long enough ago to judge
    { mls: "soldfast", firstSeen: "2026-08-10", lastSeen: "2026-08-15", listDate: "2026-08-09" },
    { mls: "soldslow", firstSeen: "2026-07-01", lastSeen: "2026-09-10", listDate: "2026-06-30" },
    { mls: "pulled", firstSeen: "2026-07-01", lastSeen: "2026-09-12", listDate: "2026-06-30" },
    // Left too recently to judge
    { mls: "recent", firstSeen: "2026-10-01", lastSeen: "2026-10-30", listDate: "2026-09-30" },
  ])));
}

test("computeSupplySnapshot reads standing supply, age, cuts, relists and pace", async () => {
  const mod = await load();
  const snap = mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF, recentSales: 6, salesWindowDays: 90, soldMlsNumbers: new Set(["soldfast", "soldslow"]) });
  assert.equal(snap.activeCount, 5, "the relisted home counts once");
  assert.equal(snap.agedCount, 4, "the undated listing is left out of age stats, not counted as new");
  assert.equal(snap.medianDays, 20);
  assert.deepEqual(snap.stageCounts, { fresh: 1, past: 1, sitting: 1, stale: 1 });
  assert.equal(snap.longListedShare, 0.25, "30 days is not MORE than 30");
  assert.equal(snap.cutCount, 2);
  assert.equal(snap.cutShare, 0.4);
  assert.ok(Math.abs(snap.medianCutPct - 0.075) < 1e-9);
  assert.equal(snap.medianCutUsd, (65000 + 140000) / 2);
  assert.equal(snap.relistCount, 1);
  assert.equal(snap.newLast7, 1);
  assert.equal(snap.newLast28, 3, "the relist on Oct 1 belongs to a period that began in August");
  assert.equal(snap.monthlySales, 2);
  assert.equal(snap.monthsOfSupply, 2.5);
});

test("age stats are withheld when too many live listings lack a list date", async () => {
  const mod = await load();
  const periods = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "old1", firstSeen: "2026-08-01", listDate: "2026-07-31" },
    { mls: "old2", firstSeen: "2026-08-05", listDate: "2026-08-04" },
    { mls: "new1", firstSeen: "2026-11-01" },
    { mls: "new2", firstSeen: "2026-11-03" },
  ])));
  const snap = mod.computeSupplySnapshot(periods, { asOf: AS_OF });
  assert.equal(snap.activeCount, 4);
  assert.equal(snap.agedCount, 0, "only the oldest listings are dated, which would read as an all-stale market");
  assert.equal(snap.medianDays, null);
  assert.equal(snap.longListedShare, null);
  assert.equal(mod.buildSupplyVerdict(snap), "4 homes are for sale in this slice.");
});

test("exits are judged sold or withdrawn only after closings have had time to record", async () => {
  const mod = await load();
  const snap = mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF, soldMlsNumbers: new Set(["soldfast", "soldslow"]) });
  assert.equal(snap.exits.all.count, 3, "the Oct 30 exit is too recent");
  assert.ok(Math.abs(snap.exits.all.soldShare - 2 / 3) < 1e-9);
  assert.equal(snap.exits.byStage.fresh.soldShare, 1);
  assert.equal(snap.exits.byStage.stale.count, 2);
  assert.equal(snap.exits.byStage.stale.withdrawnShare, 0.5);
  assert.equal(snap.exits.byStage.past.soldShare, null);
  assert.equal(mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF }).exits, null, "no sold list, no verdict on exits");
});

test("an empty slice and a missing pace stay unknown rather than zero", async () => {
  const mod = await load();
  const empty = mod.computeSupplySnapshot([], { asOf: AS_OF, recentSales: 15 });
  assert.equal(empty.activeCount, 0);
  assert.equal(empty.cutShare, null);
  assert.equal(empty.medianDays, null);
  assert.equal(empty.monthsOfSupply, null);
  assert.equal(mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF, recentSales: 0 }).monthsOfSupply, null);
  const oneSale = mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF, recentSales: 1 });
  assert.equal(oneSale.monthsOfSupply, null, "one closing in 90 days is not a pace (it would read as 15 months, roomy)");
  assert.equal(oneSale.recentSales, 1);
  assert.match(mod.buildSupplyVerdict(empty), /No homes for sale/);
});

test("weekly and monthly series count homes standing on each date", async () => {
  const mod = await load();
  const periods = fixturePeriods(mod);
  const weekly = mod.computeSupplyWeeklySeries(periods, { asOf: AS_OF, weeks: 4 });
  assert.deepEqual(weekly.map((point) => point.date), ["2026-10-18", "2026-10-25", "2026-11-01", "2026-11-08"]);
  assert.deepEqual(weekly.map((point) => point.activeCount), [3, 4, 4, 5]);
  const monthly = mod.computeSupplyMonthlySeries(periods, { asOf: AS_OF });
  assert.deepEqual(monthly.map((point) => point.month), ["2026-07", "2026-08", "2026-09", "2026-10", "2026-11"]);
  assert.deepEqual(monthly.map((point) => point.activeInventory), [2, 3, 0, 4, 5], "Sep 30 falls between the relisted home's two listings");
  assert.equal(monthly[4].sampleSize, 5);
  assert.deepEqual(mod.computeSupplyWeeklySeries([], { asOf: AS_OF }), []);
  assert.deepEqual(mod.computeSupplyMonthlySeries(periods, { asOf: "" }), []);
});

test("weeks before the ledger had run for a week are left out", async () => {
  const mod = await load();
  const weekly = mod.computeSupplyWeeklySeries(fixturePeriods(mod), { asOf: AS_OF, weeks: 30 });
  assert.equal(weekly[0].date, "2026-07-12", "the ledger's first sighting is Jul 1");
});

test("verdict and balance labels", async () => {
  const mod = await load();
  const snap = mod.computeSupplySnapshot(fixturePeriods(mod), { asOf: AS_OF, recentSales: 6 });
  assert.equal(mod.buildSupplyVerdict(snap), "5 homes are for sale in this slice, 1 of them in the first week: 25% have been listed over 30 days and 40% already ask less than they first did.");
  const base = { activeCount: 40, agedCount: 40, longListedShare: 0.75, cutShare: 0.6 };
  assert.match(mod.buildSupplyVerdict({ ...base, stageCounts: { fresh: 4 } }), /^40 homes are for sale in this slice, but only 4 are in the competitive first week: 75%/);
  assert.match(mod.buildSupplyVerdict({ ...base, stageCounts: { fresh: 1 } }), /but only 1 is in the competitive/);
  assert.match(mod.buildSupplyVerdict({ ...base, stageCounts: { fresh: 0 } }), /but none is in the competitive/);
  assert.match(mod.buildSupplyVerdict({ ...base, stageCounts: { fresh: 20 } }), /slice, 20 of them in the first week/, "half the market being fresh is not scarcity");
  assert.match(mod.buildSupplyVerdict({ activeCount: 1, agedCount: 1, longListedShare: 0, cutShare: 0, stageCounts: { fresh: 1 } }), /^1 home is for sale in this slice, 1 of them in the first week/);
  assert.equal(mod.supplyBalanceLabel(1.8), "tight");
  assert.equal(mod.supplyBalanceLabel(3), "balanced");
  assert.equal(mod.supplyBalanceLabel(5.5), "roomy");
  assert.equal(mod.supplyBalanceLabel(null), "");
});

test("a home is not standing inventory on the days between its listings", async () => {
  const mod = await load();
  const periods = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "R1", address: "3 Gap St", firstSeen: "2026-09-01", lastSeen: "2026-10-01", listDate: "2026-08-31", chainId: "R1", position: 1 },
    { mls: "R2", address: "3 Gap St", firstSeen: "2026-10-28", listDate: "2026-10-27", chainId: "R1", position: 2, chainListDate: "2026-08-31" },
  ])));
  const weekly = mod.computeSupplyWeeklySeries(periods, { asOf: AS_OF, weeks: 7 });
  assert.deepEqual(weekly.map((point) => [point.date, point.activeCount]), [
    ["2026-09-27", 1], ["2026-10-04", 0], ["2026-10-11", 0], ["2026-10-18", 0], ["2026-10-25", 0], ["2026-11-01", 1], ["2026-11-08", 1],
  ]);
});

test("dates inside a sampling gap are skipped instead of undercounted", async () => {
  const mod = await load();
  const rows = mod.parseListingLedger(ledgerText([
    { mls: "through", firstSeen: "2026-06-08", listDate: "2026-06-01" },
    { mls: "before", firstSeen: "2026-06-08", lastSeen: "2026-06-19", listDate: "2026-06-01" }, // really left some time in the gap
    { mls: "after", firstSeen: "2026-07-05", listDate: "2026-06-22" }, // really listed inside the gap
    { mls: "july", firstSeen: "2026-07-30", lastSeen: "2026-08-01" },
    { mls: "aug", firstSeen: "2026-08-30", lastSeen: "2026-09-01" },
    { mls: "sep", firstSeen: "2026-09-29", lastSeen: "2026-10-01" },
    { mls: "oct", firstSeen: "2026-10-30", lastSeen: "2026-11-01" },
  ]));
  const sampleDays = mod.ledgerSampleDays(rows);
  assert.equal(sampleDays.has("2026-06-19"), true);
  assert.equal(sampleDays.has("2026-06-30"), false);
  const monthly = mod.computeSupplyMonthlySeries(mod.buildMarketingPeriods(rows), { asOf: AS_OF, sampleDays });
  assert.deepEqual(monthly.map((point) => point.month), ["2026-07", "2026-08", "2026-09", "2026-10", "2026-11"], "June 30 sits 11 days from the nearest snapshot");
});

test("periods take the dataset's neighborhood label where their MLS number is known", async () => {
  const mod = await load();
  const periods = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "live", zip: "98146", firstSeen: "2026-11-01" },
    { mls: "gone", zip: "98146", firstSeen: "2026-08-01", lastSeen: "2026-09-01" },
  ])));
  assert.equal(periods[0].neighborhoodLabel, "Seattle (Other)");
  mod.applyDatasetNeighborhoods(periods, new Map([["live", "Arbor Heights"]]));
  assert.equal(periods.find((p) => p.id === "live").neighborhoodLabel, "Arbor Heights");
  assert.equal(periods.find((p) => p.id === "gone").neighborhoodLabel, "Seattle (Other)");
  assert.deepEqual(mod.filterPeriods(periods, { type: "All", neighborhoods: ["Arbor Heights"], maxClose: null }).map((p) => p.id), ["live"]);
});

test("the sales pace uses only the filters the ledger can also apply", async () => {
  const mod = await load();
  const sale = (overrides) => ({ hasActualClose: true, saleDate: "2026-10-01", typeLabel: "Single Family", neighborhoodLabel: "Ballard", closePrice: 1300000, lotSize: 3000, ...overrides });
  const rows = [
    sale({}), sale({ lotSize: 9000 }), sale({ typeLabel: "Townhouse" }), sale({ closePrice: 900000 }), sale({ neighborhoodLabel: "Magnolia" }),
    sale({ saleDate: "2026-08-10" }), // exactly 90 days back: outside
    sale({ saleDate: "2026-11-09" }), // after the ledger's last morning
    sale({ hasActualClose: false }),
  ];
  const filters = { type: "Single Family", minClose: 1100000, maxClose: 1600000, neighborhoods: [], excludeTypes: [], minLot: 5000, scope: "hot10", dateTo: "2026-06-30" };
  assert.equal(mod.countClosingsInLens(rows, filters, { asOf: AS_OF, windowDays: 90 }), 3, "lot, scope and date filters do not apply to either side");
  assert.equal(mod.countClosingsInLens(rows, { ...filters, neighborhoods: ["Magnolia"] }, { asOf: AS_OF, windowDays: 90 }), 1);
  const mixed = [...rows, sale({ typeLabel: "Condo" }), sale({ typeLabel: "Land" })];
  const allTypes = { ...filters, type: "All" };
  assert.equal(mod.countClosingsInLens(mixed, allTypes, { asOf: AS_OF, windowDays: 90 }), 6);
  assert.equal(mod.countClosingsInLens(mixed, allTypes, { asOf: AS_OF, windowDays: 90, trackedTypes: new Set(["Single Family", "Townhouse"]) }), 4, "condo and land closings cannot pace a feed that lists neither");
  assert.deepEqual([...mod.ledgerTrackedTypes([{ typeLabel: "Single Family" }, { typeLabel: "Townhouse" }, { typeLabel: "Single Family" }, { typeLabel: "" }])], ["Single Family", "Townhouse"]);
});

test("days off the market between listings are not days on the market", async () => {
  const mod = await load();
  const periods = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "P1", address: "4 Pause Pl", firstSeen: "2026-07-02", lastSeen: "2026-07-31", listDate: "2026-07-01", chainId: "P1", position: 1 },
    { mls: "P2", address: "4 Pause Pl", firstSeen: "2026-11-04", listDate: "2026-11-03", chainId: "P1", position: 2, chainListDate: "2026-07-01", priorDays: 30 },
  ])));
  const snap = mod.computeSupplySnapshot(periods, { asOf: AS_OF });
  assert.equal(snap.medianDays, 35, "30 days on the first listing plus 5 on this one, not the 130 calendar days since July 1");
  assert.deepEqual(snap.stageCounts, { fresh: 0, past: 0, sitting: 1, stale: 0 });
});

test("a home under contract has left the supply even while it lingers in the feed", async () => {
  const mod = await load();
  const periods = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "open", firstSeen: "2026-10-20", listDate: "2026-10-19" },
    { mls: "contract", firstSeen: "2026-10-20", listDate: "2026-10-19", status: "Active Under Contract", lastSeenActive: "2026-11-02" },
  ])));
  assert.equal(mod.computeSupplySnapshot(periods, { asOf: AS_OF }).activeCount, 1);
  const weekly = mod.computeSupplyWeeklySeries(periods, { asOf: AS_OF, weeks: 2 });
  assert.deepEqual(weekly.map((point) => point.activeCount), [2, 1]);
  const neverOpen = mod.buildMarketingPeriods(mod.parseListingLedger(ledgerText([
    { mls: "open", firstSeen: "2026-10-20", listDate: "2026-10-19" },
    { mls: "late", firstSeen: "2026-11-05", status: "Active Under Contract", lastSeenActive: "" },
  ])));
  assert.equal(mod.computeSupplySnapshot(neverOpen, { asOf: AS_OF }).activeCount, 1, "first seen already under contract: never supply");
  assert.deepEqual(mod.computeSupplyWeeklySeries(neverOpen, { asOf: AS_OF, weeks: 1 }).map((point) => point.activeCount), [1]);
});

// --- ask history --------------------------------------------------------------

// A consistent row: its latest ask and change count follow from the history given.
function withAsks(entries) {
  return [`${HEADER},askHistory`, ...entries.map((entry) => {
    const points = (entry.asks || "").split("|").filter(Boolean);
    const lastAsk = points.length ? Number(points[points.length - 1].split(":")[1]) : entry.lastAsk;
    const row = line({ ...entry, lastAsk: lastAsk ?? entry.firstAsk }).replace(",,,0,true,", `,,,${Math.max(0, points.length - 1)},true,`);
    return `${row},${entry.asks || ""}`;
  })].join("\n");
}

test("ask history is parsed per listing, and is null (not empty) on a ledger without the column", async () => {
  const { parseListingLedger, parseAskHistory, buildMarketingPeriods } = await load();
  assert.deepEqual(parseAskHistory("2026-07-15:1250000|2026-08-02:1199000"), [{ date: "2026-07-15", ask: 1250000 }, { date: "2026-08-02", ask: 1199000 }]);
  assert.deepEqual(parseAskHistory("junk|2026-08-02:"), []);
  const [row] = parseListingLedger(withAsks([{ mls: "1", firstSeen: "2026-07-15", listDate: "2026-07-14", asks: "2026-07-15:1250000|2026-08-02:1199000" }]));
  assert.equal(row.asks.length, 2);
  const old = parseListingLedger(ledgerText([{ mls: "1", firstSeen: "2026-07-15", listDate: "2026-07-14" }]));
  assert.equal(old[0].asks, null);
  assert.equal(buildMarketingPeriods(old)[0].asks, null);
  // The published copy writes every header even when the source lacks the column.
  const blank = parseListingLedger(withAsks([{ mls: "1", firstSeen: "2026-07-15", listDate: "2026-07-14", asks: "" }]));
  assert.equal(blank[0].asks, null, "a blank history is unknown, not 'never cut'");
  const mixed = buildMarketingPeriods(parseListingLedger(withAsks([
    { mls: "A1", address: "10 Relist Ave", firstSeen: "2026-08-02", lastSeen: "2026-08-31", listDate: "2026-08-01", chainId: "A1", position: 1, asks: "" },
    { mls: "A2", address: "10 Relist Ave", firstSeen: "2026-09-10", listDate: "2026-09-09", chainId: "A1", position: 2, asks: "2026-09-10:1150000" },
  ])));
  assert.equal(mixed[0].asks, null, "half a history would hide the cuts in the other half");
  // Three changes on record, two entries: a stub written where git had no depth.
  const stub = `${line({ mls: "1", firstSeen: "2026-07-15", listDate: "2026-07-14", lastAsk: 1150000 })},2026-07-15:1250000|2026-09-01:1150000`;
  assert.equal(parseListingLedger([`${HEADER},askHistory`, stub.replace(",,,0,true,", ",2026-09-01,2026-09-01,1,true,")].join("\n"))[0].asks.length, 2);
  assert.equal(parseListingLedger([`${HEADER},askHistory`, stub.replace(",,,0,true,", ",2026-09-01,2026-08-01,3,true,")].join("\n"))[0].asks, null);
  // The ask moved but no change was recorded: the history stops short of the latest ask.
  const short = `${line({ mls: "1", firstSeen: "2026-11-07", listDate: "2026-11-06", firstAsk: 1295500, lastAsk: 998500 })},2026-11-07:1295500`;
  assert.equal(parseListingLedger([`${HEADER},askHistory`, short].join("\n"))[0].asks, null);
  assert.deepEqual(parseAskHistory("2026-07-01:1250000x|2026-07-02:12.5|2026-07-03:1200000"), [{ date: "2026-07-03", ask: 1200000 }], "same strict grammar as the pipeline");
});

test("a marketing period strings its listings' asks together, so a cheaper relist reads as a cut", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const [period] = buildMarketingPeriods(parseListingLedger(withAsks([
    { mls: "A2", address: "10 Relist Ave", firstSeen: "2026-09-10", listDate: "2026-09-09", chainId: "A1", position: 2, chainListDate: "2026-08-01", priorDays: 30, asks: "2026-09-10:1150000|2026-10-01:1100000" },
    { mls: "A1", address: "10 Relist Ave", firstSeen: "2026-08-02", lastSeen: "2026-08-31", listDate: "2026-08-01", chainId: "A1", position: 1, asks: "2026-08-02:1200000|2026-08-20:1150000" },
  ])));
  assert.deepEqual(period.asks, [
    { date: "2026-08-02", ask: 1200000 }, { date: "2026-08-20", ask: 1150000 }, { date: "2026-10-01", ask: 1100000 },
  ], "relisting at the same ask is not a change");
  assert.deepEqual(period.spans, [
    { from: "2026-08-02", to: "2026-08-31", listDate: "2026-08-01", priorDays: 0 },
    { from: "2026-09-10", to: AS_OF, listDate: "2026-09-09", priorDays: 30 },
  ]);
});

test("a duplicate listing live inside another's stretch does not invent cuts", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const [period] = buildMarketingPeriods(parseListingLedger(withAsks([
    { mls: "A1", address: "3831 Dup Ave", firstSeen: "2026-08-25", listDate: "2026-08-24", firstAsk: 869000, chainId: "A1", position: 1, asks: "2026-08-25:869000" },
    { mls: "A2", address: "3831 Dup Ave", firstSeen: "2026-09-19", lastSeen: "2026-10-14", listDate: "2026-09-18", firstAsk: 849000, lastAsk: 775000, chainId: "A1", position: 2, asks: "2026-09-19:849000|2026-10-01:775000" },
  ])));
  assert.deepEqual(period.asks, [{ date: "2026-08-25", ask: 869000 }]);
  assert.equal(period.currentAsk, 869000);
});

test("a relist that overlaps the old listing by a day takes over when it appears", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const [period] = buildMarketingPeriods(parseListingLedger(withAsks([
    { mls: "B1", address: "9 Handoff St", firstSeen: "2026-08-01", lastSeen: "2026-09-02", listDate: "2026-07-31", chainId: "B1", position: 1, asks: "2026-08-01:1200000|2026-09-02:1190000" },
    { mls: "B2", address: "9 Handoff St", firstSeen: "2026-09-01", listDate: "2026-08-31", chainId: "B1", position: 2, priorDays: 31, asks: "2026-09-01:1150000" },
  ])));
  assert.deepEqual(period.asks, [{ date: "2026-08-01", ask: 1200000 }, { date: "2026-09-01", ask: 1150000 }]);
});

test("a period knows whether it was tracked from the day it listed", async () => {
  const { parseListingLedger, buildMarketingPeriods } = await load();
  const tracked = buildMarketingPeriods(parseListingLedger(ledgerText([{ mls: "1", firstSeen: "2026-10-01", listDate: "2026-09-30" }])));
  assert.equal(tracked[0].trackedFromStart, true);
  const text = ledgerText([{ mls: "1", firstSeen: "2026-06-08", listDate: "2026-03-01" }]).replace(",true,FEED,", ",false,FEED,");
  assert.equal(buildMarketingPeriods(parseListingLedger(text))[0].trackedFromStart, false);
});
