"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const {
  LEDGER_COLUMNS,
  isActiveStatus,
  toPacificDate,
  observationsFromActives,
  observationsFromEnrichedSnapshot,
  ledgerRowsToMap,
  upsertObservations,
  ledgerMapToRows,
} = require("../scripts/build_active_ledger.js");

function obs(overrides = {}) {
  return {
    mlsNumber: "2400001", date: "2026-07-17", ask: 1150000, status: "Active", dom: "0", cdom: "0",
    listDate: "2026-07-17", redfinPropertyId: "288413", redfinListingId: "223584238",
    url: "https://www.redfin.com/WA/Seattle/x/home/288413", address: "10037 15th Ave NW", zip: "98177", propertyType: "Single Family",
    ...overrides,
  };
}

test("toPacificDate dates a 06:39 PDT fetch on the morning it ran", () => {
  assert.strictEqual(toPacificDate("2026-09-19T13:39:44.613Z"), "2026-09-19");
  assert.strictEqual(toPacificDate("2026-09-20T06:30:00.000Z"), "2026-09-19"); // 23:30 PDT the day before
  assert.strictEqual(toPacificDate("garbage"), "");
});

test("isActiveStatus: Active / Coming Soon / First Look are active, Active Under Contract is not", () => {
  assert.strictEqual(isActiveStatus("Active"), true);
  assert.strictEqual(isActiveStatus("coming soon"), true);
  assert.strictEqual(isActiveStatus("First Look"), true);
  assert.strictEqual(isActiveStatus("Active Under Contract"), false);
  assert.strictEqual(isActiveStatus(""), false);
});

test("a new MLS# is inserted with first = last, and later observations advance the last-* fields only", () => {
  const map = new Map();
  let counts = upsertObservations(map, [obs()]);
  assert.deepStrictEqual(counts, { inserted: 1, updated: 0 });
  const e = map.get("2400001");
  assert.strictEqual(e.firstSeen, "2026-07-17");
  assert.strictEqual(e.lastSeen, "2026-07-17");
  assert.strictEqual(e.lastSeenActive, "2026-07-17");
  assert.strictEqual(e.firstAsk, "1150000");
  assert.strictEqual(e.lastAsk, "1150000");
  assert.strictEqual(e.lastActiveAsk, "1150000");

  counts = upsertObservations(map, [obs({ date: "2026-07-30", ask: 1100000, dom: "13", cdom: "13" })]);
  assert.deepStrictEqual(counts, { inserted: 0, updated: 1 });
  assert.strictEqual(e.firstSeen, "2026-07-17");
  assert.strictEqual(e.firstAsk, "1150000", "the opening ask is kept as firstAsk");
  assert.strictEqual(e.lastSeen, "2026-07-30");
  assert.strictEqual(e.lastAsk, "1100000", "the reduced ask becomes lastAsk");
  assert.strictEqual(e.lastActiveAsk, "1100000");
  assert.strictEqual(e.lastSeenActive, "2026-07-30");
  assert.strictEqual(e.lastDom, "13");
  assert.strictEqual(e.listDate, "2026-07-17");
});

test("Active Under Contract advances lastSeen but not lastSeenActive, so pending = last genuinely active day", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ date: "2026-08-01", ask: 1100000 }),
    obs({ date: "2026-08-05", ask: 1100000, status: "Active Under Contract", dom: "19" }),
    obs({ date: "2026-08-06", ask: 1100000, status: "Active Under Contract", dom: "20" }),
  ]);
  const e = map.get("2400001");
  assert.strictEqual(e.lastSeen, "2026-08-06");
  assert.strictEqual(e.lastStatus, "Active Under Contract");
  assert.strictEqual(e.lastSeenActive, "2026-08-01");
  assert.strictEqual(e.lastActiveAsk, "1100000");
});

test("observations are applied in date order even when given out of order, and reruns are idempotent", () => {
  const map = new Map();
  const batch = [obs({ date: "2026-07-30", ask: 1100000 }), obs({ date: "2026-07-17", ask: 1150000 })];
  upsertObservations(map, batch);
  const before = JSON.stringify(map.get("2400001"));
  assert.strictEqual(map.get("2400001").firstAsk, "1150000");
  assert.strictEqual(map.get("2400001").lastAsk, "1100000");
  upsertObservations(map, batch); // same observations again
  assert.strictEqual(JSON.stringify(map.get("2400001")), before);
});

test("blank identifiers are filled from later observations but never overwritten", () => {
  const map = new Map();
  upsertObservations(map, [obs({ redfinPropertyId: "", url: "", date: "2026-07-17" })]); // e.g. a git-seeded row
  upsertObservations(map, [obs({ redfinPropertyId: "288413", url: "https://r/x", date: "2026-07-18" })]);
  upsertObservations(map, [obs({ redfinPropertyId: "999", url: "https://r/y", date: "2026-07-19" })]);
  const e = map.get("2400001");
  assert.strictEqual(e.redfinPropertyId, "288413");
  assert.strictEqual(e.url, "https://r/x");
});

test("observationsFromActives reads the fetch CSV shape and dates rows by fetchedAt in Pacific time", () => {
  const rows = [
    { fetchedAt: "2026-09-19T13:39:44.613Z", mlsListingNumber: "2400001", mlsStatus: "Active", listPrice: "1998000", domDays: "2", redfinPropertyId: "288413", redfinListingId: "223584238", redfinUrl: "https://r/x", address: "10037 15th Ave NW", zip: "98177", uiPropertyType: "Single Family" },
    { fetchedAt: "2026-09-19T13:39:44.613Z", mlsListingNumber: "", mlsStatus: "Active", listPrice: "500000" }, // no MLS#: skipped
    { fetchedAt: "2026-09-19T13:39:44.613Z", mlsListingNumber: "2400002", mlsStatus: "Active", listPrice: "" }, // no ask: skipped
  ];
  const out = observationsFromActives(rows, "2026-01-01");
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].date, "2026-09-19");
  assert.strictEqual(out[0].ask, 1998000);
  assert.strictEqual(out[0].listDate, "2026-09-17", "list date is the fetch date minus DOM");
  assert.strictEqual(out[0].redfinPropertyId, "288413");
  assert.strictEqual(out[0].propertyType, "Single Family");
});

test("observationsFromEnrichedSnapshot takes only REDFIN_ACTIVE rows and recovers the listing id from the row id", () => {
  const text = [
    "id,address,zip,type,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsListDate,listDate,mlsDOM,mlsCDOM,mlsJoinMethod",
    "redfin-active-223584238,10037 15th Ave NW,98177,Single Family,2400001,1998000,,2026-09-17,2026-09-17,2,2,REDFIN_ACTIVE",
    "redfin-sold-1,1 Sold St,98103,Single Family,2400009,,,,,,,REDFIN_SOLD",
  ].join("\n");
  const out = observationsFromEnrichedSnapshot(text, "2026-09-19");
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].mlsNumber, "2400001");
  assert.strictEqual(out[0].redfinListingId, "223584238");
  assert.strictEqual(out[0].date, "2026-09-19");
  assert.strictEqual(out[0].status, "Active");
  assert.strictEqual(out[0].dom, "2");
});

test("ask changes on later days are counted and dated; same-day replays and first sightings are not", () => {
  const map = new Map();
  upsertObservations(map, [obs({ date: "2026-07-17", ask: 1150000 })]);
  let e = map.get("2400001");
  assert.strictEqual(e.askChangeCount, "0");
  assert.strictEqual(e.lastAskChangeDate, "");
  upsertObservations(map, [obs({ date: "2026-07-17", ask: 1150000 })]); // same day again
  assert.strictEqual(e.askChangeCount, "0");
  upsertObservations(map, [obs({ date: "2026-07-25", ask: 1100000 })]); // first cut
  assert.strictEqual(e.askChangeCount, "1");
  assert.strictEqual(e.firstAskChangeDate, "2026-07-25");
  assert.strictEqual(e.lastAskChangeDate, "2026-07-25");
  upsertObservations(map, [obs({ date: "2026-07-26", ask: 1100000 })]); // unchanged next day
  assert.strictEqual(e.askChangeCount, "1");
  upsertObservations(map, [obs({ date: "2026-08-02", ask: 1075000 })]); // second cut
  assert.strictEqual(e.askChangeCount, "2");
  assert.strictEqual(e.firstAskChangeDate, "2026-07-25", "first change date is kept");
  assert.strictEqual(e.lastAskChangeDate, "2026-08-02");
  assert.strictEqual(e.firstAsk, "1150000");
  assert.strictEqual(e.lastAsk, "1075000");
  for (const col of ["lastAskChangeDate", "firstAskChangeDate", "askChangeCount"]) assert.ok(LEDGER_COLUMNS.includes(col), col);
});

test("a doubled unit suffix is repaired on insert and when loading an older ledger", () => {
  const { dedupeUnitSuffix } = require("../scripts/build_active_ledger.js");
  assert.strictEqual(dedupeUnitSuffix("2727 Fairview Ave E #4 #4"), "2727 Fairview Ave E #4");
  assert.strictEqual(dedupeUnitSuffix("9057 Greenwood Ave N #306 #306"), "9057 Greenwood Ave N #306");
  assert.strictEqual(dedupeUnitSuffix("4032 53rd Ave SW Unit A Unit A"), "4032 53rd Ave SW Unit A", "the Unit-prefixed form");
  assert.strictEqual(dedupeUnitSuffix("1080 W Ewing Pl Unit 0-P Unit 0-P"), "1080 W Ewing Pl Unit 0-P");
  assert.strictEqual(dedupeUnitSuffix("500 Pine St Apt 3B apt 3b"), "500 Pine St Apt 3B", "prefix and case may differ between the copies");
  assert.strictEqual(dedupeUnitSuffix("500 Pine St Ste 200 Ste 200"), "500 Pine St Ste 200");
  assert.strictEqual(dedupeUnitSuffix("4032 53rd Ave SW Unit A"), "4032 53rd Ave SW Unit A", "a single unit is left alone");
  assert.strictEqual(dedupeUnitSuffix("4032 53rd Ave SW Unit A Unit B"), "4032 53rd Ave SW Unit A Unit B", "two different units are not a double");
  assert.strictEqual(dedupeUnitSuffix("2727 Fairview Ave E #4"), "2727 Fairview Ave E #4");
  assert.strictEqual(dedupeUnitSuffix("100 4th Ave #4"), "100 4th Ave #4", "a street number that matches the unit is not a doubled suffix");
  assert.strictEqual(dedupeUnitSuffix("1 Main St"), "1 Main St");
  const map = new Map();
  upsertObservations(map, [obs({ address: "2727 Fairview Ave E #4 #4" })]);
  assert.strictEqual(map.get("2400001").address, "2727 Fairview Ave E #4");
  const loaded = ledgerRowsToMap([{ mlsNumber: "9", address: "708 N 102nd St #2 #2", firstSeen: "2026-06-01", lastSeen: "2026-06-10", firstAsk: "1", lastAsk: "1" }]);
  assert.strictEqual(loaded.get("9").address, "708 N 102nd St #2");
  assert.strictEqual(loaded.get("9").askChangeCount, "0", "older ledgers get the new columns defaulted");
});

test("trackedFromListing is true only when the first sighting is within 3 days of the list date", () => {
  const { trackedFromListing } = require("../scripts/build_active_ledger.js");
  assert.strictEqual(trackedFromListing({ listDate: "2026-07-17", firstSeen: "2026-07-17" }), "true");
  assert.strictEqual(trackedFromListing({ listDate: "2026-07-17", firstSeen: "2026-07-20" }), "true");
  assert.strictEqual(trackedFromListing({ listDate: "2026-07-17", firstSeen: "2026-07-21" }), "false");
  assert.strictEqual(trackedFromListing({ listDate: "2025-07-01", firstSeen: "2026-06-08" }), "false", "already on the market when tracking began");
  assert.strictEqual(trackedFromListing({ listDate: "", firstSeen: "2026-06-08" }), "");
  const map = new Map();
  upsertObservations(map, [obs({ date: "2026-07-18", listDate: "2026-07-17" }), obs({ mlsNumber: "old", date: "2026-06-08", listDate: "2025-07-01" })]);
  const rows = ledgerMapToRows(map);
  assert.strictEqual(rows.find((r) => r.mlsNumber === "2400001").trackedFromListing, "true");
  assert.strictEqual(rows.find((r) => r.mlsNumber === "old").trackedFromListing, "false");
  assert.ok(LEDGER_COLUMNS.includes("trackedFromListing"));
});

test("ledger rows round-trip through the map and come back newest-last-seen first", () => {
  const rows = [
    { mlsNumber: "1", firstSeen: "2026-06-01", lastSeen: "2026-06-10", firstAsk: "1", lastAsk: "1" },
    { mlsNumber: "2", firstSeen: "2026-06-01", lastSeen: "2026-09-01", firstAsk: "1", lastAsk: "1" },
    { mlsNumber: "", firstSeen: "2026-06-01", lastSeen: "2026-09-02" }, // no MLS#: dropped
  ];
  const map = ledgerRowsToMap(rows);
  assert.strictEqual(map.size, 2);
  const out = ledgerMapToRows(map);
  assert.deepStrictEqual(out.map((r) => r.mlsNumber), ["2", "1"]);
  for (const col of ["mlsNumber", "firstSeen", "lastSeen", "lastSeenActive", "firstAsk", "lastAsk", "lastActiveAsk", "listDate", "lastDom", "lastCdom", "lastStatus"]) {
    assert.ok(LEDGER_COLUMNS.includes(col), `ledger must carry ${col}`);
  }
});
