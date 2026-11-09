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
  assert.strictEqual(dedupeUnitSuffix("2605 22nd AVE W Unit B Unit B Unit B"), "2605 22nd AVE W Unit B", "a tripled unit collapses fully");
  assert.strictEqual(dedupeUnitSuffix("4032 53rd Ave SW Unit A"), "4032 53rd Ave SW Unit A", "a single unit is left alone");
  assert.strictEqual(dedupeUnitSuffix("4032 53rd Ave SW Unit A Unit B"), "4032 53rd Ave SW Unit A Unit B", "two different units are not a double");
  assert.strictEqual(dedupeUnitSuffix("2727 Fairview Ave E #4"), "2727 Fairview Ave E #4");
  assert.strictEqual(dedupeUnitSuffix("100 4th Ave #4"), "100 4th Ave #4", "a street number that matches the unit is not a doubled suffix");
  assert.strictEqual(dedupeUnitSuffix("1 Main St"), "1 Main St");
  const map = new Map();
  upsertObservations(map, [obs({ address: "2727 Fairview Ave E #4 #4" })]);
  assert.strictEqual(map.get("2400001").address, "2727 Fairview Ave E #4");
  const filled = new Map();
  upsertObservations(filled, [obs({ address: "", date: "2026-07-17" }), obs({ address: "2605 22nd AVE W Unit B Unit B", date: "2026-07-18" })]);
  assert.strictEqual(filled.get("2400001").address, "2605 22nd AVE W Unit B", "a blank address back-filled from a later sighting is repaired too");
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

// --- list date from the first sighting (the feed stopped reporting DOM 2026-09-21) ---

function bare(overrides = {}) {
  return obs({ dom: "", cdom: "", listDate: "", ...overrides });
}

test("a listing with no feed DOM is dated the day before its first sighting when the feed was sampled just before", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "anchor", address: "1 Anchor St", date: "2026-10-01", listDate: "2026-09-20" }),
    obs({ mlsNumber: "anchor", address: "1 Anchor St", date: "2026-10-02", listDate: "2026-09-20" }),
    bare({ mlsNumber: "new", address: "2 New St", date: "2026-10-02" }),
  ]);
  const rows = ledgerMapToRows(map);
  const fresh = rows.find((r) => r.mlsNumber === "new");
  assert.strictEqual(fresh.listDate, "2026-10-01");
  assert.strictEqual(fresh.listDateSource, "FIRST_SEEN");
  assert.strictEqual(fresh.trackedFromListing, "true", "seen from the start, so its first ask change is its first cut");
  assert.strictEqual(rows.find((r) => r.mlsNumber === "anchor").listDateSource, "FEED");
});

test("no list date is invented on the ledger's first day or after a sampling gap", () => {
  const map = new Map();
  upsertObservations(map, [
    bare({ mlsNumber: "dayone", address: "1 Day One St", date: "2026-10-01" }),
    bare({ mlsNumber: "aftergap", address: "2 After Gap St", date: "2026-10-09" }), // 8 days since the last snapshot
  ]);
  const rows = ledgerMapToRows(map);
  for (const mls of ["dayone", "aftergap"]) {
    const r = rows.find((row) => row.mlsNumber === mls);
    assert.strictEqual(r.listDate, "", `${mls} could have been listed any time before it was seen`);
    assert.strictEqual(r.listDateSource, "");
    assert.strictEqual(r.trackedFromListing, "");
  }
});

test("a list date the feed reports later replaces the first-sighting estimate, and the estimate survives a reload", () => {
  let map = new Map();
  upsertObservations(map, [obs({ mlsNumber: "anchor", address: "1 Anchor St", date: "2026-10-01" }), bare({ date: "2026-10-02" })]);
  const estimated = ledgerMapToRows(map);
  assert.strictEqual(estimated.find((r) => r.mlsNumber === "2400001").listDate, "2026-10-01");
  map = ledgerRowsToMap(estimated); // as the next morning's run reads it back
  assert.strictEqual(ledgerMapToRows(map).find((r) => r.mlsNumber === "2400001").listDateSource, "FIRST_SEEN", "still labelled an estimate");
  upsertObservations(map, [obs({ date: "2026-10-03", dom: "6", listDate: "2026-09-27" })]);
  const corrected = ledgerMapToRows(map).find((r) => r.mlsNumber === "2400001");
  assert.strictEqual(corrected.listDate, "2026-09-27");
  assert.strictEqual(corrected.listDateSource, "FEED");
});

// --- relist chains ---

test("a cancel-and-relist under a new MLS# keeps the first listing's list date and opening ask", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "A1", date: "2026-08-12", ask: 1200000, listDate: "2026-08-11" }),
    obs({ mlsNumber: "A1", date: "2026-08-27", ask: 1200000, listDate: "2026-08-11" }),
    obs({ mlsNumber: "A2", date: "2026-08-28", ask: 1140000, listDate: "2026-08-27" }),
    obs({ mlsNumber: "A2", date: "2026-09-17", ask: 1140000, listDate: "2026-08-27" }),
    obs({ mlsNumber: "A3", date: "2026-09-18", ask: 1083000, listDate: "2026-09-17" }),
    obs({ mlsNumber: "other", address: "99 Elsewhere Ave", date: "2026-09-18", ask: 900000, listDate: "2026-09-17" }),
  ]);
  const rows = ledgerMapToRows(map);
  const pick = (mls) => rows.find((r) => r.mlsNumber === mls);
  assert.deepStrictEqual(["A1", "A2", "A3"].map((m) => pick(m).chainPosition), ["1", "2", "3"]);
  for (const mls of ["A1", "A2", "A3"]) {
    assert.strictEqual(pick(mls).chainId, "A1");
    assert.strictEqual(pick(mls).chainListDate, "2026-08-11");
    assert.strictEqual(pick(mls).chainFirstAsk, "1200000");
  }
  assert.strictEqual(pick("A3").listDate, "2026-09-17", "the listing's own list date is untouched");
  assert.deepStrictEqual(["A1", "A2", "A3"].map((m) => pick(m).chainPriorDays), ["0", "16", "37"], "Aug 11-27 is 16 days, Aug 27 - Sep 17 another 21");
  assert.strictEqual(pick("other").chainId, "other");
  assert.strictEqual(pick("other").chainPosition, "1");
  for (const col of ["listDateSource", "chainId", "chainPosition", "chainListDate", "chainFirstAsk"]) assert.ok(LEDGER_COLUMNS.includes(col), col);
});

test("a long absence or a sharply higher ask starts a new chain; the same street in another ZIP never joins one", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "B1", date: "2026-06-10", ask: 1000000, listDate: "2026-06-09" }),
    obs({ mlsNumber: "B2", date: "2026-10-01", ask: 980000, listDate: "2026-09-30" }), // 113 days later
    obs({ mlsNumber: "C1", address: "5 Flip St", date: "2026-07-01", ask: 800000, listDate: "2026-06-30" }),
    obs({ mlsNumber: "C2", address: "5 Flip St", date: "2026-08-15", ask: 1150000, listDate: "2026-08-14" }), // +44%: renovated
    obs({ mlsNumber: "D1", address: "7 Twin St", zip: "98103", date: "2026-07-01", listDate: "2026-06-30" }),
    obs({ mlsNumber: "D2", address: "7 Twin St", zip: "98118", date: "2026-07-05", listDate: "2026-07-04" }),
  ]);
  const rows = ledgerMapToRows(map);
  for (const mls of ["B1", "B2", "C1", "C2", "D1", "D2"]) {
    const r = rows.find((row) => row.mlsNumber === mls);
    assert.strictEqual(r.chainId, mls, `${mls} heads its own chain`);
    assert.strictEqual(r.chainPosition, "1");
  }
});

test("a Coming Soon teaser handing off to the real MLS# is a launch, not a relist", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "teaser", date: "2026-11-05", status: "Coming Soon", ask: 1275000, listDate: "2026-11-04" }),
    obs({ mlsNumber: "real", date: "2026-11-06", status: "Active", ask: 1275000, listDate: "2026-11-05" }),
  ]);
  const rows = ledgerMapToRows(map);
  const real = rows.find((r) => r.mlsNumber === "real");
  assert.strictEqual(real.chainId, "real");
  assert.strictEqual(real.chainPosition, "1");
  assert.strictEqual(real.chainListDate, "2026-11-05");
  assert.strictEqual(rows.find((r) => r.mlsNumber === "teaser").chainId, "teaser");
});

test("a listing still Coming Soon gets no list date, and relists written differently still chain", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "anchor", address: "1 Anchor St", date: "2026-10-28" }),
    bare({ mlsNumber: "soon", address: "226 NE 90th St", date: "2026-10-29", status: "Coming Soon" }),
    bare({ mlsNumber: "soon", address: "226 NE 90th St", date: "2026-11-08", status: "Coming Soon" }),
    obs({ mlsNumber: "U1", address: "1131 NW 60th St Unit B", zip: "98107", date: "2026-06-08", ask: 899950, listDate: "2026-06-01" }),
    obs({ mlsNumber: "U1", address: "1131 NW 60th St Unit B", zip: "98107", date: "2026-08-19", ask: 850000, listDate: "2026-06-01" }),
    obs({ mlsNumber: "U2", address: "1131 B NW 60th St", zip: "98107", date: "2026-08-21", ask: 789950, listDate: "2026-08-20" }),
    obs({ mlsNumber: "U3", address: "1131 NW 60th St Unit A", zip: "98107", date: "2026-08-22", ask: 799950, listDate: "2026-08-21" }),
  ]);
  const rows = ledgerMapToRows(map);
  const pick = (mls) => rows.find((r) => r.mlsNumber === mls);
  assert.strictEqual(pick("soon").listDate, "", "no clock before the home is on the market");
  assert.strictEqual(pick("soon").listDateSource, "");
  assert.strictEqual(pick("U2").chainId, "U1");
  assert.strictEqual(pick("U2").chainListDate, "2026-06-01");
  assert.strictEqual(pick("U2").chainFirstAsk, "899950");
  assert.strictEqual(pick("U3").chainId, "U3", "the neighbouring unit is its own home");
});

test("two offerings live at one address at once, or a relist a third cheaper, are not one marketing period", () => {
  const map = new Map();
  upsertObservations(map, [
    // A house and the lot beside it, listed together for two months.
    obs({ mlsNumber: "H", address: "8821 Dibble Ave NW", date: "2026-08-14", ask: 2298000, listDate: "2026-08-13" }),
    obs({ mlsNumber: "H", address: "8821 Dibble Ave NW", date: "2026-10-13", ask: 2298000, listDate: "2026-08-13" }),
    obs({ mlsNumber: "L", address: "8821 Dibble Ave NW", date: "2026-08-16", ask: 1550000, listDate: "2026-08-15" }),
    obs({ mlsNumber: "L", address: "8821 Dibble Ave NW", date: "2026-11-08", ask: 1500000, listDate: "2026-08-15" }),
    // The same listing entered twice at one price: a duplicate, so one period.
    obs({ mlsNumber: "D1", address: "10030 37th Ave SW", zip: "98146", date: "2026-09-18", ask: 1083000, listDate: "2026-09-17" }),
    obs({ mlsNumber: "D1", address: "10030 37th Ave SW", zip: "98146", date: "2026-10-07", ask: 1083000, listDate: "2026-09-17" }),
    obs({ mlsNumber: "D2", address: "10030 37th Ave SW", zip: "98146", date: "2026-09-19", ask: 1083000, listDate: "2026-09-17" }),
    obs({ mlsNumber: "D2", address: "10030 37th Ave SW", zip: "98146", date: "2026-09-21", ask: 1083000, listDate: "2026-09-17" }),
    obs({ mlsNumber: "D3", address: "10030 37th Ave SW", zip: "98146", date: "2026-10-08", ask: 1028000, listDate: "2026-10-07" }),
    // Back a week later at 60% off: something else is for sale.
    obs({ mlsNumber: "F1", address: "5616 Fauntleroy Way SW", zip: "98136", date: "2026-08-01", ask: 1800000, listDate: "2026-07-31" }),
    obs({ mlsNumber: "F2", address: "5616 Fauntleroy Way SW", zip: "98136", date: "2026-08-09", ask: 725000, listDate: "2026-08-08" }),
  ]);
  const rows = ledgerMapToRows(map);
  const pick = (mls) => rows.find((r) => r.mlsNumber === mls);
  assert.strictEqual(pick("L").chainId, "L");
  assert.strictEqual(pick("L").chainFirstAsk, "1550000", "no $798K 'price cut' borrowed from the other listing");
  assert.strictEqual(pick("D2").chainId, "D1");
  assert.strictEqual(pick("D3").chainId, "D1");
  assert.strictEqual(pick("D2").chainPriorDays, "0", "entered the same day as D1: nothing came before it");
  assert.strictEqual(pick("D3").chainPriorDays, "20", "Sep 17 to Oct 7 once, however many listings covered it");
  assert.strictEqual(pick("F2").chainId, "F2");
});

test("re-seeding from the snapshots that misread DOM as zero does not overwrite good list dates", () => {
  const header = "id,address,zip,type,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsListDate,listDate,mlsDOM,mlsCDOM,mlsJoinMethod";
  const bad = observationsFromEnrichedSnapshot([header, "redfin-active-1,2 New St,98177,Single Family,new,1100000,,2026-11-08,2026-11-08,0,0,REDFIN_ACTIVE"].join("\n"), "2026-11-08");
  assert.strictEqual(bad[0].listDate, "");
  assert.strictEqual(bad[0].dom, "");
  const map = new Map();
  upsertObservations(map, [obs({ mlsNumber: "anchor", address: "1 Anchor St", date: "2026-10-01" }), bare({ mlsNumber: "new", address: "2 New St", date: "2026-10-02" })]);
  const reloaded = ledgerRowsToMap(ledgerMapToRows(map));
  upsertObservations(reloaded, bad);
  const row = ledgerMapToRows(reloaded).find((r) => r.mlsNumber === "new");
  assert.strictEqual(row.listDate, "2026-10-01");
  assert.strictEqual(row.listDateSource, "FIRST_SEEN");
  const good = observationsFromEnrichedSnapshot([header, "redfin-active-1,2 New St,98177,Single Family,new,1100000,,2026-09-17,2026-09-17,2,2,REDFIN_ACTIVE"].join("\n"), "2026-09-19");
  assert.strictEqual(good[0].listDate, "2026-09-17", "a snapshot with a real DOM is still read");
});

test("prior market days never double count overlapping listings or reach past a listing's own start", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "E1", address: "3222 NE 85th St", zip: "98115", date: "2026-09-01", ask: 1000000, listDate: "2026-08-31" }),
    obs({ mlsNumber: "E1", address: "3222 NE 85th St", zip: "98115", date: "2026-11-08", ask: 1000000, listDate: "2026-08-31" }),
    obs({ mlsNumber: "E2", address: "3222 NE 85th St", zip: "98115", date: "2026-10-01", ask: 1000000, listDate: "2026-09-30" }),
    obs({ mlsNumber: "E2", address: "3222 NE 85th St", zip: "98115", date: "2026-11-08", ask: 1000000, listDate: "2026-09-30" }),
    // A lot listed alongside a house starts its own chain, then relists.
    obs({ mlsNumber: "H", address: "9 Pair Pl", date: "2026-08-14", ask: 2300000, listDate: "2026-08-13" }),
    obs({ mlsNumber: "H", address: "9 Pair Pl", date: "2026-11-08", ask: 2300000, listDate: "2026-08-13" }),
    obs({ mlsNumber: "L1", address: "9 Pair Pl", date: "2026-08-16", ask: 1500000, listDate: "2026-08-15" }),
    obs({ mlsNumber: "L1", address: "9 Pair Pl", date: "2026-10-20", ask: 1500000, listDate: "2026-08-15" }),
    obs({ mlsNumber: "L2", address: "9 Pair Pl", date: "2026-10-21", ask: 1450000, listDate: "2026-10-20" }),
  ]);
  const rows = ledgerMapToRows(map);
  const pick = (mls) => rows.find((r) => r.mlsNumber === mls);
  assert.strictEqual(pick("E2").chainId, "E1");
  assert.strictEqual(pick("E2").chainPriorDays, "30", "Aug 31 to its own Sep 30 start, so own days + prior = 69, the true span");
  assert.strictEqual(pick("L1").chainId, "L1");
  assert.strictEqual(pick("L2").chainId, "L1");
  assert.strictEqual(pick("L2").chainPriorDays, "66", "the lot's first listing counts even though it overlapped the house");
});

test("a teaser seeded from git as Active is still recognised by its placeholder id", () => {
  const map = new Map();
  upsertObservations(map, [
    obs({ mlsNumber: "2212548128419413457", address: "1425 Broadmoor Dr E", zip: "98112", date: "2026-09-05", ask: 1150000, listDate: "2026-09-04", status: "Active" }),
    obs({ mlsNumber: "2580001", address: "1425 Broadmoor Dr E", zip: "98112", date: "2026-09-15", ask: 1050000, listDate: "2026-09-14" }),
  ]);
  const real = ledgerMapToRows(map).find((r) => r.mlsNumber === "2580001");
  assert.strictEqual(real.chainPosition, "1");
  assert.strictEqual(real.chainFirstAsk, "1050000", "no phantom cut from the teaser's price");
  assert.strictEqual(real.chainPriorDays, "0");
});

test("a replayed snapshot extends a listing's history but does not restate the day the raw fetch recorded", () => {
  const map = new Map();
  upsertObservations(map, [obs({ date: "2026-11-08", status: "Active Under Contract", dom: "", listDate: "" })]);
  const header = "id,address,zip,type,mlsListingNumber,mlsListingPrice,listPriceAtPending,mlsListDate,listDate,mlsDOM,mlsCDOM,mlsStatus,mlsJoinMethod";
  const line = (status) => [header, `redfin-active-1,10037 15th Ave NW,98177,Single Family,2400001,1150000,,2026-10-20,2026-10-20,19,19,${status},REDFIN_ACTIVE`].join("\n");
  upsertObservations(map, observationsFromEnrichedSnapshot(line("Active"), "2026-11-08"));
  assert.strictEqual(map.get("2400001").lastStatus, "Active Under Contract");
  upsertObservations(map, observationsFromEnrichedSnapshot(line("Active Under Contract"), "2026-11-09"));
  assert.strictEqual(map.get("2400001").lastSeen, "2026-11-09");
  assert.strictEqual(map.get("2400001").lastStatus, "Active Under Contract", "the status the merge now writes is read back");
  assert.strictEqual(map.get("2400001").lastSeenActive, "", "never seen genuinely for sale");
});
