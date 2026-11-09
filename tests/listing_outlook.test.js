"use strict";

// Synthetic fixtures only.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const load = () => import(pathToFileURL(path.resolve(__dirname, "..", "src/domain/listingOutlook.mjs")).href);

const AS_OF = "2026-11-08";
const DAY = 86400000;
const shift = (iso, days) => new Date(Date.parse(`${iso}T00:00:00Z`) + days * DAY).toISOString().slice(0, 10);

// One never-relisted home: listed `listDate`, last for sale `until`, asks as [dayOffset, ask].
function home(id, { listDate, until = AS_OF, asks = [[0, 1_200_000]], type = "Single Family", priorDays = 0, tracked = true }) {
  const points = asks.map(([offset, ask]) => ({ date: shift(listDate, offset), ask }));
  return {
    id, mlsNumbers: [id], typeLabel: type, preMarket: false, lastForSale: until, trackedFromStart: tracked,
    latestListDate: listDate, priorDays,
    spans: [{ from: listDate, to: until, listDate, priorDays }],
    asks: points, currentAsk: points[points.length - 1].ask,
  };
}

const crowd = (count, prefix, spec) => Array.from({ length: count }, (_, i) => home(`${prefix}${i}`, spec));

test("bands and cut states file a listing-day where the header says", async () => {
  const { outlookDomBand, outlookCutState, OUTLOOK_DOM_BANDS, OUTLOOK_CUT_STATES } = await load();
  assert.deepEqual([0, 3, 4, 7, 8, 14, 15, 45, 46, 121, 4000].map((dom) => OUTLOOK_DOM_BANDS[outlookDomBand(dom)]), [3, 3, 7, 7, 14, 14, 21, 45, 60, Infinity, Infinity]);
  assert.deepEqual([null, undefined, 0, 7, 8, 21, 22, 400].map((days) => OUTLOOK_CUT_STATES[outlookCutState(days)].id), ["never", "never", "justCut", "justCut", "settling", "settling", "due", "due"]);
});

test("periodCuts reads cuts from the ask history, skipping raises and rounding tweaks", async () => {
  const { periodCuts } = await load();
  const cuts = periodCuts(home("a", { listDate: "2026-08-01", asks: [[0, 1_250_000], [10, 1_249_000], [20, 1_199_000], [30, 1_225_000], [40, 1_150_000]] }));
  assert.deepEqual(cuts.map((cut) => [cut.date, cut.ordinal, cut.usd]), [["2026-08-21", 1, 50_000], ["2026-09-10", 2, 75_000]]);
  assert.equal(cuts[0].pct.toFixed(4), "0.0400");
  assert.deepEqual(periodCuts({ asks: null }), []);
});

test("a cut counts for the 14 mornings before it, and changes the state of the mornings after", async () => {
  const { buildOutlookTable, OUTLOOK_DOM_BANDS } = await load();
  // 50 identical homes: listed Aug 1, cut on day 30, still for sale.
  const table = buildOutlookTable(crowd(50, "h", { listDate: "2026-08-01", asks: [[0, 1_200_000], [30, 1_150_000]] }), { asOf: AS_OF });
  const cell = (max, state) => table.cells[OUTLOOK_DOM_BANDS.indexOf(max)][state];
  assert.deepEqual([cell(14, 0).cutDays, cell(14, 0).cutHits], [50 * 7, 0], "days 8-14: the cut is 16+ days off");
  assert.deepEqual([cell(21, 0).cutDays, cell(21, 0).cutHits], [50 * 7, 50 * 6], "days 16-21 are within 14 days of it; day 15 is not");
  assert.deepEqual([cell(30, 0).cutDays, cell(30, 0).cutHits], [50 * 8, 50 * 8], "days 22-29");
  assert.deepEqual([cell(30, 1).cutDays, cell(30, 1).cutHits], [50, 0], "day 30 itself is already 'just cut', with no further cut ahead");
  assert.equal(cell(45, 1).cutDays, 50 * 7, "days 31-37");
  assert.equal(cell(45, 2).cutDays, 50 * 8, "days 38-45");
  assert.equal(cell(60, 3).cutDays, 50 * 9, "days 52-60");
  assert.equal(table.homes, 50);
  assert.equal(table.from, "2026-08-01");
  assert.equal(table.cutsThrough, "2026-10-25", "a morning only counts once its 14 days have played out");
  assert.equal(table.firstCutPct.toFixed(4), "0.0417");
  assert.equal(table.laterCutPct, null);
});

test("a sale counts only when the closing is on record, and only on mornings old enough to know", async () => {
  const { buildOutlookTable, OUTLOOK_DOM_BANDS } = await load();
  const sold = crowd(30, "s", { listDate: "2026-07-01", until: "2026-07-06" });
  const withdrawn = crowd(30, "w", { listDate: "2026-07-01", until: "2026-07-06" });
  const recent = crowd(30, "r", { listDate: "2026-10-01", until: "2026-10-06" }); // too recent to judge
  const soldMlsNumbers = new Set([...sold, ...recent].map((period) => period.id));
  const table = buildOutlookTable([...sold, ...withdrawn, ...recent], { asOf: AS_OF, soldMlsNumbers });
  const first = table.cells[OUTLOOK_DOM_BANDS.indexOf(3)][0];
  assert.equal(first.cutDays, 90 * 4, "all three groups inform the cut odds");
  assert.equal(first.saleDays, 60 * 4, "October's exits are not yet settled");
  assert.equal(first.saleHits, 30 * 4);
  assert.equal(table.salesThrough, "2026-09-10");
  const noSales = buildOutlookTable(sold.concat(withdrawn), { asOf: AS_OF });
  assert.equal(noSales.overall.saleDays, 0);
  assert.equal(noSales.salesThrough, "");
});

test("days on market carry over a relist, and days off the market are not observations", async () => {
  const { buildOutlookTable, OUTLOOK_DOM_BANDS } = await load();
  const relisted = Array.from({ length: 40 }, (_, i) => ({
    ...home(`x${i}`, { listDate: "2026-08-01" }),
    mlsNumbers: [`x${i}a`, `x${i}b`],
    spans: [
      { from: "2026-08-01", to: "2026-08-10", listDate: "2026-08-01", priorDays: 0 },
      { from: "2026-09-01", to: "2026-09-05", listDate: "2026-09-01", priorDays: 10 },
    ],
    lastForSale: "2026-09-05",
  }));
  const table = buildOutlookTable(relisted, { asOf: AS_OF });
  assert.equal(table.overall.cutDays, 40 * 15);
  assert.equal(table.bands[OUTLOOK_DOM_BANDS.indexOf(14)].cutDays, 40 * (2 + 5), "days 8-9 of the first listing, 10-14 of the second");
});

test("no table without ask history, an as-of date, or enough homes", async () => {
  const { buildOutlookTable, OUTLOOK_MIN_HOMES } = await load();
  const homes = crowd(OUTLOOK_MIN_HOMES, "h", { listDate: "2026-08-01" });
  assert.ok(buildOutlookTable(homes, { asOf: AS_OF }));
  assert.equal(buildOutlookTable(homes.slice(1), { asOf: AS_OF }), null);
  assert.equal(buildOutlookTable(homes, {}), null);
  assert.equal(buildOutlookTable(homes.map((period) => ({ ...period, asks: null })), { asOf: AS_OF }), null);
  assert.equal(buildOutlookTable(homes.map((period) => ({ ...period, preMarket: true })), { asOf: AS_OF }), null);
  assert.equal(buildOutlookTable([], { asOf: AS_OF }), null);
});

test("a well-filled cell speaks for itself; a thin one leans on its days-on-market band", async () => {
  const { buildOutlookTable, outlookFor } = await load();
  const cutters = crowd(200, "c", { listDate: "2026-08-01", asks: [[0, 1_200_000], [30, 1_150_000]] });
  const holders = crowd(200, "k", { listDate: "2026-08-01" });
  const table = buildOutlookTable([...cutters, ...holders], { asOf: AS_OF });
  const full = outlookFor(table, { dom: 25 });
  assert.equal(full.cutState, "never");
  // Days 22-30 uncut: 8 mornings from each cutter (all followed by a cut), 9 from each holder.
  assert.ok(Math.abs(full.cutChance - 8 / 17) < 0.01, String(full.cutChance));
  assert.equal(full.saleChance, null, "no sold set was given");
  const empty = outlookFor(table, { dom: 25, daysSinceCut: 40 }); // nobody was in this state on days 22-30
  assert.equal(empty.listingDays, 0);
  assert.ok(Math.abs(empty.cutChance - full.cutChance) < 0.05, "falls back to the band's rate, not to zero");
  assert.equal(outlookFor(table, { dom: null }), null);
  assert.equal(outlookFor(table, { dom: -1 }), null);
  assert.equal(outlookFor(null, { dom: 5 }), null);
});

test("periodSituation reads cumulative days and the time since the last cut", async () => {
  const { periodSituation } = await load();
  const period = home("a", { listDate: "2026-10-01", priorDays: 20, asks: [[-40, 1_300_000], [0, 1_250_000], [30, 1_200_000]] });
  const situation = periodSituation(period, AS_OF);
  assert.equal(situation.dom, 58);
  assert.equal(situation.daysSinceCut, 8);
  assert.equal(situation.cutCount, 2);
  assert.equal(situation.lastCut.usd, 50_000);
  assert.equal(situation.cutHistoryKnown, true);
  assert.equal(periodSituation(home("b", { listDate: "2026-11-01" }), AS_OF).daysSinceCut, null);
  assert.equal(periodSituation(home("c", { listDate: "2026-03-01", tracked: false }), AS_OF).cutHistoryKnown, false, "found mid-life and not seen to cut: cannot say");
  assert.equal(periodSituation(home("d", { listDate: "2026-03-01", tracked: false, asks: [[100, 1_300_000], [200, 1_250_000]] }), AS_OF).cutHistoryKnown, true, "seen to cut: its state is known from then on");
  assert.equal(periodSituation({ ...period, asks: null }, AS_OF), null);
  assert.equal(periodSituation({ ...period, latestListDate: "" }, AS_OF), null);
  assert.equal(periodSituation({ ...period, lastForSale: "2026-09-19" }, AS_OF), null, "under contract since September: no odds of going under contract");
});

test("the chip follows the odds", async () => {
  const { outlookLean } = await load();
  assert.equal(outlookLean({ saleChance: 0.4, cutChance: 0.1, cutState: "never" }).id, "move");
  assert.equal(outlookLean({ saleChance: 0.3, cutChance: 0.1, cutState: "justCut" }).id, "move", "likely to sell outranks the rest");
  assert.equal(outlookLean({ saleChance: 0.16, cutChance: 0.14, cutState: "justCut" }).id, "justCut");
  assert.equal(outlookLean({ saleChance: 0.08, cutChance: 0.31, cutState: "due" }).id, "wait");
  assert.equal(outlookLean({ saleChance: 0.18, cutChance: 0.3, cutState: "never" }).id, "even");
  assert.equal(outlookLean({ saleChance: null, cutChance: 0.3, cutState: "never" }), null);
  assert.equal(outlookLean(null), null);
});

test("the summary rolls bands up to the four stages and contrasts cut states after day 14", async () => {
  const { buildOutlookTable, summarizeOutlook, buildOutlookVerdict } = await load();
  const quick = crowd(60, "q", { listDate: "2026-07-01", until: "2026-07-05" });
  const slow = crowd(60, "s", { listDate: "2026-07-01", until: "2026-09-20", asks: [[0, 1_200_000], [30, 1_150_000], [60, 1_100_000]] });
  const table = buildOutlookTable([...quick, ...slow], { asOf: AS_OF, soldMlsNumbers: new Set(quick.map((period) => period.id)) });
  const summary = summarizeOutlook(table);
  assert.deepEqual(summary.stages.map((stage) => stage.id), ["fresh", "past", "sitting", "stale"]);
  assert.equal(summary.stages.reduce((sum, stage) => sum + stage.cutDays, 0), table.overall.cutDays, "every band lands in exactly one stage");
  assert.ok(summary.stages[0].saleChance > 0.35);
  assert.equal(summary.stages[2].saleChance, 0);
  assert.ok(summary.stages[2].cutChance > 0.3);
  assert.ok(summary.afterTwoWeeks.justCut.cutDays > 0);
  const verdict = buildOutlookVerdict(summary);
  assert.match(verdict.headline, /^In its first week a listing has a \d+% chance of going under contract within 14 days\.$/);
  assert.match(verdict.detail, /^After day 14 that falls to 0%/);
  assert.equal(buildOutlookVerdict(null), null);
  assert.equal(buildOutlookVerdict(summarizeOutlook(buildOutlookTable(slow, { asOf: AS_OF }))), null, "no sale outcomes, no verdict");
});

test("the verdict pools the late stages exactly, and calls out the bump after a cut only when it is there", async () => {
  const { buildOutlookVerdict } = await load();
  // Sale odds and cut odds rest on different mornings, so each row carries both tallies.
  const row = (saleChance, cutChance, saleDays = 1000, cutDays = 1000) => ({
    saleChance, cutChance, saleDays, cutDays, saleHits: saleChance * saleDays, cutHits: cutChance * cutDays,
  });
  const base = { stages: [row(0.45, 0.1), row(0.18, 0.3), row(0.1, 0.32, 3000, 1000), row(0.05, 0.2, 1000, 3000)] };
  const bumped = buildOutlookVerdict({ ...base, afterTwoWeeks: { never: row(0.08, 0.27), justCut: row(0.16, 0.15) } }).detail;
  assert.match(bumped, /^After day 14 that falls to 9%, and a price cut \(23%\) is 2\.6 times as likely as losing the home\./, "350 of 4,000 sale days, 920 of 4,000 cut days");
  assert.match(bumped, /the week after a cut, when the chance of a sale rises to 16% from 8%/);
  const flat = buildOutlookVerdict({ ...base, afterTwoWeeks: { never: row(0.08, 0.27), justCut: row(0.09, 0.15) } }).detail;
  assert.doesNotMatch(flat, /week after a cut/);
  const unshown = buildOutlookVerdict({ ...base, afterTwoWeeks: { never: row(0.08, 0.27), justCut: { ...row(0.3, 0.15), saleChance: null } } }).detail;
  assert.doesNotMatch(unshown, /week after a cut/);
  const rising = buildOutlookVerdict({ stages: [row(0.05, 0.1), row(0.1, 0.2), row(0.12, 0.13), row(0.12, 0.13)], afterTwoWeeks: { never: row(0.1, 0.1), justCut: row(0.1, 0.1) } }).detail;
  assert.match(rising, /^After day 14 that is 12%, against a 13% chance of a price cut\.$/, "no 'falls to' when it does not fall");
});

test("a rate resting on too few listing-days of its own kind is not shown", async () => {
  const { buildOutlookTable, summarizeOutlook, buildOutlookVerdict, MIN_ROW_LISTING_DAYS } = await load();
  // Plenty of recent mornings for cut odds, almost none old enough for sale odds.
  const recent = crowd(60, "r", { listDate: "2026-09-08", until: "2026-10-30" });
  const table = buildOutlookTable(recent, { asOf: AS_OF, soldMlsNumbers: new Set(["r0"]) });
  const [fresh] = summarizeOutlook(table).stages;
  assert.ok(fresh.cutDays >= MIN_ROW_LISTING_DAYS && fresh.saleDays > 0 && fresh.saleDays < MIN_ROW_LISTING_DAYS);
  assert.notEqual(fresh.cutChance, null);
  assert.equal(fresh.saleChance, null);
  assert.equal(buildOutlookVerdict(summarizeOutlook(table)), null);
});

test("odd ledgers hide the block instead of breaking the page", async () => {
  const { buildOutlookTable, OUTLOOK_MIN_HISTORY_COVERAGE } = await load();
  const homes = crowd(50, "h", { listDate: "2026-08-01" });
  const weekly = new Set(Array.from({ length: 20 }, (_, i) => shift("2026-07-01", i * 7)));
  assert.equal(buildOutlookTable(homes, { asOf: AS_OF, sampleDays: weekly }), null, "never sampled closely enough to date a cut");
  const badDate = [...homes, { ...home("bad", { listDate: "2026-08-01" }), spans: [{ from: "2026-08-01", to: AS_OF, listDate: "8/2/2026", priorDays: 0 }] }];
  assert.equal(buildOutlookTable(badDate, { asOf: AS_OF }).homes, 50, "a list date that does not parse is skipped");
  assert.equal(OUTLOOK_MIN_HISTORY_COVERAGE, 0.95);
  const holes = (count) => [...crowd(100 - count, "ok", { listDate: "2026-08-01" }), ...crowd(count, "gap", { listDate: "2026-08-01" }).map((period) => ({ ...period, asks: null }))];
  assert.ok(buildOutlookTable(holes(5), { asOf: AS_OF }));
  assert.equal(buildOutlookTable(holes(6), { asOf: AS_OF }), null, "the homes missing a history are the ones that cut most");
});

test("tables are built per home type from asks near the price band", async () => {
  const { buildOutlookTables } = await load();
  const at = (ask) => ({ listDate: "2026-08-01", asks: [[0, ask]] });
  const periods = [
    ...crowd(45, "in", at(1_300_000)), ...crowd(45, "near", at(900_000)), ...crowd(45, "far", at(600_000)),
    ...crowd(45, "th", { ...at(1_200_000), type: "Townhouse" }), ...crowd(5, "condo", { ...at(1_200_000), type: "Condo" }),
  ];
  const tables = buildOutlookTables(periods, { asOf: AS_OF, minAsk: 1_100_000, maxAsk: 1_600_000 });
  assert.deepEqual([...tables.keys()].sort(), ["Single Family", "Townhouse"], "five condos are not a table");
  assert.equal(tables.get("Single Family").homes, 90, "$900K is within 25% of the floor, $600K is not");
  assert.equal(buildOutlookTables(periods, { asOf: AS_OF }).get("Single Family").homes, 135, "no band, no limit");
});

test("a home found mid-life counts toward its band but toward no cut state until it is seen to cut", async () => {
  const { buildOutlookTable, outlookFor, summarizeOutlook, OUTLOOK_DOM_BANDS } = await load();
  const tracked = crowd(50, "t", { listDate: "2026-08-01" });
  const found = crowd(50, "f", { listDate: "2026-08-01", tracked: false, asks: [[0, 1_200_000], [40, 1_150_000]] });
  const table = buildOutlookTable([...tracked, ...found], { asOf: AS_OF });
  const band = OUTLOOK_DOM_BANDS.indexOf(30);
  assert.equal(table.cells[band][0].cutDays, 50 * 9, "only the tracked homes are known not to have cut");
  assert.equal(table.cells[band][0].cutHits, 0);
  assert.equal(table.bands[band].cutDays, 100 * 9, "both inform the band");
  assert.equal(table.bands[band].cutHits, 50 * 5, "days 26-30 of the found homes are within 14 days of their cut");
  assert.equal(table.cells[OUTLOOK_DOM_BANDS.indexOf(45)][1].cutDays, 50 * 6, "days 40-45: once seen to cut, its state is known");
  assert.equal(table.firstCutPct, null, "the first cut seen on a home found mid-life may not be its first");
  assert.equal(table.laterCutPct, null, "nor is it known to be a later one");
  const unknown = outlookFor(table, { dom: 25, cutHistoryKnown: false });
  assert.equal(unknown.cutState, "unknown");
  assert.ok(unknown.cutChance > outlookFor(table, { dom: 25 }).cutChance, "the band's odds, not the never-cut cell's");
  const late = summarizeOutlook(table).afterTwoWeeks;
  assert.deepEqual(Object.keys(late), ["never", "justCut", "settling", "due"]);
});

test("mornings whose next 14 days run into a long sampling gap are left out", async () => {
  const { buildOutlookTable } = await load();
  const homes = crowd(50, "h", { listDate: "2026-07-01", until: "2026-09-30" });
  const daily = new Set(Array.from({ length: 131 }, (_, i) => shift("2026-07-01", i)));
  const full = buildOutlookTable(homes, { asOf: AS_OF, sampleDays: daily });
  assert.equal(full.overall.cutDays, 50 * 92);
  assert.equal(full.from, "2026-07-01");
  const gappy = new Set([...daily].filter((day) => day <= "2026-07-10" || day >= "2026-07-20"));
  const table = buildOutlookTable(homes, { asOf: AS_OF, sampleDays: gappy });
  assert.equal(table.from, "2026-07-20", "Jul 1-10 look ahead into the gap, Jul 11-19 sit inside it");
  assert.equal(table.overall.cutDays, 50 * 73);
  const weekend = new Set([...daily].filter((day) => day !== "2026-08-01" && day !== "2026-08-02"));
  assert.equal(buildOutlookTable(homes, { asOf: AS_OF, sampleDays: weekend }).overall.cutDays, 50 * 92, "a missed weekend is not a gap");
});

test("two listings of one home live at once count each day once", async () => {
  const { buildOutlookTable } = await load();
  const doubled = crowd(40, "d", { listDate: "2026-08-01", until: "2026-08-20" }).map((period) => ({
    ...period,
    spans: [{ from: "2026-08-10", to: "2026-08-20", listDate: "2026-08-10", priorDays: 0 }, { from: "2026-08-01", to: "2026-08-15", listDate: "2026-08-01", priorDays: 0 }],
  }));
  assert.equal(buildOutlookTable(doubled, { asOf: AS_OF }).overall.cutDays, 40 * 20);
});
