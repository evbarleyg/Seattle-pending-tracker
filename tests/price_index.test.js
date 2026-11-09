"use strict";

// Synthetic fixtures only.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const load = () => import(pathToFileURL(path.resolve(__dirname, "..", "src/domain/priceIndex.mjs")).href);

// A small deterministic generator, so fixtures vary without being random.
function lcg(seed) {
  let state = seed;
  return () => { state = (state * 1664525 + 1013904223) % 4294967296; return state / 4294967296; };
}

const MONTHS = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"];
const TODAY = "2026-11-09";
const HOODS = { Ballard: 1.1, Magnolia: 1.0, "Lake City": 0.85 };

// Price = a fixed recipe of the home's traits x the month's true level x a little noise.
function makeSales({ levels, perMonth = 80, sizeFor = () => 1, source = () => "", seed = 7, noise = 0.05 }) {
  const rand = lcg(seed);
  const rows = [];
  MONTHS.slice(0, levels.length).forEach((month, m) => {
    const count = typeof perMonth === "function" ? perMonth(m) : perMonth;
    for (let i = 0; i < count; i += 1) {
      const hood = Object.keys(HOODS)[Math.floor(rand() * 3)];
      const trueSqft = (1200 + rand() * 2000) * sizeFor(m);
      const src = source(m, rand());
      const price = 900 * trueSqft ** 0.95 * HOODS[hood] * levels[m] * (1 + (rand() - 0.5) * 2 * noise);
      rows.push({
        hasActualClose: true, dataMode: "MLS_ENRICHED", typeLabel: "Single Family", saleDate: `${month}-15`, closePrice: Math.round(price),
        sqft: Math.round(src === "Redfin" ? trueSqft * 1.13 : trueSqft), lotSize: 3000 + Math.round(rand() * 4000), yearBuilt: 1920 + Math.floor(rand() * 90),
        beds: 3, baths: 2, neighborhoodLabel: hood, mlsSquareFootageSource: src,
      });
    }
  });
  return rows;
}

const LEVELS = [1, 0.98, 0.97, 0.97, 1.0, 1.03, 1.05, 1.07, 1.04, 1.0, 0.97, 0.96, 0.95];

test("the index recovers the true month-by-month level", async () => {
  const { computePriceIndex } = await load();
  const index = computePriceIndex(makeSales({ levels: LEVELS }), { typeLabel: "Single Family", today: TODAY });
  assert.deepEqual(index.points.map((point) => point.month), MONTHS);
  assert.equal(index.points[0].index, 100);
  index.points.forEach((point, i) => assert.ok(Math.abs(point.index - 100 * LEVELS[i]) < 1.5, `${point.month}: ${point.index.toFixed(1)} vs ${100 * LEVELS[i]}`));
  assert.equal(index.salesCount, 13 * 80);
});

test("a change in the mix of homes sold moves the median but not the index", async () => {
  const { computePriceIndex } = await load();
  // Prices never change; the homes selling get 30% smaller over the year.
  const rows = makeSales({ levels: LEVELS.map(() => 1), sizeFor: (m) => 1 - 0.025 * m });
  const medianIn = (month) => { const v = rows.filter((row) => row.saleDate.startsWith(month)).map((row) => row.closePrice).sort((a, b) => a - b); return v[v.length >> 1]; };
  assert.ok(medianIn("2026-10") / medianIn("2025-10") < 0.8, "the median falls by over a fifth");
  const last = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY }).points[12];
  assert.ok(Math.abs(last.index - 100) < 1.5, `the index holds: ${last.index.toFixed(1)}`);
});

test("a shift to a source that measures homes larger does not read as falling prices", async () => {
  const { computePriceIndex } = await load();
  // Redfin's figure runs 13% larger; its share climbs from nothing to all, as it did in 2026.
  const rows = makeSales({ levels: LEVELS.map(() => 1), source: (m, r) => (m >= 9 || (m >= 5 && r < 0.15 * (m - 3)) ? "Redfin" : "") });
  const last = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY }).points[12];
  assert.ok(Math.abs(last.index - 100) < 1.5, `with the source term: ${last.index.toFixed(1)}`);
  const blind = computePriceIndex(rows.map((row) => ({ ...row, mlsSquareFootageSource: "" })), { typeLabel: "Single Family", today: TODAY }).points[12];
  assert.ok(blind.index < 95, `without it the index sinks: ${blind.index.toFixed(1)}`);
});

test("a source that is only a leftover in a month is left out of it", async () => {
  const { computePriceIndex } = await load();
  // Prices never change. Redfin supplies a seventh of the early months, and those rows are an
  // odd subset that sold 8% dear; from month 6 it is half the market and ordinary.
  const rows = makeSales({ levels: LEVELS.map(() => 1), perMonth: 150, source: (m, r) => (m >= 9 || r < (m >= 6 ? 0.5 : 0.15) ? "Redfin" : "") })
    .map((row) => (row.mlsSquareFootageSource === "Redfin" && row.saleDate < "2026-04" ? { ...row, closePrice: Math.round(row.closePrice * 1.08) } : row));
  const index = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY });
  assert.ok(Math.abs(index.points[12].index - 100) < 1.5, `unmoved by the leftovers: ${index.points[12].index.toFixed(1)}`);
  assert.ok(index.points[0].count < 150 * 0.9 && index.points[7].count === 150, "early months count county rows only; the overlap counts both");
});

test("two sources that never overlap cannot be chained, so only the newer one's stretch is shown", async () => {
  const { computePriceIndex } = await load();
  const rows = makeSales({ levels: LEVELS, source: (m) => (m >= 6 ? "Redfin" : "") });
  const index = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY });
  assert.equal(index.points[0].month, "2026-04");
  assert.equal(index.points.length, 7);
  // One month where Redfin is a third of sales counts about half: too little to tie them with.
  const sliver = makeSales({ levels: LEVELS, perMonth: 300, source: (m, r) => (m > 6 || (m === 6 && r < 0.33) ? "Redfin" : "") });
  assert.equal(computePriceIndex(sliver, { typeLabel: "Single Family", today: TODAY }).points[0].month, "2026-05");
  const fullMonth = makeSales({ levels: LEVELS, perMonth: 300, source: (m, r) => (m > 6 || (m === 6 && r < 0.5) ? "Redfin" : "") });
  assert.equal(computePriceIndex(fullMonth, { typeLabel: "Single Family", today: TODAY }).points[0].month, "2025-10", "one month shared evenly is enough");
});

test("months both sources covered long ago do not vouch for a window that has none", async () => {
  const { computePriceIndex } = await load();
  // 13 months are fitted; the only shared months would be the two before them.
  const long = ["2025-08", "2025-09", ...MONTHS];
  const rows = makeSales({ levels: LEVELS, source: (m) => (m >= 7 ? "Redfin" : "") });
  const older = makeSales({ levels: [1, 1], source: (m, r) => (r < 0.5 ? "Redfin" : ""), seed: 3 }).map((row) => ({ ...row, saleDate: `${long[MONTHS.indexOf(row.saleDate.slice(0, 7))]}-15` }));
  const index = computePriceIndex([...older, ...rows], { typeLabel: "Single Family", today: TODAY });
  assert.equal(index.points[0].month, "2026-05", "falls back to the newer source's stretch instead of failing to fit");
});

test("no handful of rows switches a month's source on or off", async () => {
  const { computePriceIndex } = await load();
  const rows = makeSales({ levels: LEVELS, perMonth: 200, source: (m, r) => (m >= 9 || (m >= 6 && r < 0.36) ? "Redfin" : "") });
  const base = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY }).points[12].index;
  let left = 8;
  const fewer = rows.filter((row) => !(left > 0 && row.mlsSquareFootageSource === "Redfin" && row.saleDate.startsWith("2026-05") && left--));
  const after = computePriceIndex(fewer, { typeLabel: "Single Family", today: TODAY });
  assert.equal(after.points.length, 13);
  assert.ok(Math.abs(after.points[12].index - base) < 1, `${base.toFixed(1)} -> ${after.points[12].index.toFixed(1)}`);
});

test("the current calendar month is never shown, however many sales it has", async () => {
  const { computePriceIndex } = await load();
  const rows = makeSales({ levels: LEVELS });
  for (const today of ["2026-10-09", "2026-10-31"]) assert.equal(computePriceIndex(rows, { typeLabel: "Single Family", today }).points.at(-1).month, "2026-09", today);
  assert.equal(computePriceIndex(rows, { typeLabel: "Single Family", today: "2026-11-01" }).points.at(-1).month, "2026-10");
});

test("a fall from the peak has to clear a higher bar than a change between two named months", async () => {
  const { summarizePriceIndex, INDEX_CALL_Z, INDEX_PEAK_Z } = await load();
  assert.ok(INDEX_PEAK_Z > INDEX_CALL_Z);
  // Every gap has a standard error of 1%; the last month sits 2.4% under the peak and the year-ago month.
  const months = MONTHS.map((month, i) => ({ month, index: i === 0 || i === 7 ? 100 : i === 12 ? 97.6 : 98.5, count: 100 }));
  const monthCovariance = months.map((_, a) => months.map((__, b) => (a === b && a > 0 ? 0.0001 : a > 0 && b > 0 ? 0.00005 : 0)));
  const summary = summarizePriceIndex({ points: months, monthCovariance });
  assert.equal(summary.overYear.clear, true, "2.4 standard errors");
  assert.equal(summary.sincePeak.clear, false, "not enough for a month chosen for being the highest");
});

test("a change across the join between sources is less certain than one inside a source", async () => {
  const { computePriceIndex, indexChange } = await load();
  const rows = makeSales({ levels: LEVELS, perMonth: 150, source: (m, r) => (m >= 8 || (m >= 6 && r < 0.5) ? "Redfin" : "") });
  const index = computePriceIndex(rows, { typeLabel: "Single Family", today: TODAY });
  const across = indexChange(index, index.points[0], index.points[12]);
  const within = indexChange(index, index.points[9], index.points[12]);
  assert.ok(across.se > within.se * 1.2, `${across.se.toFixed(4)} vs ${within.se.toFixed(4)}`);
});

test("one source throughout is fine: a term that never varies is dropped", async () => {
  const { computePriceIndex } = await load();
  const index = computePriceIndex(makeSales({ levels: LEVELS, source: () => "Redfin" }), { typeLabel: "Single Family", today: TODAY });
  assert.ok(Math.abs(index.points[12].index - 95) < 1.5);
});

test("the part-finished current month is left out, and a thin month further back ends the run", async () => {
  const { computePriceIndex } = await load();
  const partial = computePriceIndex(makeSales({ levels: LEVELS, perMonth: (m) => (m === 12 ? 45 : 300) }), { typeLabel: "Single Family", today: TODAY });
  assert.equal(partial.points[partial.points.length - 1].month, "2026-09", "45 sales clears the floor of 40 but is 15% of a typical month");
  const broken = computePriceIndex(makeSales({ levels: LEVELS, perMonth: (m) => (m === 3 ? 10 : 80) }), { typeLabel: "Single Family", today: TODAY });
  assert.equal(broken.points[0].month, "2026-02", "no line is drawn across a month that cannot be placed");
  assert.equal(broken.points.length, 9);
  const stray = [...makeSales({ levels: LEVELS }), { ...makeSales({ levels: [1], perMonth: 1 })[0], saleDate: "2027-03-01" }];
  assert.equal(computePriceIndex(stray, { typeLabel: "Single Family", today: TODAY }).points[12].month, "2026-10", "one mistyped date far ahead does not hide the block");
});

test("only listed sales of the type, in the wide price range and the chosen neighborhoods, take part", async () => {
  const { computePriceIndex } = await load();
  const rows = makeSales({ levels: LEVELS, perMonth: 150 });
  const extra = [
    ...rows.slice(0, 200).map((row) => ({ ...row, dataMode: "PUBLIC_PROXY" })),
    ...rows.slice(0, 200).map((row) => ({ ...row, typeLabel: "Condo" })),
    ...rows.slice(0, 50).map((row) => ({ ...row, closePrice: 9_000_000 })),
    ...rows.slice(0, 50).map((row) => ({ ...row, sqft: 0 })),
    ...rows.slice(0, 50).map((row) => ({ ...row, hasActualClose: false })),
  ];
  assert.equal(computePriceIndex([...rows, ...extra], { typeLabel: "Single Family", today: TODAY }).salesCount, rows.length);
  const ballard = computePriceIndex(rows, { typeLabel: "Single Family", neighborhoods: ["Ballard"], today: TODAY });
  assert.equal(ballard.salesCount, rows.filter((row) => row.neighborhoodLabel === "Ballard").length);
});

test("too little to fit returns null rather than a wobbly line", async () => {
  const { computePriceIndex } = await load();
  assert.equal(computePriceIndex([], { typeLabel: "Single Family", today: TODAY }), null);
  assert.equal(computePriceIndex(makeSales({ levels: LEVELS, perMonth: 20 }), { typeLabel: "Single Family", today: TODAY }), null, "no month reaches 40 sales");
  assert.equal(computePriceIndex(makeSales({ levels: LEVELS.slice(0, 5) }), { typeLabel: "Single Family", today: TODAY }), null, "five months is not a trend");
  assert.equal(computePriceIndex(makeSales({ levels: LEVELS }), { typeLabel: "Townhouse", today: TODAY }), null);
});

test("a change is called only when it stands clear of the noise", async () => {
  const { computePriceIndex, summarizePriceIndex, indexChange } = await load();
  const index = computePriceIndex(makeSales({ levels: LEVELS, noise: 0.15, perMonth: 120 }), { typeLabel: "Single Family", today: TODAY });
  const summary = summarizePriceIndex(index);
  assert.equal(summary.overYear.from, "2025-10");
  assert.equal(summary.overYear.to, "2026-10");
  assert.ok(summary.overYear.se > 0.005 && summary.overYear.se < 0.05, String(summary.overYear.se));
  assert.equal(summary.peak.month, "2026-05");
  assert.equal(summary.sincePeak.clear, true, "12% is well clear");
  const small = indexChange(index, index.points[10], index.points[11]);
  assert.equal(small.clear, false, "a 1% step is inside the noise");
  assert.equal(indexChange(index, null, index.points[1]), null);
  assert.equal(summarizePriceIndex(null), null);
});

test("the verdict leads with the year, dates the peak, and says so when nothing is clear", async () => {
  const { buildPriceIndexVerdict } = await load();
  const change = (pct, clear, from = "2025-10") => ({ pct, clear, from, to: "2026-10" });
  const last = { month: "2026-10" };
  const down = buildPriceIndexVerdict({ overYear: change(-0.05, true), sincePeak: change(-0.106, true, "2026-05"), overQuarter: change(-0.036, true, "2026-07"), peak: { month: "2026-05" }, last });
  assert.equal(down.headline, "Like for like, prices are 5% lower than a year ago.");
  assert.match(down.detail, /^They are 11% below the May '26 peak, though spring usually runs above autumn and winter, so part of that is the season\./);
  const springToSpring = buildPriceIndexVerdict({ overYear: change(-0.08, true), sincePeak: change(-0.09, true, "2026-05"), overQuarter: change(0, false), peak: { month: "2026-05" }, last: { month: "2027-04" } });
  assert.match(springToSpring.detail, /^They are 9% below the May '26 peak\./, "spring against spring is not the season");
  assert.match(down.detail, /Over the last three months they moved 4% lower\.$/);
  const flat = buildPriceIndexVerdict({ overYear: change(0.01, false), sincePeak: change(-0.02, false), overQuarter: change(0.005, false), peak: { month: "2026-05" }, last });
  assert.equal(flat.headline, "Like for like, prices are about where they were a year ago.");
  assert.equal(flat.detail, "Over the last three months they have not moved by more than the noise.");
  const short = buildPriceIndexVerdict({ overYear: null, sincePeak: change(-0.08, true, "2026-11"), overQuarter: change(0.04, true, "2026-07"), peak: { month: "2025-11" }, last });
  assert.equal(short.headline, "Like for like, prices are 4% higher than in Jul '26.");
  assert.doesNotMatch(short.detail, /spring/);
  assert.equal(buildPriceIndexVerdict(null), null);
  assert.equal(buildPriceIndexVerdict({ overYear: null, overQuarter: null }), null);
});
