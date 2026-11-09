// listingOutlook — what tends to happen next to a listing in this situation.
//
// A suggested bid says what to offer. This says whether there is any hurry: over the
// next two weeks, how often did listings at the same point in their life cut their
// price, and how often did they go under contract?
//
// It is a lookup, not a forecast of one home. Every day that every listing spent on
// the market is one observation ("a listing-day"), filed by two things known that
// morning:
//
//   days on market   cumulative, in the bands below (they nest inside the stages)
//   cut state        never cut / cut in the last 7 days / 8-21 days ago / 22+ days ago
//
// and marked with what the following 14 days brought. Measured on 1,482 single-family
// homes asking $825K to $2.0M (44,000 listing-days for cuts, Jul 13 to Oct 25 2026; 30,000
// for sales, Jun 8 to Sep 10):
//
//   days on market    sells within 14 days    cuts within 14 days
//   0-3                      42%                     11%
//   4-7                      32%                     18%
//   8-14                     20%                     29%
//   15-45                    11%                     31%
//   46-120                    9%                     22%
//   121+                      3%                     13%
//
//   past day 14: no cut yet 11% sell, cut in the last week 15%, cut longer ago 10-11%;
//   another cut is least likely right after one (17%) and back to 26-27% from week two
//
// Tested walk-forward on ten Thursdays, Aug 20 to Oct 22 2026 (each table built only
// from what was known that morning, scored on the two weeks after; single-family,
// $1.1M to $1.6M):
//
//   sales   predicted 10.8%, happened 10.5%; lowest to highest quartile of predicted
//           odds: 4%, 9%, 8%, 21% sold. Brier score 6% better than the base rate.
//   cuts    ranked correctly (16%, 25%, 35%, 37% cut) but ran low overall: predicted
//           22%, happened 28%. Sellers cut more after Labor Day than in the summer the
//           tables had seen. Weighting recent weeks more barely helped (23% at a 14-day
//           half-life), so it is not done; a second year of data is the real fix.
//   chips   "Time is on your side": 6% sold, 31% cut. "Likely gone soon": 30% sold.
//
// The two-way table matched or beat gradient boosting given the same inputs plus ask,
// depth of cuts so far, build era and weekday. The skill is real but modest (AUC 0.62
// for cuts, 0.68 for sales; none for townhouse sales beyond their days on market):
// dependable as odds, no use as a prediction about one home.
import { daysBetween, nullableMedian } from "./format.mjs";
import { EXIT_SETTLE_DAYS, MAX_SAMPLE_DISTANCE_DAYS, daysOnMarket } from "./listingLedger.mjs";
import { LISTING_STAGES } from "./listingStage.mjs";

export const OUTLOOK_HORIZON_DAYS = 14;
// Upper edge of each days-on-market band. 7, 14 and 45 are the stage boundaries.
export const OUTLOOK_DOM_BANDS = [3, 7, 14, 21, 30, 45, 60, 90, 120, Infinity];
export const OUTLOOK_CUT_STATES = [
  { id: "never", label: "No cut yet", maxDays: null },
  { id: "justCut", label: "Cut in the last week", maxDays: 7 },
  { id: "settling", label: "Cut 8-21 days ago", maxDays: 21 },
  { id: "due", label: "Cut over 3 weeks ago", maxDays: Infinity },
];
// A home already on the market when tracking found it may have cut before anyone was
// looking. Until it is seen to cut, its days count toward its days-on-market band but
// toward no cut state: filing them under "no cut yet" (29% of homes, 55% of that cell's
// days past day 14) dragged the cell's odds down from 31% cut / 11% sold to 27% / 8%.
const UNKNOWN_CUT_STATE = OUTLOOK_CUT_STATES.length;
// A move smaller than this is a rounding tweak ($1,250,000 to $1,249,000).
export const MIN_CUT_PCT = 0.0025;
// A thin cell is pulled toward its days-on-market band, and a thin band toward the
// overall rate, as if this many extra listing-days had been seen at the parent's rate.
// One home contributes up to two weeks of near-identical days to a cell, so this is
// about ten homes' worth.
export const OUTLOOK_PRIOR_DAYS = 150;
// Below this many homes the whole table is an anecdote and nothing is shown.
export const OUTLOOK_MIN_HOMES = 40;
// Share of homes that must carry a complete ask history before any table is built.
export const OUTLOOK_MIN_HISTORY_COVERAGE = 0.95;
export const OUTLOOK_MIN_CUTS_FOR_SIZE = 8;
// A rate in the summary needs this many listing-days (about 15 homes) to be shown.
export const MIN_ROW_LISTING_DAYS = 200;
// The table borrows from asks this far outside the lens's price band: odds move slowly
// with price, and the band alone is often too few homes.
export const OUTLOOK_BAND_MARGIN = 0.25;

const DAY_MS = 86400000;
const dayNumber = (iso) => Math.round(Date.parse(`${iso}T00:00:00Z`) / DAY_MS);

export function outlookDomBand(dom) {
  return OUTLOOK_DOM_BANDS.findIndex((max) => dom <= max);
}

export function outlookCutState(daysSinceCut) {
  if (daysSinceCut === null || daysSinceCut === undefined || daysSinceCut < 0) return 0;
  return OUTLOOK_CUT_STATES.findIndex((state, index) => index > 0 && daysSinceCut <= state.maxDays);
}

// Price cuts inside one marketing period, oldest first. Raises reset the reference
// price but are not events (1 for every 70 cuts).
export function periodCuts(period) {
  const asks = period?.asks || [];
  const cuts = [];
  for (let i = 1; i < asks.length; i += 1) {
    const pct = 1 - asks[i].ask / asks[i - 1].ask;
    if (pct > MIN_CUT_PCT) cuts.push({ date: asks[i].date, pct, usd: asks[i - 1].ask - asks[i].ask, ordinal: cuts.length + 1 });
  }
  return cuts;
}

const emptyCell = () => ({ cutDays: 0, cutHits: 0, saleDays: 0, saleHits: 0 });

function addInto(total, cell) {
  total.cutDays += cell.cutDays;
  total.cutHits += cell.cutHits;
  total.saleDays += cell.saleDays;
  total.saleHits += cell.saleHits;
}

// Stretches the feed went unsampled for longer than MAX_SAMPLE_DISTANCE_DAYS, as
// [lastSampleBefore, firstSampleAfter] day numbers.
function samplingGaps(sampleDays) {
  if (!sampleDays?.size) return [];
  const days = [...sampleDays].map(dayNumber).sort((a, b) => a - b);
  const gaps = [];
  for (let i = 1; i < days.length; i += 1) if (days[i] - days[i - 1] > MAX_SAMPLE_DISTANCE_DAYS) gaps.push([days[i - 1], days[i]]);
  return gaps;
}

// Files every listing-day of `periods` (one home type, one price neighbourhood) into
// band x cut-state cells. Only days whose next 14 days are fully known count: for
// cuts that means 14 days before `asOf`; for sales a further EXIT_SETTLE_DAYS, because
// a home that left the market is only known to have sold once the closing records.
// For cuts, a morning whose 14 days run into a long gap in sampling is left out too: a
// cut inside the gap is dated to the day sampling resumed, and two can read as one (the
// ledger's first five weeks were sampled five times, and showed 11% cutting on days 8-14
// against 29% since). Sale odds keep those weeks: they showed no such bias, and without
// them the replay had a quarter of the outcomes to learn from and overshot (15% predicted,
// 10% happened).
export function buildOutlookTable(periods, { asOf, soldMlsNumbers = null, sampleDays = null } = {}) {
  const onMarket = (periods || []).filter((period) => !period.preMarket);
  const usable = onMarket.filter((period) => period.asks);
  // The homes without a full history are not a random few: they are the ones that
  // changed price most often. Leaving many of them out would understate cuts.
  if (!asOf || !usable.length || usable.length / onMarket.length < OUTLOOK_MIN_HISTORY_COVERAGE) return null;
  const asOfDay = dayNumber(asOf);
  const lastCutDay = asOfDay - OUTLOOK_HORIZON_DAYS;
  const lastSaleDay = lastCutDay - EXIT_SETTLE_DAYS;
  const gaps = samplingGaps(sampleDays);
  const inGap = (day) => gaps.some(([from, to]) => from < day + OUTLOOK_HORIZON_DAYS && to > day);
  const cells = OUTLOOK_DOM_BANDS.map(() => [...OUTLOOK_CUT_STATES, null].map(emptyCell));
  const firstCutPcts = [];
  const laterCutPcts = [];
  let homes = 0;
  let firstDay = Infinity;

  for (const period of usable) {
    const cuts = periodCuts(period);
    const cutDays = cuts.map((cut) => dayNumber(cut.date));
    // On a home found mid-life the first cut seen may be its third, so its cuts' order is
    // unknown and they size neither list.
    if (period.trackedFromStart) for (const cut of cuts) (cut.ordinal === 1 ? firstCutPcts : laterCutPcts).push(cut.pct);
    const sold = !!soldMlsNumbers && period.mlsNumbers.some((mls) => soldMlsNumbers.has(mls));
    const exitDay = period.lastForSale && period.lastForSale < asOf ? dayNumber(period.lastForSale) : Infinity;
    let counted = false;
    let next = 0; // index of the first cut after `day`
    let reached = -Infinity; // two listings of one home can overlap; each day counts once
    for (const span of [...period.spans].sort((a, b) => a.from.localeCompare(b.from))) {
      if (!span.listDate) continue;
      const listDay = dayNumber(span.listDate);
      const to = Math.min(dayNumber(span.to), lastCutDay);
      for (let day = Math.max(dayNumber(span.from), reached + 1); day <= to; day += 1) {
        reached = day;
        const dom = day - listDay + span.priorDays;
        if (!(dom >= 0)) continue; // also a list date that did not parse
        while (next < cutDays.length && cutDays[next] <= day) next += 1;
        const state = next > 0 ? outlookCutState(day - cutDays[next - 1]) : period.trackedFromStart ? 0 : UNKNOWN_CUT_STATE;
        const cell = cells[outlookDomBand(dom)][state];
        if (!inGap(day)) {
          cell.cutDays += 1;
          if (next < cutDays.length && cutDays[next] <= day + OUTLOOK_HORIZON_DAYS) cell.cutHits += 1;
          if (day < firstDay) firstDay = day;
        }
        if (soldMlsNumbers && day <= lastSaleDay) {
          cell.saleDays += 1;
          if (sold && exitDay <= day + OUTLOOK_HORIZON_DAYS) cell.saleHits += 1;
        }
        counted = true;
      }
    }
    if (counted) homes += 1;
  }
  // No morning clear of a sampling gap means no cut odds, and so no table.
  if (homes < OUTLOOK_MIN_HOMES || !Number.isFinite(firstDay)) return null;

  const bands = cells.map((row) => {
    const total = emptyCell();
    row.forEach((cell) => addInto(total, cell));
    return total;
  });
  const overall = emptyCell();
  bands.forEach((band) => addInto(overall, band));
  const iso = (day) => new Date(day * DAY_MS).toISOString().slice(0, 10);
  return {
    asOf,
    homes,
    from: iso(firstDay),
    cutsThrough: iso(lastCutDay),
    salesThrough: overall.saleDays ? iso(lastSaleDay) : "",
    cells,
    bands,
    overall,
    firstCutPct: firstCutPcts.length >= OUTLOOK_MIN_CUTS_FOR_SIZE ? nullableMedian(firstCutPcts) : null,
    laterCutPct: laterCutPcts.length >= OUTLOOK_MIN_CUTS_FOR_SIZE ? nullableMedian(laterCutPcts) : null,
  };
}

const shrunk = (hits, days, parentRate) => (hits + OUTLOOK_PRIOR_DAYS * parentRate) / (days + OUTLOOK_PRIOR_DAYS);
const rate = (hits, days) => (days > 0 ? hits / days : null);

// Odds for a listing `dom` days in whose last cut was `daysSinceCut` days ago (null =
// none seen; with `cutHistoryKnown` false that means "cannot say", and the odds are its
// days-on-market band's). saleChance is null when no sale outcomes are known yet.
export function outlookFor(table, { dom, daysSinceCut = null, cutHistoryKnown = true } = {}) {
  if (!table || dom === null || dom === undefined || !Number.isFinite(dom) || dom < 0) return null;
  const bandIndex = outlookDomBand(dom);
  const known = daysSinceCut !== null || cutHistoryKnown;
  const stateIndex = known ? outlookCutState(daysSinceCut) : UNKNOWN_CUT_STATE;
  const band = table.bands[bandIndex];
  const cell = table.cells[bandIndex][stateIndex];
  const chance = (hits, days) => {
    const base = rate(table.overall[hits], table.overall[days]);
    if (base === null) return null;
    const bandRate = shrunk(band[hits], band[days], base);
    return known ? shrunk(cell[hits], cell[days], bandRate) : bandRate;
  };
  return {
    cutChance: chance("cutHits", "cutDays"),
    saleChance: chance("saleHits", "saleDays"),
    cutState: known ? OUTLOOK_CUT_STATES[stateIndex].id : "unknown",
    listingDays: known ? cell.cutDays : band.cutDays,
  };
}

// Where one marketing period stands on `asOf`, or null if it is no longer for sale then
// (a home under contract can linger in the feed, and in the dataset, as "active").
export function periodSituation(period, asOf) {
  if (!period?.asks || period.lastForSale !== asOf) return null;
  const dom = daysOnMarket(period, asOf);
  if (dom === null) return null;
  const cuts = periodCuts(period);
  const last = cuts[cuts.length - 1] || null;
  return {
    dom,
    daysSinceCut: last ? daysBetween(last.date, asOf) : null,
    cutHistoryKnown: !!last || !!period.trackedFromStart,
    cutCount: cuts.length,
    lastCut: last,
  };
}

// Recent events worth a second look, read from the listing's own history rather than
// from what changed since the buyer's last visit.
export const SIGNAL_RECENT_CUT_DAYS = OUTLOOK_CUT_STATES[1].maxDays;
export const SIGNAL_RECENT_RELIST_DAYS = 14;
export const SIGNAL_HOLDING_OUT_DAYS = 21;

export const LISTING_SIGNALS = [
  { id: "justCut", label: "Just cut", detail: `price cut in the last ${SIGNAL_RECENT_CUT_DAYS} days` },
  { id: "relisted", label: "Just relisted", detail: `back under a new MLS number in the last ${SIGNAL_RECENT_RELIST_DAYS} days` },
  { id: "holdingOut", label: "No cut yet", detail: `${SIGNAL_HOLDING_OUT_DAYS}+ days listed at the opening price` },
];

// A ledger this many days behind the calendar has no business saying "lately".
export const SIGNAL_MAX_LEDGER_AGE_DAYS = 3;

// Which signals a live period shows, given its periodSituation. Everything is measured
// on the ledger's own clock (`asOf`), the same one the odds and their chip use, so a card
// labelled "Just cut" is always in the "Just cut" count. If the daily job has stalled that
// clock is wrong about "lately", so there are no signals at all.
export function listingSignals(period, situation, asOf, today = asOf) {
  if (!period || !situation) return [];
  const behind = daysBetween(asOf, today);
  if (behind === null || behind > SIGNAL_MAX_LEDGER_AGE_DAYS) return [];
  const signals = [];
  if (situation.daysSinceCut !== null && situation.daysSinceCut <= SIGNAL_RECENT_CUT_DAYS) signals.push("justCut");
  const sinceRelist = period.relistCount > 0 ? daysBetween(period.latestListDate, asOf) : null;
  if (sinceRelist !== null && sinceRelist >= 0 && sinceRelist <= SIGNAL_RECENT_RELIST_DAYS) signals.push("relisted");
  // Only where the whole history was watched: a home found mid-life may have cut unseen.
  // One ask ever: a home that raised its price is not "still at its opening price".
  if (period.asks.length === 1 && situation.cutHistoryKnown && situation.dom >= SIGNAL_HOLDING_OUT_DAYS) signals.push("holdingOut");
  return signals;
}

// How the odds lean, for the chip on a listing. Thresholds are on the odds themselves.
export function outlookLean(outlook) {
  if (!outlook || outlook.saleChance === null || outlook.cutChance === null) return null;
  if (outlook.saleChance >= 0.25) return { id: "move", tone: "hot", label: "Likely gone soon" };
  if (outlook.cutState === "justCut") return { id: "justCut", tone: "warm", label: "Just cut: buyers look again" };
  if (outlook.cutChance >= 2 * outlook.saleChance) return { id: "wait", tone: "cool", label: "Time is on your side" };
  return { id: "even", tone: "warm", label: "Could go either way" };
}

// A rate for display: null until it rests on MIN_ROW_LISTING_DAYS of its own kind of day.
const shownRate = (hits, days) => (days >= MIN_ROW_LISTING_DAYS ? hits / days : null);

const summaryRow = (meta, total) => ({
  ...meta,
  ...total,
  cutChance: shownRate(total.cutHits, total.cutDays),
  saleChance: shownRate(total.saleHits, total.saleDays),
});

// The table rolled up to the four listing stages, plus the contrast a buyer can act
// on: what a fresh cut does to the odds of a sale once a listing is past two weeks.
export function summarizeOutlook(table) {
  if (!table) return null;
  const stages = LISTING_STAGES.map((stage) => {
    const total = emptyCell();
    OUTLOOK_DOM_BANDS.forEach((max, index) => {
      if (max <= stage.maxDom && max > stage.minDom - 1) addInto(total, table.bands[index]);
    });
    return summaryRow(stage, total);
  });
  const lateBy = OUTLOOK_CUT_STATES.map((state, stateIndex) => {
    const total = emptyCell();
    OUTLOOK_DOM_BANDS.forEach((max, index) => { if (max > 14) addInto(total, table.cells[index][stateIndex]); });
    return summaryRow(state, total);
  });
  return { stages, afterTwoWeeks: Object.fromEntries(lateBy.map((entry) => [entry.id, entry])), homes: table.homes, from: table.from, cutsThrough: table.cutsThrough, salesThrough: table.salesThrough, firstCutPct: table.firstCutPct, laterCutPct: table.laterCutPct };
}

const pct = (share) => `${Math.round(share * 100)}%`;

// Answers "is there any hurry?": a headline and the sentences that qualify it, or null.
export function buildOutlookVerdict(summary) {
  if (!summary) return null;
  const [fresh, , sitting, stale] = summary.stages;
  const late = emptyCell();
  addInto(late, sitting);
  addInto(late, stale);
  const cut = shownRate(late.cutHits, late.cutDays);
  const sale = shownRate(late.saleHits, late.saleDays);
  if (fresh.saleChance === null || cut === null || sale === null) return null;
  const ratio = sale > 0 ? cut / sale : null;
  const moves = sale < fresh.saleChance ? "falls to" : "is";
  const after = ratio !== null && ratio >= 1.5
    ? `After day 14 that ${moves} ${pct(sale)}, and a price cut (${pct(cut)}) is ${ratio.toFixed(ratio >= 3 ? 0 : 1)} times as likely as losing the home.`
    : `After day 14 that ${moves} ${pct(sale)}, against a ${pct(cut)} chance of a price cut.`;
  const { never, justCut } = summary.afterTwoWeeks;
  const bump = never.saleChance > 0 && justCut.saleChance !== null && justCut.saleChance >= 1.3 * never.saleChance
    ? ` The exception is the week after a cut, when the chance of a sale rises to ${pct(justCut.saleChance)} from ${pct(never.saleChance)} for homes that have not cut.`
    : "";
  return {
    headline: `In its first week a listing has a ${pct(fresh.saleChance)} chance of going under contract within ${OUTLOOK_HORIZON_DAYS} days.`,
    detail: `${after}${bump}`,
  };
}

// One table per home type, from periods whose current ask is within
// OUTLOOK_BAND_MARGIN of the lens's price band. Neighborhood filters are ignored on
// purpose: a few neighborhoods over four months are too few homes to fill a table.
export function buildOutlookTables(periods, { asOf, soldMlsNumbers = null, sampleDays = null, minAsk = 0, maxAsk = null } = {}) {
  const low = minAsk > 0 ? minAsk * (1 - OUTLOOK_BAND_MARGIN) : 0;
  const high = maxAsk !== null && maxAsk !== undefined && maxAsk > 0 ? maxAsk * (1 + OUTLOOK_BAND_MARGIN) : Infinity;
  const byType = new Map();
  for (const period of periods || []) {
    if (period.currentAsk < low || period.currentAsk > high) continue;
    const list = byType.get(period.typeLabel);
    if (list) list.push(period); else byType.set(period.typeLabel, [period]);
  }
  const tables = new Map();
  for (const [type, list] of byType) {
    const table = buildOutlookTable(list, { asOf, soldMlsNumbers, sampleDays });
    if (table) tables.set(type, table);
  }
  return tables;
}
