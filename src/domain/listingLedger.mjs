// listingLedger — supply-side reads from the published actives ledger.
//
// The sales dataset only knows homes that sold. public/listing_ledger.csv (a slim
// copy of redfin_active_ledger.csv, one row per MLS# ever seen for sale) also
// remembers the ones that did not: when each listing appeared and vanished, what
// it first and last asked, and which listings are the same home relisted under a
// new MLS#. That is what standing inventory, months of supply, price cuts and
// withdrawals are made of.
//
// Everything here counts MARKETING PERIODS, not MLS numbers. Cancelling and
// relisting is routine (30% of the listings live on 2026-11-08 were relists, 80%
// of relists came back cheaper), so counting MLS#s overstates new supply,
// understates time on market and hides price cuts taken between listings.
//
// Pure functions, no DOM and no fetch: main.mjs loads the file and passes rows in.
import { ZIP_NEIGHBORHOOD, parseCsvLine, zip5 } from "./data.mjs";
import { addDays, daysBetween, nullableMedian, num, toIso } from "./format.mjs";
import { LISTING_STAGES, stageForDom } from "./listingStage.mjs";

export const LISTING_LEDGER_FILE = "listing_ledger.csv";

// The ledger stores the feed's label on git-seeded rows and its numeric
// uiPropertyType code on rows upserted from the raw fetch.
const TYPE_LABELS = {
  1: "Single Family",
  2: "Condo",
  3: "Townhouse",
  4: "Multi-Family",
  5: "Land",
};

function ledgerTypeLabel(raw) {
  const text = String(raw || "").trim();
  return TYPE_LABELS[text] || text;
}

export function parseListingLedger(text) {
  const lines = String(text || "").split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  if (!headers.includes("mlsNumber") || !headers.includes("firstSeen")) return [];
  const tracksForSale = headers.includes("lastSeenActive");
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const pick = (name) => {
      const index = headers.indexOf(name);
      return index >= 0 ? String(cols[index] ?? "").trim() : "";
    };
    const zip = zip5(pick("zip"));
    return {
      mlsNumber: pick("mlsNumber"),
      address: pick("address"),
      zip,
      neighborhoodLabel: ZIP_NEIGHBORHOOD[zip] || "Seattle (Other)",
      typeLabel: ledgerTypeLabel(pick("propertyType")),
      firstSeen: pick("firstSeen"),
      lastSeen: pick("lastSeen"),
      // Last morning genuinely for sale. Blank means the listing was only ever
      // seen under contract; a ledger without the column cannot tell, so every
      // sighting counts.
      lastSeenActive: tracksForSale ? pick("lastSeenActive") : pick("lastSeen"),
      firstAsk: num(pick("firstAsk")),
      lastAsk: num(pick("lastAsk")),
      listDate: pick("listDate"),
      lastStatus: pick("lastStatus"),
      chainId: pick("chainId"),
      chainPosition: num(pick("chainPosition")),
      chainListDate: pick("chainListDate"),
      chainFirstAsk: num(pick("chainFirstAsk")),
      chainPriorDays: num(pick("chainPriorDays")),
      // First seen within 3 days of listing, so nothing it did earlier went unseen.
      trackedFromListing: pick("trackedFromListing") === "true",
      // Every ask with the first morning it was seen, oldest first. A recorded
      // history always holds at least the opening ask, so blank means the ledger
      // predates the column (the published copy writes the header regardless):
      // null, "not recorded", which is different from "never changed".
      // So is a history with fewer entries than the changes on record (written
      // where git had no depth to rebuild it from): the cuts in between are missing.
      asks: completeOrNull(parseAskHistory(pick("askHistory")), num(pick("askChangeCount")), num(pick("lastAsk"))),
    };
  }).filter((row) => row.mlsNumber && row.firstSeen && row.lastSeen);
}

// Same rule as hasFullAskHistory in scripts/build_active_ledger.js.
const completeOrNull = (asks, changeCount, lastAsk) => (asks.length > changeCount && asks[asks.length - 1].ask === lastAsk ? asks : null);

// "2026-07-15:1250000|2026-08-02:1199000" -> [{ date, ask }, ...]. Same grammar as
// parseAskHistory in scripts/build_active_ledger.js: anything else is dropped.
export function parseAskHistory(text) {
  return String(text || "").split("|").flatMap((part) => {
    const match = /^(\d{4}-\d{2}-\d{2}):(\d+)$/.exec(part.trim());
    return match && Number(match[2]) > 0 ? [{ date: match[1], ask: Number(match[2]) }] : [];
  });
}

// The last morning the feed was sampled: a listing seen that day is for sale now.
export function ledgerAsOf(rows) {
  return (rows || []).reduce((latest, row) => (row.lastSeen > latest ? row.lastSeen : latest), "");
}

// Every day the feed is known to have been sampled (each is some listing's first
// or last sighting). Read from the whole ledger, before any filtering.
export function ledgerSampleDays(rows) {
  const days = new Set();
  for (const row of rows || []) {
    days.add(row.firstSeen);
    days.add(row.lastSeen);
  }
  return days;
}

// Home types the listings feed follows (single family and townhouse today). A
// type outside this set has no supply read at all, which is different from
// "nothing for sale", and its closings must not set the pace for the others.
export function ledgerTrackedTypes(rows) {
  return new Set((rows || []).map((row) => row.typeLabel).filter(Boolean));
}

// "Coming Soon" teasers run under a 19-digit placeholder id; rows seeded from old
// snapshots lost the status, so the id identifies them too (as in the pipeline).
function isTeaser(row) {
  return row.lastStatus.toUpperCase() === "COMING SOON" || /^\d{15,}$/.test(row.mlsNumber);
}

// One ask at a time. A home is sometimes live under two MLS#s at once (a duplicate
// entry, or a relist that overlaps the old listing by a day); interleaving both by date
// would read every difference between them as a cut and a raise. A listing wholly inside
// another's stretch is ignored, and otherwise each speaks until the next one appears.
function mergeAsks(group) {
  const rows = [...group].sort((a, b) => a.firstSeen.localeCompare(b.firstSeen) || b.lastSeen.localeCompare(a.lastSeen));
  const kept = rows.filter((row, index) => !rows.some((other, at) => at < index && other.lastSeen >= row.lastSeen));
  const points = kept.flatMap((row, index) => {
    const next = kept[index + 1];
    return next ? row.asks.filter((point) => point.date < next.firstSeen) : row.asks;
  });
  return points.filter((point, index) => index === 0 || point.ask !== points[index - 1].ask);
}

// One entry per home per continuous stretch on the market. A ledger written
// before relist chains existed has no chainId, so every MLS# stands alone.
export function buildMarketingPeriods(rows) {
  const groups = new Map();
  const asOf = ledgerAsOf(rows);
  for (const row of rows || []) {
    // A "Coming Soon" teaser that has gone was replaced by the real listing under
    // its own MLS#; keeping it would count the home twice and read the hand-off
    // as a withdrawal. One still showing today is genuine upcoming supply.
    if (isTeaser(row) && row.lastSeen < asOf) continue;
    const key = row.chainId || row.mlsNumber;
    const group = groups.get(key);
    if (group) group.push(row); else groups.set(key, [row]);
  }
  const periods = [];
  for (const [id, group] of groups) {
    group.sort((a, b) => (a.chainPosition - b.chainPosition) || a.firstSeen.localeCompare(b.firstSeen));
    const first = group[0];
    const last = group.reduce((latest, row) => (row.lastSeen >= latest.lastSeen ? row : latest), first);
    const openingAsk = first.chainFirstAsk || first.firstAsk;
    periods.push({
      id,
      mlsNumbers: group.map((row) => row.mlsNumber),
      address: last.address,
      zip: last.zip,
      neighborhoodLabel: last.neighborhoodLabel,
      typeLabel: last.typeLabel,
      startDate: first.chainListDate || first.listDate || "",
      // Cumulative days on market = days the earlier listings were up, plus days
      // since the latest one listed. Time off the market in between is not counted.
      latestListDate: last.listDate || "",
      priorDays: last.chainPriorDays,
      firstSeen: group.reduce((earliest, row) => (row.firstSeen < earliest ? row.firstSeen : earliest), first.firstSeen),
      lastSeen: last.lastSeen,
      // Last morning it was genuinely for sale: an "Active Under Contract" tail
      // keeps a home in the feed after it has stopped being available.
      lastForSale: last.lastSeenActive,
      // Each listing's own stretch in the feed. Between relists the home was off
      // the market, and must not count as standing inventory on those days.
      // listDate and priorDays give cumulative days on market on any day inside it.
      spans: group.filter((row) => row.lastSeenActive).map((row) => ({ from: row.firstSeen, to: row.lastSeenActive, listDate: row.listDate || "", priorDays: row.chainPriorDays })),
      // The whole period's asks in date order, across relists: coming back cheaper
      // under a new MLS# is a price cut like any other. null = history not recorded.
      asks: group.every((row) => row.asks) ? mergeAsks(group) : null,
      // False when the home was already on the market when tracking found it: cuts
      // it made before then were never seen.
      trackedFromStart: first.trackedFromListing,
      openingAsk,
      currentAsk: last.lastAsk,
      cutPct: openingAsk > 0 && last.lastAsk > 0 && last.lastAsk < openingAsk ? 1 - last.lastAsk / openingAsk : 0,
      relistCount: group.length - 1,
      chainKnown: !!first.chainId,
      // Still a "Coming Soon" teaser: announced, not yet on the market.
      preMarket: isTeaser(last),
    });
  }
  return periods;
}

// The ledger only knows a ZIP, while the dataset labels some homes by Redfin's own
// neighborhood (every ZIP outside the built-in map, such as 98146 Arbor Heights).
// Where a period's MLS# is in the dataset it takes that row's label, so one
// neighborhood filter picks the same homes on both sides. Every live listing and
// every sale is there; only withdrawn listings keep the ZIP label.
export function applyDatasetNeighborhoods(periods, labelByMls) {
  if (!labelByMls?.size) return periods || [];
  for (const period of periods || []) {
    for (let i = period.mlsNumbers.length - 1; i >= 0; i -= 1) {
      const label = labelByMls.get(period.mlsNumbers[i]);
      if (label) { period.neighborhoodLabel = label; break; }
    }
  }
  return periods || [];
}

// The same lens the rest of the app applies, on the fields the ledger has: home
// type, neighborhood, and the price band read against the current ask.
export function filterPeriods(periods, filters = {}) {
  const neighborhoods = Array.isArray(filters.neighborhoods) ? filters.neighborhoods : [];
  const excluded = Array.isArray(filters.excludeTypes) ? filters.excludeTypes : [];
  return (periods || []).filter((period) => {
    if (filters.type && filters.type !== "All" && period.typeLabel !== filters.type) return false;
    if (excluded.includes(period.typeLabel)) return false;
    if (neighborhoods.length && !neighborhoods.includes(period.neighborhoodLabel)) return false;
    if (filters.minClose && period.currentAsk < filters.minClose) return false;
    if (filters.maxClose !== null && filters.maxClose !== undefined && period.currentAsk > filters.maxClose) return false;
    return true;
  });
}

// Closings to pace supply against, filtered by exactly the fields filterPeriods
// can apply. Using the app's fully filtered sales instead would divide listings
// that ignore (say) the lot-size filter by sales that obey it.
export function countClosingsInLens(rows, filters = {}, { asOf, windowDays, trackedTypes = null }) {
  const neighborhoods = Array.isArray(filters.neighborhoods) ? filters.neighborhoods : [];
  const excluded = Array.isArray(filters.excludeTypes) ? filters.excludeTypes : [];
  const from = shiftIso(asOf, -windowDays);
  let count = 0;
  for (const row of rows || []) {
    if (!row.hasActualClose || !(row.saleDate > from) || row.saleDate > asOf) continue;
    if (trackedTypes && !trackedTypes.has(row.typeLabel)) continue;
    if (filters.type && filters.type !== "All" && row.typeLabel !== filters.type) continue;
    if (excluded.includes(row.typeLabel)) continue;
    if (neighborhoods.length && !neighborhoods.includes(row.neighborhoodLabel)) continue;
    if (filters.minClose && row.closePrice < filters.minClose) continue;
    if (filters.maxClose !== null && filters.maxClose !== undefined && row.closePrice > filters.maxClose) continue;
    count += 1;
  }
  return count;
}

export function daysOnMarket(period, onDate) {
  if (period.preMarket) return 0; // matches the 0 days the pipeline writes for a teaser
  if (!period.latestListDate) return null;
  const days = daysBetween(period.latestListDate, onDate);
  return days === null || days < 0 ? null : days + period.priorDays;
}

function isLiveOn(period, date) {
  return period.spans.some((span) => span.from <= date && span.to >= date);
}

function shiftIso(date, days) {
  return toIso(addDays(date, days));
}

// A count "on" a date is only as good as the sampling around it. In a long gap
// (the ledger has a 16-day one, Jun 19 to Jul 5 2026) only listings seen on both
// sides read as live, which undercounts badly, so such dates are skipped.
export const MAX_SAMPLE_DISTANCE_DAYS = 3;

function isSampledNear(date, sampleDays) {
  if (!sampleDays) return true;
  for (let offset = 0; offset <= MAX_SAMPLE_DISTANCE_DAYS; offset += 1) {
    if (sampleDays.has(shiftIso(date, -offset)) || sampleDays.has(shiftIso(date, offset))) return true;
  }
  return false;
}

// A closing follows the last day on the market by 24 days at the median and 30
// at the 75th percentile, so an exit is only judged sold-or-withdrawn once this
// long has passed; the look-back keeps the sample recent.
export const EXIT_SETTLE_DAYS = 45;
export const EXIT_LOOKBACK_DAYS = 135;
export const MIN_EXIT_SAMPLE = 8;
export const LONG_LISTED_DAYS = 30;
export const MIN_AGE_COVERAGE = 0.8;
// Fewer closings than this in the pace window is an anecdote, not a pace: one
// sale in 90 days would read as "9 months of supply, roomy".
export const MIN_PACE_SALES = 6;

// Where supply stands on `asOf`. `recentSales` is the lens's closings over the
// last `salesWindowDays`; `soldMlsNumbers` is every MLS# with a recorded closing.
export function computeSupplySnapshot(periods, { asOf, recentSales = 0, salesWindowDays = 90, soldMlsNumbers = null } = {}) {
  const list = periods || [];
  const live = asOf ? list.filter((period) => period.lastForSale === asOf) : [];
  const monthlySales = recentSales >= MIN_PACE_SALES ? recentSales / (salesWindowDays / 30) : null;
  // Listings without a list date are not a random subset (a ledger from before
  // first-sighting estimates lacks one on every listing since 2026-09-21, the
  // newest ones), so age stats are only drawn when nearly every home has one.
  const knownAges = live.map((period) => daysOnMarket(period, asOf)).filter((days) => days !== null);
  const ages = live.length && knownAges.length / live.length >= MIN_AGE_COVERAGE ? knownAges : [];
  const cutters = live.filter((period) => period.cutPct > 0);
  const stageCounts = Object.fromEntries(LISTING_STAGES.map((stage) => [stage.id, 0]));
  for (const days of ages) stageCounts[stageForDom(days)] += 1;
  const arrivedSince = (days) => {
    const cutoff = shiftIso(asOf, -days);
    return list.filter((period) => period.firstSeen > cutoff && period.firstSeen <= asOf).length;
  };

  // Without relist chains every relist looks like a withdrawal followed by a new
  // listing, so relist and exit reads wait for a ledger that has them.
  const chainsKnown = list.some((period) => period.chainKnown);

  let exits = null;
  if (asOf && chainsKnown && soldMlsNumbers?.size) {
    const from = shiftIso(asOf, -EXIT_LOOKBACK_DAYS);
    const to = shiftIso(asOf, -EXIT_SETTLE_DAYS);
    const settled = list.filter((period) => period.lastSeen >= from && period.lastSeen <= to);
    const summarize = (group) => {
      const sold = group.filter((period) => period.mlsNumbers.some((mls) => soldMlsNumbers.has(mls))).length;
      return { count: group.length, soldShare: group.length ? sold / group.length : null, withdrawnShare: group.length ? 1 - sold / group.length : null };
    };
    const settledByStage = new Map(LISTING_STAGES.map((stage) => [stage.id, []]));
    for (const period of settled) settledByStage.get(stageForDom(daysOnMarket(period, period.lastForSale)))?.push(period);
    exits = {
      from,
      to,
      all: summarize(settled),
      byStage: Object.fromEntries(LISTING_STAGES.map((stage) => [stage.id, summarize(settledByStage.get(stage.id))])),
    };
  }

  return {
    asOf: asOf || "",
    activeCount: live.length,
    agedCount: ages.length,
    medianDays: nullableMedian(ages),
    longListedShare: ages.length ? ages.filter((days) => days > LONG_LISTED_DAYS).length / ages.length : null,
    stageCounts,
    cutCount: cutters.length,
    cutShare: live.length ? cutters.length / live.length : null,
    medianCutPct: nullableMedian(cutters.map((period) => period.cutPct)),
    medianCutUsd: nullableMedian(cutters.map((period) => period.openingAsk - period.currentAsk)),
    chainsKnown,
    relistCount: live.filter((period) => period.relistCount > 0).length,
    relistShare: live.length && chainsKnown ? live.filter((period) => period.relistCount > 0).length / live.length : null,
    newLast7: asOf ? arrivedSince(7) : 0,
    newLast28: asOf ? arrivedSince(28) : 0,
    recentSales,
    monthlySales,
    monthsOfSupply: monthlySales && live.length ? live.length / monthlySales : null,
    exits,
  };
}

// Standing inventory each week, newest last. The ledger's first week is left out
// (it was still discovering what was already listed).
export function computeSupplyWeeklySeries(periods, { asOf, weeks = 16, sampleDays = null } = {}) {
  const list = periods || [];
  if (!asOf || !list.length) return [];
  const ledgerStart = list.reduce((earliest, period) => (period.firstSeen < earliest ? period.firstSeen : earliest), asOf);
  const firstUsable = shiftIso(ledgerStart, 7);
  const out = [];
  for (let back = weeks - 1; back >= 0; back -= 1) {
    const date = shiftIso(asOf, -7 * back);
    if (date < firstUsable || !isSampledNear(date, sampleDays)) continue;
    out.push({ date, activeCount: list.filter((period) => isLiveOn(period, date)).length });
  }
  return out;
}

// Month-end standing inventory (the current month reads as of `asOf`), in the
// { month, activeInventory, sampleSize } shape the Overview tile already plots.
export function computeSupplyMonthlySeries(periods, { asOf, months = 12, sampleDays = null } = {}) {
  const list = periods || [];
  if (!asOf || !list.length) return [];
  const ledgerStart = list.reduce((earliest, period) => (period.firstSeen < earliest ? period.firstSeen : earliest), asOf);
  const out = [];
  let [year, month] = asOf.slice(0, 7).split("-").map(Number);
  for (let i = 0; i < months; i += 1) {
    const key = `${year}-${String(month).padStart(2, "0")}`;
    const monthEnd = toIso(new Date(year, month, 0));
    const date = monthEnd > asOf ? asOf : monthEnd;
    if (date < ledgerStart) break;
    if (isSampledNear(date, sampleDays)) {
      const count = list.filter((period) => isLiveOn(period, date)).length;
      out.unshift({ month: key, activeInventory: count, sampleSize: count });
    }
    month -= 1;
    if (month === 0) { month = 12; year -= 1; }
  }
  return out;
}

// Common rule of thumb: under ~2 months favors sellers, over ~4 favors buyers.
export function supplyBalanceLabel(monthsOfSupply) {
  if (!Number.isFinite(monthsOfSupply)) return "";
  if (monthsOfSupply < 2) return "tight";
  if (monthsOfSupply <= 4) return "balanced";
  return "roomy";
}

// One sentence answering "how much choice and leverage is there right now?".
export function buildSupplyVerdict(snapshot) {
  if (!snapshot || !snapshot.activeCount) return "No homes for sale match these filters right now.";
  const homes = `${snapshot.activeCount} ${snapshot.activeCount === 1 ? "home is" : "homes are"} for sale in this slice`;
  if (!snapshot.agedCount || snapshot.longListedShare === null) return `${homes}.`;
  const fresh = snapshot.stageCounts.fresh;
  const older = Math.round(snapshot.longListedShare * 100);
  const cut = snapshot.cutShare === null ? "" : ` and ${Math.round(snapshot.cutShare * 100)}% already ask less than they first did`;
  // "but only" is a claim that fresh listings are scarce; make it only when true.
  if (snapshot.activeCount >= 8 && fresh / snapshot.agedCount <= 0.25) {
    const scarce = fresh === 0 ? "none is" : fresh === 1 ? "only 1 is" : `only ${fresh} are`;
    return `${homes}, but ${scarce} in the competitive first week: ${older}% have been listed over ${LONG_LISTED_DAYS} days${cut}.`;
  }
  return `${homes}, ${fresh} of them in the first week: ${older}% have been listed over ${LONG_LISTED_DAYS} days${cut}.`;
}
