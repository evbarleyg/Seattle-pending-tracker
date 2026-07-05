import { addDays, daysBetween, formatDateShort, toDate } from "./format.mjs";

// Single source of truth for what every headline stat means, how it is
// computed, where its data comes from, and what to watch out for. The UI
// (tiles, captions, explain popovers, freshness copy) should read from here
// so definitions cannot drift between surfaces.
//
// Every formulaWords/caveat string below is grounded in the actual code:
// - computeBaseStats / computeSlices / computeOpenMlsRows / computeActiveBidSuggestions (selectors.mjs)
// - normalizeRow provenance flags, domMetric, hot thresholds (data.mjs)
// - rowMatchesProfile / buildProfileCohort (buyerProfile.mjs)
// - competitiveDelta sign conventions (pulseMetrics.mjs)
// - the freshness card and FRESHNESS_STALE_DAYS = 7 (main.mjs)

export const BUYER_DIRECTIONS = ["higherIsWorse", "higherIsBetter", "neutral"];

export const UNIVERSE_IDS = ["allRows", "closedSlice", "savedMatches", "actives"];

export const SOURCES = {
  "kc-assessor": { id: "kc-assessor", label: "King County assessor recorded sales" },
  redfin: { id: "redfin", label: "Redfin listing feed" },
  "mls-manual": { id: "mls-manual", label: "Manual MLS exports" },
  derived: { id: "derived", label: "Computed in your browser from the loaded data" },
};

// Known publish intervals, used by formatCadenceNote to estimate the next
// update. Only the county source has a predictable cadence; Redfin and MLS
// exports refresh whenever the pipeline is run by hand.
const UPDATE_INTERVAL_DAYS = {
  "kc-assessor": 14,
};

const CADENCE = {
  "kc-assessor":
    "King County publishes recorded sales about every 2 weeks, and a sale takes about 2 more weeks to show up after closing. A newest sale that is 1 to 3 weeks old is normal, not a data problem.",
  redfin:
    "Redfin listings are pulled fresh each time the data pipeline runs, so they are as current as the last refresh.",
  "mls-manual":
    "MLS exports are downloaded by hand, so they update only when a new export is saved and the pipeline is rerun.",
  derived:
    "Recomputed instantly in your browser whenever the data or your filters change, so it is as fresh as the underlying sources.",
};

// Shared caveat: only MLS/Redfin rows ever carry a genuine list price. County
// rows fall back to the tax-assessed value, which normalizeRow refuses to
// treat as a list price (hasMarketListPrice stays false and sale/list math is
// zeroed), so list-price metrics rest on a subset of the comps shown.
const REAL_LIST_CAVEAT =
  "Only sales with a genuine MLS or Redfin list price count here. County-only rows never have one, so a large majority of all rows are excluded; check the based-on count shown with the number.";

// Shared caveat: the headline stats are computed on every closed sale that
// passes the sidebar filters (computeBaseStats runs on closedRows), BEFORE
// chart cross-filter clicks are applied, while the comps-in-slice count is
// the post-click slice. The two can legitimately differ.
const STATS_BEFORE_CLICKS_CAVEAT =
  "Computed on every closed sale that passes your filters, before any chart cross-filter clicks, so it can cover more homes than the comps-in-slice count.";

export const METRICS = {
  medianClose: {
    id: "medianClose",
    label: "Median close",
    plain: "The typical final sale price for homes like the ones in your current slice.",
    formulaWords:
      "Sort every closed sale in your slice by its closing price and take the middle one: half sold for more, half for less.",
    source: { id: "kc-assessor", label: "King County recorded sales, enriched with manual MLS exports" },
    cadence: CADENCE["kc-assessor"],
    caveats: [
      STATS_BEFORE_CLICKS_CAVEAT,
      "County recording lags closings by about 2 weeks, so the very newest sales may not be counted yet.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  medianPsf: {
    id: "medianPsf",
    label: "Median $/sqft",
    plain: "The typical price paid per square foot of living space, which lets you compare homes of different sizes.",
    formulaWords:
      "For each closed sale with a known size, divide the closing price by the square footage, then take the middle value across your slice.",
    source: { id: "kc-assessor", label: "King County recorded sales, enriched with manual MLS exports" },
    cadence: CADENCE["kc-assessor"],
    caveats: [
      "Homes with no recorded square footage are left out of this number.",
      STATS_BEFORE_CLICKS_CAVEAT,
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  overAskRatio: {
    id: "overAskRatio",
    label: "Over-ask ratio",
    plain: "How the typical sale price compares with the asking price: 1.05x means 5 percent over ask, 0.95x means 5 percent under.",
    formulaWords:
      "For each closed sale with a real list price, divide the closing price by that list price, then take the middle ratio across your slice.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      REAL_LIST_CAVEAT,
      "Sales split almost evenly just over and just under ask, so this median often lands at exactly 1.00x and hides the real spread. The cost-to-win stats below show that spread directly.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  shareOverAsk: {
    id: "shareOverAsk",
    label: "Share sold over ask",
    plain: "The share of winning buyers who had to pay more than the asking price.",
    formulaWords:
      "Of the closed sales in your slice with a real list price, count how many closed above that list price, and show it as a percentage of those sales.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [REAL_LIST_CAVEAT],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  typicalPremiumWhenOver: {
    id: "typicalPremiumWhenOver",
    label: "Typical premium when over ask",
    plain: "When a home does sell over ask, this is what the winner typically paid above the list price.",
    formulaWords:
      "Take only the closed sales that went over their real list price, compute how far over each one went in dollars and percent, and report the middle value.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "This is the typical premium if you end up in a bidding war, not the chance of being in one. Pair it with the share sold over ask.",
      REAL_LIST_CAVEAT,
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  p75Premium: {
    id: "p75Premium",
    label: "75th percentile premium",
    plain: "A bad-but-plausible case: one in four over-ask winners paid more than this above the list price.",
    formulaWords:
      "Line up the over-ask premiums from smallest to largest and take the value three quarters of the way up: 75 percent of over-ask sales paid less than this, 25 percent paid more.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Based only on sales that went over ask, which can be a small group in a quiet slice, so treat it as a rough planning number.",
      REAL_LIST_CAVEAT,
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  medianDom: {
    id: "medianDom",
    label: "Median days on market",
    plain: "How long a typical home in your slice sat before a buyer locked it up. More days means more room to breathe.",
    formulaWords:
      "For each closed sale that has one, take its days-on-market count, preferring the cumulative count that survives relistings, then the plain count, then the days from listing to going pending. Report the middle value.",
    source: { id: "mls-manual", label: "Manual MLS exports (county rows carry no days on market)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "County-only rows have no days-on-market and are left out entirely, so this reflects the MLS-covered part of your slice.",
      "The cumulative count means a home that was pulled and relisted shows its full time on market, not just the latest listing.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "closedSlice",
  },

  fastSaleShare: {
    id: "fastSaleShare",
    label: "Fast-sale share",
    plain: "The share of homes that sold fast, a gauge of how much competition you would face.",
    formulaWords:
      "Out of all closed sales in your slice, the percentage that went pending in 10 days or less.",
    source: { id: "mls-manual", label: "Manual MLS exports (speed is measured from MLS days on market)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "County-only rows have no days-on-market, so they always count as not-fast. A slice heavy on county rows will understate the true share.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "closedSlice",
  },

  activePending: {
    id: "activePending",
    label: "Active + pending (MLS only)",
    plain: "How many homes matching your filters are on the market or under contract right now. More choice generally favors you.",
    formulaWords:
      "Counts the MLS and Redfin listings in your slice that have not closed yet, either active or pending, plus projected-pending rows when that feature is on.",
    source: { id: "redfin", label: "Redfin active listings plus manual MLS exports" },
    cadence: CADENCE.redfin,
    caveats: [
      "MLS and Redfin listings only. County rows never appear here, so this is a flow gauge, not a full inventory count.",
      "Both halves are feature-flag gated: turn off Include Open/Pending MLS or Pending Projection and the count drops.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  savedMatches: {
    id: "savedMatches",
    label: "Saved-home matches",
    plain: "Closed sales in your slice that look like the homes you saved, so you can see what winning ones actually cost.",
    formulaWords:
      "Counts closed sales in your slice that sit in your watchlist areas or flex neighborhoods, share your saved home type, and score at least 60 of 100 on similarity across beds, baths, size, year built, and lot.",
    source: { id: "derived", label: "Computed in your browser from your saved-home profile" },
    cadence: CADENCE.derived,
    caveats: [
      "Matched only within your current slice, so changing filters changes the count.",
      "Pausing the saved-home lens hides this count but does not change your slice or the other stats.",
    ],
    buyerDirection: "neutral",
    universeId: "closedSlice",
  },

  bidQueue: {
    id: "bidQueue",
    label: "Bid queue",
    plain: "Active listings in your slice, and how many of them have enough recent comparable sales to back a suggested bid.",
    formulaWords:
      "Counts active MLS listings that pass your filters. Each gets a suggested bid only when at least 6 similar closed sales with a real list price from the last 90 days exist, weighted toward the most recent ones; suggestions scoring 75 or more count as high confidence.",
    source: { id: "derived", label: "Computed in your browser from active listings and recent closed comps" },
    cadence: CADENCE.derived,
    caveats: [
      "Listings without at least 6 usable comps show as insufficient comps rather than getting a guess.",
      "Filtering the MLS status to anything other than All or Active empties the queue.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  freshness: {
    id: "freshness",
    label: "Freshness",
    plain: "How current the data is: the date of the newest closed sale that matches your filters, plus whether the last data refresh passed its checks.",
    formulaWords:
      "Takes the newest closing date among sales that pass your filters, counts how many days ago that was, and pairs it with the last pipeline run's validation result and the total row count.",
    source: { id: "kc-assessor", label: "King County assessor, the slowest source in the blend" },
    cadence: CADENCE["kc-assessor"],
    caveats: [
      "The newest-sale date depends on your filters: a narrow slice can look staler than the dataset really is.",
      "A red warning on this card means the last data refresh failed its checks, not that the data is a few days old; a newest sale 1 to 3 weeks back is the county's normal rhythm.",
    ],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  // Universe definitions: the row sets that headline stats are computed over.
  // savedMatches above doubles as both a metric (the count) and a universe
  // (the matching rows themselves).

  allRows: {
    id: "allRows",
    label: "All loaded rows",
    plain: "Every row in the data file before any filters, covering roughly the last 12 months of King County sales plus current listings.",
    formulaWords:
      "The raw row count of the loaded file: county recorded sales, manual MLS export rows, and Redfin listings merged into one dataset.",
    source: { id: "kc-assessor", label: "King County assessor base, enriched with manual MLS exports and Redfin" },
    cadence: CADENCE["kc-assessor"],
    caveats: [
      "Most rows are county-only and carry no list price or days on market, which is why several stats use only a subset of them.",
    ],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  closedSlice: {
    id: "closedSlice",
    label: "Closed comps in your slice",
    plain: "The closed sales that match your current filters. Most headline numbers are computed from these.",
    formulaWords:
      "Start from rows with a real closing price and sale date, keep only those passing your price band, home type, neighborhoods, lot size, dates, and status filters, then apply any chart cross-filter clicks.",
    source: { id: "derived", label: "Computed in your browser by filtering the loaded rows" },
    cadence: CADENCE.derived,
    caveats: [
      "The tile stats are computed one step earlier, before chart cross-filter clicks, so the comps-in-slice count and the stats can cover slightly different sets.",
    ],
    buyerDirection: "neutral",
    universeId: "closedSlice",
  },

  actives: {
    id: "actives",
    label: "Active and pending listings",
    plain: "Listings in your slice that are still on the market or under contract, not yet closed.",
    formulaWords:
      "MLS and Redfin rows with no closing yet and a real list price, filtered with your same slice settings applied to the list price and the most recent activity date.",
    source: { id: "redfin", label: "Redfin active listings plus manual MLS exports" },
    cadence: CADENCE.redfin,
    caveats: [
      "County rows never appear here.",
      "Hidden entirely when the Include Open/Pending MLS feature flag is off.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },
};

export function getMetric(id) {
  const entry = METRICS[id];
  if (!entry) throw new Error(`Unknown glossary metric id: ${id}`);
  return entry;
}

/**
 * Build a plain-language freshness sentence for a metric, including a
 * next-expected-update estimate when the source has a known cadence.
 *
 * metricOrId: a METRICS entry or its id string.
 * datasetDates: { latestSaleDate?, generatedAt?, now? } where latestSaleDate
 * is the newest closed-sale date in the data, generatedAt is the last data
 * pipeline run (data_refresh_report.json generatedAt), and now is an optional
 * date override for testing.
 */
export function formatCadenceNote(metricOrId, datasetDates = {}) {
  const metric = typeof metricOrId === "string" ? getMetric(metricOrId) : metricOrId;
  if (!metric || !metric.cadence) throw new Error("formatCadenceNote needs a glossary metric or metric id");
  const now = toDate(datasetDates.now) || new Date();
  const parts = [metric.cadence];

  const latestSale = toDate(datasetDates.latestSaleDate);
  if (latestSale) {
    const age = Math.max(0, daysBetween(latestSale, now) ?? 0);
    const ageText = age === 0 ? "today" : age === 1 ? "1 day ago" : `${age} days ago`;
    parts.push(`The newest sale in the data closed ${ageText} (${formatDateShort(latestSale)}).`);
  }

  const intervalDays = UPDATE_INTERVAL_DAYS[metric.source?.id];
  if (intervalDays) {
    const basis = toDate(datasetDates.generatedAt) || latestSale;
    if (basis) {
      const nextExpected = addDays(basis, intervalDays);
      if (nextExpected && nextExpected.getTime() <= now.getTime()) {
        parts.push("The next update is due about now.");
      } else if (nextExpected) {
        parts.push(`Next update expected around ${formatDateShort(nextExpected)}.`);
      }
    }
  } else if (metric.source?.id !== "derived") {
    parts.push("It updates the next time the data pipeline runs.");
  }

  return parts.join(" ");
}
