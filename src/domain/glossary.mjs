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
// - competitiveDelta sign conventions + watchlist pockets (pulseMetrics.mjs)
// - scoreBidForRow / bidConfidenceScore (selectors.mjs), selectBidComps (bidComps.mjs)
// - computeAffordability / listingTier (affordability.mjs)
// - buildSliceWeeklySeries, ratioColor, record cells, data tiles (main.mjs)
// - the validation report shape (scripts/validate_data_refresh.js)
// - the freshness card and FRESHNESS_STALE_DAYS = 7 (main.mjs)

export const BUYER_DIRECTIONS = ["higherIsWorse", "higherIsBetter", "neutral"];

export const UNIVERSE_IDS = [
  "allRows",
  "closedSlice",
  "savedMatches",
  "actives",
  "pulseWatchlist",
  "recordRows",
  "affordScenario",
];

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
    caveats: [STATS_BEFORE_CLICKS_CAVEAT],
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
      "Out of all closed sales in your slice, the percentage counted as fast: days on market of 10 days or less, or a hot-sale tag from the data pipeline. The two rules mostly agree, but a small share of homes count as fast on the tag alone.",
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
      "MLS and Redfin listings only; county rows never appear here.",
      "The trend line is month-end standing inventory from the daily listings ledger (relisted homes counted once), which starts in June 2026.",
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
      "Counts active MLS listings that pass your filters. Each gets a suggested bid only when closed sales with a real list price from the last 12 months, weighted by how closely they resemble it, add up to the equivalent of at least 6 comps; suggestions scoring 75 or more count as high confidence.",
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
      "A red warning means the last data refresh failed its checks, not that the data is a few days old.",
    ],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  // --- Pulse tab -----------------------------------------------------------
  // Grounded in buildPulseSnapshot / summarizeRows / computeWindowSummary
  // (pulseMetrics.mjs, selectors.mjs) and buildSliceWeeklySeries /
  // competitionPocketEntries (main.mjs). The tiles compare the last 90 days
  // with the 90 days before; the charts use 12 monthly points.

  pulseSalesCount: {
    id: "pulseSalesCount",
    label: "Sales count (90-day pulse)",
    plain: "How many watchlist homes closed in the last 90 days, with the change versus the 90 days before.",
    formulaWords:
      "Counts watchlist closed sales dated within the last 90 days and compares that with the count from the previous 90-day window.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (MLS-enriched rows only)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Rising sales are shaded as market heat here, but they can also just mean more homes were listed.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "pulseWatchlist",
  },

  pulseFastSaleShare: {
    id: "pulseFastSaleShare",
    label: "Fast-sale share (watchlist)",
    plain: "The share of recent watchlist sales that moved fast, the Pulse gauge of buyer competition.",
    formulaWords:
      "Of watchlist sales in the last 90 days, the percentage counted as fast: days on market of 10 days or less, or a hot-sale tag from the data pipeline.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (MLS-enriched rows only)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Sales with no days-on-market value count as not-fast unless the pipeline tagged them, so thin data understates the share.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "pulseWatchlist",
  },

  pulseMedianDom: {
    id: "pulseMedianDom",
    label: "Median days on market (watchlist)",
    plain: "How long a typical watchlist home sat before a buyer locked it up, over the last 90 days.",
    formulaWords:
      "Takes each recent watchlist sale's days on market, preferring the cumulative count that survives relistings, reports the middle value, and compares it with the prior 90 days.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (MLS-enriched rows only)" },
    cadence: CADENCE["mls-manual"],
    caveats: ["Only sales with a days-on-market value count; the rest are left out entirely."],
    buyerDirection: "higherIsBetter",
    universeId: "pulseWatchlist",
  },

  pulseMedianSaleToList: {
    id: "pulseMedianSaleToList",
    label: "Median sale/list (watchlist)",
    plain: "How the typical watchlist sale price compares with its asking price: above 1.00x means selling over ask.",
    formulaWords:
      "For each recent watchlist sale with a real list price, divide the closing price by that list price and take the middle ratio for the last 90 days.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: ["Only sales with a real MLS or Redfin list price count in this ratio."],
    buyerDirection: "higherIsWorse",
    universeId: "pulseWatchlist",
  },

  pulseMedianBidUp: {
    id: "pulseMedianBidUp",
    label: "Median bid-up (watchlist)",
    plain: "The typical dollar amount recent watchlist winners paid above or below the asking price.",
    formulaWords:
      "For each watchlist sale in the last 90 days, take the closing price minus the list price at pending, and report the middle value.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Sales without a real list price count as zero bid-up in this tile, which can pull the median toward zero.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "pulseWatchlist",
  },

  pulseMedianClose: {
    id: "pulseMedianClose",
    label: "Median close (watchlist)",
    plain: "The typical final sale price across your watchlist pockets in the last 90 days.",
    formulaWords:
      "Sorts recent watchlist closing prices and takes the middle one, then compares it with the previous 90-day window.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (MLS-enriched rows only)" },
    cadence: CADENCE["mls-manual"],
    caveats: ["A shift in which pockets sold can move this even when no individual home got pricier."],
    buyerDirection: "higherIsWorse",
    universeId: "pulseWatchlist",
  },

  pulseWeeklyGrain: {
    id: "pulseWeeklyGrain",
    label: "Weekly trend grain",
    plain: "The whole-slice trend charts can switch from months to true calendar weeks to catch turns sooner.",
    formulaWords:
      "Each point is one 7-day week keyed on sale date, stepping back 16 weeks from the newest sale. Weeks with fewer than 5 sales show volume only, and the newest week is partial.",
    source: { id: "derived", label: "Computed in your browser from the closed sales in your slice" },
    cadence: CADENCE.derived,
    caveats: [
      "Sale/list and days-on-market stay monthly: too few weekly sales carry those fields to trust a weekly median.",
    ],
    buyerDirection: "neutral",
    universeId: "closedSlice",
  },

  competitionPockets: {
    id: "competitionPockets",
    label: "Competition pockets",
    plain: "Neighborhoods in your slice ranked by how often homes sell fast, so you can see where bidding pressure concentrates.",
    formulaWords:
      "Groups your closed slice by neighborhood, computes each one's fast-sale share, sale count, median sale/list, and median close, and ranks by fast-sale share. Neighborhoods with fewer than 10 days-on-market readings are listed after the well-sampled ones.",
    source: { id: "derived", label: "Computed in your browser by grouping your slice by neighborhood" },
    cadence: CADENCE.derived,
    caveats: ["Even 10 readings is a small sample; check the sale count next to the share."],
    buyerDirection: "neutral",
    universeId: "closedSlice",
  },

  watchlistPockets: {
    id: "watchlistPockets",
    label: "Watchlist pockets",
    plain: "The four named areas the Pulse tab tracks: Ballard, Fremont / Green Lake / Woodland Park, Queen Anne, and Magnolia.",
    formulaWords:
      "Each sale's neighborhood label maps into one pocket, so Ballard / Crown Hill counts as Ballard. The Primary Watchlist pill combines the first three pockets; Magnolia stays separate.",
    source: { id: "derived", label: "Fixed pocket definitions built into the app" },
    cadence: "The pocket list is fixed in the app and changes only with an app update.",
    caveats: ["Sales outside these pockets never appear on the Pulse tab, whatever your filters say."],
    buyerDirection: "neutral",
    universeId: "pulseWatchlist",
  },

  // --- Leverage and supply (Overview) ----------------------------------------
  // Grounded in computeStageLeverage / LISTING_STAGES (listingStage.mjs) and
  // buildMarketingPeriods / computeSupplySnapshot (listingLedger.mjs).

  stageLeverage: {
    id: "stageLeverage",
    label: "Leverage by days on market",
    plain: "How sale prices compared with asking prices, split by how long each home took to go under contract. It shows when the advantage passes to you.",
    formulaWords:
      "Groups closed sales by days on market at pending: 0 to 7, 8 to 14, 15 to 45, and 46 or more. Each group shows the share that closed over, at and under ask, and the middle dollar gap between close and ask. The last column counts homes for sale now in that group.",
    source: { id: "mls-manual", label: "Manual MLS exports, Redfin listing history and the daily listings ledger" },
    cadence: CADENCE.redfin,
    caveats: [
      "Only sales with both a genuine asking price and a days-on-market reading count.",
      "Ask means the price in force at pending, so an older listing's discount comes on top of cuts already taken.",
      "A home relisted under a new MLS number keeps its original clock.",
      "A group with fewer than 5 sales shows n/a.",
    ],
    buyerDirection: "neutral",
    universeId: "closedSlice",
  },

  supplyForSale: {
    id: "supplyForSale",
    label: "Homes for sale now",
    plain: "How many homes matching your type, neighborhood and price filters were for sale on the last morning the listings were checked.",
    formulaWords:
      "Counts homes seen in the Redfin listings feed on its most recent pull whose current asking price is inside your price band. A home relisted under a new MLS number counts once. The line shows the same count each week for the last 16 weeks; new arrivals count first-time listings only, not relists.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed" },
    cadence: CADENCE.redfin,
    caveats: [
      "The ledger knows type, ZIP and asking price only, so lot-size, affordability, special-sale and date filters do not apply here.",
      "It can differ slightly from the Active + pending tile, which counts listing rows rather than homes.",
      "Tracking began June 8, 2026 and was patchy until mid-July.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  monthsOfSupply: {
    id: "monthsOfSupply",
    label: "Months of supply",
    plain: "How long today's listings would take to sell at the recent pace of closings if nothing new came on. More months means more room to negotiate.",
    formulaWords:
      "Divides the homes for sale now by the average monthly closings over the last 90 days, both filtered by type, neighborhood and price only. Under about 2 months reads as tight, 2 to 4 as balanced, and over 4 as roomy. Fewer than 6 closings in the window shows n/a.",
    source: { id: "derived", label: "Listings ledger divided by recent closed sales" },
    cadence: CADENCE.derived,
    caveats: [
      "Listings are banded by asking price and closings by sale price, so homes near the edges of your band can fall on different sides.",
      "Closings trail contracts by about 3 to 4 weeks, so the pace describes demand from a month or so ago.",
      "While King County records are behind, recent closings come from Redfin alone, which understates the pace and overstates the months.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  supplyDaysListed: {
    id: "supplyDaysListed",
    label: "Typical time on market (for sale now)",
    plain: "How long the middle home currently for sale has been on the market.",
    formulaWords:
      "For each home for sale now, counts days since it was listed and takes the middle value. A relisted home adds the days its earlier listings were up; days off the market in between do not count.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed" },
    cadence: CADENCE.redfin,
    caveats: [
      "This describes homes that have not sold, so it always runs longer than days on market for sales: quick sellers leave the pool and slow ones pile up.",
      "Since September 21, 2026 Redfin no longer reports days on market for most listings, so newer list dates are estimated as the day before the listing first appeared. On listings with both readings that was within 1 day 94% of the time.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  priceCutShare: {
    id: "priceCutShare",
    label: "Already cut the price",
    plain: "The share of homes for sale now that ask less than they did when first listed.",
    formulaWords:
      "Compares each home's current asking price with its opening ask. For a relisted home the opening ask is the first listing's. The typical cut is the middle reduction among homes that cut, in dollars and as a percent of the opening ask.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed" },
    cadence: CADENCE.redfin,
    caveats: [
      "Homes already on the market when tracking began (June 8, 2026) may have cut earlier than the ledger can see, so this is a floor.",
      "About 4 in 5 relists come back at a lower price, a cut that listing sites do not show as one.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  relistShare: {
    id: "relistShare",
    label: "Relisted to reset the clock",
    plain: "The share of homes for sale now that were cancelled and listed again under a new MLS number, which restarts the days-on-market count buyers see.",
    formulaWords:
      "Links listings at the same address and ZIP when the next one appears within 90 days of the last and does not raise the ask by more than 10 percent. A home with two or more linked listings counts as relisted.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed" },
    cadence: CADENCE.redfin,
    caveats: [
      "Matching is by address text, so a relist with a differently written address is missed.",
      "A listing that fell out of contract and came back under the same MLS number is not a relist.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  withdrawnShare: {
    id: "withdrawnShare",
    label: "Stale listings that never sold",
    plain: "Of homes that left the market after sitting 46 days or more, the share that were pulled without a sale. A high share means many slow sellers would rather wait than meet the market.",
    formulaWords:
      "Takes homes last seen for sale between 135 and 45 days ago, so a closing has had time to record, and checks whether any of their MLS numbers appears among closed sales. Those with no closing count as withdrawn.",
    source: { id: "derived", label: "Listings ledger matched to closed sales by MLS number" },
    cadence: CADENCE.derived,
    caveats: [
      "A sale the Redfin sold feed missed would be counted as withdrawn, so treat this as an upper bound.",
      "Shown only when at least 8 such homes are in your slice.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  listingOutlook: {
    id: "listingOutlook",
    label: "What tends to happen next",
    plain: "Of listings in the same situation, how many cut their price and how many went under contract within the next 14 days: is waiting likely to cost you the home or save you money?",
    formulaWords:
      "Every day each listing spent on the market is filed by its cumulative days on market and by how long ago it last cut (never, within 7 days, 8 to 21, or 22 plus), then marked with what the next 14 days brought. Uses the same home type, asking within 25% of your price band, any neighborhood. Thin groups lean on the average for their days on market.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed, matched to recorded closings" },
    cadence: CADENCE.derived,
    caveats: [
      "Odds across many listings, not one home's forecast.",
      "Replayed on ten past dates, predicted sales matched (11% against 10%). Cuts ranked correctly but came more often than predicted (28% against 22%): sellers cut more in autumn than in summer.",
      "Sale odds lag two months, until closings record.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  priceIndex: {
    id: "priceIndex",
    label: "Like-for-like price index",
    plain: "What the same home would have sold for in each month, with the first month set to 100. A median cannot show this: your price band pins it, and the mix of homes sold shifts with the season.",
    formulaWords:
      "One regression over listed sales of the home type from $400K to $4M: log price against size, lot, age, newer build, beds, baths, neighborhood and whose square footage it is, plus a term per month. The month terms are the index. The current month is left out.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (county-only rows are left out)" },
    cadence: CADENCE.derived,
    caveats: [
      "A change is called only when it clears its margin of error: roughly 3% within a few months, 4% across a year.",
      "County and Redfin rows measure size differently; each counts fully only in months where it supplies 40% of sales, and not at all under 25%. Comparisons across their switch (spring 2026) are least certain.",
      "It cannot see condition or remodels.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "allRows",
  },

  listingSignals: {
    id: "listingSignals",
    label: "What happened lately",
    plain: "Listings where something just changed, or conspicuously has not. Click a count to narrow the queue to it.",
    formulaWords:
      "Read from each listing's own daily history. Just cut: the ask came down in the last 7 days. Just relisted: it came back under a new MLS number in the last 14 days. No cut yet: 21 or more days listed, still at its opening price.",
    source: { id: "redfin", label: "Daily ledger of the Redfin listings feed" },
    cadence: CADENCE.derived,
    caveats: [
      "Past day 14, 15% of just-cut homes went under contract within two weeks, against 10 to 11% of the rest.",
      "Homes that had not cut by then were the likeliest to: 31% did within two weeks.",
      "A home already on the market when daily tracking began is never counted as No cut yet, because earlier cuts went unseen.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  bidStage: {
    id: "bidStage",
    label: "Listing stage",
    plain: "Where a listing is in its life: first week, just past its offer-review date, sitting, or stale. Time on the market is the strongest single clue to what a home will sell for relative to its ask.",
    formulaWords:
      "Reads the listing's cumulative days on market: 0 to 7 is First week, 8 to 14 Missed review, 15 to 45 Sitting, 46 or more Stale. A first-week listing is priced only against first-week sales. After that the labels are just labels: the bid weights each later sale by how close its days to pending were to this listing's days listed.",
    source: { id: "derived", label: "Computed in your browser from days on market" },
    cadence: CADENCE.derived,
    caveats: [
      "A listing with no days-on-market reading has no stage; its bid falls back to comps from every stage and loses 25 confidence points.",
      "A first-week suggestion is the price of competing at offer review. If the home is still there in week two, the math changes in your favor.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  // --- Bids / Offer Lab tab --------------------------------------------------
  // Grounded in scoreBidForRow, bidConfidenceScore and computeActiveBidSuggestions
  // (selectors.mjs) and selectBidComps (bidComps.mjs): 12-month comp window, 60-day
  // half-life, similarity weights, strategy percentiles, ratio clamp 0.85x to 1.25x.

  suggestedBid: {
    id: "suggestedBid",
    label: "Suggested bid",
    plain: "A starting offer for an active listing, based on what the most similar homes closed at relative to their asking prices.",
    formulaWords:
      "Takes the sale-to-ask ratios of same-type sales from the last 12 months, weights each by how closely it resembles this listing (how recent, how long it took to go pending, how close its ask, how near, same build era), picks the strategy percentile (Conservative 50th, Balanced 60th, Aggressive 70th), and multiplies the ask by that ratio, rounded to $1,000.",
    source: { id: "derived", label: "Computed in your browser from closed comps in the last 12 months" },
    cadence: CADENCE.derived,
    caveats: [
      "A starting point, not an appraisal: it cannot see condition, layout or how many offers are in.",
      "Replayed on about 1,100 past sales using only what had closed beforehand, the three strategies met or beat the winning price 51%, 62% and 74% of the time.",
      "First-week outcomes are wide: a quarter sell at ask or less, a quarter about 10% over.",
      "Ratios are clamped between 0.85x and 1.25x of ask.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  bidRange: {
    id: "bidRange",
    label: "Bid range",
    plain: "Bids one step more cautious and one step more aggressive than the suggestion. It is a range of offers to consider, not a forecast of the sale price.",
    formulaWords:
      "Uses the same weighted comp ratios as the suggested bid but takes the percentile 10 points lower and 10 points higher, for example the 50th to 70th for Balanced: bids that would have won about 5 in 10 and 7 in 10 comparable sales.",
    source: { id: "derived", label: "Computed in your browser from closed comps in the last 12 months" },
    cadence: CADENCE.derived,
    caveats: ["The final price lands inside this narrow band only about a quarter of the time. For where the sale is likely to land, read Likely sale."],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  bidOutcomeRange: {
    id: "bidOutcomeRange",
    label: "Likely sale",
    plain: "Where the sale price is likely to land: 8 in 10 of the most similar homes sold inside this range, applied to this listing's ask.",
    formulaWords:
      "Takes the 10th and 90th percentiles of the same weighted sale-to-ask ratios behind the suggested bid and multiplies each by the asking price.",
    source: { id: "derived", label: "Computed in your browser from closed comps in the last 12 months" },
    cadence: CADENCE.derived,
    caveats: [
      "Checked against about 1,100 past sales: 83% closed inside the range the model would have shown before they went pending.",
      "Measured against today's ask. A listing that cuts its price first will land lower.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  bidLadder: {
    id: "bidLadder",
    label: "Chance of winning at each price",
    plain: "What each step up in price buys you: the offer that would have met or beaten the winning price on 5, 6, 7, 8 and 9 out of 10 comparable sales.",
    formulaWords:
      "Reads the 50th, 60th, 70th, 80th and 90th percentiles of the weighted sale-to-ask ratios behind the suggested bid and multiplies each by the asking price.",
    source: { id: "derived", label: "Computed in your browser from closed comps in the last 12 months" },
    cadence: CADENCE.derived,
    caveats: [
      "Replayed on about 1,100 past sales, the five steps won 51%, 62%, 74%, 83% and 92% of the time.",
      "Price only. Contingencies, financing and timing also decide who wins.",
      "On a listing past its first week there may be no other bidder, so the seller's patience matters more than these odds.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  bidConfidence: {
    id: "bidConfidence",
    label: "Bid confidence",
    plain: "A 0 to 100 score for how close the suggested bid is likely to be to the real sale price: high when similar homes sold at similar ratios, low when they were all over the map.",
    formulaWords:
      "Starts at 100. Takes off 5 points for every percentage point between the comps' 20th and 80th percentile sale-to-ask ratios, 1.5 for each comp short of the equivalent of 20, up to 25 if the newest comp that matters closed more than 45 days ago, and 25 if the comps could not be matched to the listing's days on market. 75 or more shows as High, 55 to 74 as Medium.",
    source: { id: "derived", label: "Computed in your browser from the comps behind each bid" },
    cadence: CADENCE.derived,
    caveats: [
      "Replayed on about 1,100 past sales, High bids missed the sale price by 2.4% on average, Medium by 3.5%, Low by 5.9%.",
      "Nearly every first-week listing scores Low: nobody can tell in advance which ones draw a bidding war.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "actives",
  },

  bidCompBasis: {
    id: "bidCompBasis",
    label: "Comp basis",
    plain: "Which past sales stand behind a suggested bid, and how much each one counts.",
    formulaWords:
      "Every same-type sale from the last 12 months with a real list price counts, weighted by resemblance. Weight halves every 60 days, falls as the comp's days to pending and ask move away from this listing's, counts up to 20 times over in the same neighborhood or ZIP, and is cut to a twentieth when only one of the two homes is under 15 years old. First-week listings are priced off first-week sales and later ones off later sales, unless too few exist. The count is the effective number (many sales that barely count add up to few); a suggestion needs 6 before the local boost.",
    source: { id: "derived", label: "Computed in your browser from closed sales in the last 12 months" },
    cadence: CADENCE.derived,
    caveats: [
      "Mostly nearby: at least half the weight is local. A lone nearby sale counts 3 times.",
      "Likely presold new builds are excluded; your price band does not limit comps.",
    ],
    buyerDirection: "neutral",
    universeId: "actives",
  },

  suggestedSaleList: {
    id: "suggestedSaleList",
    label: "Bid vs ask price",
    plain: "The suggested bid expressed as a multiple of the asking price: 1.05x means bidding 5 percent over ask.",
    formulaWords: "Divides the suggested bid by the listing's asking price.",
    source: { id: "derived", label: "Computed in your browser from the suggested bid" },
    cadence: CADENCE.derived,
    caveats: ["Clamped between 0.85x and 1.25x, so extreme comp readings are trimmed."],
    buyerDirection: "higherIsWorse",
    universeId: "actives",
  },

  bidMedianOverAsk: {
    id: "bidMedianOverAsk",
    label: "Median over ask (bid queue)",
    plain: "Across all scored active listings, the typical gap between the suggested bid and the asking price.",
    formulaWords:
      "For each scored listing, computes the suggested bid minus ask as a percent of ask, then takes the middle value.",
    source: { id: "derived", label: "Computed in your browser from the scored bid queue" },
    cadence: CADENCE.derived,
    caveats: ["Summarizes the model's suggestions, not what winners actually paid; the cost-to-win stats show real outcomes."],
    buyerDirection: "higherIsWorse",
    universeId: "actives",
  },

  // --- Afford tab ------------------------------------------------------------
  // Grounded in computeAffordability, maxHomeForCap, ownerCost, sellableEquity,
  // and listingTier (affordability.mjs). Every input comes from the private,
  // gitignored config file plus the on-page scenario controls.

  affordDecision: {
    id: "affordDecision",
    label: "Affordability decision",
    plain: "A one-line verdict for your current scenario, from rent-and-wait to buy-now.",
    formulaWords:
      "Checked in order: rent and wait if the monthly cost tops the stretch cap, cash flow goes negative, or the down payment exceeds what you can raise; wait for liquidity if it needs equity you cannot sell yet; buy now within cap when cash-funded, comfortable, and the reserve holds; otherwise buy within comfort or only for an exceptional home.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Only as good as the config file's assumptions; revisit them when rates or income change."],
    buyerDirection: "neutral",
    universeId: "affordScenario",
  },

  affordMaxComfortable: {
    id: "affordMaxComfortable",
    label: "Max comfortable price",
    plain: "The most expensive home whose full monthly cost stays inside your comfort cap.",
    formulaWords:
      "Finds the highest price where mortgage payment, property tax, insurance, HOA dues, and maintenance stay at or under your comfort cap, given the down-payment funds you could deploy. A minimum down-payment percent plus closing costs also limit it.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Moves with the scenario controls: a longer wait can unlock more sellable equity and raise it."],
    buyerDirection: "higherIsBetter",
    universeId: "affordScenario",
  },

  affordMaxStretch: {
    id: "affordMaxStretch",
    label: "Max stretch price",
    plain: "The price ceiling if you accept the higher stretch payment instead of the comfortable one.",
    formulaWords: "Same math as the max comfortable price but tested against your higher stretch cap.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Living at the stretch cap leaves less slack for rate changes, repairs, or income dips."],
    buyerDirection: "higherIsBetter",
    universeId: "affordScenario",
  },

  affordCarry: {
    id: "affordCarry",
    label: "All-in monthly carry",
    plain: "What owning the target home would cost every month, all-in.",
    formulaWords:
      "Adds the mortgage payment at your configured rate minus any lender discount, monthly property tax from the configured tax rate, insurance, HOA dues, and a maintenance allowance set as a percent of the price per year.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Rate, taxes, insurance, HOA, and maintenance all come from your config file, not from the listing."],
    buyerDirection: "higherIsWorse",
    universeId: "affordScenario",
  },

  affordFreeCashFlow: {
    id: "affordFreeCashFlow",
    label: "Free cash flow per month",
    plain: "What is left of your monthly take-home pay after normal spending and the home's carry.",
    formulaWords:
      "After-tax monthly income minus your baseline non-housing spending, inflated to the purchase month, minus the all-in monthly carry.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Negative free cash flow triggers a rent-and-wait decision regardless of the other numbers."],
    buyerDirection: "higherIsBetter",
    universeId: "affordScenario",
  },

  affordPostCloseLiquidity: {
    id: "affordPostCloseLiquidity",
    label: "Post-close liquidity",
    plain: "The cash cushion left right after buying, to compare against your reserve target.",
    formulaWords:
      "Rolls your cash forward month by month at the configured yield, adding each month's leftover income, then at the purchase month adds equity sale proceeds and subtracts the down payment plus closing costs.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Falling below the reserve target raises a flag and blocks the buy-now verdict."],
    buyerDirection: "higherIsBetter",
    universeId: "affordScenario",
  },

  affordDeployable: {
    id: "affordDeployable",
    label: "Deployable for down payment",
    plain: "The most you could put toward a down payment from cash-like funds plus equity you can actually sell.",
    formulaWords:
      "Adds bank cash, the housing gift, brokerage cash, and a set share of brokerage investments and vested stock, minus your reserve target, plus equity that is vested, past any lockup, and after tax at the scenario's wait month.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Equity counts only after vesting and lockup; before that the tab says wait for liquidity."],
    buyerDirection: "higherIsBetter",
    universeId: "affordScenario",
  },

  affordTier: {
    id: "affordTier",
    label: "In budget / Stretch / Over badge",
    plain: "The badge on listings comparing their price with your affordability ceilings.",
    formulaWords:
      "In budget when the price is at or under your max comfortable price, Stretch when it still fits under the max stretch price, Over beyond that. Active listings are judged on asking price; sold homes on their closing price.",
    source: { id: "derived", label: "Computed in your browser from your private affordability config" },
    cadence: CADENCE.derived,
    caveats: ["Badges appear only when the private config file is loaded on this device."],
    buyerDirection: "neutral",
    universeId: "affordScenario",
  },

  // --- Geo tab ---------------------------------------------------------------
  // Grounded in ratioColor, geoLegendHtml, geoMappableRows, and the ZIP_COORDS
  // fallback in normalizeRow (main.mjs, data.mjs).

  geoPressure: {
    id: "geoPressure",
    label: "Map pressure colors",
    plain: "Each dot is one home; its color shows how the sale price compared with the asking price.",
    formulaWords:
      "Colored by sale-to-list ratio: green under ask (0.90x to 0.99x), blue at ask (1.00x to 1.02x), amber over ask (1.03x to 1.09x), red hot (1.10x and up). Grey means sold with no real list price; a violet ring is an active listing.",
    source: { id: "mls-manual", label: "Sale-to-list ratios from manual MLS exports and Redfin; county-only sales show grey" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Homes without exact coordinates are placed at the center of their zip code, so dots can stack.",
      "Only about 1,200 sold dots draw at once for speed; active listings always draw.",
    ],
    buyerDirection: "higherIsWorse",
    universeId: "recordRows",
  },

  // --- Records tab -----------------------------------------------------------
  // Grounded in recordRowHtml and its cell helpers (main.mjs), normalizeRow
  // lineage flags (data.mjs), and computeProjectedPendingRows (selectors.mjs).

  recordSource: {
    id: "recordSource",
    label: "Row lineage (county vs MLS vs Redfin)",
    plain: "Where each row came from, which decides which columns can be filled in.",
    formulaWords:
      "County-only rows come from assessor recorded sales and carry price, size, and parcel facts but no list price or days on market. MLS-enriched rows add listing detail from manual MLS exports or Redfin; the pipeline folds both into the same MLS-enriched kind.",
    source: { id: "kc-assessor", label: "King County assessor base, enriched with manual MLS exports and Redfin" },
    cadence: CADENCE["kc-assessor"],
    caveats: ["A blank MLS field means that export did not include it, not that the value is zero."],
    buyerDirection: "neutral",
    universeId: "recordRows",
  },

  recordAsk: {
    id: "recordAsk",
    label: "Ask (list price) column",
    plain: "The asking price the ratio and bid-up columns compare against.",
    formulaWords:
      "Uses the MLS or Redfin listing price when one exists. On county-only rows it falls back to the tax-assessed value or the sale price itself, just to fill the column.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "Treat it as a real asking price only on MLS or Redfin rows; the ratio and bid-up math already ignores the county fallback.",
      "Rows marked \"last day listed\" took their asking price from the last morning the listing was still seen for sale. That is the price it went under contract at, after any cuts, so the sale-versus-ask math is sound.",
    ],
    buyerDirection: "neutral",
    universeId: "recordRows",
  },

  recordDom: {
    id: "recordDom",
    label: "DOM column",
    plain: "Days on market: how long this home took to go under contract.",
    formulaWords:
      "Shows the cumulative count that survives relistings when available, otherwise the plain count, otherwise the days from listing to pending. A second small number shows the other count when they differ.",
    source: { id: "mls-manual", label: "Manual MLS exports (county rows carry no days on market)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "County-only rows show n/a: the assessor records no marketing timeline.",
      "On rows marked \"last day listed\" the count is as of the last morning the listing was seen for sale, so the true figure can be up to a day longer. That only matters right at the 10-day fast-sale line.",
    ],
    buyerDirection: "higherIsBetter",
    universeId: "recordRows",
  },

  recordSaleToList: {
    id: "recordSaleToList",
    label: "S/List column",
    plain: "This home's sale price as a multiple of its asking price: 1.05x means it closed 5 percent over ask.",
    formulaWords:
      "Divides the closing price by the list price at pending. A small orig figure appears when the ratio against the original list price differs, which usually means the price was cut or raised along the way.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: ["Shows n/a without a real list price, which covers all county-only rows."],
    buyerDirection: "higherIsWorse",
    universeId: "recordRows",
  },

  recordBidUp: {
    id: "recordBidUp",
    label: "Bid-up column",
    plain: "The dollars this buyer paid above or below the asking price.",
    formulaWords: "Closing price minus the list price at pending, with the percent shown underneath.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin listing history" },
    cadence: CADENCE["mls-manual"],
    caveats: ["Shows n/a without a real list price; a county-only sale never gets a bid-up."],
    buyerDirection: "higherIsWorse",
    universeId: "recordRows",
  },

  recordHot: {
    id: "recordHot",
    label: "Hot column",
    plain: "A speed tag for each sale: Ultra Hot, Hot, or Normal.",
    formulaWords:
      "Ultra Hot means the home went under contract in 5 days or less, Hot in 10 days or less, judged from days on market or the pipeline's hot tag. Everything else shows Normal.",
    source: { id: "derived", label: "Computed from MLS days on market and the pipeline hot tag" },
    cadence: CADENCE.derived,
    caveats: ["County-only rows have no timeline, so they always show Normal even if they actually sold fast."],
    buyerDirection: "neutral",
    universeId: "recordRows",
  },

  projectedClose: {
    id: "projectedClose",
    label: "Projected close (est.)",
    plain: "A modeled guess at what a pending listing will close for, marked with a Projected pill.",
    formulaWords:
      "Multiplies the pending listing's asking price by the median sale-to-list ratio of similar recent closed sales. It starts with the same neighborhood and home type, then widens to home type only, then to the whole city, trying to reach at least 6 comps but using whatever pool it lands on. The 25th and 75th percentile ratios give a low and a high.",
    source: { id: "derived", label: "Computed in your browser from closed comps in your slice" },
    cadence: CADENCE.derived,
    caveats: [
      "An estimate, not a recorded sale; it disappears when the Pending Projection flag is off.",
      "In a thin slice the comp pool can end up below 6 sales, which makes the estimate less reliable.",
    ],
    buyerDirection: "neutral",
    universeId: "recordRows",
  },

  // --- Data tab --------------------------------------------------------------
  // Grounded in renderDataView (main.mjs) and the validation report written by
  // scripts/validate_data_refresh.js (status, counts, realtor file manifest).

  dataValidation: {
    id: "dataValidation",
    label: "Validation status",
    plain: "Whether the last data refresh passed its automated checks.",
    formulaWords:
      "The pipeline verifies both dataset files exist, required columns are present, MLS-enriched rows exist, and every active listing carries an asking price. Any failed check flips the status to fail.",
    source: { id: "derived", label: "The pipeline's validation report, written next to the data" },
    cadence: "Written each time the data pipeline runs; the Validation time tile shows when that was.",
    caveats: [
      "Fail means the refresh needs attention, but the dashboard keeps showing the last good data.",
      "Warnings, like missing realtor export files, do not flip the status to fail.",
    ],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  dataRowCounts: {
    id: "dataRowCounts",
    label: "Row counts by stage",
    plain: "How many rows each stage produced, so you can spot a refresh that quietly shrank the data.",
    formulaWords:
      "Rows counts what this browser loaded. Output rows is the validation report's count of the file the pipeline built. Realtor files counts the manual MLS export files the pipeline found.",
    source: { id: "derived", label: "This browser's loaded dataset plus the pipeline's validation report" },
    cadence: "Rows updates on page load; the report counts update each time the data pipeline runs.",
    caveats: ["If Rows and Output rows differ, the app may be showing an older file than the last pipeline build."],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  dataSourceCadence: {
    id: "dataSourceCadence",
    label: "Source update rhythms",
    plain: "How often each of the three data sources refreshes.",
    formulaWords:
      "County recorded sales publish about every 2 weeks and take about 2 more weeks to record a closing. Redfin listings are pulled fresh on every pipeline run. Manual MLS exports update only when a new export is saved.",
    source: { id: "kc-assessor", label: "King County assessor, Redfin, and manual MLS exports" },
    cadence: "Each source moves on its own clock; the Overview freshness card tracks the slowest one.",
    caveats: ["The blend is only as fresh as its slowest source, the county feed."],
    buyerDirection: "neutral",
    universeId: "allRows",
  },

  // Universe definitions: the row sets that headline stats are computed over.
  // savedMatches above doubles as both a metric (the count) and a universe
  // (the matching rows themselves).

  allRows: {
    id: "allRows",
    label: "All loaded rows",
    plain: "Every row in the data file before any filters: about 12 months of King County sales, MLS and Redfin sales back to January 2025, and current listings.",
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

  pulseWatchlist: {
    id: "pulseWatchlist",
    label: "Watchlist closed sales",
    plain: "MLS-enriched closed sales inside your watchlist pockets. Everything on the Pulse tab is computed from these.",
    formulaWords:
      "Start from closed sales with MLS or Redfin detail that pass your filters, keep only homes in a watchlist pocket, then narrow to the pocket selected in the pills.",
    source: { id: "mls-manual", label: "Manual MLS exports and Redfin sales (county-only rows never qualify)" },
    cadence: CADENCE["mls-manual"],
    caveats: [
      "County-only sales are excluded, so Pulse sees fewer homes than the closed-comps count on other tabs.",
    ],
    buyerDirection: "neutral",
    universeId: "pulseWatchlist",
  },

  recordRows: {
    id: "recordRows",
    label: "Rows in the Records table and Geo map",
    plain: "The rows both the Records table and the map show: closed comps in your slice plus projected and open or pending listings.",
    formulaWords:
      "Starts from your closed slice, adds projected-pending rows and open MLS listings when those feature flags are on, then applies the record-view selector (all, projected only, open only, or both).",
    source: { id: "derived", label: "Computed in your browser by combining your slice with open and projected rows" },
    cadence: CADENCE.derived,
    caveats: [
      "Turning off the Pending Projection or Include Open/Pending MLS flags removes those rows here and on the map.",
    ],
    buyerDirection: "neutral",
    universeId: "recordRows",
  },

  affordScenario: {
    id: "affordScenario",
    label: "Your affordability scenario",
    plain: "The private money assumptions the Afford tab runs on. No market rows are involved.",
    formulaWords:
      "Reads your local affordability config file (income, assets, equity, housing costs, caps) plus the scenario controls: wait months, valuation, target price, and down payment.",
    source: { id: "derived", label: "Your private affordability config file, read in your browser" },
    cadence:
      "Recomputed instantly whenever you change a scenario control. The config file lives only on this device and is never uploaded.",
    caveats: ["A planning model, not tax or lender advice."],
    buyerDirection: "neutral",
    universeId: "affordScenario",
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
