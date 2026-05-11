import {
  addDays,
  clamp,
  median,
  nullableMedian,
  safeNumber,
  toDate,
} from "./format.mjs";

export const STORAGE_KEY = "buyer_lens_profile_memory_v1";
export const MICRO_GROUPS = [
  "Ballard",
  "Fremont / Green Lake / Woodland Park",
  "Queen Anne",
  "Magnolia",
];

export const DEFAULT_PROFILE_MEMORY = {
  version: 1,
  id: "north-seattle-character-homes",
  name: "North Seattle Character Homes",
  sourceLabel: "My Favorite Homes _ Zillow.pdf",
  savedAt: "2026-04-19",
  homesCount: 12,
  sampleAddresses: [
    "9500 Phinney Avenue N, Seattle, WA 98103",
    "3244 NW 64th Street, Seattle, WA 98107",
    "144 NW 79th Street, Seattle, WA 98117",
    "4701 Woodland Park Ave N, Seattle, WA 98103",
    "2419 2nd Avenue W, Seattle, WA 98119",
  ],
  watchlistGroups: MICRO_GROUPS.slice(),
  flexNeighborhoodLabels: ["Capitol Hill / Eastlake"],
  typeLabel: "Single Family",
  ranges: {
    beds: [3, 5],
    baths: [1.5, 3.5],
    sqft: [1450, 3000],
    yearBuilt: [1900, 1945],
    lotSize: [3000, 9000],
  },
  traits: [
    "Detached single-family",
    "1900-1945 vintage",
    "3-4 beds",
    "1,450-3,000 sqft",
    "Real yard / lot",
    "Light, views, or outdoor upside",
    "Parking or flexible lower level",
  ],
  summary: "Classic north Seattle houses with character, real lots, and enough space for everyday living, guests, or work-from-home flexibility.",
};

function normalizeStringList(values) {
  return Array.isArray(values)
    ? values.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
}

function rangeScore(value, range, weight, toleranceRatio = 0.16) {
  const n = safeNumber(value);
  if (!n || !Array.isArray(range) || range.length < 2) return 0;
  const low = Number(range[0]);
  const high = Number(range[1]);
  if (!Number.isFinite(low) || !Number.isFinite(high)) return 0;
  if (n >= low && n <= high) return weight;
  const span = Math.max(high - low, 1);
  const tolerance = span * toleranceRatio;
  const distance = n < low ? low - n : n - high;
  if (distance > tolerance) return 0;
  return weight * (1 - (distance / tolerance)) * 0.45;
}

export function normalizeProfileMemory(rawProfile) {
  const source = rawProfile && typeof rawProfile === "object" ? rawProfile : DEFAULT_PROFILE_MEMORY;
  const fallbackRanges = DEFAULT_PROFILE_MEMORY.ranges;
  const rawRanges = source.ranges && typeof source.ranges === "object" ? source.ranges : {};
  return {
    version: 1,
    id: String(source.id || DEFAULT_PROFILE_MEMORY.id),
    name: String(source.name || DEFAULT_PROFILE_MEMORY.name),
    sourceLabel: String(source.sourceLabel || DEFAULT_PROFILE_MEMORY.sourceLabel),
    savedAt: String(source.savedAt || DEFAULT_PROFILE_MEMORY.savedAt),
    homesCount: Math.max(0, Math.round(Number(source.homesCount || DEFAULT_PROFILE_MEMORY.homesCount))),
    sampleAddresses: normalizeStringList(source.sampleAddresses).slice(0, 20),
    watchlistGroups: normalizeStringList(source.watchlistGroups).length
      ? normalizeStringList(source.watchlistGroups)
      : DEFAULT_PROFILE_MEMORY.watchlistGroups.slice(),
    flexNeighborhoodLabels: normalizeStringList(source.flexNeighborhoodLabels),
    typeLabel: String(source.typeLabel || DEFAULT_PROFILE_MEMORY.typeLabel),
    ranges: {
      beds: Array.isArray(rawRanges.beds) ? rawRanges.beds.slice(0, 2).map(Number) : fallbackRanges.beds.slice(),
      baths: Array.isArray(rawRanges.baths) ? rawRanges.baths.slice(0, 2).map(Number) : fallbackRanges.baths.slice(),
      sqft: Array.isArray(rawRanges.sqft) ? rawRanges.sqft.slice(0, 2).map(Number) : fallbackRanges.sqft.slice(),
      yearBuilt: Array.isArray(rawRanges.yearBuilt) ? rawRanges.yearBuilt.slice(0, 2).map(Number) : fallbackRanges.yearBuilt.slice(),
      lotSize: Array.isArray(rawRanges.lotSize) ? rawRanges.lotSize.slice(0, 2).map(Number) : fallbackRanges.lotSize.slice(),
    },
    traits: normalizeStringList(source.traits).length ? normalizeStringList(source.traits) : DEFAULT_PROFILE_MEMORY.traits.slice(),
    summary: String(source.summary || DEFAULT_PROFILE_MEMORY.summary),
  };
}

function domValueForRow(row) {
  if (Number.isFinite(Number(row?.domValue))) return Number(row.domValue);
  if (Number.isFinite(Number(row?.mlsCDOM)) && Number(row.mlsCDOM) >= 0) return Number(row.mlsCDOM);
  if (Number.isFinite(Number(row?.mlsDOM)) && Number(row.mlsDOM) >= 0) return Number(row.mlsDOM);
  if (Number.isFinite(Number(row?.daysToPending)) && Number(row.daysToPending) >= 0) return Number(row.daysToPending);
  return null;
}

export function profileScore(row, rawProfile) {
  const profile = normalizeProfileMemory(rawProfile);
  const pulseGroup = String(row?.pulseWatchlistGroup || "").trim();
  const neighborhoodLabel = String(row?.neighborhoodLabel || row?.neighborhood || "").trim();
  const typeLabel = String(row?.typeLabel || row?.type || "").trim();
  let score = 0;

  if (pulseGroup && profile.watchlistGroups.includes(pulseGroup)) {
    score += 28;
  } else if (neighborhoodLabel && profile.flexNeighborhoodLabels.includes(neighborhoodLabel)) {
    score += 16;
  }

  if (typeLabel && typeLabel === profile.typeLabel) score += 18;
  score += rangeScore(row?.beds, profile.ranges.beds, 10);
  score += rangeScore(row?.baths, profile.ranges.baths, 8);
  score += rangeScore(row?.sqft, profile.ranges.sqft, 16);
  score += rangeScore(row?.yearBuilt, profile.ranges.yearBuilt, 12);
  score += rangeScore(row?.lotSize ?? row?.sqFtLot, profile.ranges.lotSize, 6);

  if (String(row?.mlsParkingType || "").trim()) score += 2;
  if (String(row?.mlsView || "").trim()) score += 2;
  const close = safeNumber(row?.closePrice);
  if (close && close >= 900000 && close <= 1900000) score += 2;

  return clamp(Math.round(score), 0, 100);
}

export function rowMatchesProfile(row, rawProfile) {
  const profile = normalizeProfileMemory(rawProfile);
  const pulseGroup = String(row?.pulseWatchlistGroup || "").trim();
  const neighborhoodLabel = String(row?.neighborhoodLabel || row?.neighborhood || "").trim();
  const typeLabel = String(row?.typeLabel || row?.type || "").trim();
  const hasLocationFit = profile.watchlistGroups.includes(pulseGroup)
    || profile.flexNeighborhoodLabels.includes(neighborhoodLabel);
  return hasLocationFit && typeLabel === profile.typeLabel && profileScore(row, profile) >= 60;
}

export function summarizeCompetition(rows) {
  const data = Array.isArray(rows) ? rows : [];
  const domValues = data.map(domValueForRow).filter((value) => value !== null);
  const ratioValues = data.map((row) => safeNumber(row?.saleToList)).filter((value) => value !== null && value > 0);
  const bidValues = data.map((row) => safeNumber(row?.delta)).filter((value) => value !== null);
  const closeValues = data.map((row) => safeNumber(row?.closePrice)).filter((value) => value !== null && value > 0);
  return {
    salesCount: data.length,
    hotShare: data.length ? data.filter((row) => !!row?.isHotMarket).length / data.length : null,
    medianDom: nullableMedian(domValues),
    medianSaleToList: nullableMedian(ratioValues),
    medianBidUp: nullableMedian(bidValues),
    medianClosePrice: nullableMedian(closeValues),
  };
}

export function buildProfileCohort(rows, rawProfile) {
  const profile = normalizeProfileMemory(rawProfile);
  const matches = (Array.isArray(rows) ? rows : []).filter((row) => rowMatchesProfile(row, profile));
  const competitionRows = matches.filter((row) => String(row?.dataMode || "") === "MLS_ENRICHED");
  const summary = summarizeCompetition(competitionRows);
  const byGroup = {};
  matches.forEach((row) => {
    const group = String(row.pulseWatchlistGroup || row.neighborhoodLabel || "Seattle").trim();
    byGroup[group] = (byGroup[group] || 0) + 1;
  });
  const topMicromarket = Object.entries(byGroup)
    .sort((a, b) => b[1] - a[1])[0]?.[0] || "";
  return {
    rows: matches,
    summary: {
      count: matches.length,
      medianClosePrice: median(matches.map((row) => safeNumber(row?.closePrice)).filter((value) => value !== null && value > 0)),
      medianSaleToList: summary.medianSaleToList,
      medianDom: summary.medianDom,
      medianBidUp: summary.medianBidUp,
      hotShare: summary.hotShare,
      topMicromarket,
    },
  };
}

function recentRows(rows, nowValue, days = 90) {
  const end = toDate(nowValue) || new Date();
  const start = addDays(end, -Math.max(1, Number(days || 90)));
  const endTime = end.getTime();
  const startTime = start ? start.getTime() : endTime;
  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const saleDate = toDate(row?.saleDate);
    if (!saleDate) return false;
    const time = saleDate.getTime();
    return time >= startTime && time <= endTime;
  });
}

export function fitLabel(profileShare) {
  const share = Number(profileShare || 0);
  if (share >= 0.5) return "Very Strong Fit";
  if (share >= 0.32) return "Strong Fit";
  if (share >= 0.18) return "Partial Fit";
  return "Peripheral Fit";
}

function priceBandLabel(value) {
  const amount = safeNumber(value);
  if (!amount || amount <= 0) return "Price signal unavailable";
  if (amount >= 1700000) return "Upper-end watchlist pricing";
  if (amount >= 1400000) return "Premium family-home pricing";
  if (amount >= 1150000) return "Core watchlist pricing";
  return "Below core watchlist pricing";
}

function formatShortMoney(value) {
  const v = Number(value || 0);
  if (!Number.isFinite(v) || v === 0) return "$0";
  const abs = Math.abs(v);
  if (abs >= 1000000) return `${v < 0 ? "-" : ""}$${(abs / 1000000).toFixed(abs >= 10000000 ? 1 : 2)}M`;
  if (abs >= 1000) return `${v < 0 ? "-" : ""}$${Math.round(abs / 1000)}K`;
  return `${v < 0 ? "-" : ""}$${Math.round(abs)}`;
}

function describeMicromarket(summary, profileShare, comparison = {}) {
  if (!summary || !summary.salesCount) {
    return "Too few recent MLS-enriched sales for a confident read.";
  }
  if (summary.salesCount < 5) {
    return `Only ${summary.salesCount} recent sales — read with caution.`;
  }
  const dom = Number(summary.medianDom || 0);
  const hotShare = Number(summary.hotShare || 0);
  const bidUp = Number(summary.medianBidUp || 0);
  const ratio = Number(summary.medianSaleToList || 0);
  const watchDom = Number(comparison.medianDom || 0);
  const watchBidUp = Number(comparison.medianBidUp || 0);
  const watchRatio = Number(comparison.medianSaleToList || 0);

  // Lead with the strongest comparative signal vs the watchlist baseline.
  if (watchDom > 0 && dom > 0 && dom <= Math.max(2, watchDom - 2)) {
    return `Fastest-moving in your watchlist — typical home pends in ${dom}d (vs ${watchDom}d watchlist median).`;
  }
  if (watchDom > 0 && dom >= watchDom + 4) {
    return `Slowest pace in your watchlist — ${dom}d typical DOM (vs ${watchDom}d median). Less competition.`;
  }
  if (bidUp > 0 && watchBidUp >= 0 && bidUp >= Math.max(25000, watchBidUp + 15000)) {
    return `Bid-up runs hot — median ${formatShortMoney(bidUp)} over ask (vs ${formatShortMoney(watchBidUp)} watchlist median).`;
  }
  if (ratio > 0 && watchRatio > 0 && ratio >= watchRatio + 0.02) {
    return `Pays a premium — sale/list ${ratio.toFixed(2)}x (vs ${watchRatio.toFixed(2)}x watchlist median).`;
  }
  if (ratio > 0 && ratio < 0.99) {
    return `Buyers winning under ask — median sells ${((1 - ratio) * 100).toFixed(1)}% below list.`;
  }
  if (hotShare >= 0.65 && dom > 0 && dom <= 8) {
    return `${Math.round(hotShare * 100)}% of homes pend within 10 days — sustained pressure.`;
  }
  if (hotShare > 0 && hotShare < 0.3 && dom >= 15) {
    return `Slower pocket — ${Math.round(hotShare * 100)}% pend within 10d, ${dom}d typical DOM.`;
  }
  // Last-resort: profile-fit framing (only when nothing else stood out).
  if (profileShare >= 0.5) {
    return `${Math.round(profileShare * 100)}% of recent sales match your saved-home profile.`;
  }
  return `${summary.salesCount} sales · ${dom}d typical DOM · ${ratio.toFixed(2)}x sale/list. In line with your watchlist.`;
}

function watchlistBaseline(perGroup) {
  return {
    medianDom: median(perGroup.map((g) => Number(g.summary?.medianDom || 0)).filter((n) => n > 0)),
    medianBidUp: median(perGroup.map((g) => Number(g.summary?.medianBidUp || 0))),
    medianSaleToList: median(perGroup.map((g) => Number(g.summary?.medianSaleToList || 0)).filter((n) => n > 0)),
  };
}

export function buildMicromarketProfiles(rows, rawProfile, nowValue) {
  const profile = normalizeProfileMemory(rawProfile);
  const sourceRows = Array.isArray(rows) ? rows : [];

  // First pass: collect summaries per group.
  const stage1 = MICRO_GROUPS.map((group) => {
    const groupRows = sourceRows.filter((row) => String(row?.pulseWatchlistGroup || "").trim() === group);
    const recent = recentRows(groupRows, nowValue || new Date(), 90);
    const summary = summarizeCompetition(recent);
    const recentMatches = recent.filter((row) => rowMatchesProfile(row, profile));
    const profileShare = summary.salesCount ? recentMatches.length / summary.salesCount : 0;
    return { group, summary, profileShare };
  });

  // Second pass: derive watchlist-wide medians for comparative descriptors.
  const baseline = watchlistBaseline(stage1);

  return stage1.map(({ group, summary, profileShare }) => ({
    group,
    summary,
    profileMatchShare: profileShare,
    fitLabel: fitLabel(profileShare),
    descriptor: describeMicromarket(summary, profileShare, baseline),
    priceBandLabel: priceBandLabel(summary.medianClosePrice),
  }));
}
