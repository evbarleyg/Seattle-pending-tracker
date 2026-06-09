import {
  addDays,
  clamp,
  daysAgo,
  median,
  monthKey,
  nullableMedian,
  quantile,
  roundToNearest,
  toIso,
  weightedQuantile,
} from "./format.mjs";
import {
  DEFAULT_MAX_CLOSE,
  DEFAULT_MIN_CLOSE,
  PRICE_SLIDER_CAP,
  domMetric,
  findDefaultSingleFamilyLabel,
  hotCategory,
  inRatioBucket,
  matchesSpecialSaleFilter,
  rowInViewport,
  uniqueValues,
  zip5,
} from "./data.mjs";
import {
  PULSE_GROUPS,
  PRIMARY_GROUP_ID,
  computeMicroNeighborhoodBreakout,
  computeMonthlyGroupSeries,
  computeMonthlySeries,
  computeRecentComparisons,
  groupsForSelection,
  normalizeSelectionId,
  summarizeRows,
} from "./pulseMetrics.mjs";
import { listingTier } from "./affordability.mjs";

export const DESKTOP_PAGE_SIZE = 100;
export const MOBILE_PAGE_SIZE = 40;
export const BID_COMP_WINDOW_DAYS = 90;
export const BID_HALFLIFE_DAYS = 30;
export const BID_MIN_COMPS = 6;
export const BID_RATIO_MIN = 0.9;
export const BID_RATIO_MAX = 1.25;

export const BID_STRATEGIES = {
  conservative: { point: 0.5, low: 0.4, high: 0.6, label: "Conservative" },
  balanced: { point: 0.6, low: 0.5, high: 0.7, label: "Balanced" },
  aggressive: { point: 0.7, low: 0.6, high: 0.8, label: "Aggressive" },
};

export const DEFAULT_FILTERS = {
  neighborhoods: [],
  type: "Single Family",
  excludeTypes: [],
  mode: "All",
  mlsStatus: "All",
  specialSale: "all",
  scope: "all",
  recordView: "all",
  minClose: DEFAULT_MIN_CLOSE,
  maxClose: DEFAULT_MAX_CLOSE,
  minLot: 0,
  maxLot: 0,
  affordability: "all",
  dateFrom: "",
  dateTo: "",
};

export function createDefaultFilters(typeOptions = []) {
  return {
    ...DEFAULT_FILTERS,
    neighborhoods: [],
    type: findDefaultSingleFamilyLabel(typeOptions),
  };
}

export function createEmptyInteractions() {
  return {
    month: null,
    neighborhood: null,
    type: null,
    ratioBucket: null,
    hotMarket: null,
    mode: null,
    closeTier: null,
    psfTier: null,
  };
}

export function buildOptions(rows) {
  const neighborhoods = Array.from(new Set((rows || []).map((row) => row.neighborhoodLabel).filter(Boolean))).sort();
  const types = uniqueValues(rows, "typeLabel");
  const mlsStatuses = [
    "All",
    ...Array.from(new Set((rows || []).map((row) => row.mlsStatusLabel).filter(Boolean))).sort(),
  ];
  return {
    neighborhoods,
    types,
    mlsStatuses,
    defaultType: findDefaultSingleFamilyLabel(types),
  };
}

export function filtersToSummary(filters) {
  const entries = [];
  const f = filters || DEFAULT_FILTERS;
  if (f.type && f.type !== "All") entries.push(f.type);
  if (Array.isArray(f.excludeTypes) && f.excludeTypes.length) entries.push(`Excl: ${f.excludeTypes.join(", ")}`);
  entries.push(`${formatPriceSummary(f.minClose)}-${f.maxClose === null ? "No max" : formatPriceSummary(f.maxClose)}`);
  if (f.minLot > 0 || f.maxLot > 0) {
    const lo = f.minLot > 0 ? `${f.minLot.toLocaleString()} sf` : "0";
    const hi = f.maxLot > 0 ? `${f.maxLot.toLocaleString()} sf` : "no max";
    entries.push(`Lot ${lo}-${hi}`);
  }
  if (f.neighborhoods?.length) entries.push(`${f.neighborhoods.length} neighborhoods`);
  if (f.affordability === "in_budget") entries.push("In budget");
  if (f.affordability === "in_budget_stretch") entries.push("In budget + stretch");
  if (f.scope === "hot10") entries.push("Hot <=10 DOM");
  if (f.scope === "ultra5") entries.push("Ultra <=5 DOM");
  if (f.mlsStatus && f.mlsStatus !== "All") entries.push(f.mlsStatus);
  if (f.specialSale === "only") entries.push("Special-sale only");
  if (f.specialSale === "exclude") entries.push("Excluding special sales");
  return entries;
}

function formatPriceSummary(value) {
  const amount = Number(value || 0);
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  if (amount >= 1000) return `$${Math.round(amount / 1000)}K`;
  return `$${Math.round(amount)}`;
}

export function normalizeFilters(filters, options = {}) {
  const typeOptions = options.types || [];
  const next = {
    ...DEFAULT_FILTERS,
    ...filters,
    neighborhoods: Array.isArray(filters?.neighborhoods) ? filters.neighborhoods.slice() : [],
    excludeTypes: Array.isArray(filters?.excludeTypes) ? filters.excludeTypes.slice() : [],
  };
  if (!next.type) next.type = findDefaultSingleFamilyLabel(typeOptions);
  if (next.maxClose !== null && Number(next.maxClose) >= PRICE_SLIDER_CAP) next.maxClose = null;
  if (!Number.isFinite(Number(next.minClose))) next.minClose = DEFAULT_MIN_CLOSE;
  if (next.maxClose !== null && !Number.isFinite(Number(next.maxClose))) next.maxClose = DEFAULT_MAX_CLOSE;
  next.minLot = Number.isFinite(Number(next.minLot)) ? Math.max(0, Number(next.minLot)) : 0;
  next.maxLot = Number.isFinite(Number(next.maxLot)) ? Math.max(0, Number(next.maxLot)) : 0;
  if (!["all", "in_budget", "in_budget_stretch"].includes(next.affordability)) next.affordability = "all";
  return next;
}

export function recordViewLabel(value) {
  if (value === "projOnly") return "Projected Only";
  if (value === "openOnly") return "Open/Pending Only";
  if (value === "projAndOpen") return "Projected + Open/Pending";
  return "All Visible Rows";
}

export function computeBaseStats(rows) {
  return {
    medianClose: median((rows || []).map((row) => row.closePrice).filter((value) => Number.isFinite(value))),
    medianPsf: median((rows || []).filter((row) => row.pricePerSqft > 0).map((row) => row.pricePerSqft)),
    medianDom: nullableMedian((rows || []).map(domMetric).filter((value) => value !== null)),
    medianSaleToList: nullableMedian((rows || []).map((row) => row.saleToList).filter((value) => value > 0)),
    medianBidUp: nullableMedian((rows || []).map((row) => row.delta).filter((value) => Number.isFinite(value))),
    hotShare: rows?.length ? rows.filter((row) => row.isHotMarket).length / rows.length : null,
  };
}

export function matchesSharedGlobalFilters(row, filterState, opts = {}) {
  const f = normalizeFilters(filterState);
  const priceField = opts.priceField || "closePrice";
  const dateField = opts.dateField || "saleDate";
  const price = Number(row[priceField] || 0);
  const date = String(row[dateField] || "");
  if (f.neighborhoods.length && !f.neighborhoods.includes(row.neighborhoodLabel)) return false;
  if (f.type !== "All" && row.typeLabel !== f.type) return false;
  if (f.excludeTypes.length && f.excludeTypes.includes(row.typeLabel)) return false;
  if (f.minLot > 0 && (Number(row.lotSize) || 0) < f.minLot) return false;
  if (f.maxLot > 0 && (Number(row.lotSize) || 0) > f.maxLot) return false;
  if (f.affordability === "in_budget" && row.affordTier !== "in_budget") return false;
  if (f.affordability === "in_budget_stretch" && !(row.affordTier === "in_budget" || row.affordTier === "stretch")) return false;
  if (f.mode !== "All" && row.dataMode !== f.mode) return false;
  if (!matchesSpecialSaleFilter(row, f.specialSale)) return false;
  if (f.minClose && price < f.minClose) return false;
  if (f.maxClose !== null && price > f.maxClose) return false;
  if (f.dateFrom && (!date || date < f.dateFrom)) return false;
  if (f.dateTo && (!date || date > f.dateTo)) return false;
  if (f.scope === "hot10" && !row.isHotMarket) return false;
  if (f.scope === "ultra5" && !row.isUltraHot) return false;
  return true;
}

export function computeBaseRows(filterState, normalizedRows, flags = {}) {
  const f = normalizeFilters(filterState);
  let rows = (normalizedRows || []).filter((row) => row.closePrice > 0 && !!row.saleDate);
  rows = rows.filter((row) => matchesSharedGlobalFilters(row, f, { priceField: "closePrice", dateField: "saleDate" }));
  if (f.mlsStatus !== "All") rows = rows.filter((row) => row.mlsStatusNorm === String(f.mlsStatus || "").toUpperCase());
  const presoldCandidatesInSlice = rows.filter((row) => row.isLikelyPresoldNewBuild).length;
  if (flags.excludeLikelyPresoldNewBuild) rows = rows.filter((row) => !row.isLikelyPresoldNewBuild);
  return {
    rows,
    meta: {
      presoldCandidatesInSlice,
      presoldExcludedInSlice: flags.excludeLikelyPresoldNewBuild ? presoldCandidatesInSlice : 0,
    },
  };
}

export function applyInteractions(baseRows, stats, interactions = {}, geo = {}) {
  const selectedMapKeys = Array.isArray(geo.filterPropertyKeys) ? geo.filterPropertyKeys : [];
  const viewportFilterActive = !!geo.viewportFilter && !!geo.mapBounds;
  return (baseRows || []).filter((row) => {
    if (selectedMapKeys.length && !selectedMapKeys.includes(row.mapPropertyKey)) return false;
    if (viewportFilterActive && !rowInViewport(row, geo.mapBounds)) return false;
    if (interactions.month && monthKey(row.saleDate) !== interactions.month) return false;
    if (interactions.neighborhood && row.neighborhoodLabel !== interactions.neighborhood) return false;
    if (interactions.type && row.typeLabel !== interactions.type) return false;
    if (interactions.mode && row.dataMode !== interactions.mode) return false;
    if (interactions.ratioBucket && !inRatioBucket(row.saleToList, interactions.ratioBucket)) return false;
    if (interactions.hotMarket === "hot" && !row.isHotMarket) return false;
    if (interactions.hotMarket === "ultra" && !row.isUltraHot) return false;
    if (interactions.closeTier === "aboveMedian" && row.closePrice < stats.medianClose) return false;
    if (interactions.psfTier === "aboveMedian") {
      if (row.pricePerSqft <= 0 || row.pricePerSqft < stats.medianPsf) return false;
    }
    return true;
  });
}

export function computeProjectedPendingRows(referenceRows, stats, filterState, normalizedRows, flags = {}, interactions = {}, geo = {}) {
  const f = normalizeFilters(filterState);
  const result = {
    rows: [],
    candidatesFiltered: 0,
    compSourceCount: 0,
    reason: "",
  };
  if (!flags.projection) {
    result.reason = "Projection disabled. Turn on Pending Projection in Feature Flags.";
    return result;
  }
  if (f.mode === "PUBLIC_PROXY") {
    result.reason = "Projection is unavailable in Public Proxy mode.";
    return result;
  }

  const compSource = (referenceRows || []).filter((row) => row.hasActualClose && row.pendingListPrice > 0 && row.saleToList > 0);
  result.compSourceCount = compSource.length;
  const existing = new Set((referenceRows || []).map((row) => row.mapPropertyKey));
  let pendingRows = (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED" && !row.hasActualClose && row.pendingListPrice > 0)
    .filter((row) => !existing.has(row.mapPropertyKey))
    .filter((row) => matchesSharedGlobalFilters(row, f, { priceField: "pendingListPrice", dateField: "effectiveDate" }));

  if (f.mlsStatus !== "All") pendingRows = pendingRows.filter((row) => row.mlsStatusNorm === String(f.mlsStatus || "").toUpperCase());
  result.candidatesFiltered = pendingRows.length;
  if (!pendingRows.length) {
    result.reason = "No pending/open candidates after filters.";
    return result;
  }
  if (!compSource.length) {
    result.reason = "No eligible comps for projection in this slice.";
    return result;
  }

  const projected = pendingRows.map((row) => {
    let localComps = compSource.filter((comp) => comp.typeLabel === row.typeLabel && comp.neighborhoodLabel === row.neighborhoodLabel);
    if (localComps.length < 6) localComps = compSource.filter((comp) => comp.typeLabel === row.typeLabel);
    if (localComps.length < 6) localComps = compSource;
    const ratios = localComps.map((comp) => comp.saleToList).filter((value) => value > 0);
    const ratioMed = ratios.length ? median(ratios) : 1;
    const ratioLow = ratios.length ? quantile(ratios, 0.25) : ratioMed;
    const ratioHigh = ratios.length ? quantile(ratios, 0.75) : ratioMed;
    const projectedClosePrice = Math.round(row.pendingListPrice * ratioMed);
    const projectedCloseLow = Math.round(row.pendingListPrice * ratioLow);
    const projectedCloseHigh = Math.round(row.pendingListPrice * ratioHigh);
    const projectedDelta = projectedClosePrice - row.pendingListPrice;
    const projectedPsf = row.sqft > 0 ? projectedClosePrice / row.sqft : 0;
    return {
      ...row,
      isProjectionRow: true,
      saleDate: row.pendingDate || row.listDate || row.saleDate,
      effectiveDate: row.pendingDate || row.listDate || row.effectiveDate,
      closePrice: projectedClosePrice,
      projectedClosePrice,
      projectedCloseLow,
      projectedCloseHigh,
      projectionBasisCount: localComps.length,
      saleToList: ratioMed,
      saleToOriginalList: row.originalListPrice > 0 ? projectedClosePrice / row.originalListPrice : 0,
      delta: projectedDelta,
      deltaPct: row.pendingListPrice > 0 ? projectedDelta / row.pendingListPrice : 0,
      pricePerSqft: projectedPsf,
    };
  });

  result.rows = applyInteractions(projected, stats, interactions, geo);
  if (!result.rows.length && !result.reason) result.reason = "No projected rows in current cross-filters.";
  return result;
}

export function computeOpenMlsRows(stats, projectedKeys, filterState, normalizedRows, flags = {}, interactions = {}, geo = {}) {
  const f = normalizeFilters(filterState);
  const result = {
    rows: [],
    candidatesFiltered: 0,
    reason: "",
  };
  if (f.mode === "PUBLIC_PROXY") {
    result.reason = "Open/Pending MLS rows are unavailable in Public Proxy mode.";
    return result;
  }

  let openRows = (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED" && !row.hasActualClose && row.pendingListPrice > 0)
    .filter((row) => matchesSharedGlobalFilters(row, f, { priceField: "pendingListPrice", dateField: "effectiveDate" }));
  if (f.mlsStatus !== "All") openRows = openRows.filter((row) => row.mlsStatusNorm === String(f.mlsStatus || "").toUpperCase());
  result.candidatesFiltered = openRows.length;

  if (!flags.includeOpenMls) {
    result.reason = "Open/Pending MLS rows are hidden. Turn on Include Open/Pending MLS.";
    return result;
  }

  let visible = applyInteractions(openRows, stats, interactions, geo);
  if (projectedKeys?.size) visible = visible.filter((row) => !projectedKeys.has(row.mapPropertyKey));
  result.rows = visible;
  if (!result.rows.length && !result.reason) {
    result.reason = result.candidatesFiltered ? "No open/pending rows in current cross-filters." : "No pending/open candidates after filters.";
  }
  return result;
}

export function buildRecordGeoRows(filterState, closedRows, projectedRows, openRows) {
  if (filterState.recordView === "projOnly") return [...projectedRows];
  if (filterState.recordView === "openOnly") return [...openRows];
  if (filterState.recordView === "projAndOpen") return [...projectedRows, ...openRows];
  return [...closedRows, ...projectedRows, ...openRows];
}

export function buildRecordGeoEmptyMessage(filterState, projectionResult, openResult, flags = {}) {
  const view = filterState.recordView || "all";
  if (view === "projOnly") {
    if (!flags.projection) return "Projection disabled. Turn on Pending Projection in Feature Flags.";
    if (!projectionResult.candidatesFiltered) return "No pending/open candidates after filters.";
    if (!projectionResult.compSourceCount) return "No eligible comps for projection in this slice.";
    return "No projected rows in current cross-filters.";
  }
  if (view === "openOnly") {
    if (!flags.includeOpenMls) return "Open/Pending MLS rows are hidden. Turn on Include Open/Pending MLS.";
    if (!openResult.candidatesFiltered) return "No pending/open candidates after filters.";
    return "No open/pending rows in current cross-filters.";
  }
  if (view === "projAndOpen") {
    if (!flags.projection && !flags.includeOpenMls) {
      return "Projection disabled and Open/Pending MLS hidden. Enable one or both feature flags.";
    }
    if (!projectionResult.candidatesFiltered && !openResult.candidatesFiltered) return "No pending/open candidates after filters.";
    if (flags.projection && projectionResult.candidatesFiltered && !projectionResult.compSourceCount && !openResult.rows.length) {
      return "No eligible comps for projection in this slice.";
    }
    return "No projected/open rows in current cross-filters.";
  }
  return "No rows for current filters.";
}

export function computeSlices(filterState, normalizedRows, options = {}) {
  const flags = options.flags || {};
  const interactions = options.interactions || {};
  const geo = options.geo || {};
  const base = computeBaseRows(filterState, normalizedRows, flags);
  const stats = computeBaseStats(base.rows);
  const closedSlice = applyInteractions(base.rows, stats, interactions, geo);
  const projectionResult = computeProjectedPendingRows(closedSlice, stats, filterState, normalizedRows, flags, interactions, geo);
  const projectedRows = projectionResult.rows;
  const projectedKeys = new Set(projectedRows.map((row) => row.mapPropertyKey));
  const openResult = computeOpenMlsRows(stats, projectedKeys, filterState, normalizedRows, flags, interactions, geo);
  const openRows = openResult.rows;
  const recordGeoRows = buildRecordGeoRows(filterState, closedSlice, projectedRows, openRows);
  return {
    filterState: normalizeFilters(filterState),
    stats,
    closedRows: base.rows,
    closedSlice,
    projectedRows,
    openRows,
    recordGeoRows,
    projectionResult,
    openResult,
    emptyMessage: buildRecordGeoEmptyMessage(normalizeFilters(filterState), projectionResult, openResult, flags),
    meta: {
      ...base.meta,
      projectedRowsInSlice: projectedRows.length,
      openRowsAvailable: openResult.candidatesFiltered,
      openRowsInSlice: openRows.length,
    },
  };
}

export function pulseFilterRows(normalizedRows, filterState, flags = {}) {
  const f = normalizeFilters(filterState);
  let rows = (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED")
    .filter((row) => row.hasActualClose && row.closePrice > 0)
    .filter((row) => !!row.pulseWatchlistGroup);
  rows = rows.filter((row) => matchesSharedGlobalFilters(row, f, { priceField: "closePrice", dateField: "saleDate" }));
  if (flags.excludeLikelyPresoldNewBuild) rows = rows.filter((row) => !row.isLikelyPresoldNewBuild);
  return rows;
}

export function pulseRowSummary(row) {
  return {
    neighborhoodLabel: row.neighborhoodLabel,
    pulseWatchlistGroup: row.pulseWatchlistGroup,
    saleDate: row.saleDate,
    isHotMarket: row.isHotMarket,
    isUltraHot: row.isUltraHot,
    domValue: domMetric(row),
    saleToList: row.saleToList,
    delta: row.delta,
    closePrice: row.closePrice,
  };
}

export function buildPulseSnapshot(normalizedRows, filterState, options = {}) {
  const selectedGroup = normalizeSelectionId(options.selectedGroup || PRIMARY_GROUP_ID);
  const nowValue = options.nowValue || new Date();
  const filtered = pulseFilterRows(normalizedRows, filterState, options.flags || {}).map(pulseRowSummary);
  const activeGroups = groupsForSelection(selectedGroup);
  const selectedRows = filtered.filter((row) => activeGroups.includes(row.pulseWatchlistGroup));
  return {
    selectedGroup,
    selectedLabel: PULSE_GROUPS.find((group) => group.id === selectedGroup)?.label || "Primary Watchlist",
    activeGroups,
    filteredRows: filtered,
    selectedRows,
    recentComparisons: computeRecentComparisons(selectedRows, nowValue, [30, 90]),
    monthlyByGroup: computeMonthlyGroupSeries(filtered, nowValue, 12),
    selectedMonthlySeries: computeMonthlySeries(selectedRows, nowValue, 12),
    microBreakout: computeMicroNeighborhoodBreakout(filtered, nowValue, 90),
    summary: summarizeRows(selectedRows),
  };
}

export function bidConfidenceLabel(score) {
  if (!Number.isFinite(score) || score <= 0) return "N/A";
  if (score >= 75) return "High";
  if (score >= 55) return "Medium";
  return "Low";
}

export function bidTierLabel(value) {
  if (value === "T1_NEIGHBORHOOD_TYPE") return "Neighborhood + Type";
  if (value === "T2_ZIP_TYPE") return "Zip + Type";
  if (value === "T3_CITY_TYPE") return "Seattle Type";
  return "Insufficient comps";
}

export function computeBidCompTiers(targetRow, compPool) {
  const zip = zip5(targetRow.zip);
  const byType = (compPool || []).filter((row) => row.typeLabel === targetRow.typeLabel);
  const tier1 = byType.filter((row) => row.neighborhoodLabel === targetRow.neighborhoodLabel);
  if (tier1.length >= BID_MIN_COMPS) return { tier: "T1_NEIGHBORHOOD_TYPE", rows: tier1 };
  const tier2 = byType.filter((row) => zip && zip5(row.zip) === zip);
  if (tier2.length >= BID_MIN_COMPS) return { tier: "T2_ZIP_TYPE", rows: tier2 };
  if (byType.length >= BID_MIN_COMPS) return { tier: "T3_CITY_TYPE", rows: byType };
  return { tier: "NONE", rows: byType };
}

export function buildBidCompPool(filterState, normalizedRows, nowValue = new Date()) {
  const windowStart = addDays(nowValue, -BID_COMP_WINDOW_DAYS);
  const windowStartIso = toIso(windowStart);
  return (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED" && row.hasActualClose && row.pendingListPrice > 0 && row.saleToList > 0)
    .filter((row) => !row.isLikelyPresoldNewBuild)
    .filter((row) => !!row.saleDate && row.saleDate >= windowStartIso)
    .filter((row) => matchesSharedGlobalFilters(row, filterState, { priceField: "closePrice", dateField: "saleDate" }));
}

export function scoreBidForRow(row, compPool, strategy = "balanced", includeCompRows = false) {
  const strat = BID_STRATEGIES[strategy] || BID_STRATEGIES.balanced;
  const tier = computeBidCompTiers(row, compPool);
  const compRows = tier.rows;
  const compRowsSorted = compRows.slice().sort((a, b) => String(b.saleDate || "").localeCompare(String(a.saleDate || "")));
  if (tier.tier === "NONE" || compRows.length < BID_MIN_COMPS) {
    return {
      ...row,
      bidStrategy: strat.label,
      bidSuggested: 0,
      bidLow: 0,
      bidHigh: 0,
      bidRatio: 0,
      bidConfidence: 0,
      bidConfidenceLabel: "N/A",
      bidCompCount: compRows.length,
      bidCompTier: "NONE",
      bidStatus: "InsufficientComps",
      bidCompRows: includeCompRows ? compRowsSorted.slice(0, 12) : [],
    };
  }

  const ratios = compRows.map((comp) => comp.saleToList);
  const recencies = compRows.map((comp) => daysAgo(comp.saleDate)).filter((value) => value !== null);
  const weights = compRows.map((comp) => {
    const age = daysAgo(comp.saleDate) ?? BID_COMP_WINDOW_DAYS;
    return Math.pow(0.5, age / BID_HALFLIFE_DAYS);
  });

  let pointRatio = weightedQuantile(ratios, weights, strat.point);
  let lowRatio = weightedQuantile(ratios, weights, strat.low);
  let highRatio = weightedQuantile(ratios, weights, strat.high);
  let heatAdj = 0;
  if (row.isUltraHot) heatAdj += 0.01;
  else if (row.isHotMarket) heatAdj += 0.005;
  const marketDom = domMetric(row);
  if (marketDom !== null && marketDom >= 30) heatAdj -= 0.005;

  pointRatio = clamp(pointRatio + heatAdj, BID_RATIO_MIN, BID_RATIO_MAX);
  lowRatio = clamp(lowRatio + heatAdj, BID_RATIO_MIN, BID_RATIO_MAX);
  highRatio = clamp(highRatio + heatAdj, BID_RATIO_MIN, BID_RATIO_MAX);
  if (lowRatio > pointRatio) lowRatio = pointRatio;
  if (highRatio < pointRatio) highRatio = pointRatio;

  const suggestedBid = roundToNearest(row.pendingListPrice * pointRatio, 1000);
  const bidLow = roundToNearest(row.pendingListPrice * lowRatio, 1000);
  const bidHigh = roundToNearest(row.pendingListPrice * highRatio, 1000);
  const bidRatio = row.pendingListPrice > 0 ? suggestedBid / row.pendingListPrice : 0;
  const countScore = clamp((compRows.length / 20) * 45, 0, 45);
  const tierScore = tier.tier === "T1_NEIGHBORHOOD_TYPE" ? 35 : (tier.tier === "T2_ZIP_TYPE" ? 25 : 15);
  const medianRecency = recencies.length ? median(recencies) : BID_COMP_WINDOW_DAYS;
  const recencyScore = clamp((1 - (medianRecency / BID_COMP_WINDOW_DAYS)) * 20, 0, 20);
  const bidConfidence = Math.round(clamp(countScore + tierScore + recencyScore, 0, 100));

  return {
    ...row,
    bidStrategy: strat.label,
    bidSuggested: suggestedBid,
    bidLow,
    bidHigh,
    bidRatio,
    bidConfidence,
    bidConfidenceLabel: bidConfidenceLabel(bidConfidence),
    bidCompCount: compRows.length,
    bidCompTier: tier.tier,
    bidStatus: "SCored",
    bidCompRows: includeCompRows ? compRowsSorted.slice(0, 12) : [],
  };
}

export function computeActiveBidSuggestions(filterState, normalizedRows, strategy = "balanced", options = {}) {
  const f = normalizeFilters(filterState);
  const empty = {
    rows: [],
    byKey: new Map(),
    stats: {
      activeCount: 0,
      scoredCount: 0,
      highConfidenceCount: 0,
      insufficientCount: 0,
      medianOverAskPct: 0,
    },
    compPool: [],
  };
  if (f.mlsStatus !== "All" && String(f.mlsStatus || "").toUpperCase() !== "ACTIVE") return empty;

  const activeRowsBase = (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED")
    .filter((row) => row.mlsStatusNorm === "ACTIVE")
    .filter((row) => !row.hasActualClose && row.pendingListPrice > 0)
    .filter((row) => matchesSharedGlobalFilters(row, f, { priceField: "pendingListPrice", dateField: "effectiveDate" }));
  const compPool = options.compPool || buildBidCompPool(f, normalizedRows, options.nowValue || new Date());
  const scoredRows = activeRowsBase.map((row) => scoreBidForRow(row, compPool, strategy));
  const scored = scoredRows.filter((row) => row.bidStatus === "SCored");
  const highConfidenceCount = scoredRows.filter((row) => row.bidConfidence >= 75).length;
  const medianOverAskPct = scored.length
    ? median(scored.map((row) => ((row.bidSuggested - row.pendingListPrice) / row.pendingListPrice) * 100))
    : 0;
  const byKey = new Map(scoredRows.map((row) => [row.mapPropertyKey, row]));
  return {
    rows: scoredRows,
    byKey,
    stats: {
      activeCount: scoredRows.length,
      scoredCount: scored.length,
      highConfidenceCount,
      insufficientCount: scoredRows.length - scored.length,
      medianOverAskPct,
    },
    compPool,
  };
}

export function applyBidViewInteractions(rows, interactions = {}, geo = {}) {
  const selectedMapKeys = Array.isArray(geo.filterPropertyKeys) ? geo.filterPropertyKeys : [];
  const viewportFilterActive = !!geo.viewportFilter && !!geo.mapBounds;
  return (rows || []).filter((row) => {
    if (selectedMapKeys.length && !selectedMapKeys.includes(row.mapPropertyKey)) return false;
    if (viewportFilterActive && !rowInViewport(row, geo.mapBounds)) return false;
    if (interactions.neighborhood && row.neighborhoodLabel !== interactions.neighborhood) return false;
    if (interactions.type && row.typeLabel !== interactions.type) return false;
    if (interactions.mode && row.dataMode !== interactions.mode) return false;
    if (interactions.hotMarket === "hot" && !row.isHotMarket) return false;
    if (interactions.hotMarket === "ultra" && !row.isUltraHot) return false;
    return true;
  });
}

export function mergeBidFields(row, bidByKey) {
  const bid = bidByKey.get(row.mapPropertyKey);
  if (!bid) {
    return {
      ...row,
      bidStrategy: "",
      bidSuggested: 0,
      bidLow: 0,
      bidHigh: 0,
      bidRatio: 0,
      bidConfidence: 0,
      bidConfidenceLabel: "N/A",
      bidCompCount: 0,
      bidCompTier: "NONE",
      bidStatus: "",
    };
  }
  return {
    ...row,
    bidStrategy: bid.bidStrategy || "",
    bidSuggested: bid.bidSuggested || 0,
    bidLow: bid.bidLow || 0,
    bidHigh: bid.bidHigh || 0,
    bidRatio: bid.bidRatio || 0,
    bidConfidence: bid.bidConfidence || 0,
    bidConfidenceLabel: bid.bidConfidenceLabel || "N/A",
    bidCompCount: bid.bidCompCount || 0,
    bidCompTier: bid.bidCompTier || "NONE",
    bidStatus: bid.bidStatus || "",
  };
}

/**
 * Tag every row with an affordability tier (in_budget|stretch|over|null) for
 * the current scenario's price ceilings. Mutates in place for cheapness — the
 * normalized rows are recomputed wholesale on each derive, so this is safe.
 * `result` is a single computeAffordability() output; null when unconfigured.
 */
export function mergeAffordabilityTier(rows, result) {
  const list = Array.isArray(rows) ? rows : [];
  if (!result) {
    for (const row of list) row.affordTier = null;
    return list;
  }
  for (const row of list) {
    row.affordTier = listingTier(result, Number(row.affordPriceBasis) || 0);
  }
  return list;
}

export function getRecordSortValue(row, key) {
  if (key === "address") return String(row.address || "");
  if (key === "neighborhood") return String(row.neighborhoodLabel || "");
  if (key === "type") return String(row.typeLabel || "");
  if (key === "beds") return Number(row.beds || 0);
  if (key === "baths") return Number(row.baths || 0);
  if (key === "sqft") return Number(row.sqft || 0);
  if (key === "lotSize") return Number(row.lotSize || 0);
  if (key === "saleDate") return String(row.saleDate || "");
  if (key === "closePrice") return Number(row.closePrice || 0);
  if (key === "originalListPrice") return Number(row.originalListPrice || 0);
  if (key === "pendingListPrice") return Number(row.pendingListPrice || 0);
  if (key === "domMetric") return Number(domMetric(row) ?? -1);
  if (key === "hotCategory") return String(hotCategory(row));
  if (key === "saleToList") return Number(row.saleToList || 0);
  if (key === "delta") return Number(row.delta || 0);
  if (key === "dataMode") return String(row.dataMode || "");
  return "";
}

export function getBidSortValue(row, key) {
  if (key === "address") return String(row.address || "");
  if (key === "neighborhood") return String(row.neighborhoodLabel || "");
  if (key === "type") return String(row.typeLabel || "");
  if (key === "originalListPrice") return Number(row.originalListPrice || 0);
  if (key === "pendingListPrice") return Number(row.pendingListPrice || 0);
  if (key === "dom") return Number(domMetric(row) ?? -1);
  if (key === "suggestedBid") return row.bidStatus === "SCored" ? Number(row.bidSuggested || 0) : -1;
  if (key === "bidRange") return row.bidStatus === "SCored" ? Number(row.bidHigh || row.bidLow || 0) : -1;
  if (key === "ratio") return row.bidStatus === "SCored" ? Number(row.bidRatio || 0) : 0;
  if (key === "confidence") return Number(row.bidConfidence || 0);
  if (key === "compCount") return Number(row.bidCompCount || 0);
  if (key === "compTier") {
    if (row.bidCompTier === "T1_NEIGHBORHOOD_TYPE") return 3;
    if (row.bidCompTier === "T2_ZIP_TYPE") return 2;
    if (row.bidCompTier === "T3_CITY_TYPE") return 1;
    return 0;
  }
  return "";
}

export function sortRows(rows, sort, getter) {
  const key = sort?.key || "saleDate";
  const dir = sort?.dir === "asc" ? "asc" : "desc";
  const factor = dir === "asc" ? 1 : -1;
  return (rows || []).slice().sort((a, b) => {
    const av = getter(a, key);
    const bv = getter(b, key);
    if (typeof av === "number" && typeof bv === "number" && Number.isFinite(av) && Number.isFinite(bv)) {
      const diff = (av - bv) * factor;
      if (diff !== 0) return diff;
    } else {
      const diff = String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: "base" }) * factor;
      if (diff !== 0) return diff;
    }
    return String(a.address || "").localeCompare(String(b.address || ""), undefined, { sensitivity: "base" });
  });
}

export function paginateRows(rows, page, pageSize) {
  const size = Math.max(1, Number(pageSize || DESKTOP_PAGE_SIZE));
  const pageCount = Math.max(1, Math.ceil((rows || []).length / size));
  const currentPage = clamp(Math.max(1, Number(page || 1)), 1, pageCount);
  const start = (currentPage - 1) * size;
  return {
    rows: (rows || []).slice(start, start + size),
    page: currentPage,
    pageSize: size,
    pageCount,
    total: (rows || []).length,
    start: (rows || []).length ? start + 1 : 0,
    end: Math.min(start + size, (rows || []).length),
  };
}

export function escapeCsv(value) {
  const text = String(value ?? "");
  if (text.includes(",") || text.includes('"') || text.includes("\n")) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export function exportRowsToCsv(rows) {
  const headers = [
    "id", "address", "neighborhood", "type", "dataMode", "saleDate", "listDate", "pendingDate",
    "closePrice", "originalListPrice", "pendingListPrice", "listPriceAtPending", "saleToList", "saleToOriginalList", "delta", "deltaPct", "isHotMarket", "isUltraHot",
    "mlsStatus", "mlsRegion", "mlsParkingType", "mlsParkingCoveredTotal", "mlsTaxesAnnual", "mlsBuildingCondition", "mlsView", "mlsBankOwned", "mlsThirdPartyApprovalRequired", "mlsNewConstructionState", "mlsSquareFootageSource", "isSpecialSale",
    "bidStrategy", "bidSuggested", "bidLow", "bidHigh", "bidRatio", "bidConfidence", "bidConfidenceLabel", "bidCompCount", "bidCompTier", "bidStatus",
    "hotCategory", "domDays", "isLikelyPresoldNewBuild", "presoldRuleReason", "isProjectionRow", "projectedClosePrice", "projectedCloseLow", "projectedCloseHigh", "projectionBasisCount",
    "beds", "baths", "sqft", "lotSize", "yearBuilt", "zip",
  ];
  const lines = [headers.join(",")];
  (rows || []).forEach((row) => {
    const activeBidEligible = row.dataMode === "MLS_ENRICHED" && row.mlsStatusNorm === "ACTIVE" && !row.hasActualClose;
    lines.push([
      row.id,
      row.address,
      row.neighborhoodLabel,
      row.typeLabel,
      row.dataMode,
      row.saleDate,
      row.listDate,
      row.pendingDate,
      row.closePrice,
      row.originalListPrice || "",
      row.pendingListPrice || "",
      row.listPriceAtPending,
      row.saleToList,
      row.saleToOriginalList,
      row.delta,
      row.deltaPct,
      row.isHotMarket ? "true" : "false",
      row.isUltraHot ? "true" : "false",
      row.mlsStatusLabel || "",
      row.mlsRegion || "",
      row.mlsParkingType || "",
      row.hasMlsParkingCoveredTotal ? row.mlsParkingCoveredTotal : "",
      row.hasMlsTaxesAnnual ? Math.round(row.mlsTaxesAnnual) : "",
      row.mlsBuildingCondition || "",
      row.mlsView || "",
      row.mlsBankOwned || "",
      row.mlsThirdPartyApprovalRequired || "",
      row.mlsNewConstructionState || "",
      row.mlsSquareFootageSource || "",
      row.isSpecialSale ? "true" : "false",
      activeBidEligible ? row.bidStrategy || "" : "",
      activeBidEligible && row.bidStatus === "SCored" ? row.bidSuggested : "",
      activeBidEligible && row.bidStatus === "SCored" ? row.bidLow : "",
      activeBidEligible && row.bidStatus === "SCored" ? row.bidHigh : "",
      activeBidEligible && row.bidStatus === "SCored" ? row.bidRatio : "",
      activeBidEligible && row.bidStatus === "SCored" ? row.bidConfidence : "",
      activeBidEligible ? row.bidConfidenceLabel || "" : "",
      activeBidEligible ? row.bidCompCount || "" : "",
      activeBidEligible ? row.bidCompTier || "" : "",
      activeBidEligible ? row.bidStatus || "" : "",
      hotCategory(row),
      domMetric(row) ?? "",
      row.isLikelyPresoldNewBuild ? "true" : "false",
      row.presoldRuleReason || "",
      row.isProjectionRow ? "true" : "false",
      row.projectedClosePrice || "",
      row.projectedCloseLow || "",
      row.projectedCloseHigh || "",
      row.projectionBasisCount || "",
      row.beds,
      row.baths,
      row.sqft,
      row.lotSize,
      row.yearBuilt,
      row.zip,
    ].map(escapeCsv).join(","));
  });
  return lines.join("\n");
}
