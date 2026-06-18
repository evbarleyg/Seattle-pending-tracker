import {
  addDays,
  median,
  monthKey,
  nullableMedian,
  toDate,
  toIso,
} from "./format.mjs";

export const PRIMARY_GROUP_ID = "primary";
export const WATCHLIST_GROUP_ORDER = [
  "Ballard",
  "Fremont / Green Lake / Woodland Park",
  "Queen Anne",
  "Magnolia",
];
export const PRIMARY_WATCHLIST_GROUPS = [
  "Ballard",
  "Fremont / Green Lake / Woodland Park",
  "Queen Anne",
];
export const PULSE_GROUPS = [
  {
    id: PRIMARY_GROUP_ID,
    label: "Primary Watchlist",
    groups: PRIMARY_WATCHLIST_GROUPS.slice(),
  },
  { id: "Ballard", label: "Ballard", groups: ["Ballard"] },
  {
    id: "Fremont / Green Lake / Woodland Park",
    label: "Fremont / Green Lake / Woodland Park",
    groups: ["Fremont / Green Lake / Woodland Park"],
  },
  { id: "Queen Anne", label: "Queen Anne", groups: ["Queen Anne"] },
  { id: "Magnolia", label: "Magnolia", groups: ["Magnolia"] },
];

export function pulseWatchlistGroup(neighborhoodLabel) {
  const value = String(neighborhoodLabel || "").trim();
  if (!value) return "";
  if (value === "Ballard" || value === "Ballard / Crown Hill") return "Ballard";
  if (value === "Fremont / Green Lake / Wallingford") return "Fremont / Green Lake / Woodland Park";
  if (value === "Queen Anne / Magnolia" || value === "South Lake Union / Queen Anne") return "Queen Anne";
  if (value === "Magnolia") return "Magnolia";
  return "";
}

export function isWatchlistGroup(groupName) {
  return WATCHLIST_GROUP_ORDER.includes(String(groupName || "").trim());
}

export function canonicalPulseGroup(groupName, neighborhoodLabel) {
  const direct = String(groupName || "").trim();
  if (isWatchlistGroup(direct)) return direct;
  return pulseWatchlistGroup(neighborhoodLabel || direct);
}

export function normalizeSelectionId(selectionId) {
  const wanted = String(selectionId || PRIMARY_GROUP_ID).trim();
  return PULSE_GROUPS.some((group) => group.id === wanted) ? wanted : PRIMARY_GROUP_ID;
}

export function groupsForSelection(selectionId) {
  const normalized = normalizeSelectionId(selectionId);
  const match = PULSE_GROUPS.find((group) => group.id === normalized);
  return match ? match.groups.slice() : PRIMARY_WATCHLIST_GROUPS.slice();
}

export function addMonths(value, months) {
  const parsed = toDate(value);
  if (!parsed) return null;
  return new Date(parsed.getFullYear(), parsed.getMonth() + Number(months || 0), 1);
}

export function buildMonthKeys(nowValue, monthCount = 12) {
  const anchor = toDate(nowValue || new Date());
  if (!anchor) return [];
  const start = new Date(anchor.getFullYear(), anchor.getMonth(), 1);
  const count = Math.max(1, Number(monthCount || 12));
  const out = [];
  for (let index = count - 1; index >= 0; index -= 1) {
    out.push(monthKey(addMonths(start, -index)));
  }
  return out;
}

function startOfDay(value) {
  const parsed = toDate(value);
  if (!parsed) return null;
  parsed.setHours(0, 0, 0, 0);
  return parsed;
}

function inDateRange(dateValue, startInclusive, endExclusive) {
  const parsed = startOfDay(dateValue);
  if (!parsed || !startInclusive || !endExclusive) return false;
  const time = parsed.getTime();
  return time >= startInclusive.getTime() && time < endExclusive.getTime();
}

function safeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function summarizeRows(rows) {
  const data = Array.isArray(rows) ? rows : [];
  const count = data.length;
  const domValues = data.map((row) => safeNumber(row.domValue)).filter((value) => value !== null && value >= 0);
  const saleToListValues = data.map((row) => safeNumber(row.saleToList)).filter((value) => value !== null && value > 0);
  const bidUpValues = data.map((row) => safeNumber(row.delta)).filter((value) => value !== null);
  const closeValues = data.map((row) => safeNumber(row.closePrice)).filter((value) => value !== null && value > 0);
  const psfValues = data.map((row) => safeNumber(row.pricePerSqft)).filter((value) => value !== null && value > 0);

  return {
    salesCount: count,
    hotShare: count ? data.filter((row) => !!row.isHotMarket).length / count : null,
    ultraHotShare: count ? data.filter((row) => !!row.isUltraHot).length / count : null,
    medianDom: nullableMedian(domValues),
    medianSaleToList: nullableMedian(saleToListValues),
    medianBidUp: nullableMedian(bidUpValues),
    medianClosePrice: nullableMedian(closeValues),
    medianPsf: nullableMedian(psfValues),
    sampleSize: count,
    domSampleSize: domValues.length,
    ratioSampleSize: saleToListValues.length,
    bidUpSampleSize: bidUpValues.length,
    closeSampleSize: closeValues.length,
    psfSampleSize: psfValues.length,
  };
}

export function computeWindowSummary(rows, nowValue, windowDays) {
  const anchor = startOfDay(nowValue || new Date());
  if (!anchor) {
    return { current: summarizeRows([]), previous: summarizeRows([]) };
  }
  const days = Math.max(1, Number(windowDays || 30));
  const endExclusive = addDays(anchor, 1);
  const currentStart = addDays(anchor, -(days - 1));
  const previousEndExclusive = currentStart;
  const previousStart = addDays(previousEndExclusive, -days);
  const currentRows = (rows || []).filter((row) => inDateRange(row.saleDate, currentStart, endExclusive));
  const previousRows = (rows || []).filter((row) => inDateRange(row.saleDate, previousStart, previousEndExclusive));
  return {
    current: summarizeRows(currentRows),
    previous: summarizeRows(previousRows),
  };
}

export function computeRecentComparisons(rows, nowValue, windowDaysList) {
  return (windowDaysList || []).map((windowDays) => ({
    windowDays: Number(windowDays || 0),
    ...computeWindowSummary(rows, nowValue, windowDays),
  }));
}

export function computeMonthlySeries(rows, nowValue, monthCount = 12) {
  const keys = buildMonthKeys(nowValue, monthCount);
  return keys.map((key) => {
    const monthRows = (rows || []).filter((row) => monthKey(row.saleDate) === key);
    return {
      month: key,
      ...summarizeRows(monthRows),
    };
  });
}

export function computeMonthlyGroupSeries(rows, nowValue, monthCount = 12) {
  const grouped = new Map();
  WATCHLIST_GROUP_ORDER.forEach((group) => grouped.set(group, []));
  (rows || []).forEach((row) => {
    const group = canonicalPulseGroup(row.pulseWatchlistGroup, row.neighborhoodLabel);
    if (!grouped.has(group)) return;
    grouped.get(group).push(row);
  });
  const result = {};
  WATCHLIST_GROUP_ORDER.forEach((group) => {
    result[group] = computeMonthlySeries(grouped.get(group), nowValue, monthCount);
  });
  return result;
}

function compareFiniteDesc(aValue, bValue) {
  const a = safeNumber(aValue);
  const b = safeNumber(bValue);
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return b - a;
}

function compareFiniteAsc(aValue, bValue) {
  const a = safeNumber(aValue);
  const b = safeNumber(bValue);
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return a - b;
}

export function computeMicroNeighborhoodBreakout(rows, nowValue, windowDays = 90) {
  const groups = new Map();
  (rows || []).forEach((row) => {
    const pulseGroup = canonicalPulseGroup(row.pulseWatchlistGroup, row.neighborhoodLabel);
    const neighborhoodLabel = String(row.neighborhoodLabel || "").trim();
    if (!pulseGroup || !neighborhoodLabel) return;
    if (!groups.has(pulseGroup)) groups.set(pulseGroup, new Map());
    const neighborhoods = groups.get(pulseGroup);
    if (!neighborhoods.has(neighborhoodLabel)) neighborhoods.set(neighborhoodLabel, []);
    neighborhoods.get(neighborhoodLabel).push(row);
  });

  return WATCHLIST_GROUP_ORDER.map((group) => {
    const neighborhoods = Array.from((groups.get(group) || new Map()).entries())
      .map(([neighborhoodLabel, neighborhoodRows]) => {
        const summary = computeWindowSummary(neighborhoodRows, nowValue, windowDays);
        const latestSaleDate = neighborhoodRows
          .map((row) => toDate(row.saleDate))
          .filter((value) => value instanceof Date && !Number.isNaN(value.getTime()))
          .sort((a, b) => a.getTime() - b.getTime())
          .slice(-1)[0];
        return {
          neighborhoodLabel,
          pulseWatchlistGroup: group,
          latestSaleDate: latestSaleDate ? toIso(latestSaleDate) : "",
          current: summary.current,
          previous: summary.previous,
        };
      })
      .filter((entry) => (entry.current.salesCount || 0) > 0 || (entry.previous.salesCount || 0) > 0)
      .sort((a, b) => (
        compareFiniteDesc(a.current.salesCount, b.current.salesCount)
        || compareFiniteDesc(a.current.hotShare, b.current.hotShare)
        || compareFiniteDesc(a.current.medianSaleToList, b.current.medianSaleToList)
        || compareFiniteAsc(a.current.medianDom, b.current.medianDom)
        || compareFiniteDesc(a.previous.salesCount, b.previous.salesCount)
        || a.neighborhoodLabel.localeCompare(b.neighborhoodLabel)
      ));

    return {
      group,
      neighborhoods,
      totalSalesCount: neighborhoods.reduce((sum, entry) => sum + (entry.current.salesCount || 0), 0),
      windowDays: Number(windowDays || 90),
    };
  }).filter((entry) => entry.neighborhoods.length);
}

export function rollingAverage(values, windowSize = 3) {
  const window = Math.max(1, Number(windowSize || 3));
  return (values || []).map((value, index) => {
    const slice = values
      .slice(Math.max(0, index - window + 1), index + 1)
      .filter((item) => item !== null && item !== undefined && Number.isFinite(item));
    return slice.length ? slice.reduce((sum, item) => sum + item, 0) / slice.length : null;
  });
}

export function competitiveDelta(metricKey, currentValue, previousValue) {
  if (!Number.isFinite(Number(currentValue)) || !Number.isFinite(Number(previousValue))) return null;
  const current = Number(currentValue);
  const previous = Number(previousValue);
  if (metricKey === "medianDom") return previous - current;
  return current - previous;
}

export function metricDirection(metricKey, currentValue, previousValue) {
  const delta = competitiveDelta(metricKey, currentValue, previousValue);
  if (delta === null) return 0;
  if (delta > 0) return 1;
  if (delta < 0) return -1;
  return 0;
}

export { median };
