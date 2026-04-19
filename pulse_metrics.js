"use strict";

(function initPulseMetrics(rootFactory) {
  const api = rootFactory();
  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
  if (typeof window !== "undefined") {
    window.PulseMetrics = api;
  }
})(function buildPulseMetrics() {
  const DAY_MS = 24 * 60 * 60 * 1000;
  const PRIMARY_GROUP_ID = "primary";
  const WATCHLIST_GROUP_ORDER = [
    "Ballard",
    "Fremont / Green Lake / Woodland Park",
    "Queen Anne",
    "Magnolia",
  ];
  const PRIMARY_WATCHLIST_GROUPS = [
    "Ballard",
    "Fremont / Green Lake / Woodland Park",
    "Queen Anne",
  ];
  const PULSE_GROUPS = [
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

  function pulseWatchlistGroup(neighborhoodLabel) {
    const value = String(neighborhoodLabel || "").trim();
    if (!value) return "";
    if (value === "Ballard" || value === "Ballard / Crown Hill") return "Ballard";
    if (value === "Fremont / Green Lake / Wallingford") return "Fremont / Green Lake / Woodland Park";
    if (value === "Queen Anne / Magnolia" || value === "South Lake Union / Queen Anne") return "Queen Anne";
    if (value === "Magnolia") return "Magnolia";
    return "";
  }

  function normalizeSelectionId(selectionId) {
    const wanted = String(selectionId || PRIMARY_GROUP_ID).trim();
    return PULSE_GROUPS.some((group) => group.id === wanted) ? wanted : PRIMARY_GROUP_ID;
  }

  function groupsForSelection(selectionId) {
    const normalized = normalizeSelectionId(selectionId);
    const match = PULSE_GROUPS.find((group) => group.id === normalized);
    return match ? match.groups.slice() : PRIMARY_WATCHLIST_GROUPS.slice();
  }

  function toDate(value) {
    if (!value) return null;
    const raw = String(value).trim();
    if (!raw) return null;
    const iso = /^\d{4}-\d{2}-\d{2}$/;
    const parsed = iso.test(raw) ? new Date(`${raw}T00:00:00`) : new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function toIsoDate(value) {
    const parsed = value instanceof Date ? value : toDate(value);
    if (!parsed) return "";
    return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}-${String(parsed.getDate()).padStart(2, "0")}`;
  }

  function median(values) {
    if (!values.length) return null;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function startOfDay(value) {
    const parsed = value instanceof Date ? new Date(value.getTime()) : toDate(value);
    if (!parsed) return null;
    parsed.setHours(0, 0, 0, 0);
    return parsed;
  }

  function startOfMonth(value) {
    const parsed = value instanceof Date ? new Date(value.getTime()) : toDate(value);
    if (!parsed) return null;
    return new Date(parsed.getFullYear(), parsed.getMonth(), 1);
  }

  function addDays(value, days) {
    const parsed = startOfDay(value);
    if (!parsed) return null;
    parsed.setDate(parsed.getDate() + Number(days || 0));
    return parsed;
  }

  function addMonths(value, months) {
    const parsed = startOfMonth(value);
    if (!parsed) return null;
    return new Date(parsed.getFullYear(), parsed.getMonth() + Number(months || 0), 1);
  }

  function monthKey(value) {
    const parsed = value instanceof Date ? value : toDate(value);
    if (!parsed) return "";
    return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}`;
  }

  function buildMonthKeys(nowValue, monthCount = 12) {
    const anchor = startOfMonth(nowValue || new Date());
    if (!anchor) return [];
    const count = Math.max(1, Number(monthCount || 12));
    const out = [];
    for (let idx = count - 1; idx >= 0; idx -= 1) {
      out.push(monthKey(addMonths(anchor, -idx)));
    }
    return out;
  }

  function inDateRange(dateValue, startInclusive, endExclusive) {
    const parsed = startOfDay(dateValue);
    if (!parsed || !startInclusive || !endExclusive) return false;
    const time = parsed.getTime();
    return time >= startInclusive.getTime() && time < endExclusive.getTime();
  }

  function safeNumber(value) {
    return Number.isFinite(Number(value)) ? Number(value) : null;
  }

  function summarizeRows(rows) {
    const data = Array.isArray(rows) ? rows : [];
    const count = data.length;
    const domValues = data.map((row) => safeNumber(row.domValue)).filter((value) => value !== null && value >= 0);
    const saleToListValues = data.map((row) => safeNumber(row.saleToList)).filter((value) => value !== null && value > 0);
    const bidUpValues = data.map((row) => safeNumber(row.delta)).filter((value) => value !== null);
    const closeValues = data.map((row) => safeNumber(row.closePrice)).filter((value) => value !== null && value > 0);

    return {
      salesCount: count,
      hotShare: count ? data.filter((row) => !!row.isHotMarket).length / count : null,
      ultraHotShare: count ? data.filter((row) => !!row.isUltraHot).length / count : null,
      medianDom: median(domValues),
      medianSaleToList: median(saleToListValues),
      medianBidUp: median(bidUpValues),
      medianClosePrice: median(closeValues),
      sampleSize: count,
      domSampleSize: domValues.length,
      ratioSampleSize: saleToListValues.length,
      bidUpSampleSize: bidUpValues.length,
      closeSampleSize: closeValues.length,
    };
  }

  function computeWindowSummary(rows, nowValue, windowDays) {
    const anchor = startOfDay(nowValue || new Date());
    if (!anchor) {
      return { current: summarizeRows([]), previous: summarizeRows([]) };
    }
    const days = Math.max(1, Number(windowDays || 30));
    const endExclusive = addDays(anchor, 1);
    const currentStart = addDays(anchor, -(days - 1));
    const previousEndExclusive = currentStart;
    const previousStart = addDays(previousEndExclusive, -days);

    const currentRows = rows.filter((row) => inDateRange(row.saleDate, currentStart, endExclusive));
    const previousRows = rows.filter((row) => inDateRange(row.saleDate, previousStart, previousEndExclusive));
    return {
      current: summarizeRows(currentRows),
      previous: summarizeRows(previousRows),
    };
  }

  function computeRecentComparisons(rows, nowValue, windowDaysList) {
    return (windowDaysList || []).map((windowDays) => ({
      windowDays: Number(windowDays || 0),
      ...computeWindowSummary(rows, nowValue, windowDays),
    }));
  }

  function computeMonthlySeries(rows, nowValue, monthCount = 12) {
    const keys = buildMonthKeys(nowValue, monthCount);
    return keys.map((key) => {
      const monthRows = rows.filter((row) => monthKey(row.saleDate) === key);
      return {
        month: key,
        ...summarizeRows(monthRows),
      };
    });
  }

  function computeMonthlyGroupSeries(rows, nowValue, monthCount = 12) {
    const grouped = new Map();
    WATCHLIST_GROUP_ORDER.forEach((group) => grouped.set(group, []));
    (rows || []).forEach((row) => {
      const group = pulseWatchlistGroup(row.pulseWatchlistGroup || row.neighborhoodLabel || "");
      if (!grouped.has(group)) return;
      grouped.get(group).push(row);
    });
    const result = {};
    WATCHLIST_GROUP_ORDER.forEach((group) => {
      result[group] = computeMonthlySeries(grouped.get(group), nowValue, monthCount);
    });
    return result;
  }

  function rollingAverage(values, windowSize = 3) {
    const window = Math.max(1, Number(windowSize || 3));
    return values.map((value, index) => {
      const slice = values
        .slice(Math.max(0, index - window + 1), index + 1)
        .filter((item) => item !== null && item !== undefined && Number.isFinite(item));
      return slice.length ? slice.reduce((sum, item) => sum + item, 0) / slice.length : null;
    });
  }

  function competitiveDelta(metricKey, currentValue, previousValue) {
    if (!Number.isFinite(Number(currentValue)) || !Number.isFinite(Number(previousValue))) return null;
    const current = Number(currentValue);
    const previous = Number(previousValue);
    if (metricKey === "medianDom") return previous - current;
    return current - previous;
  }

  function metricDirection(metricKey, currentValue, previousValue) {
    const delta = competitiveDelta(metricKey, currentValue, previousValue);
    if (delta === null) return 0;
    if (delta > 0) return 1;
    if (delta < 0) return -1;
    return 0;
  }

  return {
    PRIMARY_GROUP_ID,
    PRIMARY_WATCHLIST_GROUPS,
    PULSE_GROUPS,
    WATCHLIST_GROUP_ORDER,
    addDays,
    addMonths,
    buildMonthKeys,
    competitiveDelta,
    computeMonthlyGroupSeries,
    computeMonthlySeries,
    computeRecentComparisons,
    computeWindowSummary,
    groupsForSelection,
    median,
    metricDirection,
    monthKey,
    normalizeSelectionId,
    pulseWatchlistGroup,
    rollingAverage,
    summarizeRows,
    toDate,
    toIsoDate,
  };
});
