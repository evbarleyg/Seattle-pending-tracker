// Price cuts on homes that are for sale right now.
//
// The pipeline keeps a ledger of every listing the daily Redfin pull has seen
// (public/listing_ledger.csv): the first asking price it was seen at and the
// latest. A listing whose latest ask is below its first has cut its price. That
// is a softness signal a buyer can act on, and unlike the "what changed since
// you last looked" feed it needs no saved baseline, so it works on a first visit.
//
// The ledger joins to the dataset on MLS number (ledger `mlsNumber`, dataset
// `mlsListingNumber`). Only listings the ledger tracks are counted, so a
// listing it has never seen is "unknown", not "no cut".
import { formatMoneyCompact, formatPct, median, safeNumber } from "./format.mjs";
import { parseCsvLine } from "./data.mjs";

const NUMERIC_COLUMNS = ["firstAsk", "lastAsk", "lastDom"];

// Minimal reader for the ledger file: header row, quoted fields, blank lines
// skipped. Numeric columns become numbers, or null when blank (a blank ask is
// unknown, not zero). Returns [] for anything that is not a ledger.
export function parseLedgerCsv(text) {
  const lines = String(text || "").split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  if (!headers.includes("mlsNumber")) return [];
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      const value = cols[index] ?? "";
      row[header] = NUMERIC_COLUMNS.includes(header) ? safeNumber(value) : value;
    });
    return row;
  }).filter((row) => row.mlsNumber);
}

export function buildLedgerIndex(rows) {
  const index = new Map();
  for (const row of rows || []) {
    if (row?.mlsNumber) index.set(String(row.mlsNumber), row);
  }
  return index;
}

function ledgerEntryFor(listing, index) {
  const key = String(listing?.mlsListingNumber || "").trim();
  if (!key || !index || typeof index.get !== "function") return null;
  return index.get(key) || null;
}

// The cut on one listing, or null when there is none to report: not tracked, no
// first ask on record, or the latest ask is not below the first (a raise or no
// change). cutPct is a fraction of the first ask.
export function priceCutFor(listing, index) {
  const entry = ledgerEntryFor(listing, index);
  if (!entry) return null;
  const { firstAsk, lastAsk } = entry;
  if (!(firstAsk > 0) || !(lastAsk > 0) || !(lastAsk < firstAsk)) return null;
  const cutAmount = firstAsk - lastAsk;
  if (cutAmount < 1) return null;
  return {
    firstAsk,
    lastAsk,
    cutAmount,
    cutPct: cutAmount / firstAsk,
    daysListed: entry.lastDom,
    listDate: entry.listDate || "",
  };
}

const BIGGEST_CUTS_SHOWN = 6;

// Across a set of listings (the active listings in the buyer's filters): how
// many the ledger tracks, how many of those have cut, the typical cut, how long
// cut and uncut listings have sat, and the largest cuts by percent.
export function summarizePriceCuts(listings, index) {
  const tracked = [];
  for (const row of listings || []) {
    const entry = ledgerEntryFor(row, index);
    if (entry) tracked.push({ row, entry, cut: priceCutFor(row, index) });
  }
  const cuts = tracked.filter((item) => item.cut);
  const uncut = tracked.filter((item) => !item.cut);
  const days = (items) => items.map((item) => item.entry.lastDom).filter((value) => value !== null && value >= 0);
  const medianOrNull = (values) => (values.length ? median(values) : null);
  return {
    trackedCount: tracked.length,
    cutCount: cuts.length,
    cutShare: tracked.length ? cuts.length / tracked.length : null,
    medianCutPct: medianOrNull(cuts.map((item) => item.cut.cutPct)),
    medianCutAmount: medianOrNull(cuts.map((item) => item.cut.cutAmount)),
    medianDaysListedCut: medianOrNull(days(cuts)),
    medianDaysListedUncut: medianOrNull(days(uncut)),
    biggest: cuts
      .slice()
      .sort((a, b) => (b.cut.cutPct - a.cut.cutPct) || (b.cut.cutAmount - a.cut.cutAmount))
      .slice(0, BIGGEST_CUTS_SHOWN)
      .map(({ row, cut }) => ({ row, cut })),
  };
}

// One plain sentence answering "are sellers cutting prices?". Empty when no
// listing in the filters is tracked, so the caller can leave the block out.
export function buildPriceCutVerdict(summary) {
  if (!summary || !summary.trackedCount) return "";
  const { trackedCount, cutCount, cutShare, medianCutPct, medianCutAmount } = summary;
  if (!cutCount) {
    return `None of the ${trackedCount} homes for sale in your filters has cut its price since it was first seen.`;
  }
  const typical = `by a typical ${formatPct(medianCutPct)} (${formatMoneyCompact(medianCutAmount, 0)})`;
  let lead;
  if (cutShare >= 0.5) lead = "More than half of the homes for sale in your filters have already cut their price";
  else if (cutShare >= 0.4) lead = "Nearly half of the homes for sale in your filters have already cut their price";
  else if (cutShare >= 0.28) lead = "About a third of the homes for sale in your filters have already cut their price";
  else if (cutShare >= 0.18) lead = "About one in five homes for sale in your filters has already cut its price";
  else lead = `Few sellers are cutting: ${cutCount} of the ${trackedCount} homes for sale in your filters have cut their price`;
  return `${lead}, ${typical}.`;
}
