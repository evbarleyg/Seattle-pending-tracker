// changesSince — pure domain logic for "What changed since you last looked".
//
// captureBaseline(rows, nowIso) takes a snapshot of the listings the buyer has
// already seen; diffSinceBaseline(baseline, rows, nowIso) compares a later
// dataset against that snapshot and returns the four feed lists (new actives,
// price cuts, went-pending-fast, new closed) plus counts.
//
// No DOM, no storage: the integrator persists the baseline (localStorage) and
// passes it back on the next visit. For an honest diff, pass the SAME row
// universe (for example all normalized rows, or closed + open MLS rows) to
// both captureBaseline and diffSinceBaseline; display-level filtering belongs
// to the caller.
import { daysBetween, num, toDate } from "./format.mjs";
import { zillowUrl } from "./data.mjs";

export const CHANGES_BASELINE_VERSION = 1;
// "Fast" = went pending within this many days of listing.
export const FAST_PENDING_DAYS = 10;
// King County recorded sales lag roughly two weeks and coverage backfills
// happen; a row that first appears with an event date older than this many
// days before the baseline is treated as backfill, not news.
export const NEW_ROW_LAG_DAYS = 45;

const DAY_MS = 24 * 60 * 60 * 1000;

// Baseline status codes: "A" active, "P" pending or under contract,
// "C" closed, "O" anything else.
const STATUS_ACTIVE = "A";
const STATUS_PENDING = "P";
const STATUS_CLOSED = "C";
const STATUS_OTHER = "O";

const PENDING_STATUS_RE = /PENDING|CONTINGENT|UNDER CONTRACT/;

function parcelDigits(row) {
  const raw = String(row.parcelNbr || `${row.major || ""}${row.minor || ""}`);
  return raw.replace(/[^0-9]/g, "");
}

function isClosedRow(row) {
  if (typeof row.hasActualClose === "boolean") return row.hasActualClose;
  return num(row.closePrice) > 0 && !!String(row.saleDate || "").trim();
}

function statusText(row) {
  return String(row.mlsStatusNorm || row.mlsStatusLabel || row.mlsStatus || "").toUpperCase().trim();
}

// County PUBLIC_PROXY rows fall back to assessed value in pendingListPrice,
// so a list price only counts when the row carries a real market list price.
function hasRealListPrice(row) {
  if (typeof row.hasMarketListPrice === "boolean") return row.hasMarketListPrice;
  return num(row.mlsListingPrice) > 0
    || num(row.mlsListPriceAtPending) > 0
    || num(row.saleToListRatio) > 0
    || num(row.bidUpAmount) !== 0;
}

function statusCode(row) {
  if (isClosedRow(row)) return STATUS_CLOSED;
  const status = statusText(row);
  if (PENDING_STATUS_RE.test(status)) return STATUS_PENDING;
  if (status === "ACTIVE") return STATUS_ACTIVE;
  return STATUS_OTHER;
}

// Price the buyer would quote for the row in its current state: close price
// once sold, otherwise the real list price (0 when only an assessed-value
// fallback exists).
function rowPrice(row, code) {
  if (code === STATUS_CLOSED) return num(row.closePrice);
  return hasRealListPrice(row) ? num(row.pendingListPrice || row.listPriceAtPending) : 0;
}

// Stable identity across pipeline rebuilds and status transitions.
// - MLS rows with a parcel: APN + list date. Both survive rebuilds and stay
//   fixed while a listing moves active -> pending -> sold (the synthetic
//   "mls-open-..."/"mls-only-..." ids embed a row index that does not).
// - Everything else (county sale records, Redfin actives): the dataset id is
//   stable; parcel + sale date disambiguate multi-parcel county records that
//   reuse one excise id.
export function changeKey(row) {
  if (!row || typeof row !== "object") return "";
  const parcel = parcelDigits(row);
  if (String(row.dataMode || "") === "MLS_ENRICHED" && parcel) {
    const anchor = String(row.listDate || row.pendingDate || row.saleDate || "").trim();
    return `apn:${parcel}|${anchor}`;
  }
  const id = String(row.id || "").trim();
  if (id) return `id:${id}|${parcel}|${String(row.saleDate || "").trim()}`;
  const address = String(row.address || "").trim().toUpperCase();
  if (address) return `addr:${address}|${String(row.listDate || row.saleDate || "").trim()}`;
  return "";
}

// Rank used when two rows collapse to one key (for example the county record
// and the MLS record of the same sale): keep the most advanced status.
function statusRank(code) {
  if (code === STATUS_CLOSED) return 3;
  if (code === STATUS_PENDING) return 2;
  if (code === STATUS_ACTIVE) return 1;
  return 0;
}

// Deduplicate rows by key, keeping the most advanced status per key.
function uniqueRowsByKey(rows) {
  const list = Array.isArray(rows) ? rows : [];
  const byKey = new Map();
  for (const row of list) {
    if (!row || typeof row !== "object") continue;
    const key = changeKey(row);
    if (!key) continue;
    const code = statusCode(row);
    const existing = byKey.get(key);
    if (!existing || statusRank(code) > statusRank(existing.code)) {
      byKey.set(key, { row, code });
    }
  }
  return byKey;
}

function parseWhen(value, fallbackMs) {
  const parsed = toDate(value);
  if (parsed) return parsed.getTime();
  return fallbackMs;
}

// Snapshot of the rows the buyer has seen. Kept deliberately small so
// thousands of rows fit in localStorage: one entry per row with a status code
// and (for open listings) the current real list price. All date comparisons
// on the next visit anchor on capturedAt, so per-row dates are not stored.
export function captureBaseline(rows, nowIso) {
  const capturedMs = parseWhen(nowIso, Date.now());
  const entries = {};
  for (const { row, code } of uniqueRowsByKey(rows).values()) {
    const entry = { s: code };
    const price = rowPrice(row, code);
    if (code !== STATUS_CLOSED && price > 0) entry.p = price;
    entries[changeKey(row)] = entry;
  }
  return {
    v: CHANGES_BASELINE_VERSION,
    capturedAt: new Date(capturedMs).toISOString(),
    rows: entries,
  };
}

// A baseline is usable when it has the expected version, a parseable capture
// time, and a plain-object row map. Anything else (missing, corrupted JSON
// shape, future schema) is treated as a first visit.
export function isValidBaseline(baseline) {
  if (!baseline || typeof baseline !== "object" || Array.isArray(baseline)) return false;
  if (baseline.v !== CHANGES_BASELINE_VERSION) return false;
  if (!toDate(baseline.capturedAt)) return false;
  if (!baseline.rows || typeof baseline.rows !== "object" || Array.isArray(baseline.rows)) return false;
  return true;
}

function feedRow(row, key, code) {
  return {
    key,
    id: String(row.id || ""),
    address: String(row.address || "").trim(),
    neighborhood: String(row.neighborhoodLabel || row.neighborhood || "").trim(),
    price: rowPrice(row, code),
    status: String(row.mlsStatusLabel || row.mlsStatusNorm || row.mlsStatus || "").trim(),
    listDate: String(row.listDate || ""),
    pendingDate: String(row.pendingDate || ""),
    saleDate: String(row.saleDate || ""),
    url: row.address ? zillowUrl(row) : "",
  };
}

function daysListToPending(row) {
  if (Number.isFinite(row.daysToPending)) return row.daysToPending;
  const days = daysBetween(row.listDate, row.pendingDate);
  return Number.isFinite(days) ? days : null;
}

function byDateDesc(field) {
  return (a, b) => {
    const cmp = String(b[field] || "").localeCompare(String(a[field] || ""));
    if (cmp !== 0) return cmp;
    return a.key.localeCompare(b.key);
  };
}

function emptyDiff(isFirstVisit, baselineAt, minutesSinceBaseline) {
  return {
    isFirstVisit,
    baselineAt,
    minutesSinceBaseline,
    newActives: [],
    priceCuts: [],
    wentPendingFast: [],
    newClosed: [],
    totals: { newActives: 0, priceCuts: 0, wentPendingFast: 0, newClosed: 0 },
  };
}

// Compare the current rows against a stored baseline. Never throws: a
// missing or malformed baseline reads as a first visit. Rows that were in the
// baseline but are gone now are ignored (sold outside the slice, delisted,
// or dropped by the pipeline).
export function diffSinceBaseline(baseline, rows, nowIso) {
  if (!isValidBaseline(baseline)) return emptyDiff(true, null, null);

  const baselineMs = toDate(baseline.capturedAt).getTime();
  const baselineAt = new Date(baselineMs).toISOString();
  const nowMs = parseWhen(nowIso, Date.now());
  const minutesSinceBaseline = Math.max(0, Math.round((nowMs - baselineMs) / 60000));
  const lagCutoffMs = baselineMs - NEW_ROW_LAG_DAYS * DAY_MS;
  const knownRows = baseline.rows;

  const result = emptyDiff(false, baselineAt, minutesSinceBaseline);

  for (const [key, { row, code }] of uniqueRowsByKey(rows).entries()) {
    const entryRaw = Object.prototype.hasOwnProperty.call(knownRows, key) ? knownRows[key] : null;
    const entry = entryRaw && typeof entryRaw === "object" ? entryRaw : null;
    const known = entry !== null;

    if (code === STATUS_CLOSED) {
      // Newly closed: either we watched it as an open listing, or it is a
      // fresh row whose sale date is recent enough to be news rather than a
      // backfilled old record (county recording lags ~2 weeks).
      const wasOpenInBaseline = known && entry.s !== STATUS_CLOSED;
      const saleMs = parseWhen(row.saleDate, null);
      const recentEnough = saleMs !== null && saleMs >= lagCutoffMs;
      if (wasOpenInBaseline || (!known && recentEnough)) {
        result.newClosed.push(feedRow(row, key, code));
      }
      continue;
    }

    if (code === STATUS_PENDING) {
      // Went pending fast: the contract date landed after the last visit and
      // within FAST_PENDING_DAYS of listing. Uses the row's own pending date;
      // active-row artifact pending dates never reach here because the status
      // gate above requires a pending-family status.
      const pendingMs = parseWhen(row.pendingDate, null);
      if (pendingMs === null || pendingMs < baselineMs) continue;
      const days = daysListToPending(row);
      if (days === null || days < 0 || days > FAST_PENDING_DAYS) continue;
      result.wentPendingFast.push(feedRow(row, key, code));
      continue;
    }

    if (code === STATUS_ACTIVE) {
      if (known) {
        // Price cut: we saw it as an open listing with a real list price and
        // that price dropped. Assessed-value fallbacks never count because
        // baseline prices are only stored for real list prices.
        const oldPrice = num(entry.p);
        const newPrice = hasRealListPrice(row) ? num(row.pendingListPrice || row.listPriceAtPending) : 0;
        const wasOpen = entry.s === STATUS_ACTIVE || entry.s === STATUS_PENDING;
        if (wasOpen && oldPrice > 0 && newPrice > 0 && newPrice < oldPrice) {
          const cutAmount = oldPrice - newPrice;
          result.priceCuts.push({
            ...feedRow(row, key, code),
            oldPrice,
            newPrice,
            cutAmount,
            cutPct: cutAmount / oldPrice,
          });
        }
        continue;
      }
      // New active: not in the baseline, and not an old listing that only
      // just entered dataset coverage.
      const listMs = parseWhen(row.listDate, null);
      const recentEnough = listMs === null || listMs >= lagCutoffMs;
      if (recentEnough) result.newActives.push(feedRow(row, key, code));
    }
  }

  result.newActives.sort(byDateDesc("listDate"));
  result.wentPendingFast.sort(byDateDesc("pendingDate"));
  result.newClosed.sort(byDateDesc("saleDate"));
  result.priceCuts.sort((a, b) => (b.cutAmount - a.cutAmount) || a.key.localeCompare(b.key));

  result.totals = {
    newActives: result.newActives.length,
    priceCuts: result.priceCuts.length,
    wentPendingFast: result.wentPendingFast.length,
    newClosed: result.newClosed.length,
  };
  return result;
}
