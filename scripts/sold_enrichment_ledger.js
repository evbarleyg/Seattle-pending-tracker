#!/usr/bin/env node
"use strict";

// Maintains redfin_sold_enrichment_ledger.csv: for every closed sale we have
// EVER known a genuine list price for, the best-known list-at-pending, list
// date, pending date, DOM and close, keyed by the sale (MLS# + sale date, with
// address + zip + sale date as the fallback key). Tracked in git, so the
// enrichment survives any rebuild, re-merge or short re-fetch.
//
// Why: on 2026-07-05 a county rebuild dropped ~2,800 Redfin-sold rows and the
// ~2,300 list prices the June history scrape had attached to them. The rows
// came back bare from later sold fetches; the list prices did not, because the
// only copies lived in old commits and in a gitignored cache. This ledger is
// the durable home for that knowledge.
//
// Sources (seed once, rerun any time; idempotent):
//   --seed-from-git     every daily snapshot of the enriched CSV on --ref: any
//                       closed row with a genuine MLS list price contributes;
//                       the richest version wins (has pending date > has list
//                       date > later snapshot).
//   --seed-from-cache   the Redfin history cache (redfin_history_cache.json)
//                       joined to the cumulative sold file by URL: a cached
//                       summary whose sold date matches the sale contributes.
//
// Apply (default): fill REDFIN_SOLD rows that still have no list price from
// the ledger, guarded by sale date within 14 days and close price within 1%.
// mlsJoinMethod stays REDFIN_SOLD (so merge:sold reruns still strip/replace);
// listPriceSource records the lineage (REDFIN_HISTORY / MLS_EXPORT / ...).

const fs = require("fs");
const path = require("path");
const { readCsvText, writeCsv, num } = require("./csv_util.js");
const { readSnapshotsFromGit } = require("./backfill_list_from_active_snapshots.js");
const { streetKeyFromAddress, zip5 } = require("./redfin_address_key.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_LEDGER = path.join(PROJECT_DIR, "redfin_sold_enrichment_ledger.csv");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_ENRICHED_REL = "public_sales_proxy_mls_enriched_last12mo.csv";
const DEFAULT_CACHE = path.join(PROJECT_DIR, "redfin_history_cache.json");
const DEFAULT_SOLD = path.join(PROJECT_DIR, "redfin_sold_cumulative.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_sold_enrichment_report.json");
const DEFAULT_REF = "main";
const DEFAULT_SINCE = "2026-01-01";
const SALE_DATE_MATCH_DAYS = 14;
const CLOSE_MATCH_TOLERANCE = 0.01;
const LIST_PRICE_SOURCE_COLUMN = "listPriceSource";

const LEDGER_COLUMNS = [
  "mlsNumber", "address", "zip", "saleDate", "closePrice",
  "listPriceAtPending", "originalListPrice", "listDate", "pendingDate", "dom", "cdom",
  "source", "sourceDate",
];

// Join-method -> lineage label for rows harvested from snapshots.
function sourceFromJoinMethod(joinMethod, listPriceSource) {
  if (listPriceSource) return listPriceSource;
  const j = String(joinMethod || "");
  if (j.startsWith("REDFIN_HISTORY")) return "REDFIN_HISTORY";
  if (j === "REDFIN_SOLD" || j === "REDFIN_ACTIVE") return "";
  return "MLS_EXPORT"; // county/MLS joins carry the realtor export's list price
}

function daysBetween(a, b) {
  const ta = Date.parse(a); const tb = Date.parse(b);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.round((tb - ta) / 86400000);
}

function addressKey(address, zip) {
  const street = streetKeyFromAddress(address || "");
  const z = zip5(zip || "");
  return street && z ? `${street}|${z}` : "";
}

function mlsKey(mls, saleDate) {
  const m = String(mls || "").trim();
  return m && saleDate ? `m:${m}|${saleDate}` : "";
}

// Richer entries win: pending date, then list date, then the later source date.
function richness(e) {
  return (e.pendingDate ? 4 : 0) + (e.listDate ? 2 : 0) + (e.dom !== "" ? 1 : 0);
}

const VALUE_FIELDS = ["listPriceAtPending", "originalListPrice", "listDate", "pendingDate", "dom", "cdom", "source"];

function better(candidate, incumbent) {
  if (!incumbent) return true;
  const rc = richness(candidate); const ri = richness(incumbent);
  if (rc !== ri) return rc > ri;
  // Same richness and same values: keep the incumbent (and its earlier
  // sourceDate) so replaying snapshots does not churn the ledger.
  if (VALUE_FIELDS.every((k) => String(candidate[k] ?? "") === String(incumbent[k] ?? ""))) return false;
  return String(candidate.sourceDate || "") >= String(incumbent.sourceDate || "");
}

function ledgerEntryFromRow(row, sourceDate) {
  // Only the MLS-derived list fields count. The generic listPriceAtPending
  // column holds the county ASSESSED value on PUBLIC_PROXY rows, which is not
  // an asking price and must never enter the ledger.
  if (String(row.dataMode || "") === "PUBLIC_PROXY") return null;
  const list = num(row.mlsListingPrice) || num(row.mlsListPriceAtPending);
  const close = num(row.closePrice);
  if (!(list > 0) || !(close > 0) || !row.saleDate) return null;
  const source = sourceFromJoinMethod(row.mlsJoinMethod, row[LIST_PRICE_SOURCE_COLUMN]);
  if (!source) return null; // bare sold-feed / actives rows carry no genuine list price
  return {
    mlsNumber: String(row.mlsListingNumber || "").trim(),
    address: String(row.address || "").trim(),
    zip: zip5(row.zip || ""),
    saleDate: String(row.saleDate).trim(),
    closePrice: String(close),
    listPriceAtPending: String(list),
    originalListPrice: String(num(row.mlsOriginalPrice) || list),
    listDate: String(row.mlsListDate || row.listDate || "").trim(),
    pendingDate: String(row.mlsPendingDate || row.pendingDate || "").trim(),
    dom: String(row.mlsDOM ?? "").trim(),
    cdom: String(row.mlsCDOM ?? "").trim(),
    source,
    sourceDate: sourceDate || "",
  };
}

// Ledger index: both keys point at the same entry object.
class Ledger {
  constructor(rows = []) {
    this.byMls = new Map();
    this.byAddr = new Map();
    this.entries = new Map(); // primary key -> entry
    for (const r of rows) this.upsert({ ...r });
  }
  static primaryKey(e) {
    return mlsKey(e.mlsNumber, e.saleDate) || `a:${addressKey(e.address, e.zip)}|${e.saleDate}`;
  }
  upsert(entry) {
    const pk = Ledger.primaryKey(entry);
    if (!pk || pk.endsWith("|" + entry.saleDate) === false) return false;
    if (pk.startsWith("a:|")) return false; // no usable key at all
    const incumbent = this.entries.get(pk);
    if (!better(entry, incumbent)) return false;
    this.entries.set(pk, entry);
    const mk = mlsKey(entry.mlsNumber, entry.saleDate);
    if (mk) this.byMls.set(mk, entry);
    const ak = addressKey(entry.address, entry.zip);
    if (ak) {
      const list = this.byAddr.get(ak) || [];
      const idx = list.findIndex((e) => e.saleDate === entry.saleDate);
      if (idx >= 0) list[idx] = entry; else list.push(entry);
      this.byAddr.set(ak, list);
    }
    return true;
  }
  // Best entry for a sold row: exact MLS# + sale date, else same MLS# within
  // the date window, else same address within the window.
  find(row) {
    const mls = String(row.mlsListingNumber || "").trim();
    const sale = String(row.saleDate || "").trim();
    if (!sale) return null;
    if (mls) {
      const exact = this.byMls.get(mlsKey(mls, sale));
      if (exact) return exact;
      for (const [k, e] of this.byMls) {
        if (k.startsWith(`m:${mls}|`) && Math.abs(daysBetween(e.saleDate, sale)) <= SALE_DATE_MATCH_DAYS) return e;
      }
    }
    const ak = addressKey(row.address, row.zip);
    if (!ak) return null;
    const list = this.byAddr.get(ak) || [];
    let best = null;
    for (const e of list) {
      const gap = Math.abs(daysBetween(e.saleDate, sale));
      if (gap <= SALE_DATE_MATCH_DAYS && (!best || gap < Math.abs(daysBetween(best.saleDate, sale)))) best = e;
    }
    return best;
  }
  rows() {
    return [...this.entries.values()].sort((a, b) => {
      if (a.saleDate !== b.saleDate) return a.saleDate < b.saleDate ? 1 : -1; // newest sale first
      return Ledger.primaryKey(a) < Ledger.primaryKey(b) ? -1 : 1;
    });
  }
}

function seedFromSnapshotText(ledger, text, sourceDate) {
  const { rows } = readCsvText(text);
  let contributed = 0;
  for (const r of rows) {
    const e = ledgerEntryFromRow(r, sourceDate);
    if (e && ledger.upsert(e)) contributed += 1;
  }
  return contributed;
}

// Cache summaries -> ledger entries, via the cumulative sold file's URLs.
function seedFromCache(ledger, cacheEntries, soldRows) {
  const byUrl = new Map();
  for (const r of soldRows) if (r.redfinUrl) byUrl.set(r.redfinUrl, r);
  let contributed = 0, skipped = 0;
  for (const [url, e] of Object.entries(cacheEntries || {})) {
    const s = e && e.summary;
    if (!s || !(s.listPriceAtPending > 0) || !s.soldDate || !(s.soldPrice > 0)) continue;
    const sold = byUrl.get(url);
    if (!sold) { skipped += 1; continue; }
    if (Math.abs(daysBetween(s.soldDate, sold.soldDate)) > SALE_DATE_MATCH_DAYS) { skipped += 1; continue; }
    if (Math.abs(num(sold.soldPrice) / s.soldPrice - 1) > CLOSE_MATCH_TOLERANCE) { skipped += 1; continue; }
    const entry = {
      mlsNumber: String(sold.mlsListingNumber || s.mlsNumber || "").trim(),
      address: String(sold.address || "").trim(),
      zip: zip5(sold.zip || ""),
      saleDate: String(sold.soldDate).trim(),
      closePrice: String(num(sold.soldPrice)),
      listPriceAtPending: String(s.listPriceAtPending),
      originalListPrice: String(s.originalListPrice || s.listPriceAtPending),
      listDate: String(s.listDate || "").trim(),
      pendingDate: String(s.pendingDate || "").trim(),
      dom: s.listDate && s.pendingDate ? String(Math.max(0, daysBetween(s.listDate, s.pendingDate))) : "",
      cdom: s.listDate && s.pendingDate ? String(Math.max(0, daysBetween(s.listDate, s.pendingDate))) : "",
      source: "REDFIN_HISTORY",
      sourceDate: String(e.fetchedAt || "").slice(0, 10),
    };
    if (ledger.upsert(entry)) contributed += 1;
  }
  return { contributed, skipped };
}

function rejectReason(row, e) {
  if (row.mlsJoinMethod !== "REDFIN_SOLD") return "not_redfin_sold";
  if (num(row.mlsListingPrice) > 0 || num(row.listPriceAtPending) > 0) return "already_has_list";
  if (!e) return "no_entry";
  const close = num(row.closePrice);
  if (!(close > 0)) return "no_close_price";
  if (Math.abs(close / num(e.closePrice) - 1) > CLOSE_MATCH_TOLERANCE) return "close_mismatch";
  if (e.pendingDate && daysBetween(e.pendingDate, row.saleDate) < 0) return "pending_after_sale";
  return null;
}

function applyEntryToRow(row, e) {
  if (rejectReason(row, e)) return false;
  const list = num(e.listPriceAtPending);
  const close = num(row.closePrice);
  const orig = num(e.originalListPrice) || list;
  row.listPriceAtPending = String(list);
  row.mlsListPriceAtPending = String(list);
  row.mlsListingPrice = String(list);
  row.mlsOriginalPrice = String(orig);
  if (e.listDate) { row.listDate = e.listDate; row.mlsListDate = e.listDate; }
  if (e.pendingDate) { row.pendingDate = e.pendingDate; row.mlsPendingDate = e.pendingDate; row.mlsContractualDate = row.mlsContractualDate || e.pendingDate; }
  if (e.dom !== "") row.mlsDOM = e.dom;
  if (e.cdom !== "") row.mlsCDOM = e.cdom;
  row.bidUpAmount = String(Math.round(close - list));
  row.bidUpPct = String((close - list) / list);
  row.saleToListRatio = String(close / list);
  row.saleToOriginalListRatio = String(close / orig);
  if (e.listDate && e.pendingDate) { const d = daysBetween(e.listDate, e.pendingDate); if (d !== null && d >= 0) row.mlsDaysToPending = String(d); }
  if (e.pendingDate) { const d = daysBetween(e.pendingDate, row.saleDate); if (d !== null && d >= 0) row.mlsDaysPendingToSale = String(d); }
  row[LIST_PRICE_SOURCE_COLUMN] = e.source || "REDFIN_HISTORY";
  return true;
}

function applyLedger(rows, ledger) {
  const report = { candidates: 0, applied: 0, skipped: {}, byMonth: {}, bySource: {} };
  for (const row of rows) {
    if (row.mlsJoinMethod !== "REDFIN_SOLD") continue;
    report.candidates += 1;
    const month = String(row.saleDate || "").slice(0, 7) || "unknown";
    const bucket = report.byMonth[month] || (report.byMonth[month] = { candidates: 0, applied: 0 });
    bucket.candidates += 1;
    const e = ledger.find(row);
    const reason = rejectReason(row, e);
    if (reason) { report.skipped[reason] = (report.skipped[reason] || 0) + 1; continue; }
    applyEntryToRow(row, e);
    report.applied += 1;
    bucket.applied += 1;
    report.bySource[e.source] = (report.bySource[e.source] || 0) + 1;
  }
  return report;
}

function parseArgs(argv) {
  const opts = {
    ledger: DEFAULT_LEDGER, enriched: DEFAULT_ENRICHED, cache: DEFAULT_CACHE, sold: DEFAULT_SOLD, report: DEFAULT_REPORT,
    seedFromGit: false, seedFromCache: false, ref: DEFAULT_REF, since: DEFAULT_SINCE, noApply: false, dryRun: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i]; const next = argv[i + 1];
    if (a === "--ledger") { opts.ledger = next; i += 1; }
    else if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--cache") { opts.cache = next; i += 1; }
    else if (a === "--sold") { opts.sold = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--seed-from-git") { opts.seedFromGit = true; }
    else if (a === "--seed-from-cache") { opts.seedFromCache = true; }
    else if (a === "--ref") { opts.ref = next; i += 1; }
    else if (a === "--since") { opts.since = next; i += 1; }
    else if (a === "--no-apply") { opts.noApply = true; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/sold_enrichment_ledger.js [options]",
    "",
    "Keeps the tracked ledger of genuine list-at-pending data per closed sale, and",
    "applies it to REDFIN_SOLD rows that have no list price.",
    "",
    "Options:",
    "  --seed-from-git     Harvest every enriched-CSV snapshot on --ref (since --since) into the ledger",
    "  --seed-from-cache   Harvest the Redfin history cache, joined to the cumulative sold file by URL",
    `  --ref REF           Git ref for --seed-from-git (default: ${DEFAULT_REF})`,
    `  --since DATE        Ignore snapshots committed before this date (default: ${DEFAULT_SINCE})`,
    "  --no-apply          Seed only; do not touch the enriched CSV",
    `  --ledger FILE       Ledger CSV (default: ${path.relative(PROJECT_DIR, DEFAULT_LEDGER)})`,
    `  --enriched FILE     Enriched CSV to update in place (default: ${DEFAULT_ENRICHED_REL})`,
    `  --cache FILE        History cache JSON (default: ${path.relative(PROJECT_DIR, DEFAULT_CACHE)})`,
    `  --sold FILE         Cumulative sold CSV, for URL -> sale joins (default: ${path.relative(PROJECT_DIR, DEFAULT_SOLD)})`,
    `  --report FILE       Report JSON (default: ${path.relative(PROJECT_DIR, DEFAULT_REPORT)})`,
    "  --dry-run           Compute and report, write nothing",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const ledgerPath = path.resolve(opts.ledger);
  const ledger = new Ledger(fs.existsSync(ledgerPath) ? readCsvText(fs.readFileSync(ledgerPath, "utf8")).rows : []);
  const report = { generatedAt: new Date().toISOString(), ledger: path.relative(PROJECT_DIR, ledgerPath), entriesBefore: ledger.entries.size, dryRun: opts.dryRun };

  if (opts.seedFromGit) {
    const snapshots = readSnapshotsFromGit(opts.ref, DEFAULT_ENRICHED_REL, opts.since);
    let contributed = 0;
    for (const snap of snapshots) contributed += seedFromSnapshotText(ledger, snap.text, snap.date);
    report.seedFromGit = { ref: opts.ref, snapshots: snapshots.length, range: snapshots.length ? { min: snapshots[0].date, max: snapshots[snapshots.length - 1].date } : null, contributed };
    console.log(`Seeded from git: ${snapshots.length} snapshots, ${contributed} entries added/upgraded; ledger now ${ledger.entries.size}.`);
  }
  if (opts.seedFromCache) {
    const cachePath = path.resolve(opts.cache); const soldPath = path.resolve(opts.sold);
    if (fs.existsSync(cachePath) && fs.existsSync(soldPath)) {
      const cache = JSON.parse(fs.readFileSync(cachePath, "utf8"));
      const sold = readCsvText(fs.readFileSync(soldPath, "utf8")).rows;
      const r = seedFromCache(ledger, cache.entries || {}, sold);
      report.seedFromCache = { cache: path.relative(PROJECT_DIR, cachePath), sold: path.relative(PROJECT_DIR, soldPath), ...r };
      console.log(`Seeded from cache: ${r.contributed} entries added/upgraded (${r.skipped} cached summaries had no matching sale); ledger now ${ledger.entries.size}.`);
    } else {
      report.seedFromCache = { skipped: "cache or sold file missing" };
      console.log("Cache seed skipped: history cache or cumulative sold file missing.");
    }
  }

  // Also harvest the CURRENT enriched CSV, so anything enriched by another
  // path (history scrape, MLS export) is captured the moment it exists.
  const enrichedPath = path.resolve(opts.enriched);
  const { headers, rows } = readCsvText(fs.readFileSync(enrichedPath, "utf8"));
  const fromCurrent = seedFromSnapshotText(ledger, fs.readFileSync(enrichedPath, "utf8"), new Date().toISOString().slice(0, 10));
  report.seedFromCurrent = { contributed: fromCurrent };
  report.entriesAfter = ledger.entries.size;

  if (!opts.dryRun) writeCsv(ledgerPath, LEDGER_COLUMNS, ledger.rows());
  console.log(`Ledger: ${ledger.entries.size} sales with a genuine list price (${fromCurrent} harvested from the current CSV).`);

  if (!opts.noApply) {
    if (!headers.includes(LIST_PRICE_SOURCE_COLUMN)) headers.push(LIST_PRICE_SOURCE_COLUMN);
    const applied = applyLedger(rows, ledger);
    report.apply = applied;
    console.log(`Apply: ${applied.candidates} REDFIN_SOLD candidates, ${applied.applied} filled (${JSON.stringify(applied.bySource)}); skipped ${JSON.stringify(applied.skipped)}`);
    console.log(`By sale month: ${Object.entries(applied.byMonth).sort().map(([m, v]) => `${m}=${v.applied}/${v.candidates}`).join("  ")}`);
    if (!opts.dryRun) writeCsv(enrichedPath, headers, rows);
  }
  fs.writeFileSync(path.resolve(opts.report), `${JSON.stringify(report, null, 2)}\n`);
  if (opts.dryRun) console.log("Dry run: nothing written.");
}

module.exports = {
  Ledger, ledgerEntryFromRow, seedFromSnapshotText, seedFromCache, applyLedger, applyEntryToRow, rejectReason,
  sourceFromJoinMethod, LEDGER_COLUMNS, SALE_DATE_MATCH_DAYS, CLOSE_MATCH_TOLERANCE,
};

if (require.main === module) main();
