#!/usr/bin/env node
"use strict";

// Recovers list-at-pending for REDFIN_SOLD rows from the daily active-listing
// snapshots already committed to git — no network needed.
//
// Why: the Redfin sold feed carries no list price, and as of 2026-09-19 Redfin
// serves an AWS WAF challenge to server-side fetches of property pages, so
// backfill_redfin_history.js (the price-history scrape) cannot run from Node.
// But the daily job has committed the enriched CSV every morning since June,
// and each commit holds that day's REDFIN_ACTIVE rows with their asking price,
// list date and days-on-market. For a home that later sold, the LAST snapshot
// in which its MLS# was still active gives:
//   - its asking price at that moment  -> listPriceAtPending (a genuine ask,
//     so sold-at-asking legitimately yields sale/list = 1.000; this is not the
//     fabricated list==close artifact the 1.00 fix removed)
//   - the date it was last seen active -> pendingDate (true pending date is
//     within one snapshot interval, normally a day)
//   - its DOM/CDOM as of that day      -> mlsDOM / mlsCDOM
//   - its list date                    -> listDate / mlsListDate
//
// Supplement-only: touches REDFIN_SOLD rows that have no list price yet, and
// never mutates county/MLS/history rows. Rerun after every merge:sold, because
// merge_redfin_sold.js re-appends bare REDFIN_SOLD rows.
//
// Lineage lives in its own additive column, `listPriceSource = ACTIVE_SNAPSHOT`
// (added to the header when absent). `addressSource` and `mlsJoinMethod` are
// left alone: the former describes where the ADDRESS came from, the latter is
// what merge_redfin_sold.js keys its strip-and-replace on.

const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_snapshot_backfill_report.json");
const DEFAULT_REF = "main";
const DEFAULT_SNAPSHOT_SINCE = "2026-06-01";
const REDFIN_SOLD = "REDFIN_SOLD";
const LIST_PRICE_SOURCE_COLUMN = "listPriceSource";
const LIST_PRICE_SOURCE = "ACTIVE_SNAPSHOT";
const MAX_PENDING_TO_SALE_DAYS = 180;
// A close more than 35% away from the last ask is far likelier to be an MLS#
// collision than a real outcome; such rows are rejected AND written to the
// report so a bad join shows up there instead of in the data.
const MAX_RATIO_DEVIATION = 0.35;
const MAX_REJECT_SAMPLES = 200;

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") { cur += "\""; i += 1; }
      else inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

function safeCsv(value) {
  const s = value == null ? "" : String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, "\"\"")}"` : s;
}

function readCsvText(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.length);
  if (!lines.length) return { headers: [], rows: [] };
  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((h, j) => { row[h] = cols[j] ?? ""; });
    rows.push(row);
  }
  return { headers, rows };
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) lines.push(headers.map((h) => safeCsv(row[h])).join(","));
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function num(value) {
  const n = Number(String(value ?? "").replace(/[$,]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function daysBetween(a, b) {
  const ta = Date.parse(a);
  const tb = Date.parse(b);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.round((tb - ta) / 86400000);
}

// Build MLS# -> last-seen active record from a chronologically ordered list of
// snapshots. Each snapshot is { date, text } where text is the enriched CSV at
// that commit. Later snapshots overwrite earlier ones, so the map ends holding
// the last day each listing was seen active.
function collectActiveSnapshots(snapshots) {
  const lastSeen = new Map();
  for (const snap of snapshots) {
    const lines = snap.text.split(/\r?\n/);
    if (!lines.length) continue;
    const headers = parseCsvLine(lines[0]).map((h) => h.trim());
    const ix = Object.fromEntries(headers.map((h, i) => [h, i]));
    if (ix.mlsJoinMethod === undefined || ix.mlsListingNumber === undefined) continue;
    for (let i = 1; i < lines.length; i += 1) {
      const line = lines[i];
      if (!line || !line.includes("REDFIN_ACTIVE")) continue; // cheap prefilter
      const cols = parseCsvLine(line);
      if (cols[ix.mlsJoinMethod] !== "REDFIN_ACTIVE") continue;
      const mls = String(cols[ix.mlsListingNumber] || "").trim();
      if (!mls) continue;
      const list = num(cols[ix.mlsListingPrice]) || num(cols[ix.listPriceAtPending]);
      if (!(list > 0)) continue;
      lastSeen.set(mls, {
        date: snap.date,
        list,
        originalList: num(cols[ix.mlsOriginalPrice]) || list,
        listDate: String(cols[ix.mlsListDate] || cols[ix.listDate] || "").trim(),
        dom: String(cols[ix.mlsDOM] ?? "").trim(),
        cdom: String(cols[ix.mlsCDOM] ?? "").trim(),
        zip: String(cols[ix.zip] || "").trim(),
      });
    }
  }
  return lastSeen;
}

// Why a snapshot was NOT applied to a row (null = apply it).
function rejectReason(row, rec) {
  if (row.mlsJoinMethod !== REDFIN_SOLD) return "not_redfin_sold";
  if (num(row.listPriceAtPending) > 0) return "already_has_list";
  if (!rec) return "no_snapshot";
  const close = num(row.closePrice);
  if (!(close > 0)) return "no_close_price";
  const gap = daysBetween(rec.date, row.saleDate);
  if (gap === null) return "bad_dates";
  if (gap < 0) return "active_after_sale"; // seen active after it closed: a relist, not this sale
  if (gap > MAX_PENDING_TO_SALE_DAYS) return "stale_snapshot";
  if (Math.abs(close / rec.list - 1) > MAX_RATIO_DEVIATION) return "implausible_ratio";
  return null;
}

function applySnapshotToRow(row, rec) {
  if (rejectReason(row, rec)) return false;
  const close = num(row.closePrice);
  const list = rec.list;
  const orig = rec.originalList > 0 ? rec.originalList : list;
  row.listPriceAtPending = String(list);
  row.mlsListPriceAtPending = String(list);
  row.mlsListingPrice = String(list);
  row.mlsOriginalPrice = String(orig);
  row.pendingDate = rec.date;
  row.mlsPendingDate = rec.date;
  row.mlsContractualDate = row.mlsContractualDate || rec.date;
  if (rec.listDate) {
    row.listDate = rec.listDate;
    row.mlsListDate = rec.listDate;
  }
  if (rec.dom !== "") { row.mlsDOM = rec.dom; }
  if (rec.cdom !== "") { row.mlsCDOM = rec.cdom; }
  row.bidUpAmount = String(Math.round(close - list));
  row.bidUpPct = String((close - list) / list);
  row.saleToListRatio = String(close / list);
  row.saleToOriginalListRatio = String(close / orig);
  const toPending = rec.listDate ? daysBetween(rec.listDate, rec.date) : null;
  if (toPending !== null && toPending >= 0) row.mlsDaysToPending = String(toPending);
  const toSale = daysBetween(rec.date, row.saleDate);
  if (toSale !== null && toSale >= 0) row.mlsDaysPendingToSale = String(toSale);
  row[LIST_PRICE_SOURCE_COLUMN] = LIST_PRICE_SOURCE;
  return true;
}

// Reasons worth surfacing row-by-row: each one means a snapshot DID match the
// MLS# but the pairing looked wrong, which is the signature of a bad join.
const REPORTED_REJECTS = new Set(["implausible_ratio", "active_after_sale", "stale_snapshot"]);

function backfillFromSnapshots(rows, lastSeen) {
  const report = { candidates: 0, applied: 0, skipped: {}, byMonth: {}, rejected: [] };
  for (const row of rows) {
    if (row.mlsJoinMethod !== REDFIN_SOLD) continue;
    report.candidates += 1;
    const month = String(row.saleDate || "").slice(0, 7) || "unknown";
    const bucket = report.byMonth[month] || (report.byMonth[month] = { candidates: 0, applied: 0 });
    bucket.candidates += 1;
    const rec = lastSeen.get(String(row.mlsListingNumber || "").trim());
    const reason = rejectReason(row, rec);
    if (reason) {
      report.skipped[reason] = (report.skipped[reason] || 0) + 1;
      if (REPORTED_REJECTS.has(reason) && report.rejected.length < MAX_REJECT_SAMPLES) {
        const close = num(row.closePrice);
        report.rejected.push({
          reason,
          address: row.address || "",
          zip: row.zip || "",
          mlsListingNumber: row.mlsListingNumber || "",
          saleDate: row.saleDate || "",
          closePrice: close,
          lastSeenActive: rec.date,
          lastAsk: rec.list,
          ratio: rec.list > 0 ? Number((close / rec.list).toFixed(3)) : null,
        });
      }
      continue;
    }
    applySnapshotToRow(row, rec);
    report.applied += 1;
    bucket.applied += 1;
  }
  return report;
}

// --- git plumbing (not exercised by tests) ---------------------------------

function listSnapshotCommits(ref, relFile, sinceDate) {
  const out = execFileSync("git", ["log", "--format=%H %cs", ref, "--", relFile], {
    cwd: PROJECT_DIR, encoding: "utf8", maxBuffer: 1 << 26,
  });
  return out.trim().split("\n").filter(Boolean)
    .map((line) => { const [sha, date] = line.split(" "); return { sha, date }; })
    .filter((c) => !sinceDate || c.date >= sinceDate)
    .reverse(); // oldest -> newest so later snapshots overwrite earlier ones
}

function readSnapshotsFromGit(ref, relFile, sinceDate) {
  return listSnapshotCommits(ref, relFile, sinceDate).map((c) => ({
    date: c.date,
    text: execFileSync("git", ["show", `${c.sha}:${relFile}`], { cwd: PROJECT_DIR, encoding: "utf8", maxBuffer: 1 << 28 }),
  }));
}

function parseArgs(argv) {
  const opts = { enriched: DEFAULT_ENRICHED, report: DEFAULT_REPORT, ref: DEFAULT_REF, since: DEFAULT_SNAPSHOT_SINCE, dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--ref") { opts.ref = next; i += 1; }
    else if (a === "--since") { opts.since = next; i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/backfill_list_from_active_snapshots.js [options]",
    "",
    "Fills listPriceAtPending / pendingDate / DOM on REDFIN_SOLD rows from the daily",
    "active-listing snapshots committed to git (last day each MLS# was seen active).",
    "",
    "Options:",
    `  --enriched FILE   Enriched CSV to update in place (default: ${path.relative(PROJECT_DIR, DEFAULT_ENRICHED)})`,
    `  --ref REF         Git ref whose history holds the daily snapshots (default: ${DEFAULT_REF})`,
    `  --since DATE      Ignore snapshots committed before this date (default: ${DEFAULT_SNAPSHOT_SINCE})`,
    `  --report FILE     Report JSON (default: ${path.relative(PROJECT_DIR, DEFAULT_REPORT)})`,
    "  --dry-run         Compute and report, but do not write the CSV",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const enrichedPath = path.resolve(opts.enriched);
  const relFile = path.relative(PROJECT_DIR, enrichedPath);
  const snapshots = readSnapshotsFromGit(opts.ref, relFile, opts.since);
  if (!snapshots.length) {
    console.error(`No snapshots of ${relFile} found on ${opts.ref} since ${opts.since}.`);
    process.exit(1);
  }
  const lastSeen = collectActiveSnapshots(snapshots);
  console.log(`Snapshots: ${snapshots.length} (${snapshots[0].date} -> ${snapshots[snapshots.length - 1].date}); active MLS# seen: ${lastSeen.size}`);
  const { headers, rows } = readCsvText(fs.readFileSync(enrichedPath, "utf8"));
  if (!headers.includes(LIST_PRICE_SOURCE_COLUMN)) headers.push(LIST_PRICE_SOURCE_COLUMN); // additive column
  const report = backfillFromSnapshots(rows, lastSeen);
  const summary = {
    generatedAt: new Date().toISOString(),
    ref: opts.ref,
    snapshotCount: snapshots.length,
    snapshotRange: { min: snapshots[0].date, max: snapshots[snapshots.length - 1].date },
    activeListingsSeen: lastSeen.size,
    listPriceSource: LIST_PRICE_SOURCE,
    maxRatioDeviation: MAX_RATIO_DEVIATION,
    dryRun: opts.dryRun,
    ...report,
  };
  fs.writeFileSync(path.resolve(opts.report), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(`REDFIN_SOLD candidates: ${report.candidates}; applied list@pending to ${report.applied}; skipped: ${JSON.stringify(report.skipped)}`);
  if (report.rejected.length) console.log(`Rejected joins written to the report: ${report.rejected.length} (first: ${report.rejected[0].reason} ${report.rejected[0].address} close ${report.rejected[0].closePrice} vs ask ${report.rejected[0].lastAsk})`);
  console.log(`By sale month: ${Object.entries(report.byMonth).sort().map(([m, v]) => `${m}=${v.applied}/${v.candidates}`).join("  ")}`);
  if (opts.dryRun) { console.log("Dry run: CSV not written."); return; }
  writeCsv(enrichedPath, headers, rows);
  console.log(`Wrote ${relFile} (${rows.length} rows). Report: ${path.relative(PROJECT_DIR, path.resolve(opts.report))}`);
}

module.exports = {
  collectActiveSnapshots, applySnapshotToRow, backfillFromSnapshots, rejectReason, daysBetween,
  LIST_PRICE_SOURCE, LIST_PRICE_SOURCE_COLUMN, MAX_RATIO_DEVIATION,
};

if (require.main === module) main();
