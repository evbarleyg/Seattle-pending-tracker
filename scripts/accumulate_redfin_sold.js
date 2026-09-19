#!/usr/bin/env node
"use strict";

// Folds a fresh Redfin sold fetch (any window, e.g. the daily 30-day one) into
// the tracked cumulative file redfin_sold_cumulative.csv, so merge_redfin_sold.js
// always sees every sale we have ever fetched.
//
// Why: merge_redfin_sold.js strips every prior REDFIN_SOLD row and re-appends
// only what is in its input CSV. Feeding it a short window after a long one
// silently deletes the out-of-window sales (that is how the spring cohort went
// missing in August). Accumulating first makes any fetch window safe.
//
// Rules: rows are keyed by Redfin property id + sold date (falling back to MLS#
// + sold date, then address + zip + sold date). On a key collision the row with
// the newer fetchedAt wins. Rows whose sold date is older than --retain-days
// are pruned so the file stays bounded.

const fs = require("fs");
const path = require("path");
const { readCsvText, writeCsv } = require("./csv_util.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_IN = path.join(PROJECT_DIR, "redfin_sold_listings.csv");
const DEFAULT_OUT = path.join(PROJECT_DIR, "redfin_sold_cumulative.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_sold_accumulate_report.json");
const DEFAULT_RETAIN_DAYS = 400;

function soldKey(row) {
  const sold = String(row.soldDate || "").trim();
  const pid = String(row.redfinPropertyId || "").trim();
  if (pid) return `pid:${pid}|${sold}`;
  const mls = String(row.mlsListingNumber || "").trim();
  if (mls) return `mls:${mls}|${sold}`;
  return `addr:${String(row.address || "").trim().toUpperCase()}|${String(row.zip || "").trim()}|${sold}`;
}

function shiftDate(isoDate, days) {
  return new Date(Date.parse(isoDate) + days * 86400000).toISOString().slice(0, 10);
}

// Pure merge. `today` and `retainDays` bound the prune; pass retainDays <= 0 to keep everything.
function accumulateSold(existingRows, incomingRows, { retainDays = DEFAULT_RETAIN_DAYS, today } = {}) {
  const map = new Map();
  for (const r of existingRows) map.set(soldKey(r), r);
  const report = { existing: existingRows.length, incoming: incomingRows.length, added: 0, replaced: 0, keptOlder: 0, pruned: 0 };
  for (const r of incomingRows) {
    const key = soldKey(r);
    const prev = map.get(key);
    if (!prev) { map.set(key, r); report.added += 1; continue; }
    if (String(r.fetchedAt || "") >= String(prev.fetchedAt || "")) { map.set(key, r); report.replaced += 1; }
    else report.keptOlder += 1;
  }
  let rows = [...map.values()];
  if (retainDays > 0) {
    const cutoff = shiftDate(today || new Date().toISOString().slice(0, 10), -retainDays);
    const before = rows.length;
    rows = rows.filter((r) => String(r.soldDate || "") >= cutoff);
    report.pruned = before - rows.length;
    report.cutoff = cutoff;
  }
  rows.sort((a, b) => {
    const d = String(b.soldDate || "").localeCompare(String(a.soldDate || "")); // newest sale first
    return d !== 0 ? d : soldKey(a).localeCompare(soldKey(b));
  });
  report.total = rows.length;
  report.soldDateRange = rows.length ? { min: rows[rows.length - 1].soldDate, max: rows[0].soldDate } : null;
  return { rows, report };
}

function unionHeaders(existingHeaders, incomingHeaders) {
  const out = [...existingHeaders];
  for (const h of incomingHeaders) if (!out.includes(h)) out.push(h);
  return out;
}

function parseArgs(argv) {
  const opts = { in: DEFAULT_IN, out: DEFAULT_OUT, report: DEFAULT_REPORT, retainDays: DEFAULT_RETAIN_DAYS, dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--in") { opts.in = next; i += 1; }
    else if (a === "--out") { opts.out = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--retain-days") { opts.retainDays = Number(next); i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/accumulate_redfin_sold.js [options]",
    "",
    "Folds a fresh Redfin sold fetch into the tracked cumulative sold file, keyed by",
    "property id + sold date (newest fetch wins), pruning sales older than --retain-days.",
    "",
    "Options:",
    `  --in FILE           Fresh sold fetch CSV (default: ${path.relative(PROJECT_DIR, DEFAULT_IN)})`,
    `  --out FILE          Cumulative sold CSV, updated in place (default: ${path.relative(PROJECT_DIR, DEFAULT_OUT)})`,
    `  --retain-days N     Drop sales older than N days (default: ${DEFAULT_RETAIN_DAYS}; 0 keeps all)`,
    `  --report FILE       Report JSON (default: ${path.relative(PROJECT_DIR, DEFAULT_REPORT)})`,
    "  --dry-run           Compute and report, but do not write",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const inPath = path.resolve(opts.in);
  const outPath = path.resolve(opts.out);
  if (!fs.existsSync(inPath)) {
    console.error(`No sold fetch at ${path.relative(PROJECT_DIR, inPath)}; run npm run fetch:sold first.`);
    process.exit(1);
  }
  const incoming = readCsvText(fs.readFileSync(inPath, "utf8"));
  const existing = fs.existsSync(outPath) ? readCsvText(fs.readFileSync(outPath, "utf8")) : { headers: [], rows: [] };
  const headers = unionHeaders(existing.headers.length ? existing.headers : incoming.headers, incoming.headers);
  const { rows, report } = accumulateSold(existing.rows, incoming.rows, { retainDays: opts.retainDays });
  const summary = { generatedAt: new Date().toISOString(), in: path.relative(PROJECT_DIR, inPath), out: path.relative(PROJECT_DIR, outPath), dryRun: opts.dryRun, ...report };
  fs.writeFileSync(path.resolve(opts.report), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(`Sold accumulate: ${report.incoming} incoming -> ${report.added} added, ${report.replaced} replaced, ${report.keptOlder} kept older, ${report.pruned} pruned; total ${report.total} (${report.soldDateRange?.min} -> ${report.soldDateRange?.max}).`);
  if (opts.dryRun) { console.log("Dry run: cumulative file not written."); return; }
  writeCsv(outPath, headers, rows);
  console.log(`Wrote ${path.relative(PROJECT_DIR, outPath)}.`);
}

module.exports = { accumulateSold, soldKey, unionHeaders, DEFAULT_RETAIN_DAYS };

if (require.main === module) main();
