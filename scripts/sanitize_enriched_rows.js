#!/usr/bin/env node
"use strict";

// Two honesty rules applied to the enriched CSV after the feeds are merged.
//
// 1. Expire stale open rows (plan item L3). The March realtor export left
//    hundreds of listings marked Active / Pending that are long gone; they
//    inflate the active count and sit on the map as ghosts. A non-feed
//    open-status row is dropped when its list date is older than
//    --max-open-age days (or missing) AND its MLS# has not been seen in the
//    actives ledger within --ledger-grace days. Rows from today's Redfin feed
//    (REDFIN_ACTIVE) and rows with a close price are never touched. Dropped
//    rows are written to the report so the decision is auditable.
//
// 2. Blank fabricated timelines. Some history-scraped rows carry
//    listDate == pendingDate with no genuine MLS list price (Redfin showed a
//    single record, not a list -> pending timeline) and therefore a DOM of 0.
//    The pending date, DOM and days-to-pending on those rows are blanked so
//    the app reads them as unknown; the list date and close are kept.

const fs = require("fs");
const path = require("path");
const { readCsvText, writeCsv, num } = require("./csv_util.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_LEDGER = path.join(PROJECT_DIR, "redfin_active_ledger.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_sanitize_report.json");
const DEFAULT_MAX_OPEN_AGE_DAYS = 45;
const DEFAULT_LEDGER_GRACE_DAYS = 7;
const OPEN_STATUS = /^(active|pending|contingent|coming soon|first look)/i;

function daysBetween(a, b) {
  const ta = Date.parse(a); const tb = Date.parse(b);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.round((tb - ta) / 86400000);
}

function isOpenStatus(status) {
  return OPEN_STATUS.test(String(status || "").trim());
}

// Why a row is a stale open row (null = keep it).
function staleReason(row, ledgerLastSeen, { today, maxOpenAgeDays, ledgerGraceDays }) {
  if (!isOpenStatus(row.mlsStatus)) return null;
  if (num(row.closePrice) > 0) return null;
  if (row.mlsJoinMethod === "REDFIN_ACTIVE") return null;
  const listDate = String(row.mlsListDate || row.listDate || "").trim();
  const age = listDate ? daysBetween(listDate, today) : null;
  if (age !== null && age <= maxOpenAgeDays) return null;
  const seen = ledgerLastSeen.get(String(row.mlsListingNumber || "").trim());
  if (seen && daysBetween(seen, today) <= ledgerGraceDays) return null;
  return age === null ? "no_list_date_and_not_in_ledger" : "list_date_stale_and_not_in_ledger";
}

function hasFabricatedTimeline(row) {
  // County proxy rows carry listDate == pendingDate by construction (their
  // "pending" is a recording-date proxy, and the app never reads it as a
  // timeline), so only MLS-enriched rows are candidates.
  if (String(row.dataMode || "") !== "MLS_ENRICHED") return false;
  if (!(num(row.closePrice) > 0)) return false;
  if (num(row.mlsListingPrice) > 0 || num(row.mlsListPriceAtPending) > 0) return false;
  const listDate = String(row.mlsListDate || row.listDate || "").trim();
  const pending = String(row.mlsPendingDate || row.pendingDate || "").trim();
  return !!listDate && !!pending && listDate === pending;
}

function blankTimeline(row) {
  for (const k of ["pendingDate", "mlsPendingDate", "mlsContractualDate", "mlsDOM", "mlsCDOM", "mlsDaysToPending", "mlsDaysPendingToSale"]) {
    if (k in row) row[k] = "";
  }
}

// 3. Repair doubled unit suffixes in addresses ("… Unit A Unit A", "… #4 #4"),
//    left by the actives fetcher before it stopped appending a unit the
//    street line already carried. Idempotent; see build_active_ledger.js.
const { dedupeUnitSuffix } = require("./build_active_ledger.js");

function sanitize(rows, ledgerLastSeen, opts) {
  const kept = [];
  const report = {
    today: opts.today, maxOpenAgeDays: opts.maxOpenAgeDays, ledgerGraceDays: opts.ledgerGraceDays,
    rowsBefore: rows.length, staleOpenDropped: 0, staleOpenKeptViaLedger: 0, staleReasons: {}, dropped: [],
    fabricatedTimelinesBlanked: 0, blanked: [], doubledUnitsRepaired: 0,
  };
  for (const row of rows) {
    if (row.address) {
      const repaired = dedupeUnitSuffix(row.address);
      if (repaired !== String(row.address).trim().replace(/\s+/g, " ")) { row.address = repaired; report.doubledUnitsRepaired += 1; }
    }
    const reason = staleReason(row, ledgerLastSeen, opts);
    if (reason) {
      report.staleOpenDropped += 1;
      report.staleReasons[reason] = (report.staleReasons[reason] || 0) + 1;
      if (report.dropped.length < 1000) report.dropped.push({ address: row.address, zip: row.zip, mls: row.mlsListingNumber, status: row.mlsStatus, listDate: row.mlsListDate || row.listDate, joinMethod: row.mlsJoinMethod });
      continue;
    }
    if (isOpenStatus(row.mlsStatus) && !(num(row.closePrice) > 0) && row.mlsJoinMethod !== "REDFIN_ACTIVE") {
      const seen = ledgerLastSeen.get(String(row.mlsListingNumber || "").trim());
      if (seen && daysBetween(seen, opts.today) <= opts.ledgerGraceDays) report.staleOpenKeptViaLedger += 1;
    }
    if (hasFabricatedTimeline(row)) {
      blankTimeline(row);
      report.fabricatedTimelinesBlanked += 1;
      if (report.blanked.length < 200) report.blanked.push({ address: row.address, mls: row.mlsListingNumber, saleDate: row.saleDate, listDate: row.mlsListDate || row.listDate });
    }
    kept.push(row);
  }
  report.rowsAfter = kept.length;
  return { rows: kept, report };
}

function loadLedgerLastSeen(text) {
  const map = new Map();
  for (const r of readCsvText(text).rows) {
    const mls = String(r.mlsNumber || "").trim();
    if (mls && r.lastSeen) map.set(mls, r.lastSeen);
  }
  return map;
}

function parseArgs(argv) {
  const opts = { enriched: DEFAULT_ENRICHED, ledger: DEFAULT_LEDGER, report: DEFAULT_REPORT, maxOpenAgeDays: DEFAULT_MAX_OPEN_AGE_DAYS, ledgerGraceDays: DEFAULT_LEDGER_GRACE_DAYS, today: new Date().toISOString().slice(0, 10), dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i]; const next = argv[i + 1];
    if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--ledger") { opts.ledger = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--max-open-age") { opts.maxOpenAgeDays = Number(next); i += 1; }
    else if (a === "--ledger-grace") { opts.ledgerGraceDays = Number(next); i += 1; }
    else if (a === "--today") { opts.today = next; i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/sanitize_enriched_rows.js [options]",
    "",
    "Drops stale open-status rows from old realtor exports (not in today's Redfin feed,",
    "list date older than --max-open-age, MLS# not seen in the actives ledger within",
    "--ledger-grace days) and blanks fabricated list==pending timelines.",
    "",
    "Options:",
    `  --max-open-age N   Days a non-feed open row may be listed before it needs ledger proof (default: ${DEFAULT_MAX_OPEN_AGE_DAYS})`,
    `  --ledger-grace N   Days since the ledger last saw the MLS# that still count as live (default: ${DEFAULT_LEDGER_GRACE_DAYS})`,
    "  --today DATE       Reference date (default: today)",
    `  --enriched FILE    Enriched CSV to update in place (default: ${path.relative(PROJECT_DIR, DEFAULT_ENRICHED)})`,
    `  --ledger FILE      Actives ledger (default: ${path.relative(PROJECT_DIR, DEFAULT_LEDGER)}; missing = no ledger proof)`,
    `  --report FILE      Report JSON with every dropped/blanked row (default: ${path.relative(PROJECT_DIR, DEFAULT_REPORT)})`,
    "  --dry-run          Report only",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const enrichedPath = path.resolve(opts.enriched);
  const ledgerPath = path.resolve(opts.ledger);
  const ledgerLastSeen = fs.existsSync(ledgerPath) ? loadLedgerLastSeen(fs.readFileSync(ledgerPath, "utf8")) : new Map();
  const { headers, rows } = readCsvText(fs.readFileSync(enrichedPath, "utf8"));
  const { rows: kept, report } = sanitize(rows, ledgerLastSeen, opts);
  report.generatedAt = new Date().toISOString();
  report.dryRun = opts.dryRun;
  fs.writeFileSync(path.resolve(opts.report), `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Sanitize: dropped ${report.staleOpenDropped} stale open rows (${JSON.stringify(report.staleReasons)}; ${report.staleOpenKeptViaLedger} kept because the ledger saw them recently); blanked ${report.fabricatedTimelinesBlanked} fabricated timelines; repaired ${report.doubledUnitsRepaired} doubled unit suffixes. Rows ${report.rowsBefore} -> ${report.rowsAfter}.`);
  if (opts.dryRun) { console.log("Dry run: CSV not written."); return; }
  writeCsv(enrichedPath, headers, kept);
}

module.exports = { sanitize, staleReason, hasFabricatedTimeline, isOpenStatus, loadLedgerLastSeen, DEFAULT_MAX_OPEN_AGE_DAYS, DEFAULT_LEDGER_GRACE_DAYS };

if (require.main === module) main();
