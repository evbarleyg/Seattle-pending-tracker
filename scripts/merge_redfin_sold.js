#!/usr/bin/env node
"use strict";

// Merges Redfin recently-sold listings into the MLS-enriched dataset as
// REDFIN_SOLD rows, so recent sale-price / $-per-sqft / DOM cohorts that the
// county (closings lag ~2wk) and stale MLS exports don't have yet become
// visible — the "can't see recent softening" fix.
//
// Supplement-only by design: appends sales NOT already present; on a match
// (the same sale is already a county/MLS row) it SKIPS — it never mutates a
// curated row. The sold feed carries no list price, so list/ratio/bid-up
// fields are left BLANK (we never fabricate list = close; see the 1.00-ratio
// fix). List-at-pending enrichment stays backfill_redfin_history.js's job.

const fs = require("fs");
const path = require("path");
const { streetKeyFromAddress, zip5 } = require("./redfin_address_key.js");
const { uiPropertyTypeToAppType, zipNeighborhood } = require("./merge_redfin_actives.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_REDFIN = path.join(PROJECT_DIR, "redfin_sold_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_sold_merge_report.json");

const SALE_DATE_MATCH_DAYS = 14;
const REDFIN_SOLD = "REDFIN_SOLD";

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

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
    return `"${s.replace(/"/g, "\"\"")}"`;
  }
  return s;
}

function readCsv(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/);
  while (lines.length && !lines[lines.length - 1]) lines.pop();
  if (!lines.length) return { headers: [], rows: [] };
  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const rows = lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => { row[h] = cols[i] ?? ""; });
    return row;
  });
  return { headers, rows };
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => safeCsv(row[h])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function num(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

// Absolute day gap between two YYYY-MM-DD strings; Infinity if either is unparseable.
function daysBetween(isoA, isoB) {
  const a = Date.parse(`${String(isoA).slice(0, 10)}T00:00:00Z`);
  const b = Date.parse(`${String(isoB).slice(0, 10)}T00:00:00Z`);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return Infinity;
  return Math.abs(a - b) / 86400000;
}

// Build one enriched-schema row from a Redfin sold CSV row. Only sold-relevant
// fields are set; everything else stays blank (the caller pre-fills all headers
// with ""). List price / sale-to-list ratio / bid-up are intentionally blank.
function buildSoldEnrichedRow(redfin, headers) {
  const row = {};
  for (const h of headers) row[h] = "";

  const soldPrice = Math.round(num(redfin.soldPrice));
  const soldPriceStr = soldPrice > 0 ? String(soldPrice) : "";
  const dom = num(redfin.domDays);
  const domStr = dom ? String(Math.round(dom)) : "";
  const zip = zip5(redfin.zip);
  const typeInfo = uiPropertyTypeToAppType(redfin.uiPropertyType);
  const idBase = redfin.redfinListingId || redfin.redfinPropertyId || redfin.mlsListingNumber
    || `${redfin.address || "x"}|${zip}`;
  const soldDate = redfin.soldDate || "";

  row.dataMode = "MLS_ENRICHED";
  row.id = `redfin-sold-${idBase}`;
  row.address = redfin.address || "";
  row.neighborhood = zipNeighborhood(zip) || redfin.neighborhood || "Seattle (Other)";
  row.type = typeInfo.type;
  row.typeCode = typeInfo.typeCode;
  row.addressSource = REDFIN_SOLD;
  row.listDate = "";
  row.pendingDate = "";
  row.saleDate = soldDate;
  row.listPriceAtPending = ""; // sold feed has no list price — never fabricate
  row.closePrice = soldPriceStr;
  row.beds = redfin.beds || "";
  row.baths = redfin.baths || "";
  row.sqft = redfin.sqft || "";
  row.yearBuilt = redfin.yearBuilt || "";
  row.zip = zip;
  row.districtName = "Seattle";
  row.sqFtLot = redfin.lotSize || "";
  row.lat = redfin.lat || "";
  row.lon = redfin.lon || "";

  row.mlsListDate = "";
  row.mlsPendingDate = "";
  row.mlsListPriceAtPending = "";
  row.mlsClosePrice = soldPriceStr;
  row.mlsListingNumber = redfin.mlsListingNumber || "";
  row.mlsStatus = "Sold";
  row.mlsRegion = redfin.queryLabel || "";
  row.mlsSellingDate = soldDate;
  row.mlsListingPrice = "";
  row.mlsSellingPrice = soldPriceStr;
  row.mlsOriginalPrice = "";
  row.mlsDOM = domStr;
  row.mlsCDOM = domStr;
  row.mlsSquareFootageSource = "Redfin";
  row.mlsJoinMethod = REDFIN_SOLD;

  row.saleToListRatio = "";
  row.saleToOriginalListRatio = "";
  row.bidUpAmount = "";
  row.bidUpPct = "";

  return row;
}

// Pure merge: returns { finalRows, report }. No file IO so it stays testable.
function mergeSold(headers, enrichedRows, redfinRows, options = {}) {
  const matchDays = Number.isFinite(options.matchDays) ? options.matchDays : SALE_DATE_MATCH_DAYS;

  // Optional sold-date window — keep only sales on/after minSoldDate (e.g. the
  // last 18 months out of a deeper 24-month scrape).
  const minSoldDate = options.minSoldDate || "";
  const windowed = minSoldDate
    ? redfinRows.filter((r) => (r.soldDate || "") >= minSoldDate)
    : redfinRows;
  const droppedOutOfWindow = redfinRows.length - windowed.length;

  // Drop prior REDFIN_SOLD rows so reruns replace rather than duplicate.
  const cleanedExisting = enrichedRows.filter((r) => r.mlsJoinMethod !== REDFIN_SOLD);
  const priorRemoved = enrichedRows.length - cleanedExisting.length;

  // Index existing rows: by MLS# (any status) and, for the no-MLS county case,
  // existing *sold* rows by normalized street+zip (date-matched at compare time).
  const byMls = new Map();
  const byStreet = new Map();
  for (const r of cleanedExisting) {
    const mls = (r.mlsListingNumber || "").trim();
    if (mls && !byMls.has(mls)) byMls.set(mls, r);
    const saleDate = r.saleDate || r.mlsSellingDate || "";
    if (saleDate) {
      const k = streetKeyFromAddress(r.address, r.zip);
      if (k) {
        const arr = byStreet.get(k) || [];
        arr.push(r);
        byStreet.set(k, arr);
      }
    }
  }

  const newRows = [];
  let matchedByMls = 0;
  let matchedByAddr = 0;
  const appendedByMonth = {};
  const appendedByRegion = {};

  for (const r of windowed) {
    const mls = (r.mlsListingNumber || "").trim();
    if (mls && byMls.has(mls)) { matchedByMls += 1; continue; }

    const k = streetKeyFromAddress(r.address, r.zip);
    if (k && byStreet.has(k)) {
      const sameSale = byStreet.get(k).some(
        (c) => daysBetween(c.saleDate || c.mlsSellingDate, r.soldDate) <= matchDays,
      );
      if (sameSale) { matchedByAddr += 1; continue; }
    }

    newRows.push(buildSoldEnrichedRow(r, headers));
    const month = (r.soldDate || "").slice(0, 7);
    if (month) appendedByMonth[month] = (appendedByMonth[month] || 0) + 1;
    appendedByRegion[r.queryLabel || "Unknown"] = (appendedByRegion[r.queryLabel || "Unknown"] || 0) + 1;
  }

  return {
    finalRows: [...cleanedExisting, ...newRows],
    report: {
      rowsBefore: enrichedRows.length,
      priorRedfinSoldRemoved: priorRemoved,
      redfinRowsRead: redfinRows.length,
      minSoldDate: minSoldDate || null,
      droppedOutOfWindow,
      matchedByMls,
      matchedByAddr,
      matchedTotal: matchedByMls + matchedByAddr,
      appended: newRows.length,
      rowsAfter: cleanedExisting.length + newRows.length,
      appendedByMonth,
      appendedByRegion,
    },
  };
}

function parseArgs(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--redfin") { opts.redfin = next; i += 1; }
    else if (a === "--out") { opts.out = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--match-days") { opts.matchDays = Number(next); i += 1; }
    else if (a === "--since") { opts.since = next; i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
    else throw new Error(`Unknown argument: ${a}`);
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/merge_redfin_sold.js [options]",
    "",
    "Merges Redfin recently-sold listings into the MLS-enriched dataset as",
    "REDFIN_SOLD rows (supplement-only: appends new sales, skips ones already",
    "present, never mutates existing rows).",
    "",
    "Options:",
    "  --enriched FILE    Enriched CSV (default: public_sales_proxy_mls_enriched_last12mo.csv)",
    "  --redfin FILE      Redfin sold CSV (default: redfin_sold_listings.csv)",
    "  --out FILE         Output CSV (default: same as --enriched, replaced in place)",
    "  --report FILE      Merge report JSON (default: redfin_sold_merge_report.json)",
    "  --match-days N     Sale-date window for address dedupe (default: 14)",
    "  --since YYYY-MM-DD Only merge sales on/after this date (e.g. 18-month window)",
    "  --dry-run          Compute and print the report but do not write outputs",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }

  const enrichedPath = path.resolve(opts.enriched || DEFAULT_ENRICHED);
  const redfinPath = path.resolve(opts.redfin || DEFAULT_REDFIN);
  const outPath = path.resolve(opts.out || enrichedPath);
  const reportPath = path.resolve(opts.report || DEFAULT_REPORT);

  if (!fs.existsSync(enrichedPath)) throw new Error(`Enriched CSV not found: ${enrichedPath}`);
  if (!fs.existsSync(redfinPath)) {
    process.stderr.write(`Redfin sold CSV not found: ${redfinPath}\n`);
    process.stderr.write("Run `npm run fetch:sold` first to generate it.\n");
    process.exit(1);
  }

  const { headers, rows: enrichedRows } = readCsv(enrichedPath);
  const { rows: redfinRows } = readCsv(redfinPath);

  const { finalRows, report } = mergeSold(headers, enrichedRows, redfinRows, { matchDays: opts.matchDays, minSoldDate: opts.since });
  const fullReport = {
    mergedAt: new Date().toISOString(),
    enrichedInput: path.relative(PROJECT_DIR, enrichedPath),
    redfinInput: path.relative(PROJECT_DIR, redfinPath),
    matchDays: Number.isFinite(opts.matchDays) ? opts.matchDays : SALE_DATE_MATCH_DAYS,
    ...report,
  };

  if (opts.dryRun) {
    console.log("Dry run.");
    console.log(JSON.stringify(fullReport, null, 2));
    return;
  }

  writeCsv(outPath, headers, finalRows);
  fs.writeFileSync(reportPath, `${JSON.stringify({ ...fullReport, outFile: path.relative(PROJECT_DIR, outPath) }, null, 2)}\n`);
  console.log(`Appended ${report.appended} REDFIN_SOLD rows, skipped ${report.matchedTotal} already-present sales.`);
  console.log(`Output: ${path.relative(PROJECT_DIR, outPath)}  (${finalRows.length} rows)`);
  console.log(`Report: ${path.relative(PROJECT_DIR, reportPath)}`);
}

if (require.main === module) {
  try { main(); }
  catch (err) { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); }
}

module.exports = {
  mergeSold,
  buildSoldEnrichedRow,
  daysBetween,
};
