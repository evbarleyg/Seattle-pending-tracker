#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_SOURCE_DIR = path.join(PROJECT_DIR, "realtor_exports");
const DEFAULT_YEAR = "2026";
const DEFAULT_OUT_DIR = path.join(PROJECT_DIR, "tmp");

const OUTPUT_COLUMNS = [
  "sourceFile",
  "region",
  "listingNumber",
  "status",
  "apn",
  "address",
  "zip",
  "listingDate",
  "pendingDate",
  "contractualDate",
  "sellingDate",
  "listingPrice",
  "originalPrice",
  "sellingPrice",
  "bidUpAmount",
  "bidUpPct",
  "saleToListRatio",
  "dom",
  "cdom",
  "daysToPending",
  "daysPendingToSale",
  "beds",
  "baths",
  "sqft",
  "yearBuilt",
  "styleCode",
  "subdivision",
  "hasPendingContext",
  "missingReasons",
  "zillowUrl",
  "kingCountyUrl",
];

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        cur += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur.trim());
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur.trim());
  return out;
}

function safeCsv(value) {
  const s = String(value ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) return `"${s.replace(/"/g, "\"\"")}"`;
  return s;
}

function toIsoDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)?$/);
  if (!m) return "";
  return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
}

function toDate(value) {
  const iso = toIsoDate(value);
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function dayDiff(a, b) {
  const d1 = toDate(a);
  const d2 = toDate(b);
  if (!d1 || !d2) return "";
  return String(Math.round((d2 - d1) / (1000 * 60 * 60 * 24)));
}

function num(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function normalizeHeaderNames(headers) {
  const seen = new Map();
  return headers.map((header) => {
    const key = String(header || "").trim();
    const count = (seen.get(key) || 0) + 1;
    seen.set(key, count);
    return count === 1 ? key : `${key} (${count})`;
  });
}

function normalizeStatus(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw
    .toLowerCase()
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function normalizeApn(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length === 10 ? digits : "";
}

function zip5(value) {
  return (String(value || "").match(/[0-9]{5}/) || [])[0] || "";
}

function regionFromFilename(file) {
  const stem = String(file || "")
    .replace(/\.csv$/i, "")
    .replace(/\bsold\s+and\b/ig, "")
    .replace(/\b(?:sale\s+)?stats?\b/ig, "")
    .replace(/\b(?:rich\s+snapshot|snapshot|full)\b/ig, "")
    .replace(/\b\d{1,2}_\d{1,2}\s+to\s+\d{1,2}_\d{1,2}\b/ig, "")
    .replace(/\bcental\b/ig, "Central")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (/^qa$/i.test(stem)) return "QA Magnolia";
  if (/^ne$/i.test(stem)) return "NE Seattle";
  if (/^nw$/i.test(stem)) return "NW Seattle";
  if (/^central$/i.test(stem)) return "Central Seattle";
  return stem || "Seattle";
}

function findHeaderIndex(lines) {
  for (let i = 0; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (cols.includes("Listing Number") && cols.includes("Status")) return i;
  }
  return -1;
}

function mlsAddressFromParts(row) {
  const street = [
    row["Street Number"],
    row["Street Direction"],
    row["Street Name"],
    row["Street Suffix"],
    row["Street Post Direction"],
    row.Unit,
  ].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  const tail = [row.City, row.State, row["Zip Code"]].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  return [street, tail].filter(Boolean).join(", ");
}

function zillowUrl(address, zip) {
  const raw = String(address || "").replace(/\s+/g, " ").trim();
  if (!raw) return "";
  const core = raw.replace(/,\s*Seattle\b.*$/i, "").trim() || raw;
  const full = `${core} Seattle WA ${zip5(zip)}`.trim();
  const slug = full.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  return slug ? `https://www.zillow.com/homes/${slug}_rb/` : "";
}

function kingCountyUrl(apn) {
  const parcel = normalizeApn(apn);
  return parcel ? `https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr=${parcel}` : "";
}

function discoverSourceFiles(sourceDir) {
  if (!fs.existsSync(sourceDir)) return [];
  return fs.readdirSync(sourceDir)
    .filter((file) => /\.csv$/i.test(file) && !file.startsWith("."))
    .sort((a, b) => a.localeCompare(b))
    .map((file) => ({
      file,
      full: path.join(sourceDir, file),
      region: regionFromFilename(file),
    }));
}

function rowFromRealtorExport(row, context) {
  const status = normalizeStatus(row.Status);
  const sellingDate = toIsoDate(row["Selling Date"]);
  const sellingPrice = Math.round(num(row["Selling Price"]));
  const isClosed = /sold|closed/i.test(status) || (!!sellingDate && sellingPrice > 0 && !/active|pending|contingent/i.test(status));
  if (!isClosed || !sellingDate.startsWith(context.year)) return null;

  const listingDate = toIsoDate(row["Listing Date"]);
  const pendingDate = toIsoDate(row["Pending Date"]);
  const contractualDate = toIsoDate(row["Contractual Date"]);
  const listingPrice = Math.round(num(row["Listing Price"]));
  const originalPrice = Math.round(num(row["Original Price"]));
  const apn = normalizeApn(row.APN);
  const zip = zip5(row["Zip Code"]);
  const address = mlsAddressFromParts(row);
  const listingNumber = String(row["Listing Number"] || row["Listing Number (2)"] || "").trim();
  const bidUpAmount = listingPrice > 0 && sellingPrice > 0 ? sellingPrice - listingPrice : "";
  const bidUpPct = listingPrice > 0 && sellingPrice > 0 ? (sellingPrice - listingPrice) / listingPrice : "";
  const saleToListRatio = listingPrice > 0 && sellingPrice > 0 ? sellingPrice / listingPrice : "";
  const missing = [];
  if (!pendingDate) missing.push("missing_pending_date");
  if (!(listingPrice > 0)) missing.push("missing_listing_price");

  return {
    sourceFile: context.file,
    region: context.region,
    listingNumber,
    status,
    apn,
    address,
    zip,
    listingDate,
    pendingDate,
    contractualDate,
    sellingDate,
    listingPrice: listingPrice > 0 ? String(listingPrice) : "",
    originalPrice: originalPrice > 0 ? String(originalPrice) : "",
    sellingPrice: sellingPrice > 0 ? String(sellingPrice) : "",
    bidUpAmount: bidUpAmount === "" ? "" : String(Math.round(bidUpAmount)),
    bidUpPct: bidUpPct === "" ? "" : String(bidUpPct),
    saleToListRatio: saleToListRatio === "" ? "" : String(saleToListRatio),
    dom: row.DOM ? String(Math.round(num(row.DOM))) : "",
    cdom: row.CDOM ? String(Math.round(num(row.CDOM))) : "",
    daysToPending: dayDiff(listingDate, pendingDate),
    daysPendingToSale: dayDiff(pendingDate, sellingDate),
    beds: row.Bedrooms ? String(num(row.Bedrooms)) : "",
    baths: row.Bathrooms ? String(num(row.Bathrooms)) : "",
    sqft: row["Square Footage"] ? String(Math.round(num(row["Square Footage"]))) : "",
    yearBuilt: row["Year Built"] ? String(Math.round(num(row["Year Built"]))) : "",
    styleCode: row["Style Code"] || "",
    subdivision: row.Subdivision || "",
    hasPendingContext: missing.length ? "false" : "true",
    missingReasons: missing.join("|"),
    zillowUrl: zillowUrl(address, zip),
    kingCountyUrl: kingCountyUrl(apn),
  };
}

function dedupeRows(rows) {
  const byKey = new Map();
  rows.forEach((row, index) => {
    const key = row.listingNumber
      || [row.apn, row.sellingDate, row.sellingPrice, row.address].join("|")
      || String(index);
    const current = byKey.get(key);
    const rowScore = (row.hasPendingContext === "true" ? 10 : 0) + (row.apn ? 2 : 0) + (row.address ? 1 : 0);
    const currentScore = current
      ? (current.hasPendingContext === "true" ? 10 : 0) + (current.apn ? 2 : 0) + (current.address ? 1 : 0)
      : -1;
    if (!current || rowScore > currentScore) byKey.set(key, row);
  });
  return [...byKey.values()].sort((a, b) => {
    const dateCmp = String(b.sellingDate).localeCompare(String(a.sellingDate));
    if (dateCmp) return dateCmp;
    return String(a.address).localeCompare(String(b.address), undefined, { sensitivity: "base" });
  });
}

function extractPendingContext(options = {}) {
  const year = String(options.year || DEFAULT_YEAR);
  const sourceDir = path.resolve(options.sourceDir || DEFAULT_SOURCE_DIR);
  const files = discoverSourceFiles(sourceDir);
  const rows = [];
  const skippedFiles = [];
  let parsedRows = 0;

  files.forEach(({ file, full, region }) => {
    const lines = fs.readFileSync(full, "utf8").split(/\r?\n/).filter((line) => line.trim());
    const headerIndex = findHeaderIndex(lines);
    if (headerIndex < 0 || headerIndex >= lines.length - 1) {
      skippedFiles.push({ file, reason: "missing_listing_header" });
      return;
    }
    const headers = normalizeHeaderNames(parseCsvLine(lines[headerIndex]));
    const idx = Object.fromEntries(headers.map((h, i) => [h, i]));
    lines.slice(headerIndex + 1).forEach((line) => {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((header, i) => {
        row[header] = cols[i] || "";
      });
      parsedRows += 1;
      const extracted = rowFromRealtorExport(row, { file, region, year });
      if (extracted) rows.push(extracted);
    });
  });

  const dedupedRows = dedupeRows(rows);
  const missingRows = dedupedRows.filter((row) => row.hasPendingContext !== "true");
  const report = {
    year,
    sourceDir: path.relative(PROJECT_DIR, sourceDir) || ".",
    filesScanned: files.length,
    filesSkipped: skippedFiles.length,
    parsedRows,
    soldRows: dedupedRows.length,
    rowsWithPendingContext: dedupedRows.length - missingRows.length,
    rowsMissingPendingContext: missingRows.length,
    rowsMissingPendingDate: missingRows.filter((row) => row.missingReasons.includes("missing_pending_date")).length,
    rowsMissingListingPrice: missingRows.filter((row) => row.missingReasons.includes("missing_listing_price")).length,
    sourceFiles: files.map((entry) => entry.file),
    skippedFiles,
  };

  return { rows: dedupedRows, missingRows, report };
}

function writeCsv(filePath, rows) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const body = [
    OUTPUT_COLUMNS.join(","),
    ...rows.map((row) => OUTPUT_COLUMNS.map((column) => safeCsv(row[column])).join(",")),
  ].join("\n");
  fs.writeFileSync(filePath, `${body}\n`);
}

function writeOutputs(result, options = {}) {
  const year = String(options.year || result.report.year || DEFAULT_YEAR);
  const outFile = path.resolve(options.outFile || path.join(DEFAULT_OUT_DIR, `sold_${year}_pending_context.csv`));
  const missingOutFile = path.resolve(options.missingOutFile || path.join(DEFAULT_OUT_DIR, `sold_${year}_pending_context_missing.csv`));
  const reportFile = path.resolve(options.reportFile || path.join(DEFAULT_OUT_DIR, `sold_${year}_pending_context_report.json`));

  writeCsv(outFile, result.rows);
  writeCsv(missingOutFile, result.missingRows);
  fs.mkdirSync(path.dirname(reportFile), { recursive: true });
  fs.writeFileSync(reportFile, `${JSON.stringify({
    ...result.report,
    outFile: path.relative(PROJECT_DIR, outFile),
    missingOutFile: path.relative(PROJECT_DIR, missingOutFile),
  }, null, 2)}\n`);

  return { outFile, missingOutFile, reportFile };
}

function parseArgs(argv) {
  const options = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--year") {
      options.year = next;
      i += 1;
    } else if (arg === "--source-dir") {
      options.sourceDir = next;
      i += 1;
    } else if (arg === "--out") {
      options.outFile = next;
      i += 1;
    } else if (arg === "--missing-out") {
      options.missingOutFile = next;
      i += 1;
    } else if (arg === "--report") {
      options.reportFile = next;
      i += 1;
    } else if (arg === "--help" || arg === "-h") {
      options.help = true;
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return options;
}

function printHelp() {
  // eslint-disable-next-line no-console
  console.log([
    "Usage: node scripts/export_2026_pending_context.js [options]",
    "",
    "Extracts sold-listing pending context from local Realtor/MLS CSV exports.",
    "",
    "Options:",
    "  --year YYYY              Sold year to extract. Default: 2026",
    "  --source-dir DIR         Realtor export directory. Default: realtor_exports",
    "  --out FILE               Full output CSV. Default: tmp/sold_YYYY_pending_context.csv",
    "  --missing-out FILE       Missing-context queue CSV. Default: tmp/sold_YYYY_pending_context_missing.csv",
    "  --report FILE            JSON summary. Default: tmp/sold_YYYY_pending_context_report.json",
  ].join("\n"));
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  const result = extractPendingContext(options);
  const outputs = writeOutputs(result, options);
  // eslint-disable-next-line no-console
  console.log(`Sold ${result.report.year} rows: ${result.report.soldRows}`);
  // eslint-disable-next-line no-console
  console.log(`Rows with pending context: ${result.report.rowsWithPendingContext}`);
  // eslint-disable-next-line no-console
  console.log(`Rows missing pending context: ${result.report.rowsMissingPendingContext}`);
  // eslint-disable-next-line no-console
  console.log(`Output: ${path.relative(PROJECT_DIR, outputs.outFile)}`);
  // eslint-disable-next-line no-console
  console.log(`Missing queue: ${path.relative(PROJECT_DIR, outputs.missingOutFile)}`);
  // eslint-disable-next-line no-console
  console.log(`Report: ${path.relative(PROJECT_DIR, outputs.reportFile)}`);
}

if (require.main === module) {
  main();
}

module.exports = {
  extractPendingContext,
  parseCsvLine,
  toIsoDate,
  writeOutputs,
};
