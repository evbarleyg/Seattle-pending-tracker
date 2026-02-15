#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const PUBLIC_FILE = path.join(PROJECT_DIR, "public_sales_proxy_all_prices_last12mo.csv");
const ENRICHED_FILE = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const REALTOR_DIR = path.join(PROJECT_DIR, "realtor_exports");
const REPORT_FILE = path.join(PROJECT_DIR, "data_refresh_report.json");

const REQUIRED_PUBLIC_COLUMNS = [
  "id",
  "address",
  "type",
  "closePrice",
];

const REQUIRED_ENRICHED_COLUMNS = [
  "dataMode",
  "id",
  "address",
  "major",
  "minor",
  "parcelNbr",
  "saleDate",
  "listPriceAtPending",
  "closePrice",
  "mlsStatus",
  "mlsListingPrice",
  "mlsOriginalPrice",
  "mlsDOM",
  "mlsCDOM",
  "saleToListRatio",
  "saleToOriginalListRatio",
  "bidUpAmount",
  "bidUpPct",
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

function readHeader(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const firstLine = text.split(/\r?\n/).find((line) => line.trim());
  if (!firstLine) return [];
  return parseCsvLine(firstLine);
}

function readRows(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] || "";
    });
    return row;
  });
}

function hasColumns(headers, required) {
  const set = new Set(headers);
  return required.filter((c) => !set.has(c));
}

function listRealtorFiles() {
  if (!fs.existsSync(REALTOR_DIR)) return [];
  return fs.readdirSync(REALTOR_DIR).filter((n) => /\.csv$/i.test(n)).sort();
}

function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function updateReport(result) {
  let report = {};
  if (fs.existsSync(REPORT_FILE)) {
    try {
      report = JSON.parse(fs.readFileSync(REPORT_FILE, "utf8"));
    } catch (err) {
      report = {};
    }
  }
  report.validation = result;
  fs.writeFileSync(REPORT_FILE, `${JSON.stringify(report, null, 2)}\n`);
}

function main() {
  const errors = [];
  const warnings = [];

  if (!fs.existsSync(PUBLIC_FILE)) errors.push(`Missing file: ${PUBLIC_FILE}`);
  if (!fs.existsSync(ENRICHED_FILE)) errors.push(`Missing file: ${ENRICHED_FILE}`);

  const realtorFiles = listRealtorFiles();
  if (!realtorFiles.length) {
    warnings.push(`No realtor CSV files found in ${REALTOR_DIR}. MLS enrichment may be stale.`);
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    status: "fail",
    errors: [],
    warnings: [],
    files: {
      public: path.basename(PUBLIC_FILE),
      enriched: path.basename(ENRICHED_FILE),
      realtorCsvCount: realtorFiles.length,
    },
    counts: {},
  };

  if (!errors.length) {
    const publicHeaders = readHeader(PUBLIC_FILE);
    const enrichedHeaders = readHeader(ENRICHED_FILE);
    const missingPublic = hasColumns(publicHeaders, REQUIRED_PUBLIC_COLUMNS);
    const missingEnriched = hasColumns(enrichedHeaders, REQUIRED_ENRICHED_COLUMNS);
    if (missingPublic.length) errors.push(`Public dataset missing columns: ${missingPublic.join(", ")}`);
    if (missingEnriched.length) errors.push(`Enriched dataset missing columns: ${missingEnriched.join(", ")}`);

    if (!missingEnriched.length) {
      const rows = readRows(ENRICHED_FILE);
      const modes = rows.reduce((acc, row) => {
        const mode = String(row.dataMode || "UNKNOWN");
        acc[mode] = (acc[mode] || 0) + 1;
        return acc;
      }, {});
      const mlsRows = rows.filter((r) => String(r.dataMode || "") === "MLS_ENRICHED");
      const mlsSoldRows = mlsRows.filter((r) => num(r.closePrice) > 0);
      const mlsOpenRows = mlsRows.filter((r) => num(r.closePrice) <= 0);
      const mlsActiveRows = mlsRows.filter(
        (r) => String(r.mlsStatus || "").trim().toUpperCase() === "ACTIVE" && num(r.closePrice) <= 0
      );
      const activeMissingAsk = mlsActiveRows.filter((r) => num(r.mlsListingPrice || r.listPriceAtPending) <= 0).length;

      summary.counts = {
        outputRows: rows.length,
        modes,
        mlsRows: mlsRows.length,
        mlsSoldRows: mlsSoldRows.length,
        mlsOpenRows: mlsOpenRows.length,
        mlsActiveRows: mlsActiveRows.length,
      };

      if (activeMissingAsk > 0) {
        errors.push(`Found ${activeMissingAsk} active MLS rows without listing/pending ask.`);
      }
      if (!mlsRows.length) {
        errors.push("No MLS_ENRICHED rows found in enriched dataset.");
      }
      if (!mlsSoldRows.length) {
        warnings.push("No MLS sold rows found. Bid model comp pool may be empty.");
      }
    }
  }

  summary.errors = errors;
  summary.warnings = warnings;
  summary.status = errors.length ? "fail" : "pass";
  updateReport(summary);

  // eslint-disable-next-line no-console
  console.log(`Validation status: ${summary.status.toUpperCase()}`);
  if (summary.counts && Object.keys(summary.counts).length) {
    // eslint-disable-next-line no-console
    console.log(`Counts: ${JSON.stringify(summary.counts)}`);
  }
  if (warnings.length) {
    // eslint-disable-next-line no-console
    console.warn(`Warnings:\n- ${warnings.join("\n- ")}`);
  }
  if (errors.length) {
    // eslint-disable-next-line no-console
    console.error(`Errors:\n- ${errors.join("\n- ")}`);
    process.exitCode = 1;
  }
}

main();
