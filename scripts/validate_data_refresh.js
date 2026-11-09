#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const { extractUnitFromAddress, houseNumber, zip5 } = require("./redfin_address_key.js");

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

function readExistingReport() {
  if (!fs.existsSync(REPORT_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(REPORT_FILE, "utf8"));
  } catch (err) {
    return {};
  }
}

function reportRealtorFiles(report) {
  const files = report?.source?.realtorFiles;
  return Array.isArray(files) ? files.filter(Boolean) : [];
}

function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

// ---------------------------------------------------------------------------
// Content checks. The checks above prove the file has the right shape; these
// ask whether the numbers inside still mean anything. Each one is a failure
// that has actually shipped while validation said "pass, no warnings":
//   - Redfin dropped days-on-market from the actives feed (2026-09-21) and every
//     listing read as "listed today, 0 days" for seven weeks.
//   - That starved recent sales of a days-on-market reading as well.
//   - King County sat five months behind while the file rebuilt every morning.
//   - 417 sales were in the file twice, once from the county and once from Redfin.
// They are warnings, not errors: a stale or thin source should be said out loud
// on the Data tab, not stop the fresher sources from deploying.
const QUALITY_LIMITS = {
  activeZeroDomShare: 0.25, // a normal week lists ~8% of standing inventory
  activeMissingDomShare: 0.25,
  recentSaleWindowDays: 60,
  recentSaleMinRows: 20,
  recentSaleFieldShare: 0.6,
  countyMaxAgeDays: 60,
  duplicateSaleShare: 0.01,
};

function pct(share) {
  return `${Math.round(share * 100)}%`;
}

function ageDays(isoDate, today) {
  const gap = (Date.parse(today) - Date.parse(isoDate)) / 86400000;
  return Number.isFinite(gap) ? Math.round(gap) : null;
}

function dataQualityWarnings(rows, { today = new Date().toISOString().slice(0, 10), limits = QUALITY_LIMITS } = {}) {
  const warnings = [];
  const list = Array.isArray(rows) ? rows : [];
  const isMls = (r) => String(r.dataMode || "") === "MLS_ENRICHED";

  const actives = list.filter((r) => isMls(r) && String(r.mlsStatus || "").trim().toUpperCase() === "ACTIVE" && num(r.closePrice) <= 0);
  if (actives.length) {
    const domText = (r) => String(r.mlsCDOM ?? "").trim() || String(r.mlsDOM ?? "").trim();
    const zeroShare = actives.filter((r) => domText(r) !== "" && num(domText(r)) === 0).length / actives.length;
    const missingShare = actives.filter((r) => domText(r) === "").length / actives.length;
    if (zeroShare > limits.activeZeroDomShare) {
      warnings.push(`${pct(zeroShare)} of active listings show 0 days on market, which cannot be real. The listings feed has probably stopped reporting it`);
    }
    if (missingShare > limits.activeMissingDomShare) {
      warnings.push(`${pct(missingShare)} of active listings have no days-on-market reading, so bid suggestions cannot tell a fresh listing from a stale one`);
    }
  }

  const sold = list.filter((r) => num(r.closePrice) > 0 && /^\d{4}-\d{2}-\d{2}/.test(String(r.saleDate || "")));
  const recent = sold.filter((r) => {
    const age = ageDays(r.saleDate, today);
    return isMls(r) && age !== null && age >= 0 && age <= limits.recentSaleWindowDays;
  });
  if (recent.length >= limits.recentSaleMinRows) {
    const askShare = recent.filter((r) => num(r.mlsListingPrice) > 0).length / recent.length;
    const domShare = recent.filter((r) => String(r.mlsCDOM ?? "").trim() !== "" || String(r.mlsDOM ?? "").trim() !== "" || String(r.mlsDaysToPending ?? "").trim() !== "").length / recent.length;
    if (askShare < limits.recentSaleFieldShare) {
      warnings.push(`only ${pct(askShare)} of sales from the last ${limits.recentSaleWindowDays} days carry an asking price, so sale-versus-ask reads rest on a thin sample`);
    }
    if (domShare < limits.recentSaleFieldShare) {
      warnings.push(`only ${pct(domShare)} of sales from the last ${limits.recentSaleWindowDays} days carry days on market, so fast-sale share and median days are going dark`);
    }
  }

  const countyNewest = sold.filter((r) => String(r.dataMode || "") === "PUBLIC_PROXY").map((r) => String(r.saleDate).slice(0, 10)).sort().pop();
  const countyAge = countyNewest ? ageDays(countyNewest, today) : null;
  if (countyAge !== null && countyAge > limits.countyMaxAgeDays) {
    warnings.push(`the newest King County sale is ${countyNewest} (${countyAge} days old). Months after it count Redfin sales only, so monthly sales volume looks lower than it was. Run npm run refresh:kc-fresh`);
  }

  // Same house number, ZIP and price within 60 days, under DIFFERENT record
  // ids. One county sale spanning several parcels repeats its excise id on every
  // parcel row, and townhouse neighbours can share a price; neither is a
  // duplicate the merges could have prevented, so neither may hold this on.
  const seen = new Map();
  let duplicates = 0;
  for (const r of sold) {
    const house = houseNumber(r.address);
    if (!house) continue;
    const key = `${house}|${Math.round(num(r.closePrice))}|${zip5(r.zip)}`;
    const held = seen.get(key) || [];
    const unit = extractUnitFromAddress(r.address);
    const twin = held.find((h) => (
      h.id !== String(r.id || "")
      && Math.abs(ageDays(h.date, r.saleDate) ?? 999) <= 60
      && (!h.unit || !unit || h.unit === unit)
    ));
    if (twin) duplicates += 1;
    held.push({ id: String(r.id || ""), date: String(r.saleDate).slice(0, 10), unit });
    seen.set(key, held);
  }
  if (sold.length && duplicates / sold.length > limits.duplicateSaleShare) {
    warnings.push(`${duplicates} closed sales look like duplicates (same house number, ZIP and price within 60 days)`);
  }

  return warnings;
}

function updateReport(result, report = readExistingReport()) {
  report.validation = result;
  fs.writeFileSync(REPORT_FILE, `${JSON.stringify(report, null, 2)}\n`);
}

function main() {
  const errors = [];
  const warnings = [];

  if (!fs.existsSync(PUBLIC_FILE)) errors.push(`Missing file: ${PUBLIC_FILE}`);
  if (!fs.existsSync(ENRICHED_FILE)) errors.push(`Missing file: ${ENRICHED_FILE}`);

  const existingReport = readExistingReport();
  const realtorFiles = listRealtorFiles();
  const manifestedRealtorFiles = reportRealtorFiles(existingReport);
  const realtorCsvCount = realtorFiles.length || manifestedRealtorFiles.length;
  if (!realtorCsvCount) {
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
      realtorCsvCount,
      realtorSource: realtorFiles.length ? "local" : (manifestedRealtorFiles.length ? "report-manifest" : "missing"),
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
      warnings.push(...dataQualityWarnings(rows));
    }
  }

  summary.errors = errors;
  summary.warnings = warnings;
  summary.status = errors.length ? "fail" : "pass";
  updateReport(summary, existingReport);

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
  return summary;
}

if (require.main === module) {
  main();
}

module.exports = {
  main,
  parseCsvLine,
  hasColumns,
  num,
  readHeader,
  readRows,
  reportRealtorFiles,
  dataQualityWarnings,
  QUALITY_LIMITS,
};
