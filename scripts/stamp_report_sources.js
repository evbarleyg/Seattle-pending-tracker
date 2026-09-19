#!/usr/bin/env node
"use strict";

// Writes a per-source freshness block into data_refresh_report.json so the app
// header can state when each feed was actually fetched instead of inferring
// it from row dates (a morning with no brand-new listing otherwise reads a
// day stale). Additive and merge-only: every other key in the report is kept,
// and validate_data_refresh.js merges its own `validation` block the same way.
//
// Contract (docs/superpowers/COORDINATION_2026-09-19.md, section 7): the app
// reads `sources.redfinActives.fetchedAt` (ISO timestamp). The other entries
// are provided for the Data tab's "where each source stands" table.

const fs = require("fs");
const path = require("path");
const { readCsvText } = require("./csv_util.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const REPORT_FILE = path.join(PROJECT_DIR, "data_refresh_report.json");
const ACTIVES_REPORT = path.join(PROJECT_DIR, "redfin_fetch_report.json");
const SOLD_REPORTS = ["redfin_sold_recent_report.json", "redfin_sold_report.json"].map((f) => path.join(PROJECT_DIR, f));
const SOLD_CUMULATIVE = path.join(PROJECT_DIR, "redfin_sold_cumulative.csv");
const ACTIVE_LEDGER = path.join(PROJECT_DIR, "redfin_active_ledger.csv");
const ENRICHMENT_LEDGER = path.join(PROJECT_DIR, "redfin_sold_enrichment_ledger.csv");
const ENRICHMENT_REPORT = path.join(PROJECT_DIR, "redfin_sold_enrichment_report.json");

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}

function csvRowsAndMax(file, column) {
  if (!fs.existsSync(file)) return null;
  const { rows } = readCsvText(fs.readFileSync(file, "utf8"));
  let max = "";
  for (const r of rows) { const v = String(r[column] || ""); if (v > max) max = v; }
  return { rows: rows.length, max };
}

// Pure: build the sources block from whatever inputs exist.
function buildSources({ activesReport, soldReport, soldCumulative, activeLedger, enrichmentLedger, enrichmentReport, now }) {
  const sources = {};
  if (activesReport && activesReport.fetchedAt) {
    sources.redfinActives = { fetchedAt: activesReport.fetchedAt, rows: Number(activesReport.totalRows || 0) };
  }
  if (soldReport && soldReport.fetchedAt) {
    sources.redfinSold = {
      fetchedAt: soldReport.fetchedAt,
      rowsFetched: Number(soldReport.totalRows || 0),
      windowDays: Number(soldReport.soldWithinDays || 0),
      newestSaleDate: (soldReport.soldDateRange && soldReport.soldDateRange.max) || null,
    };
  }
  if (soldCumulative) {
    sources.redfinSold = { ...(sources.redfinSold || {}), cumulativeRows: soldCumulative.rows, cumulativeNewestSaleDate: soldCumulative.max || null };
  }
  if (activeLedger) {
    sources.activeLedger = { rows: activeLedger.rows, lastSeen: activeLedger.max || null };
  }
  if (enrichmentLedger) {
    sources.soldEnrichmentLedger = {
      rows: enrichmentLedger.rows,
      newestSaleDate: enrichmentLedger.max || null,
      appliedAt: (enrichmentReport && enrichmentReport.generatedAt) || null,
      appliedRows: (enrichmentReport && enrichmentReport.apply && enrichmentReport.apply.applied) || null,
    };
  }
  sources.stampedAt = now || new Date().toISOString();
  return sources;
}

function main() {
  const report = readJson(REPORT_FILE);
  if (!report) {
    console.error(`No ${path.relative(PROJECT_DIR, REPORT_FILE)} to stamp.`);
    process.exit(1);
  }
  const soldReportFile = SOLD_REPORTS.find((f) => fs.existsSync(f));
  const sources = buildSources({
    activesReport: readJson(ACTIVES_REPORT),
    soldReport: soldReportFile ? readJson(soldReportFile) : null,
    soldCumulative: csvRowsAndMax(SOLD_CUMULATIVE, "soldDate"),
    activeLedger: csvRowsAndMax(ACTIVE_LEDGER, "lastSeen"),
    enrichmentLedger: csvRowsAndMax(ENRICHMENT_LEDGER, "saleDate"),
    enrichmentReport: readJson(ENRICHMENT_REPORT),
  });
  report.sources = { ...(report.sources || {}), ...sources };
  fs.writeFileSync(REPORT_FILE, `${JSON.stringify(report, null, 2)}\n`);
  const a = sources.redfinActives ? sources.redfinActives.fetchedAt : "n/a";
  const s = sources.redfinSold ? (sources.redfinSold.fetchedAt || "n/a") : "n/a";
  console.log(`Stamped report.sources: actives fetched ${a}; sold fetched ${s}; ledger ${sources.activeLedger ? sources.activeLedger.rows : 0} rows; enrichment ${sources.soldEnrichmentLedger ? sources.soldEnrichmentLedger.rows : 0} rows.`);
}

module.exports = { buildSources };

if (require.main === module) main();
