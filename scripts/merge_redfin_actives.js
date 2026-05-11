#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_REDFIN = path.join(PROJECT_DIR, "redfin_active_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_merge_report.json");

const ZIP_NEIGHBORHOOD = {
  "98101": "Downtown",
  "98102": "Capitol Hill / Eastlake",
  "98103": "Fremont / Green Lake / Wallingford",
  "98104": "Pioneer Square / International District",
  "98105": "University District / Laurelhurst",
  "98106": "Delridge / South Park",
  "98107": "Ballard / Crown Hill",
  "98108": "Georgetown / South Park",
  "98109": "South Lake Union / Queen Anne",
  "98112": "Capitol Hill / Madison Park",
  "98115": "Ravenna / Wedgwood",
  "98116": "West Seattle",
  "98117": "Ballard / Crown Hill",
  "98118": "Columbia City / Rainier Valley",
  "98119": "Queen Anne / Magnolia",
  "98121": "Belltown",
  "98122": "Capitol Hill / Central District",
  "98125": "Lake City / North Seattle",
  "98126": "West Seattle / Delridge",
  "98133": "Northgate / Bitter Lake",
  "98134": "SoDo",
  "98136": "West Seattle / Fauntleroy",
  "98144": "Mount Baker / Central District",
  "98177": "North Beach / Crown Hill",
  "98199": "Magnolia",
};

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

function zip5(value) {
  return (String(value || "").match(/[0-9]{5}/) || [])[0] || "";
}

function zipNeighborhood(zip) {
  return ZIP_NEIGHBORHOOD[zip5(zip)] || "";
}

function uiPropertyTypeToAppType(uipt) {
  switch (Number(uipt)) {
    case 1: return { type: "Single Family", typeCode: "11" };
    case 2: return { type: "Residential condominiums", typeCode: "14" };
    case 3: return { type: "Townhouse", typeCode: "32" };
    case 4: return { type: "Multi-Family", typeCode: "12" };
    case 5: return { type: "Vacant Land", typeCode: "20" };
    default: return { type: "Other", typeCode: "" };
  }
}

function num(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function isoToday() {
  return new Date().toISOString().slice(0, 10);
}

function listDateFromDom(domDays) {
  const days = Math.max(0, Math.round(num(domDays)));
  if (!days) return isoToday();
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

function normalizeAddressKey(value) {
  return String(value || "").toUpperCase().replace(/[.,#]/g, " ").replace(/\s+/g, " ").trim();
}

function buildEnrichedRowFromRedfin(redfin, headers, fetchedAt) {
  const row = {};
  for (const h of headers) row[h] = "";

  const listPrice = Math.round(num(redfin.listPrice));
  const dom = num(redfin.domDays);
  const zip = zip5(redfin.zip);
  const typeInfo = uiPropertyTypeToAppType(redfin.uiPropertyType);
  const idBase = redfin.redfinListingId || redfin.mlsListingNumber || `${redfin.address || "x"}|${zip}`;

  row.dataMode = "MLS_ENRICHED";
  row.id = `redfin-active-${idBase}`;
  row.address = redfin.address || "";
  row.neighborhood = zipNeighborhood(zip) || redfin.neighborhood || "Seattle (Other)";
  row.type = typeInfo.type;
  row.typeCode = typeInfo.typeCode;
  row.addressSource = "REDFIN_ACTIVE";
  row.major = "";
  row.minor = "";
  row.parcelNbr = "";
  row.listDate = listDateFromDom(dom);
  row.pendingDate = "";
  row.saleDate = "";
  row.listPriceAtPending = listPrice > 0 ? String(listPrice) : "";
  row.closePrice = "";
  row.assessedValue = "";
  row.beds = redfin.beds || "";
  row.baths = redfin.baths || "";
  row.sqft = redfin.sqft || "";
  row.yearBuilt = redfin.yearBuilt || "";
  row.zip = zip;
  row.districtName = "Seattle";
  row.area = "";
  row.subArea = "";
  row.sqFtLot = redfin.lotSize || "";
  row.zoning = "";
  row.lat = redfin.lat || "";
  row.lon = redfin.lon || "";

  row.mlsListDate = row.listDate;
  row.mlsPendingDate = "";
  row.mlsListPriceAtPending = listPrice > 0 ? String(listPrice) : "";
  row.mlsClosePrice = "";
  row.mlsListingNumber = redfin.mlsListingNumber || "";
  row.mlsStatus = "Active";
  row.mlsRegion = redfin.queryLabel || "";
  row.mlsSellingDate = "";
  row.mlsContractualDate = "";
  row.mlsListingPrice = listPrice > 0 ? String(listPrice) : "";
  row.mlsSellingPrice = "";
  row.mlsOriginalPrice = listPrice > 0 ? String(listPrice) : "";
  row.mlsDOM = String(Math.round(dom));
  row.mlsCDOM = String(Math.round(dom));
  row.mlsStyleCode = "";
  row.mlsSubdivision = redfin.neighborhood || "";
  row.mlsParkingType = "";
  row.mlsParkingCoveredTotal = "";
  row.mlsTaxesAnnual = "";
  row.mlsBuildingCondition = "";
  row.mlsView = "";
  row.mlsBankOwned = "";
  row.mlsThirdPartyApprovalRequired = "";
  row.mlsNewConstructionState = String(redfin.isNewConstruction || "false") === "true" ? "New" : "";
  row.mlsSquareFootageSource = "Redfin";
  row.mlsDateLagDays = "0";
  row.mlsJoinMethod = "REDFIN_ACTIVE";
  row.mlsDaysToPending = "";
  row.mlsDaysPendingToSale = "";

  row.hotMarketTag = String(redfin.isHot || "false") === "true" ? "REDFIN_HOT" : "";
  row.saleToListRatio = "";
  row.saleToOriginalListRatio = "";
  row.bidUpAmount = "";
  row.bidUpPct = "";

  return row;
}

function indexBy(rows, keyFn) {
  const map = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!key) continue;
    if (!map.has(key)) map.set(key, row);
  }
  return map;
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
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
    else throw new Error(`Unknown argument: ${a}`);
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/merge_redfin_actives.js [options]",
    "",
    "Merges Redfin active listings into the MLS-enriched dataset so they appear",
    "in the dashboard's MLS_ENRICHED Active rows alongside realtor-fed data.",
    "",
    "Options:",
    "  --enriched FILE   Enriched CSV (default: public_sales_proxy_mls_enriched_last12mo.csv)",
    "  --redfin FILE     Redfin actives CSV (default: redfin_active_listings.csv)",
    "  --out FILE        Output CSV (default: same as --enriched, replaced in-place)",
    "  --report FILE     Merge report JSON (default: redfin_merge_report.json)",
    "  --dry-run         Compute but do not write outputs",
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
    process.stderr.write(`Redfin actives CSV not found: ${redfinPath}\n`);
    process.stderr.write("Run `npm run fetch:actives` first to generate it.\n");
    process.exit(1);
  }

  const { headers: enrichedHeaders, rows: enrichedRows } = readCsv(enrichedPath);
  const { rows: redfinRows } = readCsv(redfinPath);

  // Strip prior REDFIN_ACTIVE rows so reruns don't duplicate.
  const cleanedExisting = enrichedRows.filter((r) => r.mlsJoinMethod !== "REDFIN_ACTIVE");
  const removed = enrichedRows.length - cleanedExisting.length;

  // Index existing rows by MLS# and by normalized address+zip for augmentation.
  const byMls = indexBy(cleanedExisting, (r) => (r.mlsListingNumber || "").trim());
  const byAddrZip = indexBy(cleanedExisting, (r) => {
    const a = normalizeAddressKey(r.address);
    const z = zip5(r.zip);
    return a && z ? `${a}|${z}` : "";
  });

  const fetchedAt = new Date().toISOString();
  const newRows = [];
  let augmentedCount = 0;
  let appendedCount = 0;
  let skippedCount = 0;
  const augmentedExamples = [];
  const appendedByRegion = {};

  for (const r of redfinRows) {
    if (!r.mlsStatus || /sold|closed|expired|cancelled/i.test(r.mlsStatus)) {
      skippedCount += 1;
      continue;
    }
    const mlsKey = (r.mlsListingNumber || "").trim();
    const addrKey = (() => {
      const a = normalizeAddressKey(r.address);
      const z = zip5(r.zip);
      return a && z ? `${a}|${z}` : "";
    })();

    let existing = mlsKey ? byMls.get(mlsKey) : null;
    if (!existing && addrKey) existing = byAddrZip.get(addrKey);

    if (existing) {
      const listPrice = Math.round(num(r.listPrice));
      let augmented = false;
      if (listPrice > 0 && (!num(existing.mlsListingPrice) || existing.mlsJoinMethod === "MLS_SOLD_NOT_IN_COUNTY")) {
        existing.mlsListingPrice = String(listPrice);
        if (!num(existing.mlsListPriceAtPending)) existing.mlsListPriceAtPending = String(listPrice);
        if (!num(existing.listPriceAtPending)) existing.listPriceAtPending = String(listPrice);
        if (!num(existing.mlsOriginalPrice)) existing.mlsOriginalPrice = String(listPrice);
        augmented = true;
      }
      if (r.lat && !existing.lat) { existing.lat = r.lat; augmented = true; }
      if (r.lon && !existing.lon) { existing.lon = r.lon; augmented = true; }
      if (r.mlsStatus && existing.mlsStatus !== r.mlsStatus) {
        existing.mlsStatus = r.mlsStatus;
        augmented = true;
      }
      if (augmented) {
        augmentedCount += 1;
        if (augmentedExamples.length < 5) augmentedExamples.push(`${existing.address} (MLS# ${mlsKey || "—"})`);
      }
      continue;
    }

    const newRow = buildEnrichedRowFromRedfin(r, enrichedHeaders, fetchedAt);
    newRows.push(newRow);
    appendedCount += 1;
    appendedByRegion[r.queryLabel || "Unknown"] = (appendedByRegion[r.queryLabel || "Unknown"] || 0) + 1;
  }

  const finalRows = [...cleanedExisting, ...newRows];
  const report = {
    mergedAt: fetchedAt,
    enrichedInput: path.relative(PROJECT_DIR, enrichedPath),
    redfinInput: path.relative(PROJECT_DIR, redfinPath),
    rowsBefore: enrichedRows.length,
    priorRedfinActiveRowsRemoved: removed,
    redfinRowsRead: redfinRows.length,
    redfinRowsSkippedNonActive: skippedCount,
    augmentedExisting: augmentedCount,
    appended: appendedCount,
    rowsAfter: finalRows.length,
    appendedByRegion,
    augmentedExamples,
  };

  if (opts.dryRun) {
    console.log("Dry run.");
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  writeCsv(outPath, enrichedHeaders, finalRows);
  fs.writeFileSync(reportPath, `${JSON.stringify({ ...report, outFile: path.relative(PROJECT_DIR, outPath) }, null, 2)}\n`);
  console.log(`Augmented ${augmentedCount} existing rows, appended ${appendedCount} new Redfin actives.`);
  console.log(`Output: ${path.relative(PROJECT_DIR, outPath)}  (${finalRows.length} rows)`);
  console.log(`Report: ${path.relative(PROJECT_DIR, reportPath)}`);
}

if (require.main === module) {
  try { main(); }
  catch (err) { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); }
}

module.exports = {
  buildEnrichedRowFromRedfin,
  uiPropertyTypeToAppType,
  zipNeighborhood,
  normalizeAddressKey,
};
