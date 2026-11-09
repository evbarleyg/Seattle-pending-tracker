#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_REDFIN = path.join(PROJECT_DIR, "redfin_active_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_merge_report.json");
// The actives ledger (build_active_ledger.js) remembers each MLS#'s list date,
// opening ask and relist chain, which the feed alone no longer provides.
const DEFAULT_LEDGER = path.join(PROJECT_DIR, "redfin_active_ledger.csv");
// One definition each of "which Seattle day is this fetch" and "which home is
// this address", shared with the ledger so the two can never disagree.
const { toPacificDate, shiftDate, chainAddressKey } = require("./build_active_ledger.js");

const ZIP_NEIGHBORHOOD = {
  "98101": "Downtown",
  "98102": "Capitol Hill / Eastlake",
  "98103": "Fremont / Green Lake / Wallingford",
  "98104": "Pioneer Square / International District",
  "98105": "University District / Laurelhurst",
  "98106": "Delridge / South Park",
  "98107": "Ballard", // matches src/domain/data.mjs and the county rows; 98117 is Ballard / Crown Hill
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

function daysSince(isoDate, asOfDate) {
  const gap = (Date.parse(asOfDate) - Date.parse(isoDate)) / 86400000;
  return Number.isFinite(gap) && gap >= 0 ? Math.round(gap) : null;
}

// List date, days on market and opening ask for one active listing.
//
// A blank feed DOM means "not reported", never "listed today": Redfin stopped
// returning DOM for ~99% of actives on 2026-09-21, and reading that blank as 0
// stamped every listing as brand new for seven weeks (each one scored as
// ultra-hot, and its identity in the "what changed" feed moved every morning).
// Order of trust: the feed's own DOM, then the ledger's list date, then blank.
// Cumulative DOM adds the days earlier listings in the ledger's relist chain spent
// on the market, and the original ask is the chain's, so a cancel-and-relist
// under a new MLS# resets neither.
function resolveListingTimeline(redfin, ledgerEntry, asOfDate) {
  const listPrice = Math.round(num(redfin.listPrice));
  const feedDom = String(redfin.domDays ?? "").trim();
  let listDate = "";
  let dom = null;
  if (/^coming soon$/i.test(String(redfin.mlsStatus || "").trim())) {
    // Not on the market yet: a true zero, and no earlier listing's clock applies.
    return { listDate: "", dom: "0", cdom: "0", originalPrice: listPrice > 0 ? String(listPrice) : "" };
  }
  if (feedDom !== "" && num(feedDom) >= 0) {
    dom = Math.round(num(feedDom));
    listDate = shiftDate(asOfDate, -dom);
  } else if (ledgerEntry?.listDate) {
    dom = daysSince(ledgerEntry.listDate, asOfDate);
    if (dom !== null) listDate = ledgerEntry.listDate;
  }
  const cdom = dom === null ? null : dom + Math.max(0, Math.round(num(ledgerEntry?.chainPriorDays)));
  const openingAsk = Math.round(num(ledgerEntry?.chainFirstAsk) || num(ledgerEntry?.firstAsk)) || listPrice;
  return {
    listDate,
    dom: dom === null ? "" : String(dom),
    cdom: cdom === null ? "" : String(cdom),
    originalPrice: openingAsk > 0 ? String(openingAsk) : "",
  };
}

function loadLedgerIndex(ledgerPath) {
  const map = new Map();
  if (!ledgerPath || !fs.existsSync(ledgerPath)) return map;
  for (const row of readCsv(ledgerPath).rows) {
    const mls = String(row.mlsNumber || "").trim();
    if (mls) map.set(mls, row);
  }
  return map;
}

function normalizeAddressKey(value) {
  return String(value || "").toUpperCase().replace(/[.,#]/g, " ").replace(/\s+/g, " ").trim();
}

function buildEnrichedRowFromRedfin(redfin, headers, fetchedAt, ledgerEntry = null) {
  const row = {};
  for (const h of headers) row[h] = "";

  const listPrice = Math.round(num(redfin.listPrice));
  const asOfDate = toPacificDate(redfin.fetchedAt) || toPacificDate(fetchedAt) || toPacificDate(new Date().toISOString());
  const timeline = resolveListingTimeline(redfin, ledgerEntry, asOfDate);
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
  row.listDate = timeline.listDate;
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
  // Under contract is still in the feed but no longer for sale, so it must not
  // reach the Bids queue as an active listing.
  row.mlsStatus = /under contract/i.test(String(redfin.mlsStatus || "")) ? "Active Under Contract" : "Active";
  row.mlsRegion = redfin.queryLabel || "";
  row.mlsSellingDate = "";
  row.mlsContractualDate = "";
  row.mlsListingPrice = listPrice > 0 ? String(listPrice) : "";
  row.mlsSellingPrice = "";
  row.mlsOriginalPrice = timeline.originalPrice;
  row.mlsDOM = timeline.dom;
  row.mlsCDOM = timeline.cdom;
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

// Additive columns on active rows: the home's most recent closing already in the
// dataset, so a flip or a quick resale shows what the seller paid.
const PRIOR_SALE_COLUMNS = ["priorSaleDate", "priorSalePrice"];
// Outside this ask-to-prior-price range the earlier record is almost always a
// different thing (a builder's multi-lot purchase, a partial-interest deed).
const PRIOR_SALE_MIN_RATIO = 0.5;
const PRIOR_SALE_MAX_RATIO = 3.5;

function homeKey(address, zip) {
  return chainAddressKey({ address, zip });
}

// Closed sales per home (street + ZIP + unit), oldest first.
function indexPriorSales(rows) {
  const map = new Map();
  for (const r of rows) {
    const price = num(r.closePrice);
    const date = String(r.saleDate || "").slice(0, 10);
    if (!(price > 0) || !date) continue;
    const key = homeKey(r.address, r.zip);
    if (!key) continue;
    const sales = map.get(key);
    if (sales) sales.push({ date, price: Math.round(price) }); else map.set(key, [{ date, price: Math.round(price) }]);
  }
  for (const sales of map.values()) sales.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  return map;
}

// `listedSince` is the start of the current marketing period. A deed recorded
// after the home was already for sale (a transfer into an LLC, a partial
// interest) is not what the seller paid to acquire it.
function applyPriorSale(activeRow, priorSales, listedSince = "") {
  const sales = priorSales.get(homeKey(activeRow.address, activeRow.zip)) || [];
  const prior = sales.filter((sale) => !listedSince || sale.date < listedSince).pop();
  const ask = num(activeRow.mlsListingPrice);
  const ratio = prior && ask > 0 ? ask / prior.price : 0;
  const usable = ratio >= PRIOR_SALE_MIN_RATIO && ratio <= PRIOR_SALE_MAX_RATIO;
  activeRow.priorSaleDate = usable ? prior.date : "";
  activeRow.priorSalePrice = usable ? String(prior.price) : "";
  return usable;
}

function hasClosed(row) {
  return num(row.closePrice) > 0 || num(row.mlsClosePrice) > 0;
}

function openRowAtAddress(row) {
  return row && !hasClosed(row) ? row : null;
}

// Returns true when an open (never closed) row's ask was moved to today's.
function refreshOpenRowAsk(existing, listPrice) {
  if (!(listPrice > 0) || hasClosed(existing)) return false;
  const previous = num(existing.mlsListingPrice);
  if (!(previous > 0) || previous === listPrice) return false;
  if (!num(existing.mlsOriginalPrice)) existing.mlsOriginalPrice = String(previous);
  existing.mlsListingPrice = String(listPrice);
  existing.mlsListPriceAtPending = String(listPrice);
  existing.listPriceAtPending = String(listPrice);
  return true;
}

// An open row carried over from a realtor export holds the days on market of the
// day it was exported. Days on market now picks the comps behind a bid, so it is
// recounted: from the row's own MLS list date where it has one, else the ledger's.
function refreshOpenRowTimeline(existing, redfin, ledgerEntry, asOfDate) {
  if (hasClosed(existing)) return false;
  const ownListDate = String(existing.mlsListDate || "").trim();
  const timeline = resolveListingTimeline(redfin, ownListDate ? { ...ledgerEntry, listDate: ownListDate } : ledgerEntry, asOfDate);
  if (timeline.dom === "") return false;
  // The export's CDOM minus DOM is time an earlier listing accrued. It is kept as
  // an offset, which survives reruns because both figures advance together.
  const earlierListings = Math.max(0, num(existing.mlsCDOM) - num(existing.mlsDOM));
  const cdom = String(Math.max(num(timeline.cdom), num(timeline.dom) + earlierListings));
  if (existing.mlsDOM === timeline.dom && existing.mlsCDOM === cdom) return false;
  existing.mlsDOM = timeline.dom;
  existing.mlsCDOM = cdom;
  return true;
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
    else if (a === "--ledger") { opts.ledger = next; i += 1; }
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
    "  --ledger FILE     Actives ledger for list dates, relist chains and opening asks",
    "                    (default: redfin_active_ledger.csv; run ledger:upsert first)",
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
  const ledgerIndex = loadLedgerIndex(path.resolve(opts.ledger || DEFAULT_LEDGER));

  // Strip prior REDFIN_ACTIVE rows so reruns don't duplicate.
  const cleanedExisting = enrichedRows.filter((r) => r.mlsJoinMethod !== "REDFIN_ACTIVE");
  const removed = enrichedRows.length - cleanedExisting.length;

  // Index existing rows by MLS# and by normalized address+zip for augmentation.
  const byMls = indexBy(cleanedExisting, (r) => (r.mlsListingNumber || "").trim());
  const byAddrZip = indexBy(cleanedExisting.filter((r) => !hasClosed(r)), (r) => {
    const a = normalizeAddressKey(r.address);
    const z = zip5(r.zip);
    return a && z ? `${a}|${z}` : "";
  });

  for (const column of PRIOR_SALE_COLUMNS) {
    if (!enrichedHeaders.includes(column)) enrichedHeaders.push(column);
  }
  const priorSales = indexPriorSales(cleanedExisting);
  let priorSaleCount = 0;

  const fetchedAt = new Date().toISOString();
  const newRows = [];
  let augmentedCount = 0;
  let appendedCount = 0;
  let skippedCount = 0;
  let missingDomCount = 0;
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
    // An address match only means "same listing" while that row is still open.
    // A row that already closed is the home's PREVIOUS sale: treating today's
    // listing as that row hid every resale (a flip bought in April and relisted
    // in November never reached the Bids queue) and overwrote the old sale's status.
    if (!existing && addrKey) existing = openRowAtAddress(byAddrZip.get(addrKey));

    if (existing) {
      const listPrice = Math.round(num(r.listPrice));
      let augmented = false;
      // A row that has not closed is still this listing, so today's ask wins
      // over the one frozen in an old realtor export (mlsOriginalPrice keeps
      // the earlier figure, so the cut stays visible).
      if (refreshOpenRowAsk(existing, listPrice)) {
        augmented = true;
      } else if (listPrice > 0 && (!num(existing.mlsListingPrice) || existing.mlsJoinMethod === "MLS_SOLD_NOT_IN_COUNTY")) {
        existing.mlsListingPrice = String(listPrice);
        if (!num(existing.mlsListPriceAtPending)) existing.mlsListPriceAtPending = String(listPrice);
        if (!num(existing.listPriceAtPending)) existing.listPriceAtPending = String(listPrice);
        if (!num(existing.mlsOriginalPrice)) existing.mlsOriginalPrice = String(listPrice);
        augmented = true;
      }
      if (refreshOpenRowTimeline(existing, r, ledgerIndex.get(mlsKey) || null, toPacificDate(r.fetchedAt) || toPacificDate(fetchedAt))) augmented = true;
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

    const ledgerEntry = ledgerIndex.get(mlsKey) || null;
    const newRow = buildEnrichedRowFromRedfin(r, enrichedHeaders, fetchedAt, ledgerEntry);
    if (newRow.mlsDOM === "") missingDomCount += 1;
    if (applyPriorSale(newRow, priorSales, ledgerEntry?.chainListDate || newRow.mlsListDate)) priorSaleCount += 1;
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
    appendedWithoutDom: missingDomCount,
    appendedWithPriorSale: priorSaleCount,
    ledgerListings: ledgerIndex.size,
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
  console.log(`Augmented ${augmentedCount} existing rows, appended ${appendedCount} new Redfin actives (${missingDomCount} with no days-on-market reading).`);
  console.log(`Output: ${path.relative(PROJECT_DIR, outPath)}  (${finalRows.length} rows)`);
  console.log(`Report: ${path.relative(PROJECT_DIR, reportPath)}`);
}

if (require.main === module) {
  try { main(); }
  catch (err) { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); }
}

module.exports = {
  buildEnrichedRowFromRedfin,
  resolveListingTimeline,
  refreshOpenRowAsk,
  refreshOpenRowTimeline,
  openRowAtAddress,
  indexPriorSales,
  applyPriorSale,
  uiPropertyTypeToAppType,
  zipNeighborhood,
  normalizeAddressKey,
};
