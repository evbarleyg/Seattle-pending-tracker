#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_URL_INDEX = path.join(PROJECT_DIR, "redfin_url_index.json");
const DEFAULT_HISTORY_CACHE = path.join(PROJECT_DIR, "redfin_history_cache.json");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_backfill_report.json");

const { fetchPropertyHtml, parsePropertyHistory, summarizeMostRecentSale } = require("./scrape_redfin_property_history.js");
const { streetKeyFromAddress, extractUnitFromAddress } = require("./redfin_address_key.js");
const { resolveBuildingEntry } = require("./build_redfin_url_index.js");

function parseCsvLine(line) {
  const out = [];
  let cur = "", inQ = false;
  for (let i = 0; i < line.length; i += 1) {
    const c = line[i];
    if (c === "\"") {
      if (inQ && line[i + 1] === "\"") { cur += "\""; i += 1; }
      else inQ = !inQ;
    } else if (c === "," && !inQ) { out.push(cur); cur = ""; }
    else cur += c;
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

function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function zip5(value) {
  return (String(value || "").match(/[0-9]{5}/) || [])[0] || "";
}

function rowAddressKey(row) {
  return streetKeyFromAddress(row.address, row.zip);
}

function rowUnit(row) {
  return extractUnitFromAddress(row.address);
}

function loadJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) return fallback;
  try { return JSON.parse(fs.readFileSync(filePath, "utf8")); }
  catch { return fallback; }
}

function saveJson(filePath, data) {
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`);
}

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Sale-to-list ratio outside this band almost always indicates a wrong-property
// match (most often a multi-unit building where address normalization stripped
// the unit number). Rows that violate the sanity check get marked as
// REDFIN_HISTORY_SUSPECT and have the bid-up fields cleared so they don't
// pollute downstream metrics — close price still reflects the deed truth.
const PLAUSIBLE_SALE_TO_LIST_MIN = 0.7;
const PLAUSIBLE_SALE_TO_LIST_MAX = 1.6;
// Also flag if the property's existing closePrice is wildly different from the
// Redfin sold price (meaning the page describes a different sale event).
const CLOSE_PRICE_MISMATCH_TOLERANCE = 0.1;

function isImplausibleMatch(row, summary) {
  const list = Number(summary.listPriceAtPending) || 0;
  const soldFromPage = Number(summary.soldPrice) || 0;
  const knownClose = Number(row.closePrice) || 0;
  if (!list || !soldFromPage) return { suspect: false };
  const ratio = soldFromPage / list;
  if (ratio < PLAUSIBLE_SALE_TO_LIST_MIN || ratio > PLAUSIBLE_SALE_TO_LIST_MAX) {
    return { suspect: true, reason: `sale_to_list_${ratio.toFixed(2)}_out_of_band` };
  }
  if (knownClose > 0 && Math.abs(soldFromPage - knownClose) / knownClose > CLOSE_PRICE_MISMATCH_TOLERANCE) {
    return { suspect: true, reason: `redfin_sold_${soldFromPage}_vs_kc_${knownClose}_mismatch` };
  }
  return { suspect: false };
}

function applyHistoryToRow(row, summary) {
  if (!summary || !summary.listPriceAtPending) return false;
  const list = summary.listPriceAtPending;
  const close = summary.soldPrice;
  // The scraped "list" sometimes equals sold (Redfin history shows only the final
  // price, or a missing list event defaults to sold) -> a fake sale/list = 1.000.
  // Only treat list as genuine when it differs from close; otherwise keep the real
  // dates / DOM / sold price but do NOT fabricate a list price or sale-to-list ratio.
  const hasGenuineList = list > 0 && close > 0 && Math.abs(list - close) >= 1;
  const audit = isImplausibleMatch(row, summary);
  if (audit.suspect) {
    row.mlsJoinMethod = "REDFIN_HISTORY_SUSPECT";
    row.mlsRegion = row.mlsRegion || audit.reason;
    return false;
  }
  if (hasGenuineList) {
    row.mlsListingPrice = String(list);
    row.mlsListPriceAtPending = String(list);
    row.listPriceAtPending = String(list);
    row.mlsOriginalPrice = row.mlsOriginalPrice && num(row.mlsOriginalPrice) > 0 ? row.mlsOriginalPrice : String(list);
  }
  if (summary.listDate) row.mlsListDate = summary.listDate;
  if (summary.pendingDate) {
    row.mlsPendingDate = summary.pendingDate;
    row.pendingDate = summary.pendingDate;
  }
  if (summary.soldDate) row.mlsSellingDate = summary.soldDate;
  if (close && close > 0) {
    row.mlsSellingPrice = String(close);
    if (!num(row.mlsClosePrice)) row.mlsClosePrice = String(close);
  }
  if (summary.mlsNumber && !row.mlsListingNumber) row.mlsListingNumber = summary.mlsNumber;
  if (hasGenuineList) {
    row.bidUpAmount = String(Math.round(close - list));
    row.bidUpPct = String((close - list) / list);
    row.saleToListRatio = String(close / list);
  }
  if (summary.listDate && summary.pendingDate) {
    const days = Math.round((Date.parse(summary.pendingDate) - Date.parse(summary.listDate)) / 86400000);
    if (days >= 0) {
      row.mlsDOM = String(days);
      row.mlsCDOM = String(days);
      row.mlsDaysToPending = String(days);
    }
  }
  if (summary.pendingDate && summary.soldDate) {
    const days = Math.round((Date.parse(summary.soldDate) - Date.parse(summary.pendingDate)) / 86400000);
    if (days >= 0) row.mlsDaysPendingToSale = String(days);
  }
  row.mlsJoinMethod = "REDFIN_HISTORY";
  if (!row.dataMode || row.dataMode === "PUBLIC_PROXY") row.dataMode = "MLS_ENRICHED";
  return true;
}

function rowNeedsBackfill(row, opts) {
  const close = num(row.closePrice);
  if (close <= 0) return false;
  if (close < opts.minPrice || close > opts.maxPrice) return false;
  // Recency scope: skip sales before --since (keeps the run to the recent cliff).
  if (opts.since && (row.saleDate || row.mlsSellingDate || "") < opts.since) return false;
  // Only count the MLS-derived list fields. The generic `listPriceAtPending`
  // column is populated with county assessed value for PUBLIC_PROXY rows,
  // which is NOT a real MLS list price and shouldn't disqualify backfill.
  const mlsList = num(row.mlsListingPrice) || num(row.mlsListPriceAtPending);
  if (mlsList > 0) return false;
  // Skip rows already cleanly enriched. Allow re-attempting suspect rows since
  // a code update (e.g. unit-aware matching) may now produce a clean match.
  if (row.mlsJoinMethod === "REDFIN_HISTORY") return false;
  return true;
}

function parseArgs(argv) {
  const opts = {
    minPrice: 1000000,
    maxPrice: 2000000,
    throttleMs: 3000,
    limit: Infinity,
    dryRun: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--min-price") { opts.minPrice = Number(next); i += 1; }
    else if (a === "--max-price") { opts.maxPrice = Number(next); i += 1; }
    else if (a === "--throttle") { opts.throttleMs = Number(next); i += 1; }
    else if (a === "--limit") { opts.limit = Number(next); i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--url-index") { opts.urlIndex = next; i += 1; }
    else if (a === "--cache") { opts.cache = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--since") { opts.since = next; i += 1; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/backfill_redfin_history.js [options]",
    "",
    "Backfills list@pending / bid-up for closed rows missing the realtor MLS data,",
    "by fetching each property's Redfin page and parsing its price-history strip.",
    "",
    "Options:",
    "  --min-price N       Lower close-price bound (default: 1000000)",
    "  --max-price N       Upper close-price bound (default: 2000000)",
    "  --limit N           Cap number of rows processed (default: no cap)",
    "  --throttle MS       Delay between fetches (default: 3000)",
    "  --dry-run           Compute matches but do not fetch or write",
    "  --enriched FILE     Enriched CSV path (default: public_sales_proxy_mls_enriched_last12mo.csv)",
    "  --url-index FILE    URL index path (default: redfin_url_index.json)",
    "  --cache FILE        History cache JSON (default: redfin_history_cache.json)",
    "  --report FILE       Report JSON (default: redfin_backfill_report.json)",
  ].join("\n"));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }

  const enrichedPath = path.resolve(opts.enriched || DEFAULT_ENRICHED);
  const indexPath = path.resolve(opts.urlIndex || DEFAULT_URL_INDEX);
  const cachePath = path.resolve(opts.cache || DEFAULT_HISTORY_CACHE);
  const reportPath = path.resolve(opts.report || DEFAULT_REPORT);

  if (!fs.existsSync(enrichedPath)) throw new Error(`Enriched CSV not found: ${enrichedPath}`);
  if (!fs.existsSync(indexPath)) throw new Error(`URL index not found at ${indexPath}. Run build_redfin_url_index.js first.`);

  const { headers, rows } = readCsv(enrichedPath);
  const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
  const indexAddresses = index.addresses || {};
  const cache = loadJson(cachePath, { fetchedAt: new Date().toISOString(), entries: {} });

  // Identify candidate rows.
  const candidates = [];
  for (const row of rows) {
    if (!rowNeedsBackfill(row, opts)) continue;
    const key = rowAddressKey(row);
    if (!key) continue;
    candidates.push({ row, buildingKey: key, unit: rowUnit(row) });
  }

  // Resolve URL for each — unit-aware so multi-unit buildings don't cross-match.
  const resolved = [];
  const unresolved = [];
  let unresolvedAmbiguousUnit = 0;
  for (const c of candidates) {
    const entry = resolveBuildingEntry(indexAddresses, c.buildingKey, c.unit);
    if (entry?.url) {
      resolved.push({ ...c, url: entry.url, matchedUnit: entry.unit || "" });
    } else {
      unresolved.push(c);
      const list = indexAddresses[c.buildingKey];
      if (Array.isArray(list) && list.length > 0) unresolvedAmbiguousUnit += 1;
    }
  }

  console.log(`Backfill scope: $${opts.minPrice.toLocaleString()}-$${opts.maxPrice.toLocaleString()}`);
  console.log(`Candidates needing list@pending: ${candidates.length}`);
  console.log(`URL resolved: ${resolved.length}`);
  console.log(`URL unresolved (skipped): ${unresolved.length} (of which ${unresolvedAmbiguousUnit} are ambiguous unit / building-known but unit doesn't match)`);

  if (opts.dryRun) {
    saveJson(reportPath, {
      mode: "dry-run", timestamp: new Date().toISOString(),
      candidates: candidates.length, resolved: resolved.length, unresolved: unresolved.length,
      sampleResolved: resolved.slice(0, 5).map((c) => ({ address: c.row.address, url: c.url })),
      sampleUnresolved: unresolved.slice(0, 10).map((c) => c.row.address),
    });
    console.log(`Dry-run report: ${path.relative(PROJECT_DIR, reportPath)}`);
    return;
  }

  // Process each resolved candidate, with cache + throttle.
  const startTime = Date.now();
  let processed = 0, enriched = 0, hadHistory = 0, fetchErrors = 0, parseEmpty = 0;
  const errorSamples = [];
  const limit = Math.min(resolved.length, opts.limit);
  for (let i = 0; i < limit; i += 1) {
    const c = resolved[i];
    processed += 1;
    let summary = cache.entries[c.url]?.summary;
    let used = "cache";
    if (summary === undefined) {
      try {
        const html = await fetchPropertyHtml(c.url);
        const events = parsePropertyHistory(html);
        summary = summarizeMostRecentSale(events);
        cache.entries[c.url] = { fetchedAt: new Date().toISOString(), summary };
        used = "fetched";
      } catch (err) {
        fetchErrors += 1;
        cache.entries[c.url] = { fetchedAt: new Date().toISOString(), error: err.message };
        if (errorSamples.length < 5) errorSamples.push({ url: c.url, error: err.message });
        if (i < limit - 1) await sleep(opts.throttleMs);
        continue;
      }
    }
    if (summary && summary.listPriceAtPending) {
      hadHistory += 1;
      if (applyHistoryToRow(c.row, summary)) enriched += 1;
    } else {
      parseEmpty += 1;
    }
    if (processed % 25 === 0) {
      saveJson(cachePath, cache);
      const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
      console.log(`[${processed}/${limit}] enriched=${enriched} parseEmpty=${parseEmpty} errors=${fetchErrors} (${elapsed} min)`);
    }
    if (used === "fetched" && i < limit - 1) await sleep(opts.throttleMs);
  }

  // Persist cache + write enriched CSV
  saveJson(cachePath, cache);
  writeCsv(enrichedPath, headers, rows);

  const report = {
    mode: "backfill",
    fetchedAt: new Date().toISOString(),
    enrichedPath: path.relative(PROJECT_DIR, enrichedPath),
    minPrice: opts.minPrice,
    maxPrice: opts.maxPrice,
    candidates: candidates.length,
    urlResolved: resolved.length,
    urlUnresolved: unresolved.length,
    processed,
    enriched,
    hadHistory,
    fetchErrors,
    parseEmpty,
    elapsedMin: Number(((Date.now() - startTime) / 60000).toFixed(1)),
    errorSamples,
    unresolvedSample: unresolved.slice(0, 10).map((c) => c.row.address),
  };
  saveJson(reportPath, report);
  console.log(`\nDONE. Enriched ${enriched} rows out of ${processed} processed.`);
  console.log(`Cache: ${path.relative(PROJECT_DIR, cachePath)}`);
  console.log(`Report: ${path.relative(PROJECT_DIR, reportPath)}`);
}

if (require.main === module) {
  main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
}

module.exports = { applyHistoryToRow, rowNeedsBackfill, rowAddressKey };
