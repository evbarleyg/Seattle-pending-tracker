#!/usr/bin/env node
"use strict";

// Compares the two most recent active-listing snapshots and emits a list of
// listingIds that disappeared (likely went pending or off-market). For each
// disappeared listing, looks up its Redfin URL from the URL index, fetches
// the property page, and parses the price-history strip to recover
// list@pending / pending date / sold date / sold price.

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_SNAPSHOT_DIR = path.join(PROJECT_DIR, "redfin_active_snapshots");
const DEFAULT_URL_INDEX = path.join(PROJECT_DIR, "redfin_url_index.json");
const DEFAULT_HISTORY_CACHE = path.join(PROJECT_DIR, "redfin_history_cache.json");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_transitions_report.json");
const DEFAULT_ENRICHED = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");

const { fetchPropertyHtml, parsePropertyHistory, summarizeMostRecentSale } = require("./scrape_redfin_property_history.js");
const { applyHistoryToRow } = require("./backfill_redfin_history.js");

function listSnapshots(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .sort();
}

function loadSnapshot(dir, file) {
  return JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
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

function findUrlByListingId(urlIndex, listingId) {
  if (!urlIndex?.addresses) return null;
  for (const list of Object.values(urlIndex.addresses)) {
    const entries = Array.isArray(list) ? list : [list];
    for (const entry of entries) {
      if (String(entry.listingId) === String(listingId)) return entry;
    }
  }
  return null;
}

function parseCsvLine(line) {
  const out = []; let cur = "", inQ = false;
  for (let i = 0; i < line.length; i += 1) {
    const c = line[i];
    if (c === "\"") { if (inQ && line[i+1] === "\"") { cur += "\""; i += 1; } else inQ = !inQ; }
    else if (c === "," && !inQ) { out.push(cur); cur = ""; }
    else cur += c;
  }
  out.push(cur);
  return out;
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) return `"${s.replace(/"/g, "\"\"")}"`;
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
  for (const row of rows) lines.push(headers.map((h) => safeCsv(row[h])).join(","));
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function parseArgs(argv) {
  const opts = { throttleMs: 3000, dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i]; const next = argv[i + 1];
    if (a === "--snapshot-dir") { opts.snapshotDir = next; i += 1; }
    else if (a === "--url-index") { opts.urlIndex = next; i += 1; }
    else if (a === "--cache") { opts.cache = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--enriched") { opts.enriched = next; i += 1; }
    else if (a === "--throttle") { opts.throttleMs = Number(next); i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--no-enrich") { opts.noEnrich = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/detect_listing_transitions.js [options]",
    "",
    "Compares the two most recent active-listing snapshots, detects which listings",
    "disappeared, fetches their Redfin pages, and applies pending/sold history.",
    "",
    "Options:",
    "  --snapshot-dir DIR  Snapshot directory (default: redfin_active_snapshots)",
    "  --url-index FILE    URL index path (default: redfin_url_index.json)",
    "  --cache FILE        History cache path (default: redfin_history_cache.json)",
    "  --enriched FILE     Enriched CSV path (default: public_sales_proxy_mls_enriched_last12mo.csv)",
    "  --throttle MS       Per-fetch delay (default: 3000)",
    "  --dry-run           Detect transitions but do not fetch",
    "  --no-enrich         Fetch and parse but do not write back to enriched CSV",
  ].join("\n"));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const snapshotDir = path.resolve(opts.snapshotDir || DEFAULT_SNAPSHOT_DIR);
  const indexPath = path.resolve(opts.urlIndex || DEFAULT_URL_INDEX);
  const cachePath = path.resolve(opts.cache || DEFAULT_HISTORY_CACHE);
  const reportPath = path.resolve(opts.report || DEFAULT_REPORT);
  const enrichedPath = path.resolve(opts.enriched || DEFAULT_ENRICHED);

  const snapshots = listSnapshots(snapshotDir);
  if (snapshots.length < 2) {
    console.log(`Need at least 2 snapshots to detect transitions; found ${snapshots.length}.`);
    saveJson(reportPath, { mode: "no-op", reason: "insufficient snapshots", snapshotCount: snapshots.length, fetchedAt: new Date().toISOString() });
    return;
  }
  const previous = loadSnapshot(snapshotDir, snapshots[snapshots.length - 2]);
  const current = loadSnapshot(snapshotDir, snapshots[snapshots.length - 1]);
  const prevIds = new Set(previous.listingIds || []);
  const currIds = new Set(current.listingIds || []);
  const disappeared = [...prevIds].filter((id) => !currIds.has(id));
  console.log(`Snapshots compared: ${snapshots[snapshots.length - 2]} -> ${snapshots[snapshots.length - 1]}`);
  console.log(`Previously active: ${prevIds.size}, currently active: ${currIds.size}`);
  console.log(`Disappeared listings (candidates for pending/sold transition): ${disappeared.length}`);

  if (opts.dryRun) {
    saveJson(reportPath, { mode: "dry-run", disappearedCount: disappeared.length, sampleIds: disappeared.slice(0, 20), fetchedAt: new Date().toISOString() });
    return;
  }

  const urlIndex = loadJson(indexPath, { addresses: {} });
  const cache = loadJson(cachePath, { fetchedAt: new Date().toISOString(), entries: {} });
  const enriched = !opts.noEnrich ? readCsv(enrichedPath) : null;

  const transitions = [];
  let urlMissing = 0, fetchErrors = 0, parseEmpty = 0, enrichedCount = 0;
  for (let i = 0; i < disappeared.length; i += 1) {
    const id = disappeared[i];
    const entry = findUrlByListingId(urlIndex, id);
    if (!entry?.url) { urlMissing += 1; continue; }
    let summary = cache.entries[entry.url]?.summary;
    let used = "cache";
    if (summary === undefined) {
      try {
        const html = await fetchPropertyHtml(entry.url);
        const events = parsePropertyHistory(html);
        summary = summarizeMostRecentSale(events);
        cache.entries[entry.url] = { fetchedAt: new Date().toISOString(), summary };
        used = "fetched";
      } catch (err) {
        fetchErrors += 1;
        cache.entries[entry.url] = { fetchedAt: new Date().toISOString(), error: err.message };
        if (i < disappeared.length - 1) await sleep(opts.throttleMs);
        continue;
      }
    }
    if (!summary?.listPriceAtPending) { parseEmpty += 1; }
    transitions.push({ listingId: id, url: entry.url, summary });
    if (used === "fetched" && i < disappeared.length - 1) await sleep(opts.throttleMs);
  }

  // Apply to enriched CSV if requested
  if (!opts.noEnrich && enriched) {
    const byUrl = new Map(transitions.map((t) => [t.url, t.summary]));
    for (const row of enriched.rows) {
      // Match by Redfin URL if previously seeded by REDFIN_ACTIVE merge
      // (we don't store URL on the row; match by listingId via mlsListingNumber as best-effort)
      const matchTransition = transitions.find((t) => t.summary?.mlsNumber && t.summary.mlsNumber === row.mlsListingNumber);
      if (matchTransition && applyHistoryToRow(row, matchTransition.summary)) enrichedCount += 1;
    }
    writeCsv(enrichedPath, enriched.headers, enriched.rows);
  }

  saveJson(cachePath, cache);
  saveJson(reportPath, {
    mode: "transition-scan",
    fetchedAt: new Date().toISOString(),
    snapshotsCompared: [snapshots[snapshots.length - 2], snapshots[snapshots.length - 1]],
    previouslyActive: prevIds.size,
    currentlyActive: currIds.size,
    disappeared: disappeared.length,
    urlMissing,
    transitionsRecorded: transitions.length,
    parseEmpty,
    fetchErrors,
    enrichedRows: enrichedCount,
    sampleTransitions: transitions.slice(0, 10).map((t) => ({ listingId: t.listingId, url: t.url, summary: t.summary })),
  });
  console.log(`\nDONE. Recorded ${transitions.length} transitions, enriched ${enrichedCount} rows.`);
  console.log(`Cache: ${path.relative(PROJECT_DIR, cachePath)}`);
  console.log(`Report: ${path.relative(PROJECT_DIR, reportPath)}`);
}

if (require.main === module) {
  main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
}
