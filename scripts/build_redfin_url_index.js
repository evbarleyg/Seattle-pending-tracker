#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_INDEX = path.join(PROJECT_DIR, "redfin_url_index.json");
const REDFIN_ORIGIN = "https://www.redfin.com";
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";
const { streetKeyFromRedfinPath, unitFromRedfinPath } = require("./redfin_address_key.js");

const SEATTLE_ZIPS = [
  "98101","98102","98103","98104","98105","98106","98107","98108","98109",
  "98112","98115","98116","98117","98118","98119","98121","98122","98125",
  "98126","98133","98134","98136","98144","98146","98155","98166","98168",
  "98177","98178","98188","98198","98199",
];

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// Insert a property into the address index. The index value is now an ARRAY
// of entries per building so multi-unit buildings keep one slot per unit.
// Returns true if a NEW entry was added, false if it was a duplicate.
function insertProp(addresses, prop) {
  const list = addresses[prop.buildingKey] || [];
  if (list.some((e) => (e.unit || "") === (prop.unit || "") && e.url === prop.url)) {
    return false;
  }
  // If we already have an entry with same unit but different URL, prefer the
  // most recent (overwrite); for strict no-unit (or both empty unit) entries
  // we just keep the first seen and skip subsequent.
  const sameUnit = list.findIndex((e) => (e.unit || "") === (prop.unit || ""));
  if (sameUnit >= 0) {
    list[sameUnit] = { ...list[sameUnit], ...prop };
    addresses[prop.buildingKey] = list;
    return false;
  }
  list.push(prop);
  addresses[prop.buildingKey] = list;
  return true;
}

// Resolve an entry from the index given a building key + optional unit.
// Lookup logic:
//   - If unit is provided, try to find an entry with the same unit.
//     If none exists with that unit but a no-unit entry does, fall back to it.
//   - If unit is empty, prefer the no-unit entry; if none, refuse to match
//     (returning a random unit's URL would silently produce wrong-property data).
function resolveBuildingEntry(addresses, buildingKey, unit) {
  const list = addresses[buildingKey];
  if (!Array.isArray(list) || !list.length) return null;
  const wantedUnit = unit || "";
  if (wantedUnit) {
    const exact = list.find((e) => (e.unit || "") === wantedUnit);
    if (exact) return exact;
    if (list.length === 1 && !list[0].unit) return list[0];
    return null;
  }
  if (list.length === 1 && !list[0].unit) return list[0];
  if (list.length === 1) return null; // single entry but it has a unit and we wanted no unit
  // Multiple entries — prefer the no-unit one if it exists; otherwise refuse.
  const noUnit = list.find((e) => !e.unit);
  return noUnit || null;
}

function extractPropertiesFromZipPage(html) {
  // Each property card has an anchor like <a href="/WA/Seattle/8518-11th-Ave-NW-98117/home/100611">
  const out = [];
  const seen = new Set();
  const linkRe = /href="(\/[A-Z]{2}\/[^\"]+\/home\/(\d+))(?:\?[^"]*)?"/g;
  let m;
  while ((m = linkRe.exec(html))) {
    const linkPath = m[1];
    const propertyId = m[2];
    if (seen.has(linkPath)) continue;
    seen.add(linkPath);
    const buildingKey = streetKeyFromRedfinPath(linkPath);
    if (!buildingKey) continue;
    const unit = unitFromRedfinPath(linkPath);
    const [streetPart, zip] = buildingKey.split("|");
    out.push({
      url: `${REDFIN_ORIGIN}${linkPath}`,
      propertyId,
      buildingKey,
      unit,
      addressShort: streetPart,
      city: "Seattle",
      state: "WA",
      zip,
    });
  }
  return out;
}

function extractPageCountFromZipPage(html) {
  // Prefer the explicit "page X of Y" text from the paginator.
  const pageOf = html.match(/page\s+\d+\s+of\s+(\d+)/i);
  if (pageOf) {
    const n = Number(pageOf[1]);
    if (n >= 1 && n <= 200) return n;
  }
  // Fallback: highest page-N referenced in pagination links.
  const pageRefs = [...html.matchAll(/href="\/zipcode\/\d{5}\/[^"]*page-(\d+)"/g)].map((m) => Number(m[1]));
  if (pageRefs.length) return Math.max(...pageRefs);
  return 1;
}

async function fetchZipSoldPage(zip, page, yearWindow = 1) {
  const filterFragment = `/filter/include=sold-${yearWindow}yr`;
  const suffix = page > 1 ? `/page-${page}` : "";
  const url = `${REDFIN_ORIGIN}/zipcode/${zip}${filterFragment}${suffix}`;
  const headers = {
    "User-Agent": UA,
    Referer: `${REDFIN_ORIGIN}/zipcode/${zip}`,
    Accept: "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
  };
  // Retry with exponential backoff on transient network errors / 5xx / 429.
  let lastErr = null;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    try {
      const res = await fetch(url, { headers });
      if (res.status === 429 || res.status >= 500) {
        const wait = Math.min(30000, 2000 * attempt * attempt);
        process.stdout.write(`  retry attempt ${attempt} after ${wait}ms (HTTP ${res.status})\n`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) return { ok: false, status: res.status, html: "" };
      return { ok: true, status: res.status, html: await res.text() };
    } catch (err) {
      lastErr = err;
      const wait = Math.min(30000, 2000 * attempt * attempt);
      process.stdout.write(`  retry attempt ${attempt} after ${wait}ms (${err.message})\n`);
      await sleep(wait);
    }
  }
  return { ok: false, status: 0, html: "", error: lastErr?.message || "max retries exceeded" };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "", q = false;
  for (let i = 0; i < line.length; i += 1) {
    const c = line[i];
    if (c === "\"") { if (q && line[i + 1] === "\"") { cur += "\""; i += 1; } else q = !q; }
    else if (c === "," && !q) { out.push(cur); cur = ""; }
    else cur += c;
  }
  out.push(cur);
  return out;
}

// Ingest a Redfin sold CSV (with a redfinUrl column) straight into the index —
// no scraping, we already have the canonical property URLs from the sold fetch.
function ingestSoldCsv(addresses, csvPath) {
  const lines = fs.readFileSync(csvPath, "utf8").split(/\r?\n/).filter(Boolean);
  const headers = parseCsvLine(lines[0]);
  const ui = headers.indexOf("redfinUrl");
  if (ui < 0) throw new Error("sold CSV has no redfinUrl column");
  let added = 0, seen = 0;
  for (const line of lines.slice(1)) {
    const url = (parseCsvLine(line)[ui] || "").trim();
    if (!url) continue;
    const linkPath = url.replace(REDFIN_ORIGIN, "");
    const buildingKey = streetKeyFromRedfinPath(linkPath);
    if (!buildingKey) continue;
    const unit = unitFromRedfinPath(linkPath);
    const [streetPart, zip] = buildingKey.split("|");
    seen += 1;
    if (insertProp(addresses, { url, buildingKey, unit, addressShort: streetPart, city: "Seattle", state: "WA", zip })) added += 1;
  }
  return { added, seen };
}

function loadExistingIndex(filePath) {
  if (!fs.existsSync(filePath)) {
    return { fetchedAt: new Date().toISOString(), addresses: {} };
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return { fetchedAt: new Date().toISOString(), addresses: {} };
  }
}

function saveIndex(filePath, index) {
  fs.writeFileSync(filePath, `${JSON.stringify(index, null, 2)}\n`);
}

function parseArgs(argv) {
  const opts = { zips: SEATTLE_ZIPS.slice(), throttleMs: 2500, maxPagesPerZip: 30 };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--zips") { opts.zips = String(next || "").split(","); i += 1; }
    else if (a === "--out") { opts.out = next; i += 1; }
    else if (a === "--throttle") { opts.throttleMs = Number(next || 2500); i += 1; }
    else if (a === "--max-pages") { opts.maxPagesPerZip = Number(next || 30); i += 1; }
    else if (a === "--from-sold") { opts.fromSold = next; i += 1; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) {
    console.log("Usage: node scripts/build_redfin_url_index.js [--zips 98103,98117] [--throttle 2500] [--max-pages 30] [--out FILE]");
    return;
  }
  const indexPath = path.resolve(opts.out || DEFAULT_INDEX);
  const index = loadExistingIndex(indexPath);
  index.addresses = index.addresses || {};

  // --from-sold: ingest a sold CSV's URLs directly (no scraping), then exit.
  if (opts.fromSold) {
    const csvPath = path.resolve(opts.fromSold);
    const { added, seen } = ingestSoldCsv(index.addresses, csvPath);
    index.fetchedAt = new Date().toISOString();
    index.totalBuildings = Object.keys(index.addresses).length;
    index.totalEntries = Object.values(index.addresses).reduce((s, l) => s + (Array.isArray(l) ? l.length : 1), 0);
    saveIndex(indexPath, index);
    console.log(`Ingested ${seen} sold URLs (${added} new) from ${path.relative(PROJECT_DIR, csvPath)}.`);
    console.log(`Index now: ${index.totalEntries} entries across ${index.totalBuildings} buildings -> ${path.relative(PROJECT_DIR, indexPath)}`);
    return;
  }

  const startTime = Date.now();
  let totalAdded = 0;
  let totalFetches = 0;
  let totalFailures = 0;

  for (const zip of opts.zips) {
    process.stdout.write(`\n[zip ${zip}] page 1...`);
    const first = await fetchZipSoldPage(zip, 1);
    totalFetches += 1;
    if (!first.ok) {
      process.stdout.write(` HTTP ${first.status}, skipping\n`);
      totalFailures += 1;
      continue;
    }
    const pageCount = Math.min(extractPageCountFromZipPage(first.html), opts.maxPagesPerZip);
    let zipAdded = 0;
    const firstProps = extractPropertiesFromZipPage(first.html);
    for (const prop of firstProps) {
      if (insertProp(index.addresses, prop)) { zipAdded += 1; totalAdded += 1; }
    }
    process.stdout.write(` ${firstProps.length} props, est ${pageCount} pages\n`);

    let consecutiveEmpty = 0;
    for (let page = 2; page <= pageCount; page += 1) {
      await sleep(opts.throttleMs);
      const r = await fetchZipSoldPage(zip, page);
      totalFetches += 1;
      if (!r.ok) {
        process.stdout.write(`  page ${page} HTTP ${r.status}\n`);
        totalFailures += 1;
        consecutiveEmpty += 1;
        if (consecutiveEmpty >= 3) {
          process.stdout.write(`  bailing out after ${consecutiveEmpty} consecutive failures/empties\n`);
          break;
        }
        continue;
      }
      const props = extractPropertiesFromZipPage(r.html);
      if (props.length === 0) {
        consecutiveEmpty += 1;
        process.stdout.write(`  page ${page}: 0 props\n`);
        if (consecutiveEmpty >= 3) {
          process.stdout.write(`  bailing out after ${consecutiveEmpty} consecutive empty pages\n`);
          break;
        }
        continue;
      }
      consecutiveEmpty = 0;
      for (const prop of props) {
        if (insertProp(index.addresses, prop)) { zipAdded += 1; totalAdded += 1; }
      }
      process.stdout.write(`  page ${page}: ${props.length} props (${zipAdded} new from zip ${zip})\n`);
    }
    // Save incrementally so a crash doesn't lose progress.
    index.fetchedAt = new Date().toISOString();
    saveIndex(indexPath, index);
  }

  index.fetchedAt = new Date().toISOString();
  index.totalBuildings = Object.keys(index.addresses).length;
  index.totalEntries = Object.values(index.addresses).reduce((sum, list) => sum + (Array.isArray(list) ? list.length : 1), 0);
  saveIndex(indexPath, index);
  const elapsedMin = ((Date.now() - startTime) / 60000).toFixed(1);
  console.log(`\nDONE. ${totalAdded} new URLs, ${index.totalEntries} entries across ${index.totalBuildings} buildings.`);
  console.log(`Fetches: ${totalFetches}, failures: ${totalFailures}, elapsed: ${elapsedMin} min.`);
  console.log(`Index: ${path.relative(PROJECT_DIR, indexPath)}`);
}

if (require.main === module) {
  main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
}

module.exports = { extractPropertiesFromZipPage, extractPageCountFromZipPage, insertProp, resolveBuildingEntry, SEATTLE_ZIPS };
