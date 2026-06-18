#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_CONFIG = path.join(PROJECT_DIR, "redfin_searches.json");
const DEFAULT_OUT = path.join(PROJECT_DIR, "redfin_active_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_fetch_report.json");
const DEFAULT_URL_INDEX = path.join(PROJECT_DIR, "redfin_url_index.json");
const DEFAULT_SNAPSHOT_DIR = path.join(PROJECT_DIR, "redfin_active_snapshots");

const GIS_ENDPOINT = "https://www.redfin.com/stingray/api/gis";
const REDFIN_ORIGIN = "https://www.redfin.com";

const UI_PROPERTY_TYPE_LABELS = {
  1: "Single Family",
  2: "Condo",
  3: "Townhouse",
  4: "Multi-Family",
  5: "Land",
  6: "Other",
};

const OUTPUT_COLUMNS = [
  "fetchedAt",
  "queryLabel",
  "redfinPropertyId",
  "redfinListingId",
  "mlsListingNumber",
  "mlsStatus",
  "address",
  "unitNumber",
  "city",
  "state",
  "zip",
  "neighborhood",
  "propertyType",
  "uiPropertyType",
  "yearBuilt",
  "beds",
  "baths",
  "fullBaths",
  "sqft",
  "lotSize",
  "stories",
  "listPrice",
  "pricePerSqFt",
  "hoaMonthly",
  "domDays",
  "timeOnRedfinMs",
  "isNewConstruction",
  "isHot",
  "listingAgentName",
  "listingBrokerName",
  "lat",
  "lon",
  "redfinUrl",
];

function safeCsv(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
    return `"${s.replace(/"/g, "\"\"")}"`;
  }
  return s;
}

function numFromLeveled(obj) {
  if (!obj || typeof obj !== "object") return "";
  const v = obj.value;
  return v === null || v === undefined || v === "" ? "" : v;
}

function strFromLeveled(obj) {
  if (!obj || typeof obj !== "object") return "";
  const v = obj.value;
  return v === null || v === undefined ? "" : String(v);
}

async function fetchGis(query, options) {
  const params = new URLSearchParams({
    al: "1",
    include_nearby_homes: "true",
    market: query.market || "seattle",
    mpt: "1",
    num_homes: String(options.maxHomes),
    ord: "redfin-recommended-asc",
    page_number: "1",
    region_id: String(query.regionId),
    region_type: String(query.regionType),
    sf: "1,2,3,5,6,7",
    start: "0",
    status: "9",
    uipt: query.uiptParam || "1,2,3,4,5,6,7,8",
    v: "8",
  });
  const url = `${GIS_ENDPOINT}?${params.toString()}`;
  const res = await fetch(url, {
    headers: {
      "User-Agent": options.userAgent,
      Referer: query.referer || REDFIN_ORIGIN,
      Accept: "application/json,text/plain,*/*",
      "Accept-Language": "en-US,en;q=0.9",
      "Sec-Fetch-Site": "same-origin",
      "Sec-Fetch-Mode": "cors",
      "Sec-Fetch-Dest": "empty",
    },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for query ${query.label}`);
  }
  const raw = await res.text();
  const cleaned = raw.replace(/^\{\}&&/, "");
  const json = JSON.parse(cleaned);
  if (json.resultCode !== 0) {
    throw new Error(`Redfin error ${json.resultCode} (${json.errorMessage}) for query ${query.label}`);
  }
  return json.payload?.homes || [];
}

// Redfin's gis endpoint intermittently returns an empty payload under light
// rate-limiting. Retry empty or errored responses a few times before accepting
// a zero, so a transient blip doesn't silently drop a whole neighborhood.
async function fetchGisWithRetry(query, options) {
  const retries = Number(options.emptyRetries ?? 2);
  const backoff = Number(options.retryBackoffMs ?? 4000);
  let homes = [];
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      homes = await fetchGis(query, options);
      if (homes.length > 0) return homes;
    } catch (err) {
      if (attempt === retries) throw err;
    }
    if (attempt < retries) await sleep(backoff * (attempt + 1));
  }
  return homes;
}

function homeToRow(home, query, fetchedAt) {
  const lat = home.latLong?.value?.latitude;
  const lon = home.latLong?.value?.longitude;
  const street = strFromLeveled(home.streetLine);
  const unit = strFromLeveled(home.unitNumber);
  const fullAddress = unit ? `${street} ${unit}` : street;
  return {
    fetchedAt,
    queryLabel: query.label,
    redfinPropertyId: home.propertyId ?? "",
    redfinListingId: home.listingId ?? "",
    mlsListingNumber: home.mlsId?.value ?? "",
    mlsStatus: home.mlsStatus ?? "",
    address: fullAddress,
    unitNumber: unit,
    city: home.city ?? "",
    state: home.state ?? "",
    zip: home.zip ?? "",
    neighborhood: strFromLeveled(home.location),
    propertyType: UI_PROPERTY_TYPE_LABELS[home.uiPropertyType] || "",
    uiPropertyType: home.uiPropertyType ?? "",
    yearBuilt: numFromLeveled(home.yearBuilt),
    beds: home.beds ?? "",
    baths: home.baths ?? "",
    fullBaths: home.fullBaths ?? "",
    sqft: numFromLeveled(home.sqFt),
    lotSize: numFromLeveled(home.lotSize),
    stories: home.stories ?? "",
    listPrice: numFromLeveled(home.price),
    pricePerSqFt: numFromLeveled(home.pricePerSqFt),
    hoaMonthly: numFromLeveled(home.hoa),
    domDays: numFromLeveled(home.dom),
    timeOnRedfinMs: numFromLeveled(home.timeOnRedfin),
    isNewConstruction: home.isNewConstruction ? "true" : "false",
    isHot: home.isHot ? "true" : "false",
    listingAgentName: home.listingAgent?.name ?? "",
    listingBrokerName: home.listingBroker?.name ?? "",
    lat: lat ?? "",
    lon: lon ?? "",
    redfinUrl: home.url ? `${REDFIN_ORIGIN}${home.url}` : "",
  };
}

function passesFilters(row, filters) {
  const price = Number(row.listPrice) || 0;
  if (filters.minPrice && price > 0 && price < filters.minPrice) return false;
  if (filters.maxPrice && price > 0 && price > filters.maxPrice) return false;
  if (Array.isArray(filters.uiPropertyTypes) && filters.uiPropertyTypes.length > 0) {
    const code = Number(row.uiPropertyType);
    if (!filters.uiPropertyTypes.includes(code)) return false;
  }
  return true;
}

function dedupeRows(rows) {
  const byKey = new Map();
  for (const row of rows) {
    const key = row.redfinListingId || row.redfinPropertyId || row.mlsListingNumber || `${row.address}|${row.zip}`;
    if (!byKey.has(key)) byKey.set(key, row);
  }
  return [...byKey.values()];
}

function writeCsv(filePath, rows) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const lines = [OUTPUT_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(OUTPUT_COLUMNS.map((col) => safeCsv(row[col])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const { streetKeyFromAddress, extractUnitFromAddress, unitFromRedfinPath } = require("./redfin_address_key.js");

function updateUrlIndex(rows, indexPath, fetchedAt) {
  let index = { fetchedAt, addresses: {} };
  if (fs.existsSync(indexPath)) {
    try { index = JSON.parse(fs.readFileSync(indexPath, "utf8")); }
    catch { /* keep fresh index */ }
  }
  index.addresses = index.addresses || {};
  let added = 0;
  for (const row of rows) {
    if (!row.redfinUrl || !row.address || !row.zip) continue;
    const buildingKey = streetKeyFromAddress(row.address, row.zip);
    if (!buildingKey) continue;
    // Prefer unit derived from the canonical Redfin URL slug; fall back to
    // parsing it out of the row's free-form address.
    const unit = unitFromRedfinPath(row.redfinUrl) || extractUnitFromAddress(row.address);
    const entry = {
      url: row.redfinUrl,
      propertyId: row.redfinPropertyId,
      listingId: row.redfinListingId,
      buildingKey,
      unit,
      addressShort: String(row.address || "").toUpperCase(),
      city: row.city || "Seattle",
      state: row.state || "WA",
      zip: row.zip,
      lastSeenActive: fetchedAt,
    };
    const list = index.addresses[buildingKey] || [];
    const existingIdx = list.findIndex((e) => (e.unit || "") === (unit || ""));
    if (existingIdx >= 0) {
      const prev = list[existingIdx];
      list[existingIdx] = { ...prev, ...entry, firstSeenActive: prev.firstSeenActive || fetchedAt };
    } else {
      entry.firstSeenActive = fetchedAt;
      list.push(entry);
      added += 1;
    }
    index.addresses[buildingKey] = list;
  }
  index.fetchedAt = fetchedAt;
  index.totalBuildings = Object.keys(index.addresses).length;
  index.totalEntries = Object.values(index.addresses).reduce((s, l) => s + (Array.isArray(l) ? l.length : 1), 0);
  fs.writeFileSync(indexPath, `${JSON.stringify(index, null, 2)}\n`);
  return { added, total: index.totalEntries };
}

function writeActiveSnapshot(rows, snapshotDir, fetchedAt) {
  fs.mkdirSync(snapshotDir, { recursive: true });
  const stamp = fetchedAt.replace(/[:.]/g, "-");
  const file = path.join(snapshotDir, `${stamp}.json`);
  const snapshot = {
    fetchedAt,
    listingIds: rows.map((r) => r.redfinListingId).filter(Boolean),
    rowCount: rows.length,
  };
  fs.writeFileSync(file, `${JSON.stringify(snapshot)}\n`);
  return file;
}

function parseArgs(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--config") { opts.config = next; i += 1; }
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
    "Usage: node scripts/fetch_redfin_actives.js [options]",
    "",
    "Fetches Redfin active listings via the public gis JSON endpoint.",
    "",
    "Options:",
    "  --config FILE   Search config JSON (default: redfin_searches.json)",
    "  --out FILE      Output CSV (default: redfin_active_listings.csv)",
    "  --report FILE   Fetch report JSON (default: redfin_fetch_report.json)",
    "  --dry-run       Run queries but do not write outputs",
  ].join("\n"));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }

  const configPath = path.resolve(opts.config || DEFAULT_CONFIG);
  const outPath = path.resolve(opts.out || DEFAULT_OUT);
  const reportPath = path.resolve(opts.report || DEFAULT_REPORT);

  if (!fs.existsSync(configPath)) {
    throw new Error(`Config not found: ${configPath}`);
  }
  const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
  const fetchedAt = new Date().toISOString();
  const allRows = [];
  const queryStats = [];

  // Note: Redfin's gis endpoint ignores uipt/min_price/max_price server-side
  // and hard-caps each region response at 350 homes (verified empirically).
  // So the only way to avoid dropping listings in a dense region is to query
  // narrower sub-regions. Central Seattle (760187) exceeds 350 SFR/townhouse
  // listings, but its major sub-areas (Capitol Hill, Madison Park, Madrona,
  // First Hill, Eastlake, Montlake) are queried separately below, which
  // recovers the overflow via dedupe. The residual cap loss is limited to the
  // lowest Redfin-recommended listings in Central District / Leschi / Madison
  // Valley that aren't separately queryable.
  const fetchOptions = {
    userAgent: config.userAgent || "Mozilla/5.0",
    maxHomes: Number(config.maxHomesPerQuery) || 350,
    emptyRetries: config.emptyRetries ?? 2,
    retryBackoffMs: config.retryBackoffMs ?? 4000,
  };

  for (let i = 0; i < (config.queries || []).length; i += 1) {
    const query = config.queries[i];
    const stat = { label: query.label, regionId: query.regionId, regionType: query.regionType };
    try {
      const homes = await fetchGisWithRetry(query, fetchOptions);
      const rows = homes.map((h) => homeToRow(h, query, fetchedAt));
      const passing = rows.filter((r) => passesFilters(r, config.filters || {}));
      stat.homesReturned = rows.length;
      stat.homesPassingFilters = passing.length;
      stat.capHit = rows.length >= fetchOptions.maxHomes;
      allRows.push(...passing);
      process.stdout.write(`[${i + 1}/${config.queries.length}] ${query.label}: ${rows.length} fetched, ${passing.length} kept${stat.capHit ? " (CAP HIT)" : ""}\n`);
    } catch (err) {
      stat.error = err.message;
      process.stderr.write(`[${i + 1}/${config.queries.length}] ${query.label}: ERROR ${err.message}\n`);
    }
    queryStats.push(stat);
    if (i < config.queries.length - 1 && config.throttleMs) {
      await sleep(Number(config.throttleMs));
    }
  }

  const deduped = dedupeRows(allRows);
  const report = {
    fetchedAt,
    configPath: path.relative(PROJECT_DIR, configPath),
    queries: queryStats,
    totalRowsBeforeDedupe: allRows.length,
    totalRows: deduped.length,
    statusBreakdown: deduped.reduce((acc, r) => { acc[r.mlsStatus || "Unknown"] = (acc[r.mlsStatus || "Unknown"] || 0) + 1; return acc; }, {}),
    typeBreakdown: deduped.reduce((acc, r) => { acc[r.propertyType || "Unknown"] = (acc[r.propertyType || "Unknown"] || 0) + 1; return acc; }, {}),
    anyCapHit: queryStats.some((s) => s.capHit),
  };

  if (opts.dryRun) {
    process.stdout.write(`Dry run. Would write ${deduped.length} rows.\n`);
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    return;
  }

  writeCsv(outPath, deduped);
  const indexPath = path.resolve(opts.urlIndex || DEFAULT_URL_INDEX);
  const snapshotDir = path.resolve(opts.snapshotDir || DEFAULT_SNAPSHOT_DIR);
  const indexResult = updateUrlIndex(deduped, indexPath, fetchedAt);
  const snapshotFile = writeActiveSnapshot(deduped, snapshotDir, fetchedAt);
  fs.writeFileSync(reportPath, `${JSON.stringify({
    ...report,
    outFile: path.relative(PROJECT_DIR, outPath),
    urlIndexAdded: indexResult.added,
    urlIndexTotal: indexResult.total,
    snapshotFile: path.relative(PROJECT_DIR, snapshotFile),
  }, null, 2)}\n`);
  process.stdout.write(`Wrote ${deduped.length} rows -> ${path.relative(PROJECT_DIR, outPath)}\n`);
  process.stdout.write(`URL index: +${indexResult.added} new, ${indexResult.total} total\n`);
  process.stdout.write(`Snapshot: ${path.relative(PROJECT_DIR, snapshotFile)}\n`);
  process.stdout.write(`Report -> ${path.relative(PROJECT_DIR, reportPath)}\n`);
  if (report.anyCapHit) {
    process.stdout.write("WARNING: at least one query hit the 350-row cap. Add narrower neighborhood/zip queries to redfin_searches.json to avoid dropped listings.\n");
  }
}

if (require.main === module) {
  main().catch((err) => {
    process.stderr.write(`${err.stack || err.message}\n`);
    process.exit(1);
  });
}

module.exports = {
  fetchGis,
  homeToRow,
  passesFilters,
  dedupeRows,
  UI_PROPERTY_TYPE_LABELS,
  OUTPUT_COLUMNS,
};
