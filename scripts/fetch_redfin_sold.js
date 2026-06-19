#!/usr/bin/env node
"use strict";

// Fetches Redfin RECENTLY SOLD listings via the public gis JSON endpoint.
// Mirrors fetch_redfin_actives.js, but targets the sold cohort.
//
// Sold query — verified empirically 2026-06 by capturing Redfin's own
// `include=sold-1yr` search request (the `srp` object it embeds in the page):
//   KEEP `status=9` and ADD `sold_within_days=N`.
// Do NOT send `sf`. The actives fetch sends `sf=1,2,3,5,6,7`, which pollutes a
// sold search with active listings and roughly halves the genuine-sold share
// (Ballard 1yr: 333/350 Closed without `sf`, 183/350 with it). A prior attempt
// that DROPPED `status` returned 0 homes — `status=9` must stay.
// `include_nearby_homes` is left off to keep region attribution clean.

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_CONFIG = path.join(PROJECT_DIR, "redfin_searches.json");
const DEFAULT_OUT = path.join(PROJECT_DIR, "redfin_sold_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_sold_report.json");

const GIS_ENDPOINT = "https://www.redfin.com/stingray/api/gis";
const REDFIN_ORIGIN = "https://www.redfin.com";
const DEFAULT_SOLD_WITHIN_DAYS = 365;
const PRICE_CAP_CEILING = 100000000; // sentinel "no upper bound" for price banding
const DEFAULT_MIN_BAND_WIDTH = 25000;
const DEFAULT_MAX_BAND_DEPTH = 12;

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
  "soldDate",
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
  "soldPrice",
  "pricePerSqFt",
  "hoaMonthly",
  "domDays",
  "timeOnRedfinMs",
  "listingAgentName",
  "listingBrokerName",
  "sellingBrokerName",
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

// Redfin reports soldDate as epoch milliseconds (stamped at local-midnight
// Pacific, i.e. 08:00Z), so a UTC date slice yields the correct sale day.
function epochMsToIso(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) return "";
  const d = new Date(n);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString().slice(0, 10);
}

// A home counts as sold when Redfin marks it Closed / searchStatus 4 AND
// exposes a sold date. The sold-within-days search returns a small tail of
// non-sold rows (recently relisted / pending); those are dropped here.
function isSoldHome(home) {
  if (!home) return false;
  const closed = home.mlsStatus === "Closed" || Number(home.searchStatus) === 4;
  return Boolean(closed && home.soldDate);
}

// Redfin honors only fixed sold-window buckets (the UI's 1wk/1mo/3mo/6mo/1yr/…);
// a non-canonical value is snapped down to the nearest bucket server-side
// (verified: sold_within_days=120 returns exactly the 90-day result). Snap here
// too so the reported window matches what actually comes back.
const SOLD_WINDOW_BUCKETS = [7, 30, 90, 180, 365, 730, 1095, 1825];

function snapSoldWithinDays(requested) {
  const n = Number(requested);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_SOLD_WITHIN_DAYS;
  let bucket = SOLD_WINDOW_BUCKETS[0];
  for (const b of SOLD_WINDOW_BUCKETS) { if (b <= n) bucket = b; }
  return bucket;
}

// Verified sold param set — see file header. No `sf`, no include_nearby_homes.
function buildSoldParams(query, options) {
  const params = new URLSearchParams({
    al: "1",
    market: query.market || "seattle",
    mpt: "99",
    num_homes: String(options.maxHomes),
    ord: "redfin-recommended-asc",
    page_number: "1",
    region_id: String(query.regionId),
    region_type: String(query.regionType),
    sold_within_days: String(options.soldWithinDays),
    start: "0",
    status: "9",
    uipt: query.uiptParam || "1,2,3,4,5,6,7,8",
    v: "8",
  });
  // Price banding: Redfin DOES honor min_price/max_price on the sold search
  // (verified — unlike the actives endpoint, which ignores them). This lets us
  // subdivide a region that exceeds the 350-row cap so deep windows (e.g. 18
  // months) stay complete.
  if (Number(options.minPrice) > 0) params.set("min_price", String(Math.round(options.minPrice)));
  if (Number(options.maxPrice) > 0 && Number(options.maxPrice) < PRICE_CAP_CEILING) {
    params.set("max_price", String(Math.round(options.maxPrice)));
  }
  return params;
}

// Pick a price split point strictly inside (lo, hi). Uses the median of the
// returned (capped) sample so the split adapts to the band's price density.
function splitBandPoint(lo, hi, prices) {
  const sorted = (prices || []).filter((p) => p > lo && p < hi).sort((a, b) => a - b);
  const mid = sorted.length ? sorted[Math.floor(sorted.length / 2)] : Math.floor((lo + hi) / 2);
  return Math.max(lo + 1, Math.min(hi - 1, mid));
}

async function fetchSoldGis(query, options) {
  const url = `${GIS_ENDPOINT}?${buildSoldParams(query, options).toString()}`;
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

// Same transient-empty retry behaviour as the actives fetch: Redfin's gis
// endpoint intermittently returns an empty payload under light rate-limiting.
async function fetchSoldGisWithRetry(query, options) {
  const retries = Number(options.emptyRetries ?? 2);
  const backoff = Number(options.retryBackoffMs ?? 4000);
  let homes = [];
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      homes = await fetchSoldGis(query, options);
      if (homes.length > 0) return homes;
    } catch (err) {
      if (attempt === retries) throw err;
    }
    if (attempt < retries) await sleep(backoff * (attempt + 1));
  }
  return homes;
}

// Fetch one region completely by recursively price-banding around the 350-row
// cap: query [lo, hi]; if it caps, split at the sample median and recurse on
// each half. Dedups raw homes by listing across bands; throttles each gis call.
// Returns { homes, residualCaps, calls } — residualCaps flags any band that
// still capped at the minimum width (true coverage gap).
async function fetchRegionBanded(query, options) {
  const collected = new Map();
  const residualCaps = [];
  let calls = 0;
  const minBand = Number(options.minBandWidth) > 0 ? options.minBandWidth : DEFAULT_MIN_BAND_WIDTH;
  const maxDepth = Number(options.maxBandDepth) > 0 ? options.maxBandDepth : DEFAULT_MAX_BAND_DEPTH;

  async function band(lo, hi, depth) {
    const homes = await fetchSoldGisWithRetry(query, {
      ...options,
      minPrice: lo > 0 ? lo : 0,
      maxPrice: hi < PRICE_CAP_CEILING ? hi : 0,
    });
    calls += 1;
    if (options.throttleMs) await sleep(Number(options.throttleMs));
    for (const h of homes) {
      const key = h.listingId || h.propertyId
        || `${strFromLeveled(h.streetLine)}|${h.zip}|${h.soldDate}`;
      if (!collected.has(key)) collected.set(key, h);
    }
    const capped = homes.length >= options.maxHomes;
    if (capped && (hi - lo) > minBand && depth < maxDepth) {
      const prices = homes.map((h) => Number(numFromLeveled(h.price)) || 0);
      const splitAt = splitBandPoint(lo, hi, prices);
      await band(lo, splitAt, depth + 1);
      await band(splitAt + 1, hi, depth + 1);
    } else if (capped) {
      residualCaps.push({ lo, hi });
    }
  }

  await band(0, PRICE_CAP_CEILING, 0);
  return { homes: [...collected.values()], residualCaps, calls };
}

function homeToRow(home, query, fetchedAt) {
  const lat = home.latLong?.value?.latitude;
  const lon = home.latLong?.value?.longitude;
  const street = strFromLeveled(home.streetLine);
  const unit = strFromLeveled(home.unitNumber);
  // Redfin's sold streetLine already embeds the unit; only append when the street
  // doesn't already end with it, otherwise the unit doubles ("... Unit B Unit B").
  const streetHasUnit = unit && street.toUpperCase().trimEnd().endsWith(unit.toUpperCase());
  const fullAddress = unit && !streetHasUnit ? `${street} ${unit}` : street;
  return {
    fetchedAt,
    queryLabel: query.label,
    soldDate: epochMsToIso(home.soldDate),
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
    soldPrice: numFromLeveled(home.price),
    pricePerSqFt: numFromLeveled(home.pricePerSqFt),
    hoaMonthly: numFromLeveled(home.hoa),
    domDays: numFromLeveled(home.dom),
    timeOnRedfinMs: numFromLeveled(home.timeOnRedfin),
    listingAgentName: home.listingAgent?.name ?? "",
    listingBrokerName: home.listingBroker?.name ?? "",
    sellingBrokerName: home.sellingBroker?.name ?? "",
    lat: lat ?? "",
    lon: lon ?? "",
    redfinUrl: home.url ? `${REDFIN_ORIGIN}${home.url}` : "",
  };
}

function passesFilters(row, filters) {
  const price = Number(row.soldPrice) || 0;
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
    const key = row.redfinListingId || row.redfinPropertyId || row.mlsListingNumber
      || `${row.address}|${row.zip}|${row.soldDate}`;
    if (!byKey.has(key)) byKey.set(key, row);
  }
  return [...byKey.values()];
}

function monthOf(isoDate) {
  return typeof isoDate === "string" && isoDate.length >= 7 ? isoDate.slice(0, 7) : "";
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

function parseArgs(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--config") { opts.config = next; i += 1; }
    else if (a === "--out") { opts.out = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--sold-within-days") { opts.soldWithinDays = Number(next); i += 1; }
    else if (a === "--limit") { opts.limit = Number(next); i += 1; }
    else if (a === "--bands") { opts.bands = true; }
    else if (a === "--min-band") { opts.minBandWidth = Number(next); i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
    else throw new Error(`Unknown argument: ${a}`);
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/fetch_redfin_sold.js [options]",
    "",
    "Fetches Redfin recently-sold listings via the public gis JSON endpoint.",
    "",
    "Options:",
    "  --config FILE          Search config JSON (default: redfin_searches.json)",
    "  --out FILE             Output CSV (default: redfin_sold_listings.csv)",
    "  --report FILE          Fetch report JSON (default: redfin_sold_report.json)",
    "  --sold-within-days N   Sold lookback window in days (default: 365; 730 = ~24mo)",
    "  --limit N              Only run the first N queries (smoke testing)",
    "  --bands                Price-band each region to beat the 350-row cap (needed for deep windows)",
    "  --min-band N           Smallest price band width before giving up splitting (default: 25000)",
    "  --dry-run              Run queries but do not write outputs",
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
  const requestedDays = Number(opts.soldWithinDays) > 0 ? Number(opts.soldWithinDays) : DEFAULT_SOLD_WITHIN_DAYS;
  const soldWithinDays = snapSoldWithinDays(requestedDays);
  if (soldWithinDays !== requestedDays) {
    process.stdout.write(`Note: Redfin only honors fixed sold windows; --sold-within-days ${requestedDays} snapped to ${soldWithinDays}.\n`);
  }

  const fetchOptions = {
    userAgent: config.userAgent || "Mozilla/5.0",
    maxHomes: Number(config.maxHomesPerQuery) || 350,
    emptyRetries: config.emptyRetries ?? 2,
    retryBackoffMs: config.retryBackoffMs ?? 4000,
    soldWithinDays,
    bands: Boolean(opts.bands),
    throttleMs: Number(config.throttleMs) || 0,
    minBandWidth: opts.minBandWidth,
    maxBandDepth: config.maxBandDepth,
  };

  let queries = config.queries || [];
  if (Number(opts.limit) > 0) queries = queries.slice(0, Number(opts.limit));

  const allRows = [];
  const queryStats = [];

  for (let i = 0; i < queries.length; i += 1) {
    const query = queries[i];
    const stat = { label: query.label, regionId: query.regionId, regionType: query.regionType };
    try {
      let homes;
      let residualCaps = [];
      if (fetchOptions.bands) {
        const banded = await fetchRegionBanded(query, fetchOptions);
        homes = banded.homes;
        residualCaps = banded.residualCaps;
        stat.bandCalls = banded.calls;
      } else {
        homes = await fetchSoldGisWithRetry(query, fetchOptions);
      }
      const sold = homes.filter(isSoldHome);
      const rows = sold.map((h) => homeToRow(h, query, fetchedAt));
      const passing = rows.filter((r) => passesFilters(r, config.filters || {}));
      stat.homesReturned = homes.length;
      stat.soldHomes = sold.length;
      stat.homesPassingFilters = passing.length;
      stat.capHit = fetchOptions.bands ? residualCaps.length > 0 : homes.length >= fetchOptions.maxHomes;
      if (residualCaps.length) stat.residualCaps = residualCaps;
      allRows.push(...passing);
      const detail = fetchOptions.bands ? `${homes.length} fetched (${stat.bandCalls} bands)` : `${homes.length} fetched`;
      process.stdout.write(`[${i + 1}/${queries.length}] ${query.label}: ${detail}, ${sold.length} sold, ${passing.length} kept${stat.capHit ? " (RESIDUAL CAP)" : ""}\n`);
    } catch (err) {
      stat.error = err.message;
      process.stderr.write(`[${i + 1}/${queries.length}] ${query.label}: ERROR ${err.message}\n`);
    }
    queryStats.push(stat);
    if (i < queries.length - 1 && config.throttleMs && !fetchOptions.bands) {
      await sleep(Number(config.throttleMs));
    }
  }

  const deduped = dedupeRows(allRows);
  const soldDates = deduped.map((r) => r.soldDate).filter(Boolean).sort();
  const monthBreakdown = deduped.reduce((acc, r) => {
    const m = monthOf(r.soldDate);
    if (m) acc[m] = (acc[m] || 0) + 1;
    return acc;
  }, {});
  const report = {
    fetchedAt,
    soldWithinDays,
    configPath: path.relative(PROJECT_DIR, configPath),
    queries: queryStats,
    totalRowsBeforeDedupe: allRows.length,
    totalRows: deduped.length,
    soldDateRange: soldDates.length ? { min: soldDates[0], max: soldDates[soldDates.length - 1] } : null,
    typeBreakdown: deduped.reduce((acc, r) => { acc[r.propertyType || "Unknown"] = (acc[r.propertyType || "Unknown"] || 0) + 1; return acc; }, {}),
    monthBreakdown,
    anyCapHit: queryStats.some((s) => s.capHit),
  };

  if (opts.dryRun) {
    process.stdout.write(`Dry run. Would write ${deduped.length} rows.\n`);
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    return;
  }

  writeCsv(outPath, deduped);
  fs.writeFileSync(reportPath, `${JSON.stringify({ ...report, outFile: path.relative(PROJECT_DIR, outPath) }, null, 2)}\n`);
  process.stdout.write(`Wrote ${deduped.length} sold rows -> ${path.relative(PROJECT_DIR, outPath)}\n`);
  process.stdout.write(`Report -> ${path.relative(PROJECT_DIR, reportPath)}\n`);
  if (report.anyCapHit) {
    const hint = fetchOptions.bands
      ? "even price-banding left a residual cap (350+ sales in a <min-band window). Lower --min-band to split finer."
      : "at least one query hit the 350-row cap. Re-run with --bands to price-band around it, or narrow --sold-within-days.";
    process.stdout.write(`WARNING: ${hint}\n`);
  }
}

if (require.main === module) {
  main().catch((err) => {
    process.stderr.write(`${err.stack || err.message}\n`);
    process.exit(1);
  });
}

module.exports = {
  snapSoldWithinDays,
  buildSoldParams,
  splitBandPoint,
  fetchRegionBanded,
  fetchSoldGis,
  fetchSoldGisWithRetry,
  isSoldHome,
  epochMsToIso,
  homeToRow,
  passesFilters,
  dedupeRows,
  monthOf,
  UI_PROPERTY_TYPE_LABELS,
  OUTPUT_COLUMNS,
};
