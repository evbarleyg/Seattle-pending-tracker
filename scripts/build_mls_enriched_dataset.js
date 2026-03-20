#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_BASE_FILE = path.join(PROJECT_DIR, "public_sales_proxy_all_prices_last12mo.csv");
const DEFAULT_REALTOR_DIR = path.join(PROJECT_DIR, "realtor_exports");
const DEFAULT_OUTPUT_FILE = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const DEFAULT_REPORT_FILE = path.join(PROJECT_DIR, "data_refresh_report.json");
const BASE_FILE = path.resolve(process.env.MLS_BASE_FILE || DEFAULT_BASE_FILE);
const REALTOR_DIR = path.resolve(process.env.MLS_REALTOR_DIR || DEFAULT_REALTOR_DIR);
const OUTPUT_FILE = path.resolve(process.env.MLS_OUTPUT_FILE || DEFAULT_OUTPUT_FILE);
const REPORT_FILE = path.resolve(process.env.MLS_REPORT_FILE || DEFAULT_REPORT_FILE);
const PREVIOUS_ENRICHED_FILE = path.resolve(process.env.MLS_PREVIOUS_ENRICHED_FILE || DEFAULT_OUTPUT_FILE);
const ACCOUNT_FILE = path.join(PROJECT_DIR, "EXTR_RPAcct_NoName.csv");
const RESBLDG_FILE = path.join(PROJECT_DIR, "EXTR_ResBldg.csv");
const PARCEL_COORDS_FILE = path.join(PROJECT_DIR, "parcel_coords_major_minor.csv");
const REALTOR_FILE_PATTERN = /\.csv$/i;
const REQUIRED_REALTOR_COLUMNS = [
  "Status",
  "Listing Date",
  "Pending Date",
  "Contractual Date",
  "Selling Date",
  "Listing Price",
  "Original Price",
  "Selling Price",
  "DOM",
  "CDOM",
];
const MLS_ENRICHMENT_COLUMNS = [
  "mlsListDate",
  "mlsPendingDate",
  "mlsListPriceAtPending",
  "mlsClosePrice",
  "mlsListingNumber",
  "mlsStatus",
  "mlsRegion",
  "mlsSellingDate",
  "mlsContractualDate",
  "mlsListingPrice",
  "mlsSellingPrice",
  "mlsOriginalPrice",
  "mlsDOM",
  "mlsCDOM",
  "mlsStyleCode",
  "mlsSubdivision",
  "mlsParkingType",
  "mlsParkingCoveredTotal",
  "mlsTaxesAnnual",
  "mlsBuildingCondition",
  "mlsView",
  "mlsBankOwned",
  "mlsThirdPartyApprovalRequired",
  "mlsNewConstructionState",
  "mlsSquareFootageSource",
  "mlsDateLagDays",
  "mlsJoinMethod",
  "mlsDaysToPending",
  "mlsDaysPendingToSale",
  "hotMarketTag",
  "saleToListRatio",
  "saleToOriginalListRatio",
  "bidUpAmount",
  "bidUpPct",
];

const MAX_DATE_LAG_DAYS = 45;
const PRICE_TOLERANCE_ABS = 5000;
const PRICE_TOLERANCE_PCT = 0.005;
const LISTING_STUB_MAX_DATE_LAG_DAYS = 120;
const LISTING_STUB_MAX_PRICE_DIFF_PCT = 0.5;
const HOT_MARKET_DAYS = 10;
const ULTRA_HOT_DAYS = 5;

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

function toIsoDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const formats = [
    /^(\d{4})-(\d{2})-(\d{2})$/,
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)?$/,
  ];
  if (formats[0].test(raw)) return raw;
  const m = raw.match(formats[1]);
  if (m) {
    const mm = m[1].padStart(2, "0");
    const dd = m[2].padStart(2, "0");
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return "";
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function toDate(value) {
  const iso = toIsoDate(value);
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function dayDiff(a, b) {
  const d1 = toDate(a);
  const d2 = toDate(b);
  if (!d1 || !d2) return null;
  return Math.round((d2 - d1) / (1000 * 60 * 60 * 24));
}

function num(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function normalizeApn(v) {
  const digits = String(v || "").replace(/\D/g, "");
  return digits.length === 10 ? digits : "";
}

function zip5(v) {
  return (String(v || "").match(/[0-9]{5}/) || [])[0] || "";
}

function zipNeighborhood(zip) {
  return ZIP_NEIGHBORHOOD[zip5(zip)] || "";
}

function clean(v) {
  return String(v || "").replace(/^"|"$/g, "").trim();
}

function normalizeFreeText(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function normalizeIntegerText(value) {
  const raw = normalizeFreeText(value);
  if (!raw) return "";
  return String(Math.round(num(raw)));
}

function normalizeMoneyText(value) {
  const raw = normalizeFreeText(value);
  if (!raw) return "";
  return String(Math.round(num(raw)));
}

function normalizeBooleanText(value) {
  const raw = normalizeFreeText(value);
  if (!raw) return "";
  if (/^(true|yes|y|1)$/i.test(raw)) return "true";
  if (/^(false|no|n|0)$/i.test(raw)) return "false";
  return raw;
}

function normalizeThirdPartyApproval(value) {
  const raw = normalizeFreeText(value);
  if (!raw) return "";
  if (/^none$/i.test(raw)) return "None";
  if (/^short sale$/i.test(raw)) return "Short Sale";
  if (/^other\b/i.test(raw)) return "Other - See Remarks";
  return raw;
}

function parcelDigits(row) {
  const parcel = String(row.parcelNbr || `${row.major || ""}${row.minor || ""}`).replace(/\D/g, "");
  return parcel.length >= 10 ? parcel.slice(-10) : "";
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) return `"${s.replace(/"/g, "\"\"")}"`;
  return s;
}

function normalizeAddressText(value) {
  let s = String(value || "").toUpperCase();
  if (!s) return "";
  s = s.replace(/[.,]/g, " ");
  s = s.replace(/#/g, " UNIT ");
  s = s.replace(/\b(APARTMENT|APT|SUITE|STE)\b/g, " UNIT ");
  s = s.replace(/\bNORTHEAST\b/g, "NE");
  s = s.replace(/\bNORTHWEST\b/g, "NW");
  s = s.replace(/\bSOUTHEAST\b/g, "SE");
  s = s.replace(/\bSOUTHWEST\b/g, "SW");
  s = s.replace(/\bNORTH\b/g, "N");
  s = s.replace(/\bSOUTH\b/g, "S");
  s = s.replace(/\bEAST\b/g, "E");
  s = s.replace(/\bWEST\b/g, "W");
  s = s.replace(/\bAVENUE\b/g, "AVE");
  s = s.replace(/\bSTREET\b/g, "ST");
  s = s.replace(/\bBOULEVARD\b/g, "BLVD");
  s = s.replace(/\bPLACE\b/g, "PL");
  s = s.replace(/\bCOURT\b/g, "CT");
  s = s.replace(/\bDRIVE\b/g, "DR");
  s = s.replace(/\bROAD\b/g, "RD");
  s = s.replace(/\bLANE\b/g, "LN");
  s = s.replace(/\bTERRACE\b/g, "TER");
  s = s.replace(/\bPARKWAY\b/g, "PKWY");
  s = s.replace(/\bHIGHWAY\b/g, "HWY");
  s = s.replace(/\bCIRCLE\b/g, "CIR");
  s = s.replace(/\bJUNIOR\b/g, "JR");
  s = s.replace(/\bWY\b/g, "WAY");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

function canonicalStreet(parts) {
  return normalizeAddressText(parts.filter(Boolean).join(" "));
}

function canonicalAddressKey(street, zip) {
  const normalizedStreet = normalizeAddressText(street);
  if (!normalizedStreet) return "";
  return `${normalizedStreet}|${zip5(zip)}`;
}

function stripTrailingUnit(text) {
  const normalized = normalizeAddressText(text);
  if (!normalized) return "";
  return normalized.replace(/\bUNIT\s+[A-Z0-9-]+\s*$/, "").trim();
}

function buildHotMarketTag(dom, daysToPending) {
  const hasUltra = (dom > 0 && dom <= ULTRA_HOT_DAYS) || (daysToPending !== null && daysToPending <= ULTRA_HOT_DAYS);
  if (hasUltra) return "ULTRA_HOT_<=5D";
  const hasHot = (dom > 0 && dom <= HOT_MARKET_DAYS) || (daysToPending !== null && daysToPending <= HOT_MARKET_DAYS);
  if (hasHot) return "HOT_MARKET_<=10D";
  return "";
}

function inferTypeFromMlsStyle(styleCode) {
  const s = String(styleCode || "").toLowerCase();
  if (!s) return "Single Family";
  if (s.includes("townhouse")) return "Townhouse";
  if (s.includes("condo")) return "Condo";
  if (s.includes("multi")) return "Multi-Family";
  if (s.includes("floating")) return "Floating Home";
  return "Single Family";
}

function buildParcelSnapshotByApn(rows) {
  const out = new Map();
  rows.forEach((row) => {
    const apn = row.__parcel;
    if (!apn) return;
    const prev = out.get(apn) || {
      address: "",
      neighborhood: "",
      type: "",
      typeCode: "",
      addressSource: "",
      zip: "",
      districtName: "",
      area: "",
      subArea: "",
      sqFtLot: "",
      zoning: "",
      lat: "",
      lon: "",
      beds: "",
      baths: "",
      sqft: "",
      yearBuilt: "",
      assessedValue: "",
    };
    const next = { ...prev };
    if (!next.address && row.address) next.address = row.address;
    if (!next.neighborhood && row.neighborhood) next.neighborhood = row.neighborhood;
    if (!next.type && row.type) next.type = row.type;
    if (!next.typeCode && row.typeCode) next.typeCode = row.typeCode;
    if (!next.addressSource && row.addressSource) next.addressSource = row.addressSource;
    if (!next.zip && row.zip) next.zip = row.zip;
    if (!next.districtName && row.districtName) next.districtName = row.districtName;
    if (!next.area && row.area) next.area = row.area;
    if (!next.subArea && row.subArea) next.subArea = row.subArea;
    if (!next.sqFtLot && row.sqFtLot) next.sqFtLot = row.sqFtLot;
    if (!next.zoning && row.zoning) next.zoning = row.zoning;
    if (!next.lat && row.lat) next.lat = row.lat;
    if (!next.lon && row.lon) next.lon = row.lon;
    if (!next.beds && row.beds) next.beds = row.beds;
    if (!next.baths && row.baths) next.baths = row.baths;
    if (!next.sqft && row.sqft) next.sqft = row.sqft;
    if (!next.yearBuilt && row.yearBuilt) next.yearBuilt = row.yearBuilt;
    if (!next.assessedValue && row.assessedValue) next.assessedValue = row.assessedValue;
    out.set(apn, next);
  });
  return out;
}

function readParcelCoordsByApn() {
  if (!fs.existsSync(PARCEL_COORDS_FILE)) return new Map();
  const text = fs.readFileSync(PARCEL_COORDS_FILE, "utf8");
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length) return new Map();
  const headers = parseCsvLine(lines[0]).map((h) => String(h || "").trim());
  const idx = Object.fromEntries(headers.map((h, i) => [h, i]));
  const majorIdx = idx.major ?? idx.Major;
  const minorIdx = idx.minor ?? idx.Minor;
  const parcelIdx = idx.parcelNbr ?? idx.ParcelNbr ?? idx.parcel ?? idx.Parcel;
  const latIdx = idx.lat ?? idx.Lat;
  const lonIdx = idx.lon ?? idx.Lon ?? idx.lng ?? idx.Lng;
  if (latIdx === undefined || lonIdx === undefined) return new Map();

  const out = new Map();
  lines.slice(1).forEach((line) => {
    const cols = parseCsvLine(line);
    const major = majorIdx !== undefined ? String(cols[majorIdx] || "").replace(/\D/g, "").slice(-6).padStart(6, "0") : "";
    const minor = minorIdx !== undefined ? String(cols[minorIdx] || "").replace(/\D/g, "").slice(-4).padStart(4, "0") : "";
    let apn = `${major}${minor}`.replace(/\D/g, "");
    if (apn.length !== 10 && parcelIdx !== undefined) {
      apn = String(cols[parcelIdx] || "").replace(/\D/g, "").slice(-10);
    }
    if (apn.length !== 10) return;
    const lat = num(cols[latIdx]);
    const lon = num(cols[lonIdx]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
    if (lat < 46 || lat > 49 || lon > -121 || lon < -124) return;
    out.set(apn, { lat: String(lat), lon: String(lon) });
  });
  return out;
}

function normalizeHeaderNames(headers) {
  const seen = new Map();
  return headers.map((h) => {
    const key = String(h || "").trim();
    const n = (seen.get(key) || 0) + 1;
    seen.set(key, n);
    return n === 1 ? key : `${key} (${n})`;
  });
}

function regionFromFilename(file) {
  const stem = String(file || "")
    .replace(/\.csv$/i, "")
    .replace(/\bsold\s+and\b/ig, "")
    .replace(/\b(?:sale\s+)?stats?\b/ig, "")
    .replace(/\b(?:rich\s+snapshot|snapshot|full)\b/ig, "")
    .replace(/\b\d{1,2}_\d{1,2}\s+to\s+\d{1,2}_\d{1,2}\b/ig, "")
    .replace(/\bcental\b/ig, "Central")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (/^qa$/i.test(stem)) return "QA Magnolia";
  if (/^ne$/i.test(stem)) return "NE Seattle";
  if (/^nw$/i.test(stem)) return "NW Seattle";
  if (/^central$/i.test(stem)) return "Central Seattle";
  return stem || "Seattle";
}

function normalizeMlsStatus(raw) {
  const up = String(raw || "").trim().toUpperCase();
  if (!up) return "";
  if (up === "SOLD") return "Sold";
  if (up === "ACTIVE") return "Active";
  if (up === "PENDING") return "Pending";
  if (up === "PENDING INSPECTION") return "Pending Inspection";
  if (up === "PENDING BU REQUESTED") return "Pending BU Requested";
  if (up === "CONTINGENT") return "Contingent";
  return up
    .toLowerCase()
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function discoverRealtorFiles() {
  if (!fs.existsSync(REALTOR_DIR)) return [];
  return fs.readdirSync(REALTOR_DIR)
    .filter((name) => REALTOR_FILE_PATTERN.test(name))
    .filter((name) => !name.startsWith("."))
    .sort((a, b) => a.localeCompare(b))
    .map((file) => ({
      file,
      region: regionFromFilename(file),
      full: path.join(REALTOR_DIR, file),
    }));
}

function findHeaderIndex(lines) {
  for (let i = 0; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]).map((c) => String(c || "").trim());
    if (cols.includes("Listing Number") && cols.includes("Status")) return i;
  }
  return -1;
}

function mlsAddressFromParts(row) {
  const street = [
    row["Street Number"],
    row["Street Direction"],
    row["Street Name"],
    row["Street Suffix"],
    row["Street Post Direction"],
    row.Unit,
  ].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  const city = String(row.City || "").trim();
  const state = String(row.State || "").trim();
  const zip = String(row["Zip Code"] || "").trim();
  const tail = [city, state, zip].filter(Boolean).join(" ");
  return [street, tail].filter(Boolean).join(", ").trim();
}

function canonicalMlsStreet(row) {
  return canonicalStreet([
    row["Street Number"],
    row.Unit,
    row["Street Direction"],
    row["Street Name"],
    row["Street Suffix"],
    row["Street Post Direction"],
  ]);
}

function canonicalMlsStreetNoUnit(row) {
  return canonicalStreet([
    row["Street Number"],
    row["Street Direction"],
    row["Street Name"],
    row["Street Suffix"],
    row["Street Post Direction"],
  ]);
}

function readBaseRows() {
  const text = fs.readFileSync(BASE_FILE, "utf8");
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  const headers = parseCsvLine(lines[0]);
  const idx = Object.fromEntries(headers.map((h, i) => [h, i]));
  const rows = lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cols[i] || ""; });
    obj.__parcel = parcelDigits(obj);
    obj.__saleDate = toIsoDate(obj.saleDate);
    obj.__closePrice = Math.round(num(obj.closePrice));
    obj.__id = `${obj.id || ""}|${obj.__parcel}|${obj.__saleDate}|${obj.address || ""}`;
    obj.__index = idx;
    return obj;
  });
  return { headers, rows };
}

function addSetValue(map, key, value) {
  if (!key || !value) return;
  if (!map.has(key)) map.set(key, new Set());
  map.get(key).add(value);
}

function uniqueSetValue(map, key) {
  const values = map.get(key);
  if (!values || values.size !== 1) return "";
  return [...values][0];
}

function readPreviousListingApnMap() {
  const out = new Map();
  if (!fs.existsSync(PREVIOUS_ENRICHED_FILE)) return out;

  const text = fs.readFileSync(PREVIOUS_ENRICHED_FILE, "utf8");
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return out;

  const headers = parseCsvLine(lines[0]);
  const idx = Object.fromEntries(headers.map((h, i) => [h, i]));
  const listingIdx = idx.mlsListingNumber;
  const parcelIdx = idx.parcelNbr;
  if (listingIdx === undefined || parcelIdx === undefined) return out;

  lines.slice(1).forEach((line) => {
    const cols = parseCsvLine(line);
    const listingNumber = String(cols[listingIdx] || "").trim();
    const apn = normalizeApn(cols[parcelIdx]);
    if (!listingNumber || !apn || out.has(listingNumber)) return;
    out.set(listingNumber, apn);
  });
  return out;
}

async function readCountyAddressMaps(targetExactKeys, targetNoUnitKeys) {
  const out = {
    resBldgExactByAddress: new Map(),
    resBldgNoUnitByAddress: new Map(),
    accountExactByAddress: new Map(),
    accountNoUnitByAddress: new Map(),
  };

  if (!targetExactKeys.size && !targetNoUnitKeys.size) return out;

  if (fs.existsSync(RESBLDG_FILE)) {
    const rl = readline.createInterface({
      input: fs.createReadStream(RESBLDG_FILE),
      crlfDelay: Infinity,
    });
    let idx = null;
    for await (const line of rl) {
      if (!line) continue;
      if (!idx) {
        const headers = parseCsvLine(line).map((h) => clean(h));
        idx = Object.fromEntries(headers.map((h, i) => [h, i]));
        continue;
      }
      const cols = parseCsvLine(line);
      const apn = normalizeApn(`${clean(cols[idx.Major])}${clean(cols[idx.Minor])}`);
      if (!apn) continue;

      const zip = clean(cols[idx.ZipCode]);
      const exactKey = canonicalAddressKey(
        canonicalStreet([
          clean(cols[idx.BuildingNumber]),
          clean(cols[idx.Fraction]),
          clean(cols[idx.DirectionPrefix]),
          clean(cols[idx.StreetName]),
          clean(cols[idx.StreetType]),
          clean(cols[idx.DirectionSuffix]),
        ]),
        zip
      );
      const noUnitKey = canonicalAddressKey(
        canonicalStreet([
          clean(cols[idx.BuildingNumber]),
          clean(cols[idx.DirectionPrefix]),
          clean(cols[idx.StreetName]),
          clean(cols[idx.StreetType]),
          clean(cols[idx.DirectionSuffix]),
        ]),
        zip
      );

      if (targetExactKeys.has(exactKey)) addSetValue(out.resBldgExactByAddress, exactKey, apn);
      if (targetNoUnitKeys.has(noUnitKey)) addSetValue(out.resBldgNoUnitByAddress, noUnitKey, apn);
    }
  }

  if (fs.existsSync(ACCOUNT_FILE)) {
    const rl = readline.createInterface({
      input: fs.createReadStream(ACCOUNT_FILE),
      crlfDelay: Infinity,
    });
    let idx = null;
    for await (const line of rl) {
      if (!line) continue;
      if (!idx) {
        const headers = parseCsvLine(line).map((h) => clean(h));
        idx = Object.fromEntries(headers.map((h, i) => [h, i]));
        continue;
      }
      const cols = parseCsvLine(line);
      const apn = normalizeApn(`${clean(cols[idx.Major])}${clean(cols[idx.Minor])}`);
      if (!apn) continue;

      const addr = clean(cols[idx.AddrLine]);
      const zip = clean(cols[idx.ZipCode]);
      const exactKey = canonicalAddressKey(addr, zip);
      const noUnitKey = canonicalAddressKey(stripTrailingUnit(addr), zip);
      if (targetExactKeys.has(exactKey)) addSetValue(out.accountExactByAddress, exactKey, apn);
      if (targetNoUnitKeys.has(noUnitKey)) addSetValue(out.accountNoUnitByAddress, noUnitKey, apn);
    }
  }

  return out;
}

async function resolveRealtorApns(rows, options = {}) {
  const counts = {
    directApn: 0,
    previousListingNumber: 0,
    resBldgExact: 0,
    accountExact: 0,
    resBldgNoUnit: 0,
    accountNoUnit: 0,
    unresolved: 0,
  };
  if (!rows.length) return counts;

  const listingApnByListingNumber = options.listingApnByListingNumber || readPreviousListingApnMap();
  const targetExactKeys = new Set();
  const targetNoUnitKeys = new Set();

  rows.forEach((row) => {
    if (row.apn) return;
    if (row.addressKeyExact) targetExactKeys.add(row.addressKeyExact);
    if (row.addressKeyNoUnit) targetNoUnitKeys.add(row.addressKeyNoUnit);
  });

  const countyAddressMaps = options.countyAddressMaps || await readCountyAddressMaps(targetExactKeys, targetNoUnitKeys);

  rows.forEach((row) => {
    if (row.apn) {
      row.apnResolutionMethod = "REALTOR_APN";
      counts.directApn += 1;
      return;
    }

    const byListingNumber = row.listingNumber ? listingApnByListingNumber.get(row.listingNumber) : "";
    const byResBldgExact = uniqueSetValue(countyAddressMaps.resBldgExactByAddress, row.addressKeyExact);
    const byAccountExact = uniqueSetValue(countyAddressMaps.accountExactByAddress, row.addressKeyExact);
    const byResBldgNoUnit = uniqueSetValue(countyAddressMaps.resBldgNoUnitByAddress, row.addressKeyNoUnit);
    const byAccountNoUnit = uniqueSetValue(countyAddressMaps.accountNoUnitByAddress, row.addressKeyNoUnit);

    if (byListingNumber) {
      row.apn = byListingNumber;
      row.apnResolutionMethod = "PREVIOUS_ENRICHED_LISTING_NUMBER";
      counts.previousListingNumber += 1;
    } else if (byResBldgExact) {
      row.apn = byResBldgExact;
      row.apnResolutionMethod = "COUNTY_RESBLDG_EXACT";
      counts.resBldgExact += 1;
    } else if (byAccountExact) {
      row.apn = byAccountExact;
      row.apnResolutionMethod = "COUNTY_ACCOUNT_EXACT";
      counts.accountExact += 1;
    } else if (byResBldgNoUnit) {
      row.apn = byResBldgNoUnit;
      row.apnResolutionMethod = "COUNTY_RESBLDG_NO_UNIT";
      counts.resBldgNoUnit += 1;
    } else if (byAccountNoUnit) {
      row.apn = byAccountNoUnit;
      row.apnResolutionMethod = "COUNTY_ACCOUNT_NO_UNIT";
      counts.accountNoUnit += 1;
    } else {
      row.apnResolutionMethod = "";
      counts.unresolved += 1;
    }
  });

  return counts;
}

function hasMeaningfulValue(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "number") return Number.isFinite(value) && value !== 0;
  return String(value).trim() !== "";
}

function countMeaningfulFields(row) {
  return [
    row.apn,
    row.listingDate,
    row.pendingDate,
    row.contractualDate,
    row.sellingDate,
    row.listingPrice,
    row.sellingPrice,
    row.originalPrice,
    row.dom,
    row.cdom,
    row.styleCode,
    row.subdivision,
    row.beds,
    row.baths,
    row.sqft,
    row.yearBuilt,
    row.zip,
    row.mlsAddress,
    row.parkingType,
    row.parkingCoveredTotal,
    row.taxesAnnual,
    row.buildingCondition,
    row.view,
    row.bankOwned,
    row.thirdPartyApprovalRequired,
    row.newConstructionState,
    row.squareFootageSource,
  ].filter(hasMeaningfulValue).length;
}

function statusPriority(status) {
  const s = String(status || "").trim().toUpperCase();
  if (s === "SOLD") return 700;
  if (s === "PENDING INSPECTION") return 630;
  if (s === "PENDING BU REQUESTED") return 620;
  if (s === "PENDING FEASIBILITY") return 615;
  if (s === "PENDING SHORT SALE") return 610;
  if (s === "PENDING") return 600;
  if (s === "CONTINGENT") return 500;
  if (s === "ACTIVE") return 100;
  return 0;
}

function datePriorityValue(value) {
  const iso = toIsoDate(value);
  if (!iso) return 0;
  return Number(iso.replace(/-/g, ""));
}

function freshnessPriorityValue(row) {
  return Math.max(
    datePriorityValue(row.sellingDate),
    datePriorityValue(row.pendingDate),
    datePriorityValue(row.contractualDate)
  );
}

function compareRealtorRows(a, b) {
  const comparisons = [
    statusPriority(b.status) - statusPriority(a.status),
    freshnessPriorityValue(b) - freshnessPriorityValue(a),
    datePriorityValue(b.listingDate) - datePriorityValue(a.listingDate),
    (Number(b.cdom) || 0) - (Number(a.cdom) || 0),
    (Number(b.dom) || 0) - (Number(a.dom) || 0),
    countMeaningfulFields(b) - countMeaningfulFields(a),
    (b.apn ? 1 : 0) - (a.apn ? 1 : 0),
  ];
  for (const diff of comparisons) {
    if (diff !== 0) return diff;
  }
  return String(a.uid || "").localeCompare(String(b.uid || ""));
}

function dedupeRealtorRows(rows) {
  const byListingNumber = new Map();
  const uniques = [];

  rows.forEach((row) => {
    if (!row.listingNumber) {
      uniques.push(row);
      return;
    }
    if (!byListingNumber.has(row.listingNumber)) byListingNumber.set(row.listingNumber, []);
    byListingNumber.get(row.listingNumber).push(row);
  });

  const mergedRows = [];
  let duplicateListingCount = 0;
  let duplicateRowsCollapsed = 0;
  let apnConflictListingCount = 0;
  let apnConflictRowCount = 0;

  const fillableFields = [
    "apn",
    "listingDate",
    "pendingDate",
    "contractualDate",
    "sellingDate",
    "listingPrice",
    "sellingPrice",
    "originalPrice",
    "domRaw",
    "cdomRaw",
    "dom",
    "cdom",
    "styleCode",
    "subdivision",
    "beds",
    "baths",
    "sqft",
    "yearBuilt",
    "city",
    "state",
    "zip",
    "mlsAddress",
    "addressKeyExact",
    "addressKeyNoUnit",
    "parkingType",
    "parkingCoveredTotal",
    "taxesAnnual",
    "buildingCondition",
    "view",
    "bankOwned",
    "thirdPartyApprovalRequired",
    "newConstructionState",
    "squareFootageSource",
  ];

  byListingNumber.forEach((group) => {
    if (group.length === 1) {
      mergedRows.push(group[0]);
      return;
    }

    const apns = [...new Set(group.map((row) => normalizeApn(row.apn)).filter(Boolean))];
    if (apns.length > 1) {
      apnConflictListingCount += 1;
      apnConflictRowCount += group.length;
      mergedRows.push(...group);
      return;
    }

    duplicateListingCount += 1;
    duplicateRowsCollapsed += group.length - 1;
    const ranked = [...group].sort(compareRealtorRows);
    const merged = { ...ranked[0] };

    ranked.slice(1).forEach((candidate) => {
      fillableFields.forEach((field) => {
        if (!hasMeaningfulValue(merged[field]) && hasMeaningfulValue(candidate[field])) {
          merged[field] = candidate[field];
        }
      });
    });

    if (merged.apn) merged.apnResolutionMethod = "REALTOR_APN";
    mergedRows.push(merged);
  });

  return {
    rows: [...uniques, ...mergedRows],
    counts: {
      duplicateListingCount,
      duplicateRowsCollapsed,
      apnConflictListingCount,
      apnConflictRowCount,
    },
  };
}

function readRealtorRows(discoveredFiles) {
  const out = [];
  const discovered = discoveredFiles || discoverRealtorFiles();
  if (!discovered.length) {
    throw new Error(`No realtor CSV files found in ${REALTOR_DIR}`);
  }

  discovered.forEach(({ file, region, full }) => {
    const text = fs.readFileSync(full, "utf8");
    const lines = text.split(/\r?\n/).filter((line) => line.trim());
    if (lines.length < 2) return;

    const headerIndex = findHeaderIndex(lines);
    if (headerIndex < 0 || headerIndex >= lines.length - 1) {
      throw new Error(`Could not find header row with Listing Number/Status in realtor file: ${file}`);
    }

    const headers = normalizeHeaderNames(parseCsvLine(lines[headerIndex]));
    const missingRequired = REQUIRED_REALTOR_COLUMNS.filter((h) => !headers.includes(h));
    if (missingRequired.length) {
      throw new Error(`Realtor file ${file} missing required columns: ${missingRequired.join(", ")}`);
    }

    lines.slice(headerIndex + 1).forEach((line, rowIndex) => {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => { row[h] = (cols[i] || "").trim(); });

      const status = normalizeMlsStatus(row.Status);
      const sellingDate = toIsoDate(row["Selling Date"]);
      const sellingPrice = Math.round(num(row["Selling Price"]));

      const apn = normalizeApn(row.APN);
      const listingDate = toIsoDate(row["Listing Date"]);
      const pendingDate = toIsoDate(row["Pending Date"]);
      const contractualDate = toIsoDate(row["Contractual Date"]);
      const listingPrice = Math.round(num(row["Listing Price"]));
      const originalPrice = Math.round(num(row["Original Price"]));
      const domRaw = String(row.DOM || "").trim();
      const cdomRaw = String(row.CDOM || "").trim();
      const dom = Math.round(num(domRaw));
      const cdom = Math.round(num(cdomRaw));
      const listingNumber = String(row["Listing Number"] || row["Listing Number (2)"] || "").trim();
      if (!listingNumber) return;

      const mlsAddress = mlsAddressFromParts(row);
      const addressKeyExact = canonicalAddressKey(canonicalMlsStreet(row), row["Zip Code"]);
      const addressKeyNoUnit = canonicalAddressKey(canonicalMlsStreetNoUnit(row), row["Zip Code"]);
      const uid = [file, listingNumber, apn, status, sellingDate, sellingPrice, rowIndex].join("|");
      const isClosedStatus = /sold|closed/i.test(status);
      const hasCloseRecord = !!sellingDate && sellingPrice > 0;
      const isClosed = isClosedStatus || hasCloseRecord;

      out.push({
        uid,
        region,
        status,
        isClosed,
        apn,
        listingNumber,
        listingDate,
        pendingDate,
        contractualDate,
        sellingDate,
        listingPrice,
        sellingPrice,
        originalPrice,
        domRaw,
        cdomRaw,
        dom,
        cdom,
        styleCode: String(row["Style Code"] || "").trim(),
        subdivision: String(row.Subdivision || "").trim(),
        beds: Math.round(num(row.Bedrooms)),
        baths: num(row.Bathrooms),
        sqft: Math.round(num(row["Square Footage"] || row["Square Footage Finished"])),
        yearBuilt: Math.round(num(row["Year Built"])),
        city: String(row.City || "").trim(),
        state: String(row.State || "").trim(),
        zip: zip5(row["Zip Code"]),
        mlsAddress,
        addressKeyExact,
        addressKeyNoUnit,
        parkingType: normalizeFreeText(row["Parking Type"]),
        parkingCoveredTotal: normalizeIntegerText(row["Parking Covered Total"]),
        taxesAnnual: normalizeMoneyText(row["Taxes Annual"]),
        buildingCondition: normalizeFreeText(row["Building Condition"]),
        view: normalizeFreeText(row.View),
        bankOwned: normalizeBooleanText(row["Bank Or Real Estate Owned"]),
        thirdPartyApprovalRequired: normalizeThirdPartyApproval(row["Third Party Approval Required"]),
        newConstructionState: normalizeFreeText(row["New Construction State"]),
        squareFootageSource: normalizeFreeText(row["Square Footage Source"]),
        apnResolutionMethod: apn ? "REALTOR_APN" : "",
      });
    });
  });
  return out;
}

function chooseBestMatch(baseRow, candidates, usedIds) {
  const baseDate = toDate(baseRow.__saleDate);
  const basePrice = baseRow.__closePrice;
  if (!baseDate || !basePrice) return null;

  let best = null;
  candidates.forEach((c) => {
    if (usedIds.has(c.uid)) return;
    if (!c.sellingDate || !c.sellingPrice) return;
    const cDate = toDate(c.sellingDate);
    if (!cDate) return;
    const dateLag = Math.abs(dayDiff(baseRow.__saleDate, c.sellingDate));
    if (dateLag === null || dateLag > MAX_DATE_LAG_DAYS) return;

    const priceDiff = Math.abs(c.sellingPrice - basePrice);
    const priceDiffPct = basePrice > 0 ? (priceDiff / basePrice) : 1;
    const priceOk = priceDiff <= PRICE_TOLERANCE_ABS || priceDiffPct <= PRICE_TOLERANCE_PCT;
    if (!priceOk) return;

    const score = (dateLag * 100000) + priceDiff;
    if (!best || score < best.score) {
      best = {
        score,
        dateLag,
        priceDiff,
        candidate: c,
      };
    }
  });
  return best;
}

function stubAnchorDate(c) {
  return c.pendingDate || c.contractualDate || c.listingDate || "";
}

function statusStubWeight(status) {
  const s = String(status || "").toUpperCase();
  if (s === "PENDING") return 0;
  if (s === "PENDING INSPECTION") return 1;
  if (s === "PENDING BU REQUESTED") return 2;
  if (s === "CONTINGENT") return 3;
  if (s === "ACTIVE") return 4;
  return 5;
}

function chooseBestListingStub(baseRow, candidates, usedIds) {
  const baseDate = toDate(baseRow.__saleDate);
  const basePrice = baseRow.__closePrice;
  if (!baseDate || !basePrice) return null;

  let best = null;
  candidates.forEach((c) => {
    if (usedIds.has(c.uid)) return;
    if (c.listingPrice <= 0) return;
    const anchor = stubAnchorDate(c);
    if (!anchor) return;

    const lagDays = dayDiff(anchor, baseRow.__saleDate);
    if (lagDays === null || lagDays < 0 || lagDays > LISTING_STUB_MAX_DATE_LAG_DAYS) return;

    const priceDiff = Math.abs(c.listingPrice - basePrice);
    const priceDiffPct = basePrice > 0 ? (priceDiff / basePrice) : 1;
    if (priceDiffPct > LISTING_STUB_MAX_PRICE_DIFF_PCT) return;

    const score = (lagDays * 100000) + (statusStubWeight(c.status) * 10000) + priceDiff;
    if (!best || score < best.score) {
      best = {
        score,
        lagDays,
        priceDiff,
        candidate: c,
      };
    }
  });
  return best;
}

function writeRefreshReport(report) {
  fs.writeFileSync(REPORT_FILE, `${JSON.stringify(report, null, 2)}\n`);
}

function buildMlsFieldValues(candidate, options = {}) {
  const listPrice = Number(options.listPrice || 0);
  const closePrice = Number(options.closePrice || 0);
  const originalPrice = Number(options.originalPrice || 0);
  const includeOutcomeMetrics = options.includeOutcomeMetrics !== false;
  const daysToPending = options.daysToPending;
  const daysPendingToSale = options.daysPendingToSale;
  const saleToListRatio = Number(options.saleToListRatio || 0);
  const saleToOriginalRatio = Number(options.saleToOriginalRatio || 0);
  const bidUpAmount = Number(options.bidUpAmount || 0);
  const bidUpPct = Number(options.bidUpPct || 0);

  return {
    mlsListDate: candidate.listingDate || "",
    mlsPendingDate: candidate.pendingDate || "",
    mlsListPriceAtPending: listPrice > 0 ? String(listPrice) : "",
    mlsClosePrice: closePrice > 0 ? String(closePrice) : "",
    mlsListingNumber: candidate.listingNumber || "",
    mlsStatus: candidate.status || "",
    mlsRegion: candidate.region || "",
    mlsSellingDate: candidate.sellingDate || "",
    mlsContractualDate: candidate.contractualDate || "",
    mlsListingPrice: candidate.listingPrice > 0 ? String(candidate.listingPrice) : "",
    mlsSellingPrice: closePrice > 0 ? String(closePrice) : "",
    mlsOriginalPrice: originalPrice > 0 ? String(originalPrice) : "",
    mlsDOM: candidate.domRaw !== "" ? String(candidate.dom) : "",
    mlsCDOM: candidate.cdomRaw !== "" ? String(candidate.cdom) : "",
    mlsStyleCode: candidate.styleCode || "",
    mlsSubdivision: candidate.subdivision || "",
    mlsParkingType: candidate.parkingType || "",
    mlsParkingCoveredTotal: candidate.parkingCoveredTotal || "",
    mlsTaxesAnnual: candidate.taxesAnnual || "",
    mlsBuildingCondition: candidate.buildingCondition || "",
    mlsView: candidate.view || "",
    mlsBankOwned: candidate.bankOwned || "",
    mlsThirdPartyApprovalRequired: candidate.thirdPartyApprovalRequired || "",
    mlsNewConstructionState: candidate.newConstructionState || "",
    mlsSquareFootageSource: candidate.squareFootageSource || "",
    mlsDateLagDays: options.dateLagDays === null || options.dateLagDays === undefined || options.dateLagDays === ""
      ? ""
      : String(options.dateLagDays),
    mlsJoinMethod: options.joinMethod || "",
    mlsDaysToPending: daysToPending === null || daysToPending === undefined ? "" : String(daysToPending),
    mlsDaysPendingToSale: daysPendingToSale === null || daysPendingToSale === undefined ? "" : String(daysPendingToSale),
    hotMarketTag: buildHotMarketTag(candidate.dom, daysToPending),
    saleToListRatio: includeOutcomeMetrics && saleToListRatio > 0 ? saleToListRatio.toFixed(4) : "",
    saleToOriginalListRatio: includeOutcomeMetrics && saleToOriginalRatio > 0 ? saleToOriginalRatio.toFixed(4) : "",
    bidUpAmount: includeOutcomeMetrics ? String(Math.round(bidUpAmount)) : "",
    bidUpPct: includeOutcomeMetrics && listPrice > 0 ? bidUpPct.toFixed(4) : "",
  };
}

function clearMlsFieldValues(row) {
  MLS_ENRICHMENT_COLUMNS.forEach((column) => {
    row[column] = "";
  });
}

async function main() {
  if (!fs.existsSync(BASE_FILE)) throw new Error(`Missing base dataset: ${BASE_FILE}`);
  if (!fs.existsSync(REALTOR_DIR)) throw new Error(`Missing realtor directory: ${REALTOR_DIR}`);

  const realtorFiles = discoverRealtorFiles();
  if (!realtorFiles.length) throw new Error(`No realtor CSV files found in ${REALTOR_DIR}`);
  const { headers, rows } = readBaseRows();
  const rawMlsRows = readRealtorRows(realtorFiles);
  const dedupedMls = dedupeRealtorRows(rawMlsRows);
  const mlsRows = dedupedMls.rows;
  const apnResolutionCounts = await resolveRealtorApns(mlsRows);
  const resolvedMlsRows = mlsRows.filter((r) => !!r.apn);
  const unresolvedMlsRows = mlsRows.filter((r) => !r.apn);
  const mlsClosedRows = resolvedMlsRows.filter((r) => r.isClosed && r.sellingDate && r.sellingPrice > 0);
  const mlsOpenRows = resolvedMlsRows.filter((r) => !r.isClosed);
  const mlsActiveRows = resolvedMlsRows.filter((r) => r.status === "Active" && !r.isClosed);
  const parcelSnapshotByApn = buildParcelSnapshotByApn(rows);
  const parcelCoordsByApn = readParcelCoordsByApn();
  const knownCountySaleSignatures = new Set(
    rows
      .filter((r) => r.__parcel && r.__saleDate && r.__closePrice > 0)
      .map((r) => `${r.__parcel}|${r.__saleDate}|${r.__closePrice}`)
  );
  const mlsClosedByApn = new Map();
  mlsClosedRows.forEach((r) => {
    if (!mlsClosedByApn.has(r.apn)) mlsClosedByApn.set(r.apn, []);
    mlsClosedByApn.get(r.apn).push(r);
  });
  const mlsStubByApn = new Map();
  mlsOpenRows.forEach((r) => {
    if (!r.listingPrice || r.listingPrice <= 0) return;
    if (!mlsStubByApn.has(r.apn)) mlsStubByApn.set(r.apn, []);
    mlsStubByApn.get(r.apn).push(r);
  });

  const used = new Set();
  let matched = 0;
  let listingStubbed = 0;
  const merged = rows.map((row) => {
    const cands = mlsClosedByApn.get(row.__parcel) || [];
    const match = chooseBestMatch(row, cands, used);
    const out = { ...row };

    if (match) {
      const c = match.candidate;
      used.add(c.uid);
      matched += 1;

      out.dataMode = "MLS_ENRICHED";
      if (c.mlsAddress) {
        out.address = c.mlsAddress;
        out.addressSource = "MLS_ADDRESS";
      }
      out.listDate = c.listingDate || out.listDate;
      out.pendingDate = c.pendingDate || out.pendingDate;
      out.saleDate = c.sellingDate || out.saleDate;
      out.listPriceAtPending = c.listingPrice > 0 ? String(c.listingPrice) : out.listPriceAtPending;
      out.closePrice = c.sellingPrice > 0 ? String(c.sellingPrice) : out.closePrice;
      if (c.beds > 0) out.beds = String(c.beds);
      if (c.baths > 0) out.baths = String(c.baths);
      if (c.sqft > 0) out.sqft = String(c.sqft);
      if (c.yearBuilt > 0) out.yearBuilt = String(c.yearBuilt);

      const daysToPending = dayDiff(c.listingDate, c.pendingDate);
      const daysPendingToSale = dayDiff(c.pendingDate, c.sellingDate);
      const close = c.sellingPrice > 0 ? c.sellingPrice : Math.round(num(out.closePrice));
      const list = c.listingPrice > 0 ? c.listingPrice : Math.round(num(out.listPriceAtPending));
      const original = c.originalPrice > 0 ? c.originalPrice : 0;
      const bidUp = (close > 0 && list > 0) ? (close - list) : 0;
      const bidUpPct = (close > 0 && list > 0) ? (bidUp / list) : 0;
      const saleToListRatio = (close > 0 && list > 0) ? (close / list) : 0;
      const saleToOriginalRatio = (close > 0 && original > 0) ? (close / original) : 0;
      Object.assign(out, buildMlsFieldValues(c, {
        listPrice: list,
        closePrice: close,
        originalPrice: original,
        dateLagDays: match.dateLag,
        joinMethod: "APN_PRICE_DATE_WINDOW",
        daysToPending,
        daysPendingToSale,
        saleToListRatio,
        saleToOriginalRatio,
        bidUpAmount: bidUp,
        bidUpPct,
      }));
    } else {
      const stubCands = mlsStubByApn.get(row.__parcel) || [];
      const stub = chooseBestListingStub(row, stubCands, used);
      if (stub) {
        const c = stub.candidate;
        used.add(c.uid);
        listingStubbed += 1;

        out.dataMode = "MLS_ENRICHED";
        const addressMissing = !out.address || out.addressSource === "PARCEL_FALLBACK" || /address unavailable/i.test(out.address);
        if (c.mlsAddress && addressMissing) {
          out.address = c.mlsAddress;
          out.addressSource = "MLS_ADDRESS";
        }
        if (c.listingDate) out.listDate = c.listingDate;
        if (c.pendingDate) out.pendingDate = c.pendingDate;
        if (c.listingPrice > 0) out.listPriceAtPending = String(c.listingPrice);

        if (!out.type || out.type === "Unknown") out.type = inferTypeFromMlsStyle(c.styleCode);
        if (c.beds > 0 && !num(out.beds)) out.beds = String(c.beds);
        if (c.baths > 0 && !num(out.baths)) out.baths = String(c.baths);
        if (c.sqft > 0 && !num(out.sqft)) out.sqft = String(c.sqft);
        if (c.yearBuilt > 0 && !num(out.yearBuilt)) out.yearBuilt = String(c.yearBuilt);
        if (c.zip && !out.zip) out.zip = c.zip;

        const close = Math.round(num(out.closePrice));
        const list = c.listingPrice > 0 ? c.listingPrice : Math.round(num(out.listPriceAtPending || out.assessedValue));
        const original = c.originalPrice > 0 ? c.originalPrice : 0;
        const daysToPending = dayDiff(c.listingDate, c.pendingDate);
        const daysPendingToSale = dayDiff(c.pendingDate, out.saleDate);
        const bidUp = (close > 0 && list > 0) ? (close - list) : 0;
        const bidUpPct = (list > 0) ? (bidUp / list) : 0;
        const saleToListRatio = (close > 0 && list > 0) ? (close / list) : 0;
        const saleToOriginalRatio = (close > 0 && original > 0) ? (close / original) : 0;
        Object.assign(out, buildMlsFieldValues(c, {
          listPrice: list,
          closePrice: close,
          originalPrice: original,
          dateLagDays: stub.lagDays,
          joinMethod: "APN_LISTING_STUB",
          daysToPending,
          daysPendingToSale,
          saleToListRatio,
          saleToOriginalRatio,
          bidUpAmount: bidUp,
          bidUpPct,
        }));
      } else {
        clearMlsFieldValues(out);
      }
    }

    return out;
  });

  let mlsOnlyAdded = 0;
  const mlsOnlyRows = [];
  mlsClosedRows.forEach((c, index) => {
    if (used.has(c.uid)) return;
    if (!c.apn || !c.sellingDate || c.sellingPrice <= 0) return;
    const signature = `${c.apn}|${c.sellingDate}|${c.sellingPrice}`;
    if (knownCountySaleSignatures.has(signature)) return;

    const snapshot = parcelSnapshotByApn.get(c.apn) || {};
    const parcelCoord = parcelCoordsByApn.get(c.apn) || null;
    const row = {};
    headers.forEach((h) => { row[h] = ""; });

    const major = c.apn.slice(0, 6);
    const minor = c.apn.slice(6, 10);
    const list = c.listingPrice > 0 ? c.listingPrice : 0;
    const close = c.sellingPrice > 0 ? c.sellingPrice : 0;
    const original = c.originalPrice > 0 ? c.originalPrice : 0;
    const daysToPending = dayDiff(c.listingDate, c.pendingDate);
    const daysPendingToSale = dayDiff(c.pendingDate, c.sellingDate);
    const bidUp = (close > 0 && list > 0) ? (close - list) : 0;
    const bidUpPct = (list > 0) ? (bidUp / list) : 0;
    const saleToListRatio = (close > 0 && list > 0) ? (close / list) : 0;
    const saleToOriginalRatio = (close > 0 && original > 0) ? (close / original) : 0;

    row.dataMode = "MLS_ENRICHED";
    row.id = `mls-only-${c.listingNumber || "na"}-${c.apn}-${c.sellingDate}-${index + 1}`;
    row.address = c.mlsAddress || snapshot.address || "";
    row.neighborhood = snapshot.neighborhood || zipNeighborhood(c.zip) || c.region || "Seattle (Other)";
    row.type = snapshot.type || inferTypeFromMlsStyle(c.styleCode);
    row.typeCode = snapshot.typeCode || "11";
    row.addressSource = c.mlsAddress ? "MLS_ADDRESS" : (snapshot.addressSource || "MLS_ADDRESS");
    row.major = major;
    row.minor = minor;
    row.parcelNbr = c.apn;
    row.listDate = c.listingDate || "";
    row.pendingDate = c.pendingDate || "";
    row.saleDate = c.sellingDate || "";
    row.listPriceAtPending = list > 0 ? String(list) : "";
    row.closePrice = close > 0 ? String(close) : "";
    row.assessedValue = snapshot.assessedValue || "";
    row.beds = c.beds > 0 ? String(c.beds) : (snapshot.beds || "");
    row.baths = c.baths > 0 ? String(c.baths) : (snapshot.baths || "");
    row.sqft = c.sqft > 0 ? String(c.sqft) : (snapshot.sqft || "");
    row.yearBuilt = c.yearBuilt > 0 ? String(c.yearBuilt) : (snapshot.yearBuilt || "");
    row.zip = c.zip || snapshot.zip || "";
    row.districtName = snapshot.districtName || "";
    row.area = snapshot.area || "";
    row.subArea = snapshot.subArea || "";
    row.sqFtLot = snapshot.sqFtLot || "";
    row.zoning = snapshot.zoning || "";
    row.lat = snapshot.lat || (parcelCoord ? parcelCoord.lat : "");
    row.lon = snapshot.lon || (parcelCoord ? parcelCoord.lon : "");

    Object.assign(row, buildMlsFieldValues(c, {
      listPrice: list,
      closePrice: close,
      originalPrice: original,
      joinMethod: "MLS_SOLD_NOT_IN_COUNTY",
      daysToPending,
      daysPendingToSale,
      saleToListRatio,
      saleToOriginalRatio,
      bidUpAmount: bidUp,
      bidUpPct,
    }));

    mlsOnlyRows.push(row);
    mlsOnlyAdded += 1;
  });

  let mlsOpenAdded = 0;
  const mlsOpenOnlyRows = [];
  mlsOpenRows.forEach((c, index) => {
    if (used.has(c.uid)) return;
    const snapshot = parcelSnapshotByApn.get(c.apn) || {};
    const parcelCoord = parcelCoordsByApn.get(c.apn) || null;
    const row = {};
    headers.forEach((h) => { row[h] = ""; });

    const major = c.apn.slice(0, 6);
    const minor = c.apn.slice(6, 10);
    const list = c.listingPrice > 0 ? c.listingPrice : 0;
    const daysToPending = dayDiff(c.listingDate, c.pendingDate);

    row.dataMode = "MLS_ENRICHED";
    row.id = `mls-open-${c.listingNumber || "na"}-${c.apn}-${index + 1}`;
    row.address = c.mlsAddress || snapshot.address || "";
    row.neighborhood = snapshot.neighborhood || zipNeighborhood(c.zip) || c.region || "Seattle (Other)";
    row.type = snapshot.type || inferTypeFromMlsStyle(c.styleCode);
    row.typeCode = snapshot.typeCode || "11";
    row.addressSource = c.mlsAddress ? "MLS_ADDRESS" : (snapshot.addressSource || "MLS_ADDRESS");
    row.major = major;
    row.minor = minor;
    row.parcelNbr = c.apn;
    row.listDate = c.listingDate || "";
    row.pendingDate = c.pendingDate || "";
    row.saleDate = "";
    row.listPriceAtPending = list > 0 ? String(list) : "";
    row.closePrice = "";
    row.assessedValue = snapshot.assessedValue || "";
    row.beds = c.beds > 0 ? String(c.beds) : (snapshot.beds || "");
    row.baths = c.baths > 0 ? String(c.baths) : (snapshot.baths || "");
    row.sqft = c.sqft > 0 ? String(c.sqft) : (snapshot.sqft || "");
    row.yearBuilt = c.yearBuilt > 0 ? String(c.yearBuilt) : (snapshot.yearBuilt || "");
    row.zip = c.zip || snapshot.zip || "";
    row.districtName = snapshot.districtName || "";
    row.area = snapshot.area || "";
    row.subArea = snapshot.subArea || "";
    row.sqFtLot = snapshot.sqFtLot || "";
    row.zoning = snapshot.zoning || "";
    row.lat = snapshot.lat || (parcelCoord ? parcelCoord.lat : "");
    row.lon = snapshot.lon || (parcelCoord ? parcelCoord.lon : "");

    Object.assign(row, buildMlsFieldValues(c, {
      listPrice: list,
      closePrice: 0,
      originalPrice: c.originalPrice > 0 ? c.originalPrice : 0,
      joinMethod: "MLS_STATUS_OPEN",
      daysToPending,
      daysPendingToSale: null,
      includeOutcomeMetrics: false,
      saleToListRatio: 0,
      saleToOriginalRatio: 0,
      bidUpAmount: 0,
      bidUpPct: 0,
    }));

    mlsOpenOnlyRows.push(row);
    mlsOpenAdded += 1;
  });

  const finalRows = [...merged, ...mlsOnlyRows, ...mlsOpenOnlyRows];
  const allHeaders = [...headers];
  MLS_ENRICHMENT_COLUMNS.forEach((h) => {
    if (!allHeaders.includes(h)) allHeaders.push(h);
  });

  const lines = [allHeaders.join(",")];
  finalRows.forEach((row) => {
    lines.push(allHeaders.map((h) => safeCsv(row[h] || "")).join(","));
  });
  fs.writeFileSync(OUTPUT_FILE, `${lines.join("\n")}\n`);

  const report = {
    generatedAt: new Date().toISOString(),
    source: {
      baseFile: path.basename(BASE_FILE),
      realtorDir: path.basename(REALTOR_DIR),
      realtorFiles: realtorFiles.map((f) => f.file),
    },
    counts: {
      mlsRowsParsed: rawMlsRows.length,
      mlsRowsAfterDedupe: mlsRows.length,
      mlsDuplicateListingsCollapsed: dedupedMls.counts.duplicateListingCount,
      mlsDuplicateRowsCollapsed: dedupedMls.counts.duplicateRowsCollapsed,
      mlsApnConflictListings: dedupedMls.counts.apnConflictListingCount,
      mlsApnConflictRows: dedupedMls.counts.apnConflictRowCount,
      mlsRowsWithResolvedApn: resolvedMlsRows.length,
      mlsRowsMissingApn: unresolvedMlsRows.length,
      mlsClosedRows: mlsClosedRows.length,
      mlsActiveRows: mlsActiveRows.length,
      mlsOpenStatusRows: mlsOpenRows.length,
      baseRows: rows.length,
      matchedCountyRows: matched,
      countyRowsEnrichedWithListingStub: listingStubbed,
      mlsOnlySoldRowsAdded: mlsOnlyAdded,
      mlsOnlyOpenRowsAdded: mlsOpenAdded,
      outputRows: finalRows.length,
    },
    apnResolution: apnResolutionCounts,
    outputs: {
      enrichedCsv: path.basename(OUTPUT_FILE),
    },
  };
  writeRefreshReport(report);

  // eslint-disable-next-line no-console
  console.log(`Realtor files loaded: ${realtorFiles.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS rows parsed: ${rawMlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS rows after dedupe: ${mlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS duplicate listings collapsed: ${dedupedMls.counts.duplicateListingCount}`);
  // eslint-disable-next-line no-console
  console.log(`MLS duplicate rows collapsed: ${dedupedMls.counts.duplicateRowsCollapsed}`);
  // eslint-disable-next-line no-console
  console.log(`MLS duplicate listings with APN conflicts: ${dedupedMls.counts.apnConflictListingCount}`);
  // eslint-disable-next-line no-console
  console.log(`MLS duplicate rows with APN conflicts: ${dedupedMls.counts.apnConflictRowCount}`);
  // eslint-disable-next-line no-console
  console.log(`MLS rows with resolved APN: ${resolvedMlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS rows still missing APN: ${unresolvedMlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS closed rows: ${mlsClosedRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS active rows: ${mlsActiveRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS open-status rows: ${mlsOpenRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`APN resolution: ${JSON.stringify(apnResolutionCounts)}`);
  // eslint-disable-next-line no-console
  console.log(`Base rows: ${rows.length}`);
  // eslint-disable-next-line no-console
  console.log(`Matched rows: ${matched}`);
  // eslint-disable-next-line no-console
  console.log(`County rows enriched via APN listing stub: ${listingStubbed}`);
  // eslint-disable-next-line no-console
  console.log(`MLS-only sold rows added: ${mlsOnlyAdded}`);
  // eslint-disable-next-line no-console
  console.log(`MLS-only open rows added: ${mlsOpenAdded}`);
  // eslint-disable-next-line no-console
  console.log(`Total output rows: ${finalRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`Output: ${OUTPUT_FILE}`);
  // eslint-disable-next-line no-console
  console.log(`Refresh report: ${REPORT_FILE}`);
}

if (require.main === module) {
  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err.message || String(err));
    process.exit(1);
  });
}

module.exports = {
  canonicalAddressKey,
  canonicalMlsStreet,
  canonicalMlsStreetNoUnit,
  dedupeRealtorRows,
  findHeaderIndex,
  MLS_ENRICHMENT_COLUMNS,
  normalizeBooleanText,
  normalizeAddressText,
  normalizeThirdPartyApproval,
  regionFromFilename,
  resolveRealtorApns,
  stripTrailingUnit,
};
