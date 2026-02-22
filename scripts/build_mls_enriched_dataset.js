#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const BASE_FILE = path.join(PROJECT_DIR, "public_sales_proxy_all_prices_last12mo.csv");
const REALTOR_DIR = path.join(PROJECT_DIR, "realtor_exports");
const OUTPUT_FILE = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");
const REPORT_FILE = path.join(PROJECT_DIR, "data_refresh_report.json");
const REALTOR_FILE_PATTERN = /\.csv$/i;
const REQUIRED_REALTOR_COLUMNS = [
  "APN",
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

const MAX_DATE_LAG_DAYS = 45;
const PRICE_TOLERANCE_ABS = 5000;
const PRICE_TOLERANCE_PCT = 0.005;
const LISTING_STUB_MAX_DATE_LAG_DAYS = 120;
const LISTING_STUB_MAX_PRICE_DIFF_PCT = 0.5;
const HOT_MARKET_DAYS = 10;
const ULTRA_HOT_DAYS = 5;

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

function parcelDigits(row) {
  const parcel = String(row.parcelNbr || `${row.major || ""}${row.minor || ""}`).replace(/\D/g, "");
  return parcel.length >= 10 ? parcel.slice(-10) : "";
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) return `"${s.replace(/"/g, "\"\"")}"`;
  return s;
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
    .replace(/sale stats/ig, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
    if (cols.includes("APN") && cols.includes("Status")) return i;
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
      throw new Error(`Could not find header row with APN/Status in realtor file: ${file}`);
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
      if (!apn) return;

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
        mlsAddress: mlsAddressFromParts(row),
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

function main() {
  if (!fs.existsSync(BASE_FILE)) throw new Error(`Missing base dataset: ${BASE_FILE}`);
  if (!fs.existsSync(REALTOR_DIR)) throw new Error(`Missing realtor directory: ${REALTOR_DIR}`);

  const realtorFiles = discoverRealtorFiles();
  if (!realtorFiles.length) throw new Error(`No realtor CSV files found in ${REALTOR_DIR}`);
  const { headers, rows } = readBaseRows();
  const mlsRows = readRealtorRows(realtorFiles);
  const mlsClosedRows = mlsRows.filter((r) => r.isClosed && r.sellingDate && r.sellingPrice > 0);
  const mlsOpenRows = mlsRows.filter((r) => !r.isClosed);
  const mlsActiveRows = mlsRows.filter((r) => r.status === "Active" && !r.isClosed);
  const parcelSnapshotByApn = buildParcelSnapshotByApn(rows);
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

      out.mlsListDate = c.listingDate;
      out.mlsPendingDate = c.pendingDate;
      out.mlsListPriceAtPending = c.listingPrice > 0 ? String(c.listingPrice) : "";
      out.mlsClosePrice = c.sellingPrice > 0 ? String(c.sellingPrice) : "";

      out.mlsListingNumber = c.listingNumber;
      out.mlsStatus = c.status;
      out.mlsRegion = c.region;
      out.mlsSellingDate = c.sellingDate;
      out.mlsContractualDate = c.contractualDate;
      out.mlsListingPrice = c.listingPrice > 0 ? String(c.listingPrice) : "";
      out.mlsSellingPrice = c.sellingPrice > 0 ? String(c.sellingPrice) : "";
      out.mlsOriginalPrice = c.originalPrice > 0 ? String(c.originalPrice) : "";
      out.mlsDOM = c.domRaw !== "" ? String(c.dom) : "";
      out.mlsCDOM = c.cdomRaw !== "" ? String(c.cdom) : "";
      out.mlsStyleCode = c.styleCode;
      out.mlsSubdivision = c.subdivision;
      out.mlsDateLagDays = String(match.dateLag);
      out.mlsJoinMethod = "APN_PRICE_DATE_WINDOW";

      const daysToPending = dayDiff(c.listingDate, c.pendingDate);
      const daysPendingToSale = dayDiff(c.pendingDate, c.sellingDate);
      const close = c.sellingPrice > 0 ? c.sellingPrice : Math.round(num(out.closePrice));
      const list = c.listingPrice > 0 ? c.listingPrice : Math.round(num(out.listPriceAtPending));
      const original = c.originalPrice > 0 ? c.originalPrice : 0;
      const bidUp = (close > 0 && list > 0) ? (close - list) : 0;
      const bidUpPct = (close > 0 && list > 0) ? (bidUp / list) : 0;
      const saleToListRatio = (close > 0 && list > 0) ? (close / list) : 0;
      const saleToOriginalRatio = (close > 0 && original > 0) ? (close / original) : 0;

      out.mlsDaysToPending = daysToPending !== null ? String(daysToPending) : "";
      out.mlsDaysPendingToSale = daysPendingToSale !== null ? String(daysPendingToSale) : "";
      out.hotMarketTag = buildHotMarketTag(c.dom, daysToPending);
      out.saleToListRatio = saleToListRatio > 0 ? saleToListRatio.toFixed(4) : "";
      out.saleToOriginalListRatio = saleToOriginalRatio > 0 ? saleToOriginalRatio.toFixed(4) : "";
      out.bidUpAmount = String(Math.round(bidUp));
      out.bidUpPct = list > 0 ? bidUpPct.toFixed(4) : "";
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

        out.mlsListDate = c.listingDate;
        out.mlsPendingDate = c.pendingDate;
        out.mlsListPriceAtPending = list > 0 ? String(list) : "";
        out.mlsClosePrice = close > 0 ? String(close) : "";
        out.mlsListingNumber = c.listingNumber;
        out.mlsStatus = c.status;
        out.mlsRegion = c.region;
        out.mlsSellingDate = c.sellingDate || "";
        out.mlsContractualDate = c.contractualDate;
        out.mlsListingPrice = c.listingPrice > 0 ? String(c.listingPrice) : "";
        out.mlsSellingPrice = close > 0 ? String(close) : "";
        out.mlsOriginalPrice = c.originalPrice > 0 ? String(c.originalPrice) : "";
        out.mlsDOM = c.domRaw !== "" ? String(c.dom) : "";
        out.mlsCDOM = c.cdomRaw !== "" ? String(c.cdom) : "";
        out.mlsStyleCode = c.styleCode;
        out.mlsSubdivision = c.subdivision;
        out.mlsDateLagDays = String(stub.lagDays);
        out.mlsJoinMethod = "APN_LISTING_STUB";
        out.mlsDaysToPending = daysToPending !== null ? String(daysToPending) : "";
        out.mlsDaysPendingToSale = daysPendingToSale !== null ? String(daysPendingToSale) : "";
        out.hotMarketTag = buildHotMarketTag(c.dom, daysToPending);
        out.saleToListRatio = saleToListRatio > 0 ? saleToListRatio.toFixed(4) : "";
        out.saleToOriginalListRatio = saleToOriginalRatio > 0 ? saleToOriginalRatio.toFixed(4) : "";
        out.bidUpAmount = String(Math.round(bidUp));
        out.bidUpPct = list > 0 ? bidUpPct.toFixed(4) : "";
      } else {
        out.mlsListingNumber = "";
        out.mlsStatus = "";
        out.mlsRegion = "";
        out.mlsSellingDate = "";
        out.mlsContractualDate = "";
        out.mlsListingPrice = "";
        out.mlsSellingPrice = "";
        out.mlsOriginalPrice = "";
        out.mlsDOM = "";
        out.mlsCDOM = "";
        out.mlsStyleCode = "";
        out.mlsSubdivision = "";
        out.mlsDateLagDays = "";
        out.mlsJoinMethod = "";
        out.mlsDaysToPending = "";
        out.mlsDaysPendingToSale = "";
        out.hotMarketTag = "";
        out.saleToListRatio = "";
        out.saleToOriginalListRatio = "";
        out.bidUpAmount = "";
        out.bidUpPct = "";
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
    row.neighborhood = snapshot.neighborhood || c.region || "Seattle (Other)";
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
    row.lat = snapshot.lat || "";
    row.lon = snapshot.lon || "";

    row.mlsListDate = c.listingDate;
    row.mlsPendingDate = c.pendingDate;
    row.mlsListPriceAtPending = list > 0 ? String(list) : "";
    row.mlsClosePrice = close > 0 ? String(close) : "";
    row.mlsListingNumber = c.listingNumber;
    row.mlsStatus = c.status;
    row.mlsRegion = c.region;
    row.mlsSellingDate = c.sellingDate;
    row.mlsContractualDate = c.contractualDate;
    row.mlsListingPrice = list > 0 ? String(list) : "";
    row.mlsSellingPrice = close > 0 ? String(close) : "";
    row.mlsOriginalPrice = original > 0 ? String(original) : "";
    row.mlsDOM = c.domRaw !== "" ? String(c.dom) : "";
    row.mlsCDOM = c.cdomRaw !== "" ? String(c.cdom) : "";
    row.mlsStyleCode = c.styleCode;
    row.mlsSubdivision = c.subdivision;
    row.mlsDateLagDays = "";
    row.mlsJoinMethod = "MLS_SOLD_NOT_IN_COUNTY";
    row.mlsDaysToPending = daysToPending !== null ? String(daysToPending) : "";
    row.mlsDaysPendingToSale = daysPendingToSale !== null ? String(daysPendingToSale) : "";
    row.hotMarketTag = buildHotMarketTag(c.dom, daysToPending);
    row.saleToListRatio = saleToListRatio > 0 ? saleToListRatio.toFixed(4) : "";
    row.saleToOriginalListRatio = saleToOriginalRatio > 0 ? saleToOriginalRatio.toFixed(4) : "";
    row.bidUpAmount = String(Math.round(bidUp));
    row.bidUpPct = list > 0 ? bidUpPct.toFixed(4) : "";

    mlsOnlyRows.push(row);
    mlsOnlyAdded += 1;
  });

  let mlsOpenAdded = 0;
  const mlsOpenOnlyRows = [];
  mlsOpenRows.forEach((c, index) => {
    if (used.has(c.uid)) return;
    const snapshot = parcelSnapshotByApn.get(c.apn) || {};
    const row = {};
    headers.forEach((h) => { row[h] = ""; });

    const major = c.apn.slice(0, 6);
    const minor = c.apn.slice(6, 10);
    const list = c.listingPrice > 0 ? c.listingPrice : 0;
    const daysToPending = dayDiff(c.listingDate, c.pendingDate);

    row.dataMode = "MLS_ENRICHED";
    row.id = `mls-open-${c.listingNumber || "na"}-${c.apn}-${index + 1}`;
    row.address = c.mlsAddress || snapshot.address || "";
    row.neighborhood = snapshot.neighborhood || c.region || "Seattle (Other)";
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
    row.lat = snapshot.lat || "";
    row.lon = snapshot.lon || "";

    row.mlsListDate = c.listingDate;
    row.mlsPendingDate = c.pendingDate;
    row.mlsListPriceAtPending = list > 0 ? String(list) : "";
    row.mlsClosePrice = "";
    row.mlsListingNumber = c.listingNumber;
    row.mlsStatus = c.status;
    row.mlsRegion = c.region;
    row.mlsSellingDate = "";
    row.mlsContractualDate = c.contractualDate;
    row.mlsListingPrice = list > 0 ? String(list) : "";
    row.mlsSellingPrice = "";
    row.mlsOriginalPrice = c.originalPrice > 0 ? String(c.originalPrice) : "";
    row.mlsDOM = c.domRaw !== "" ? String(c.dom) : "";
    row.mlsCDOM = c.cdomRaw !== "" ? String(c.cdom) : "";
    row.mlsStyleCode = c.styleCode;
    row.mlsSubdivision = c.subdivision;
    row.mlsDateLagDays = "";
    row.mlsJoinMethod = "MLS_STATUS_OPEN";
    row.mlsDaysToPending = daysToPending !== null ? String(daysToPending) : "";
    row.mlsDaysPendingToSale = "";
    row.hotMarketTag = buildHotMarketTag(c.dom, daysToPending);
    row.saleToListRatio = "";
    row.saleToOriginalListRatio = "";
    row.bidUpAmount = "";
    row.bidUpPct = "";

    mlsOpenOnlyRows.push(row);
    mlsOpenAdded += 1;
  });

  const finalRows = [...merged, ...mlsOnlyRows, ...mlsOpenOnlyRows];

  const extraColumns = [
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
  const allHeaders = [...headers];
  extraColumns.forEach((h) => {
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
      mlsRowsParsed: mlsRows.length,
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
    outputs: {
      enrichedCsv: path.basename(OUTPUT_FILE),
    },
  };
  writeRefreshReport(report);

  // eslint-disable-next-line no-console
  console.log(`Realtor files loaded: ${realtorFiles.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS rows parsed: ${mlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS closed rows: ${mlsClosedRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS active rows: ${mlsActiveRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`MLS open-status rows: ${mlsOpenRows.length}`);
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

main();
