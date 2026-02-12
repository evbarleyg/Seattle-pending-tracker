#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const BASE_FILE = path.join(PROJECT_DIR, "public_sales_proxy_all_prices_last12mo.csv");
const REALTOR_DIR = path.join(PROJECT_DIR, "realtor_exports");
const OUTPUT_FILE = path.join(PROJECT_DIR, "public_sales_proxy_mls_enriched_last12mo.csv");

const REALTOR_FILES = [
  { file: "Central_South Seattle Sale Stats.csv", region: "Central / South Seattle" },
  { file: "NE Seattle Sale Stats.csv", region: "NE Seattle" },
  { file: "NW Seattle Sale Stats.csv", region: "NW Seattle" },
  { file: "QA_Magnolia Sale Stats.csv", region: "Queen Anne / Magnolia" },
];

const MAX_DATE_LAG_DAYS = 45;
const PRICE_TOLERANCE_ABS = 5000;
const PRICE_TOLERANCE_PCT = 0.005;

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

function parcelDigits(row) {
  const parcel = String(row.parcelNbr || `${row.major || ""}${row.minor || ""}`).replace(/\D/g, "");
  return parcel.length >= 10 ? parcel.slice(-10) : "";
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) return `"${s.replace(/"/g, "\"\"")}"`;
  return s;
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

function readRealtorRows() {
  const out = [];
  REALTOR_FILES.forEach(({ file, region }) => {
    const full = path.join(REALTOR_DIR, file);
    if (!fs.existsSync(full)) return;
    const text = fs.readFileSync(full, "utf8");
    const lines = text.split(/\r?\n/).filter((line) => line.trim());
    if (lines.length < 3) return;

    const headers = normalizeHeaderNames(parseCsvLine(lines[1]));
    lines.slice(2).forEach((line, rowIndex) => {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => { row[h] = (cols[i] || "").trim(); });

      const status = String(row.Status || "").trim();
      const sellingDate = toIsoDate(row["Selling Date"]);
      const sellingPrice = Math.round(num(row["Selling Price"]));
      const soldLike = /sold|closed/i.test(status) || (sellingDate && sellingPrice > 0);
      if (!soldLike) return;

      const apn = normalizeApn(row.APN);
      if (!apn) return;

      const listingDate = toIsoDate(row["Listing Date"]);
      const pendingDate = toIsoDate(row["Pending Date"]);
      const contractualDate = toIsoDate(row["Contractual Date"]);
      const listingPrice = Math.round(num(row["Listing Price"]));
      const originalPrice = Math.round(num(row["Original Price"]));
      const dom = Math.round(num(row.DOM));
      const cdom = Math.round(num(row.CDOM));
      const listingNumber = String(row["Listing Number"] || row["Listing Number (2)"] || "").trim();
      const uid = [file, listingNumber, apn, sellingDate, sellingPrice, rowIndex].join("|");

      out.push({
        uid,
        region,
        status,
        apn,
        listingNumber,
        listingDate,
        pendingDate,
        contractualDate,
        sellingDate,
        listingPrice,
        sellingPrice,
        originalPrice,
        dom,
        cdom,
        styleCode: String(row["Style Code"] || "").trim(),
        subdivision: String(row.Subdivision || "").trim(),
        beds: Math.round(num(row.Bedrooms)),
        baths: num(row.Bathrooms),
        sqft: Math.round(num(row["Square Footage"] || row["Square Footage Finished"])),
        yearBuilt: Math.round(num(row["Year Built"])),
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

function main() {
  if (!fs.existsSync(BASE_FILE)) throw new Error(`Missing base dataset: ${BASE_FILE}`);
  if (!fs.existsSync(REALTOR_DIR)) throw new Error(`Missing realtor directory: ${REALTOR_DIR}`);

  const { headers, rows } = readBaseRows();
  const mlsRows = readRealtorRows();
  const mlsByApn = new Map();
  mlsRows.forEach((r) => {
    if (!mlsByApn.has(r.apn)) mlsByApn.set(r.apn, []);
    mlsByApn.get(r.apn).push(r);
  });

  const used = new Set();
  let matched = 0;
  const merged = rows.map((row) => {
    const cands = mlsByApn.get(row.__parcel) || [];
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
      out.mlsDOM = c.dom > 0 ? String(c.dom) : "";
      out.mlsCDOM = c.cdom > 0 ? String(c.cdom) : "";
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
      const hotByPending = daysToPending !== null && daysToPending <= 10;
      const hotByDom = c.dom > 0 && c.dom <= 10;
      out.hotMarketTag = (hotByPending || hotByDom) ? "HOT_MARKET_<=10D" : "";
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

    return out;
  });

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
  merged.forEach((row) => {
    lines.push(allHeaders.map((h) => safeCsv(row[h] || "")).join(","));
  });
  fs.writeFileSync(OUTPUT_FILE, `${lines.join("\n")}\n`);

  // eslint-disable-next-line no-console
  console.log(`MLS rows parsed: ${mlsRows.length}`);
  // eslint-disable-next-line no-console
  console.log(`Base rows: ${rows.length}`);
  // eslint-disable-next-line no-console
  console.log(`Matched rows: ${matched}`);
  // eslint-disable-next-line no-console
  console.log(`Output: ${OUTPUT_FILE}`);
}

main();
