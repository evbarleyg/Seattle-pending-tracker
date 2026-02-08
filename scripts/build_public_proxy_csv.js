#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");

const PROJECT_DIR = path.resolve(__dirname, "..");
const ACCOUNT_FILE = path.join(PROJECT_DIR, "EXTR_RPAcct_NoName.csv");
const SALES_FILE = path.join(PROJECT_DIR, "EXTR_RPSale.csv");
const PARCEL_FILE = path.join(PROJECT_DIR, "EXTR_Parcel.csv");
const RESBLDG_FILE = path.join(PROJECT_DIR, "EXTR_ResBldg.csv");
const LOOKUP_FILE = path.join(PROJECT_DIR, "EXTR_LookUp.csv");
const OUTPUT_FILE = path.join(PROJECT_DIR, "public_sales_proxy_1p1m_1p4m_last6mo.csv");

const SALE_MIN = 1_100_000;
const SALE_MAX = 1_400_000;
const RANGE_START = new Date("2025-08-08T00:00:00");
const RANGE_END = new Date("2026-02-08T23:59:59");

const ZIP_NEIGHBORHOOD = {
  "98101": "Downtown",
  "98102": "Capitol Hill / Eastlake",
  "98103": "Fremont / Green Lake / Wallingford",
  "98104": "Pioneer Square / International District",
  "98105": "University District / Laurelhurst",
  "98106": "Delridge / South Park",
  "98107": "Ballard",
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
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map((v) => v.trim());
}

function toIsoDate(mmddyyyy) {
  const [mm, dd, yyyy] = mmddyyyy.split("/");
  if (!mm || !dd || !yyyy) return "";
  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

function toDate(mmddyyyy) {
  const iso = toIsoDate(mmddyyyy);
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function num(v) {
  if (!v) return 0;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function clean(v) {
  return String(v || "").replace(/^"|"$/g, "").trim();
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function looksNumeric(v) {
  return /^[0-9]+$/.test(String(v || "").trim());
}

function zipToNeighborhood(zip) {
  const z = (String(zip || "").match(/[0-9]{5}/) || [])[0] || "";
  if (ZIP_NEIGHBORHOOD[z]) return ZIP_NEIGHBORHOOD[z];
  if (/^981[0-9]{2}$/.test(z)) return `Seattle ${z}`;
  return "Seattle (Other)";
}

async function readPropertyTypeMap() {
  if (!fs.existsSync(LOOKUP_FILE)) return new Map();
  const stream = fs.createReadStream(LOOKUP_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let idx = null;
  const map = new Map();
  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      const header = parseCsvLine(line);
      idx = Object.fromEntries(header.map((h, i) => [clean(h), i]));
      continue;
    }
    const cols = parseCsvLine(line);
    const luType = clean(cols[idx.LUType]);
    if (luType !== "1") continue;
    const luItem = clean(cols[idx.LUItem]);
    const desc = clean(cols[idx.LUDescription]);
    if (!luItem || !desc) continue;
    map.set(String(Number(luItem)), desc);
  }
  return map;
}

async function readParcelMap() {
  if (!fs.existsSync(PARCEL_FILE)) return new Map();

  const stream = fs.createReadStream(PARCEL_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let idx = null;
  const map = new Map();
  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      const header = parseCsvLine(line);
      idx = Object.fromEntries(header.map((h, i) => [clean(h), i]));
      continue;
    }

    const cols = parseCsvLine(line);
    const major = clean(cols[idx.Major]);
    const minor = clean(cols[idx.Minor]);
    const district = clean(cols[idx.DistrictName]).toUpperCase();
    if (district !== "SEATTLE") continue;

    const key = `${major}-${minor}`;
    map.set(key, {
      districtName: clean(cols[idx.DistrictName]) || "Seattle",
      area: clean(cols[idx.Area]),
      subArea: clean(cols[idx.SubArea]),
      sqFtLot: num(cols[idx.SqFtLot]),
      zoning: clean(cols[idx.CurrentZoning]),
      presentUse: clean(cols[idx.PresentUse]),
    });
  }
  return map;
}

async function readResBldgMap() {
  if (!fs.existsSync(RESBLDG_FILE)) return new Map();

  const stream = fs.createReadStream(RESBLDG_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let idx = null;
  const map = new Map();
  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      const header = parseCsvLine(line);
      idx = Object.fromEntries(header.map((h, i) => [clean(h), i]));
      continue;
    }

    const cols = parseCsvLine(line);
    const key = `${clean(cols[idx.Major])}-${clean(cols[idx.Minor])}`;
    const candidate = {
      bedrooms: num(cols[idx.Bedrooms]),
      baths: num(cols[idx.BathFullCount]) + (num(cols[idx.Bath3qtrCount]) * 0.75) + (num(cols[idx.BathHalfCount]) * 0.5),
      sqft: num(cols[idx.SqFtTotLiving]),
      yearBuilt: num(cols[idx.YrBuilt]),
    };

    const current = map.get(key);
    if (!current || candidate.sqft > current.sqft) {
      map.set(key, candidate);
    }
  }
  return map;
}

async function buildSeattleAccountMap(parcelMap) {
  const stream = fs.createReadStream(ACCOUNT_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let idx = null;
  const map = new Map();
  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      const header = parseCsvLine(line);
      idx = Object.fromEntries(header.map((h, i) => [clean(h), i]));
      continue;
    }

    const cols = parseCsvLine(line);
    const major = clean(cols[idx.Major]);
    const minor = clean(cols[idx.Minor]);
    const key = `${major}-${minor}`;
    const cityState = clean(cols[idx.CityState]).toUpperCase();

    // Prefer parcel district filter when available; otherwise fall back to city text.
    const parcel = parcelMap.get(key);
    const isSeattle = parcel ? true : cityState.includes("SEATTLE");
    if (!isSeattle) continue;

    const apprLand = num(cols[idx.ApprLandVal]);
    const apprImps = num(cols[idx.ApprImpsVal]);
    const assessed = apprLand + apprImps;
    if (assessed <= 0) continue;

    const existing = map.get(key);
    const addr = clean(cols[idx.AddrLine]);
    const zip = clean(cols[idx.ZipCode]);
    const subArea = parcel?.subArea || "";
    const neighborhood = subArea && !looksNumeric(subArea) ? subArea : zipToNeighborhood(zip);

    if (!existing || assessed > existing.assessedValue) {
      map.set(key, {
        assessedValue: assessed,
        address: addr ? `${addr}, Seattle ${zip}` : `Seattle ${zip}`.trim(),
        neighborhood,
        zip,
        districtName: parcel?.districtName || "Seattle",
        area: parcel?.area || "",
        subArea: parcel?.subArea || "",
        sqFtLot: parcel?.sqFtLot || 0,
        zoning: parcel?.zoning || "",
      });
    }
  }
  return map;
}

async function buildOutput(accountMap, resBldgMap, typeMap) {
  const stream = fs.createReadStream(SALES_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = fs.createWriteStream(OUTPUT_FILE);

  out.write([
    "dataMode","id","address","neighborhood","type","typeCode",
    "listDate","pendingDate","saleDate","listPriceAtPending","closePrice","assessedValue",
    "beds","baths","sqft","yearBuilt","zip","districtName","area","subArea","sqFtLot","zoning"
  ].join(",") + "\n");

  let idx = null;
  let written = 0;
  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      const header = parseCsvLine(line);
      idx = Object.fromEntries(header.map((h, i) => [clean(h), i]));
      continue;
    }

    const cols = parseCsvLine(line);
    const major = clean(cols[idx.Major]);
    const minor = clean(cols[idx.Minor]);
    const key = `${major}-${minor}`;

    const account = accountMap.get(key);
    if (!account) continue;

    const salePrice = num(cols[idx.SalePrice]);
    if (salePrice < SALE_MIN || salePrice > SALE_MAX) continue;

    const docDateRaw = clean(cols[idx.DocumentDate]);
    const docDate = toDate(docDateRaw);
    if (!docDate || docDate < RANGE_START || docDate > RANGE_END) continue;

    const id = clean(cols[idx.ExciseTaxNbr]) || `${major}${minor}`;
    const typeCode = clean(cols[idx.PropertyType]) || "";
    const type = typeMap.get(String(Number(typeCode))) || (typeCode ? `Type ${typeCode}` : "Unknown");
    const iso = toIsoDate(docDateRaw);
    const listPriceAtPending = account.assessedValue;
    const bldg = resBldgMap.get(key) || { bedrooms: 0, baths: 0, sqft: 0, yearBuilt: 0 };

    const row = [
      "PUBLIC_PROXY",
      id,
      account.address,
      account.neighborhood,
      type,
      typeCode,
      iso,
      iso,
      iso,
      String(Math.round(listPriceAtPending)),
      String(Math.round(salePrice)),
      String(Math.round(account.assessedValue)),
      String(Math.round(bldg.bedrooms)),
      String(Number.isFinite(bldg.baths) ? bldg.baths.toFixed(2) : "0"),
      String(Math.round(bldg.sqft)),
      String(Math.round(bldg.yearBuilt)),
      account.zip,
      account.districtName,
      account.area,
      account.subArea,
      String(Math.round(account.sqFtLot || 0)),
      account.zoning,
    ].map(safeCsv).join(",");

    out.write(`${row}\n`);
    written += 1;
  }

  out.end();
  return written;
}

async function main() {
  if (!fs.existsSync(ACCOUNT_FILE)) throw new Error(`Missing file: ${ACCOUNT_FILE}`);
  if (!fs.existsSync(SALES_FILE)) throw new Error(`Missing file: ${SALES_FILE}`);

  const parcelMap = await readParcelMap();
  const resBldgMap = await readResBldgMap();
  const typeMap = await readPropertyTypeMap();
  const accountMap = await buildSeattleAccountMap(parcelMap);
  const written = await buildOutput(accountMap, resBldgMap, typeMap);
  // eslint-disable-next-line no-console
  console.log(`Wrote ${written} rows to ${OUTPUT_FILE}`);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err.message);
  process.exit(1);
});
