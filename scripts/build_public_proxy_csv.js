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
const PARCEL_COORDS_FILE = path.join(PROJECT_DIR, "parcel_coords_major_minor.csv");
const OUTPUT_FILE = path.join(PROJECT_DIR, "public_sales_proxy_all_prices_last6mo.csv");
const RANGE_END = new Date();
RANGE_END.setHours(23, 59, 59, 999);
const RANGE_START = new Date(RANGE_END);
RANGE_START.setMonth(RANGE_START.getMonth() - 6);
RANGE_START.setHours(0, 0, 0, 0);

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
  "98111": "Downtown",
  "98124": "Downtown",
  "98154": "Downtown",
  "98164": "Downtown",
  "98174": "Downtown",
  "98194": "Downtown",
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

function normalizeSpace(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function zip5(v) {
  return (String(v || "").match(/[0-9]{5}/) || [])[0] || "";
}

function isPoBox(v) {
  return /^P\.?\s*O\.?\s*BOX\b/i.test(normalizeSpace(v));
}

function normalizeSitusAddress(addrRaw) {
  let s = normalizeSpace(addrRaw);
  if (!s) return "";
  s = s.replace(/,\s*$/, "");
  s = s.replace(/\b[0-9]{5}(?:-[0-9]{4})?\s*$/, "").trim();
  return s;
}

function formatSeattleStreetAddress(street, zip) {
  const s = normalizeSitusAddress(street);
  if (!s) return "";
  if (/\bSEATTLE\b/i.test(s)) return s;
  const z = zip5(zip);
  return z ? `${s}, Seattle ${z}` : `${s}, Seattle`;
}

function formatMailingAddress(addr, cityState, zip) {
  const line = normalizeSpace(addr);
  const city = normalizeSpace(cityState);
  const z = zip5(zip);
  const parts = [line, city].filter(Boolean);
  if (z && !parts.join(" ").includes(z)) parts.push(z);
  return parts.join(", ");
}

function looksStreetLike(v) {
  const s = normalizeSpace(v);
  return /^[0-9]/.test(s) && /[A-Za-z]/.test(s) && !isPoBox(s);
}

function chooseAddress(account, bldg) {
  const accountStreet = normalizeSitusAddress(account.mailingLine);
  const accountZip = zip5(account.zip);
  const accountCity = normalizeSpace(account.mailingCityState);
  const accountLooksGood = looksStreetLike(accountStreet);
  const accountSeattleZip = accountZip.startsWith("981");

  const situsStreet = normalizeSitusAddress(bldg.situsAddress);
  const situsZip = zip5(bldg.situsZip);
  const situsLooksGood = looksStreetLike(situsStreet);
  const situsSeattleZip = !situsZip || situsZip.startsWith("981");

  // Prefer sold-property situs address whenever we have a plausible Seattle situs.
  if (situsLooksGood && situsSeattleZip) {
    return {
      address: formatSeattleStreetAddress(situsStreet, situsZip),
      addressSource: "SITUS_PROXY",
      zip: situsZip || accountZip,
    };
  }

  if (accountLooksGood) {
    const cityPart = accountCity || "SEATTLE WA";
    const z = accountZip || situsZip;
    return {
      address: `${accountStreet}, ${cityPart}${z ? ` ${z}` : ""}`.trim(),
      addressSource: "MAILING_STREET",
      zip: z,
    };
  }

  if (situsLooksGood) {
    return {
      address: formatSeattleStreetAddress(situsStreet, situsZip),
      addressSource: "SITUS_PROXY",
      zip: situsZip || accountZip,
    };
  }

  return {
    address: account.mailingAddress || formatSeattleStreetAddress(situsStreet, situsZip) || "Seattle",
    addressSource: "MAILING_FALLBACK",
    zip: accountZip || situsZip,
  };
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

function mappedZipNeighborhood(zip) {
  const z = (String(zip || "").match(/[0-9]{5}/) || [])[0] || "";
  return ZIP_NEIGHBORHOOD[z] || "";
}

function zipToNeighborhood(zip) {
  return mappedZipNeighborhood(zip) || "Seattle (Other)";
}

function areaSubKey(area, subArea) {
  return `${String(area || "").trim()}::${String(subArea || "").trim()}`;
}

function normalizeHeader(text) {
  return clean(text).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function normalizeMajorMinor(majorRaw, minorRaw) {
  const major = String(majorRaw || "").replace(/\D/g, "");
  const minor = String(minorRaw || "").replace(/\D/g, "");
  if (!major || !minor) return null;
  return `${major.padStart(6, "0").slice(-6)}-${minor.padStart(4, "0").slice(-4)}`;
}

function normalizeParcelNumber(parcelRaw) {
  const parcel = String(parcelRaw || "").replace(/\D/g, "");
  if (parcel.length < 10) return null;
  const trimmed = parcel.slice(-10);
  return `${trimmed.slice(0, 6)}-${trimmed.slice(6)}`;
}

function pickHeaderIndex(headers, aliases) {
  const norm = headers.map((h) => normalizeHeader(h));
  for (const alias of aliases) {
    const i = norm.indexOf(normalizeHeader(alias));
    if (i >= 0) return i;
  }
  return -1;
}

function validLatLon(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  return lat >= 46 && lat <= 49 && lon >= -123.5 && lon <= -121.5;
}

async function readParcelCoordsMap() {
  if (!fs.existsSync(PARCEL_COORDS_FILE)) return new Map();
  const stream = fs.createReadStream(PARCEL_COORDS_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let header = null;
  let idxMajor = -1;
  let idxMinor = -1;
  let idxParcel = -1;
  let idxLat = -1;
  let idxLon = -1;
  const out = new Map();

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = parseCsvLine(line).map((h) => clean(h));
      idxMajor = pickHeaderIndex(header, ["major"]);
      idxMinor = pickHeaderIndex(header, ["minor"]);
      idxParcel = pickHeaderIndex(header, [
        "parcelnbr", "parcelnumber", "parcel_num", "parcelid", "pin",
        "kcaparcelid", "kcaparcelnumber", "parcelnumber10digit"
      ]);
      idxLat = pickHeaderIndex(header, [
        "lat", "latitude", "latitudecentroid", "centroidlat", "latcentroid"
      ]);
      idxLon = pickHeaderIndex(header, [
        "lon", "lng", "long", "longitude", "longitudecentroid", "centroidlon", "loncentroid"
      ]);
      continue;
    }

    const cols = parseCsvLine(line);
    const key = (idxMajor >= 0 && idxMinor >= 0)
      ? normalizeMajorMinor(cols[idxMajor], cols[idxMinor])
      : normalizeParcelNumber(idxParcel >= 0 ? cols[idxParcel] : "");
    if (!key) continue;

    const lat = num(idxLat >= 0 ? cols[idxLat] : "");
    const lon = num(idxLon >= 0 ? cols[idxLon] : "");
    if (!validLatLon(lat, lon)) continue;
    out.set(key, { lat, lon });
  }

  return out;
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
    const rawAddress = clean(cols[idx.Address]);
    const bldgZip = zip5(clean(cols[idx.ZipCode]) || rawAddress);
    const candidate = {
      bedrooms: num(cols[idx.Bedrooms]),
      baths: num(cols[idx.BathFullCount]) + (num(cols[idx.Bath3qtrCount]) * 0.75) + (num(cols[idx.BathHalfCount]) * 0.5),
      sqft: num(cols[idx.SqFtTotLiving]),
      yearBuilt: num(cols[idx.YrBuilt]),
      situsAddress: normalizeSitusAddress(rawAddress),
      situsZip: bldgZip,
    };

    const current = map.get(key);
    const isBetterAddress = !!candidate.situsAddress && (!current || !current.situsAddress);
    if (!current || candidate.sqft > current.sqft || isBetterAddress) {
      map.set(key, candidate);
    }
  }
  return map;
}

async function buildSeattleAccountMap(parcelMap) {
  const areaSubCounts = new Map();

  // Pass 1: learn neighborhood label by (area, subArea) from rows with explicitly mapped Seattle ZIPs.
  {
    const stream = fs.createReadStream(ACCOUNT_FILE);
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let idx = null;
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
      const parcel = parcelMap.get(key);
      const isSeattle = parcel ? true : cityState.includes("SEATTLE");
      if (!isSeattle || !parcel) continue;

      const zip = clean(cols[idx.ZipCode]);
      const inferred = mappedZipNeighborhood(zip);
      if (!inferred) continue;
      const k = areaSubKey(parcel.area, parcel.subArea);
      if (!areaSubCounts.has(k)) areaSubCounts.set(k, new Map());
      const byLabel = areaSubCounts.get(k);
      byLabel.set(inferred, (byLabel.get(inferred) || 0) + 1);
    }
  }

  const areaSubNeighborhood = new Map();
  for (const [k, labelCounts] of areaSubCounts.entries()) {
    const best = [...labelCounts.entries()].sort((a, b) => b[1] - a[1])[0];
    if (best) areaSubNeighborhood.set(k, best[0]);
  }

  // Pass 2: build account map using area/subArea-derived neighborhood where available.
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
    const area = parcel?.area || "";
    const neighborhoodFromArea = areaSubNeighborhood.get(areaSubKey(area, subArea)) || "";
    const neighborhood = subArea && !looksNumeric(subArea)
      ? subArea
      : (neighborhoodFromArea || zipToNeighborhood(zip));

    if (!existing || assessed > existing.assessedValue) {
      map.set(key, {
        assessedValue: assessed,
        mailingLine: addr,
        mailingCityState: cityState,
        mailingZip: zip5(zip),
        mailingAddress: formatMailingAddress(addr, cityState, zip),
        neighborhood,
        zip: zip5(zip),
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

async function buildOutput(accountMap, resBldgMap, typeMap, coordsMap) {
  const stream = fs.createReadStream(SALES_FILE);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = fs.createWriteStream(OUTPUT_FILE);

  out.write([
    "dataMode","id","address","neighborhood","type","typeCode",
    "addressSource","major","minor","parcelNbr",
    "listDate","pendingDate","saleDate","listPriceAtPending","closePrice","assessedValue",
    "beds","baths","sqft","yearBuilt","zip","districtName","area","subArea","sqFtLot","zoning",
    "lat","lon"
  ].join(",") + "\n");

  let idx = null;
  let written = 0;
  let withCoords = 0;
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
    const docDateRaw = clean(cols[idx.DocumentDate]);
    const docDate = toDate(docDateRaw);
    if (!docDate || docDate < RANGE_START || docDate > RANGE_END) continue;

    const id = clean(cols[idx.ExciseTaxNbr]) || `${major}${minor}`;
    const typeCode = clean(cols[idx.PropertyType]) || "";
    const type = typeMap.get(String(Number(typeCode))) || (typeCode ? `Type ${typeCode}` : "Unknown");
    const iso = toIsoDate(docDateRaw);
    const listPriceAtPending = account.assessedValue;
    const bldg = resBldgMap.get(key) || { bedrooms: 0, baths: 0, sqft: 0, yearBuilt: 0, situsAddress: "", situsZip: "" };
    const chosen = chooseAddress(account, bldg);
    const displayZip = chosen.zip || account.zip;
    const addressSource = chosen.addressSource;
    const displayAddress = (addressSource === "MAILING_FALLBACK" && isPoBox(chosen.address))
      ? `Parcel ${major}-${minor} (address unavailable)`
      : chosen.address;
    const parcelNbr = `${major}${minor}`;
    const coord = coordsMap.get(key) || null;
    if (coord) withCoords += 1;

    const row = [
      "PUBLIC_PROXY",
      id,
      displayAddress,
      account.neighborhood,
      type,
      typeCode,
      addressSource,
      major,
      minor,
      parcelNbr,
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
      displayZip || account.zip,
      account.districtName,
      account.area,
      account.subArea,
      String(Math.round(account.sqFtLot || 0)),
      account.zoning,
      coord ? String(coord.lat) : "",
      coord ? String(coord.lon) : "",
    ].map(safeCsv).join(",");

    out.write(`${row}\n`);
    written += 1;
  }

  out.end();
  return { written, withCoords };
}

async function main() {
  if (!fs.existsSync(ACCOUNT_FILE)) throw new Error(`Missing file: ${ACCOUNT_FILE}`);
  if (!fs.existsSync(SALES_FILE)) throw new Error(`Missing file: ${SALES_FILE}`);

  const parcelMap = await readParcelMap();
  const resBldgMap = await readResBldgMap();
  const typeMap = await readPropertyTypeMap();
  const coordsMap = await readParcelCoordsMap();
  const accountMap = await buildSeattleAccountMap(parcelMap);
  const result = await buildOutput(accountMap, resBldgMap, typeMap, coordsMap);
  // eslint-disable-next-line no-console
  console.log(`Wrote ${result.written} rows to ${OUTPUT_FILE} (rows with coords: ${result.withCoords})`);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err.message);
  process.exit(1);
});
