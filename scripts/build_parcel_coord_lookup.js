#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_OUT = path.join(PROJECT_DIR, "parcel_coords_major_minor.csv");

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

function clean(v) {
  return String(v || "").replace(/^"|"$/g, "").trim();
}

function safeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function num(v) {
  const n = Number(String(v || "").replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function normalizeHeader(text) {
  return clean(text).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function pickHeaderIndex(headers, aliases) {
  const norm = headers.map((h) => normalizeHeader(h));
  for (const alias of aliases) {
    const i = norm.indexOf(normalizeHeader(alias));
    if (i >= 0) return i;
  }
  return -1;
}

function normalizeMajorMinor(majorRaw, minorRaw) {
  const major = String(majorRaw || "").replace(/\D/g, "");
  const minor = String(minorRaw || "").replace(/\D/g, "");
  if (!major || !minor) return null;
  return `${major.padStart(6, "0").slice(-6)}-${minor.padStart(4, "0").slice(-4)}`;
}

function normalizeParcelNumber(parcelRaw) {
  const digits = String(parcelRaw || "").replace(/\D/g, "");
  if (digits.length < 10) return null;
  const parcel = digits.slice(-10);
  return `${parcel.slice(0, 6)}-${parcel.slice(6)}`;
}

function keyToMajorMinor(key) {
  const [major, minor] = String(key || "").split("-");
  if (!major || !minor) return { major: "", minor: "" };
  return { major, minor };
}

function validLatLon(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  return lat >= 46 && lat <= 49 && lon >= -123.5 && lon <= -121.5;
}

function usage() {
  // eslint-disable-next-line no-console
  console.log([
    "Usage:",
    "  node scripts/build_parcel_coord_lookup.js <input_gis_csv> [output_csv]",
    "",
    "Input CSV should contain either:",
    "  - major + minor columns, or",
    "  - parcel number column (10-digit major+minor)",
    "and latitude/longitude columns.",
    "",
    "Output default:",
    `  ${DEFAULT_OUT}`,
  ].join("\n"));
}

async function main() {
  const input = process.argv[2];
  const output = process.argv[3] ? path.resolve(process.argv[3]) : DEFAULT_OUT;
  if (!input) {
    usage();
    process.exit(1);
  }
  const inputPath = path.resolve(input);
  if (!fs.existsSync(inputPath)) {
    throw new Error(`Input not found: ${inputPath}`);
  }

  const stream = fs.createReadStream(inputPath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let header = null;
  let idxMajor = -1;
  let idxMinor = -1;
  let idxParcel = -1;
  let idxLat = -1;
  let idxLon = -1;
  const coordByKey = new Map();

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = parseCsvLine(line).map((h) => clean(h));
      idxMajor = pickHeaderIndex(header, ["major"]);
      idxMinor = pickHeaderIndex(header, ["minor"]);
      idxParcel = pickHeaderIndex(header, [
        "parcelnbr", "parcelnumber", "parcel_num", "parcelid", "pin", "apn",
        "kcaparcelid", "kcaparcelnumber", "parcelnumber10digit"
      ]);
      idxLat = pickHeaderIndex(header, [
        "lat", "latitude", "latitudecentroid", "centroidlat", "latcentroid"
      ]);
      idxLon = pickHeaderIndex(header, [
        "lon", "lng", "long", "longitude", "longitudecentroid", "centroidlon", "loncentroid"
      ]);
      if (idxLat < 0 || idxLon < 0) {
        throw new Error("Could not find latitude/longitude columns in GIS CSV.");
      }
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

    if (!coordByKey.has(key)) coordByKey.set(key, { lat, lon });
  }

  const rows = [...coordByKey.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, v]) => {
      const mm = keyToMajorMinor(key);
      const parcelNbr = `${mm.major}${mm.minor}`;
      return [mm.major, mm.minor, parcelNbr, String(v.lat), String(v.lon)];
    });

  const out = fs.createWriteStream(output);
  out.write("major,minor,parcelNbr,lat,lon\n");
  rows.forEach((r) => out.write(`${r.map(safeCsv).join(",")}\n`));
  out.end();

  // eslint-disable-next-line no-console
  console.log(`Wrote ${rows.length} parcel coordinates to ${output}`);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err.message);
  process.exit(1);
});
