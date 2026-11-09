"use strict";

// Shared address key helpers for the Redfin pipeline.
//
// Building key = `STREET|ZIP` (no unit). Same form on both sides:
//   - row.address ("8518 11th Ave NW, Seattle WA 98117")
//   - Redfin URL path ("/WA/Seattle/8518-11th-Ave-NW-98117/home/100611")
//
// Unit (when present) is captured separately so callers can disambiguate
// multi-unit buildings instead of grabbing the wrong unit by accident.

function zip5(value) {
  return (String(value || "").match(/[0-9]{5}/) || [])[0] || "";
}

function normalizeUnitToken(raw) {
  if (!raw) return "";
  const cleaned = String(raw).toUpperCase().replace(/[^A-Z0-9]/g, "");
  return cleaned ? `UNIT-${cleaned}` : "";
}

function extractUnitFromAddress(rawAddress) {
  const s = String(rawAddress || "").toUpperCase();
  // Common patterns: "UNIT 3605", "APT 301", "STE 100", "#2002", "FLR 4"
  let m;
  m = s.match(/\b(?:UNIT|APT|STE|SUITE|FLR|FLOOR)\s+([A-Z0-9-]+)/);
  if (m) return normalizeUnitToken(m[1]);
  m = s.match(/#\s*([A-Z0-9-]+)/);
  if (m) return normalizeUnitToken(m[1]);
  return normalizeUnitToken(splitHouseNumberUnit(s.trim()).unit);
}

// County townhouse form: the unit letter rides on the house number
// ("4412 A CORLISS AVE N", "4113C CHILBERG AVE SW", slug "4116A-Burke-Ave-N"). A
// spaced N/S/E/W is a directional ("123 N 45TH ST"), so those four letters only
// count when attached. The one place this rule lives: the address key, the URL
// key and both unit readers all go through it.
function splitHouseNumberUnit(upperStreet) {
  const m = upperStreet.match(/^(\d+)([A-Z])\s+/) || upperStreet.match(/^(\d+)\s+([A-DF-MO-RT-VX-Z])\s+/);
  return m ? { street: `${m[1]} ${upperStreet.slice(m[0].length)}`, unit: m[2] } : { street: upperStreet, unit: "" };
}

function houseNumber(rawAddress) {
  return (String(rawAddress || "").match(/^\s*(\d+)/) || [])[1] || "";
}

// County records spell directionals out ("42ND AVENUE SOUTH"); Redfin never
// does ("42nd Ave S"). Compound forms first so NORTHWEST is not read as NORTH.
function abbreviateDirectionals(s) {
  return s
    .replace(/\bNORTHWEST\b/g, "NW")
    .replace(/\bNORTHEAST\b/g, "NE")
    .replace(/\bSOUTHWEST\b/g, "SW")
    .replace(/\bSOUTHEAST\b/g, "SE")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W");
}

function streetKeyFromAddress(rawAddress, rawZip) {
  let s = String(rawAddress || "").toUpperCase().trim();
  if (!s) return "";

  // Strip trailing city/state/zip tails
  s = s.replace(/,\s*[A-Z .]+,?\s*WA\s*\d{5}.*$/, "");
  s = s.replace(/,\s*[A-Z .]+,?\s*\d{5}.*$/, "");
  s = s.replace(/\s+WA\s+\d{5}.*$/, "");
  s = s.replace(/\s+\d{5}\s*$/, "");
  // County situs rows with no ZIP end in a bare ", Seattle", which the
  // patterns above (all anchored on a ZIP) leave in the key.
  s = s.replace(/,\s*[A-Z .]+(?:,?\s*WA)?\s*$/, "");

  // Drop unit fragments (matched separately by extractUnitFromAddress)
  s = splitHouseNumberUnit(s).street;
  s = s.replace(/[#].*$/, "");
  s = s.replace(/\b(APT|UNIT|STE|SUITE|FLR|FLOOR)\s+\S+\s*$/g, "");

  s = s.replace(/\bAVENUE\b/g, "AVE")
       .replace(/\bSTREET\b/g, "ST")
       .replace(/\bROAD\b/g, "RD")
       .replace(/\bBOULEVARD\b/g, "BLVD")
       .replace(/\bDRIVE\b/g, "DR")
       .replace(/\bCOURT\b/g, "CT")
       .replace(/\bPLACE\b/g, "PL")
       .replace(/\bLANE\b/g, "LN")
       .replace(/\bTERRACE\b/g, "TER")
       .replace(/\bPARKWAY\b/g, "PKWY")
       .replace(/\bCIRCLE\b/g, "CIR")
       .replace(/\bHIGHWAY\b/g, "HWY")
       .replace(/\bTRAIL\b/g, "TRL");
  s = abbreviateDirectionals(s);
  s = s.replace(/[.,]/g, " ").replace(/\s+/g, " ").trim();

  const zip = zip5(rawZip) || zip5(rawAddress);
  if (!s || !zip) return "";
  return `${s}|${zip}`;
}

function streetKeyFromRedfinPath(pathOrUrl) {
  const m = String(pathOrUrl || "").match(/\/[A-Z]{2}\/[A-Za-z-]+\/([^/]+)-(\d{5})(?:\/unit-[^/]+)?\/home\/\d+/i);
  if (!m) return "";
  const slug = m[1];
  const zip = m[2];
  let core = slug;
  const unitIdx = core.toLowerCase().lastIndexOf("-unit-");
  if (unitIdx > 0) core = core.slice(0, unitIdx);
  let s = core.replace(/-/g, " ").toUpperCase().trim();
  s = s.replace(/[.,]/g, " ").replace(/\s+/g, " ").trim();
  // Same form on both sides: the slug gets the address side's unit-letter and
  // directional rules ("2632 NW North Beach Dr" keys as NW N BEACH DR from either).
  // An index built before 2026-11-09 holds the old keys for such homes;
  // npm run build:url-index rewrites it.
  s = abbreviateDirectionals(splitHouseNumberUnit(s).street);
  return s ? `${s}|${zip}` : "";
}

function unitFromRedfinPath(pathOrUrl) {
  // Two possible unit positions in Redfin URLs:
  //   /WA/Seattle/2510-6th-Ave-98121/unit-1109/home/X         (post-zip unit segment)
  //   /WA/Seattle/8518-11th-Ave-NW-Unit-A-98117/home/Y        (slug-embedded unit)
  const s = String(pathOrUrl || "");
  let m = s.match(/\/unit-([A-Z0-9-]+)\/home\/\d+/i);
  if (m) return normalizeUnitToken(m[1]);
  m = s.match(/-Unit-([A-Z0-9]+)-\d{5}\/home\//i);
  if (m) return normalizeUnitToken(m[1]);
  //   /WA/Seattle/1014-B-NE-70th-St-98115/home/Z              (unit on the house number)
  m = s.match(/\/([^/]+)-\d{5}\/home\//);
  if (m) return normalizeUnitToken(splitHouseNumberUnit(m[1].replace(/-/g, " ").toUpperCase()).unit);
  return "";
}

module.exports = {
  streetKeyFromAddress,
  streetKeyFromRedfinPath,
  extractUnitFromAddress,
  houseNumber,
  unitFromRedfinPath,
  normalizeUnitToken,
  zip5,
};
