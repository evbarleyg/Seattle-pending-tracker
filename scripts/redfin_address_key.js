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
  return "";
}

function streetKeyFromAddress(rawAddress, rawZip) {
  let s = String(rawAddress || "").toUpperCase().trim();
  if (!s) return "";

  // Strip trailing city/state/zip tails
  s = s.replace(/,\s*[A-Z .]+,?\s*WA\s*\d{5}.*$/, "");
  s = s.replace(/,\s*[A-Z .]+,?\s*\d{5}.*$/, "");
  s = s.replace(/\s+WA\s+\d{5}.*$/, "");
  s = s.replace(/\s+\d{5}\s*$/, "");

  // Drop unit fragments (matched separately by extractUnitFromAddress)
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
  return "";
}

module.exports = {
  streetKeyFromAddress,
  streetKeyFromRedfinPath,
  extractUnitFromAddress,
  unitFromRedfinPath,
  normalizeUnitToken,
  zip5,
};
