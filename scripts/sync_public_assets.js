#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.join(PROJECT_DIR, "public");

const ASSETS = [
  ["public_sales_proxy_mls_enriched_last12mo.csv", "public_sales_proxy_mls_enriched_last12mo.csv"],
  ["public_sales_proxy_all_prices_last12mo.csv", "public_sales_proxy_all_prices_last12mo.csv"],
  ["data_refresh_report.json", "data_refresh_report.json"],
  ["buyer_profile_memory.json", "buyer_profile_memory.json"],
  ["favicon.ico", "favicon.ico"],
  ["favicon.svg", "favicon.svg"],
  ["assets/share-preview.svg", "assets/share-preview.svg"],
  ["assets/ebg-icon.svg", "assets/ebg-icon.svg"],
];

function copyAsset([sourceRel, destRel]) {
  const source = path.join(PROJECT_DIR, sourceRel);
  const dest = path.join(PUBLIC_DIR, destRel);
  if (!fs.existsSync(source)) {
    throw new Error(`Missing required public asset: ${sourceRel}`);
  }
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(source, dest);
  return destRel;
}

// Derived, slimmed copy of the actives ledger for the app (requested by the
// frontend session): one row per MLS# ever seen for sale, without the URL and
// Redfin ids, so the price-cut feed and Bids badges can join on mlsNumber
// (= the enriched CSV's mlsListingNumber). Optional: skipped when the ledger
// is absent (a checkout that has not run ledger:seed yet).
const LEDGER_SOURCE = "redfin_active_ledger.csv";
const LISTING_LEDGER_DEST = "listing_ledger.csv";
const LISTING_LEDGER_COLUMNS = [
  "mlsNumber", "address", "zip", "propertyType", "firstSeen", "lastSeen", "lastSeenActive",
  "firstAsk", "lastAsk", "listDate", "lastDom", "lastStatus",
  "lastAskChangeDate", "firstAskChangeDate", "askChangeCount", // for days-to-first-cut
];

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") { cur += "\""; i += 1; }
      else inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

function safeCsv(value) {
  const s = value == null ? "" : String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, "\"\"")}"` : s;
}

function slimListingLedger(text) {
  const lines = String(text || "").split(/\r?\n/).filter((l) => l.length);
  if (!lines.length) return { text: `${LISTING_LEDGER_COLUMNS.join(",")}\n`, rows: 0 };
  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const ix = LISTING_LEDGER_COLUMNS.map((c) => headers.indexOf(c));
  const out = [LISTING_LEDGER_COLUMNS.join(",")];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (!String(cols[headers.indexOf("mlsNumber")] || "").trim()) continue;
    out.push(ix.map((j) => safeCsv(j >= 0 ? cols[j] : "")).join(","));
  }
  return { text: `${out.join("\n")}\n`, rows: out.length - 1 };
}

function publishListingLedger() {
  const source = path.join(PROJECT_DIR, LEDGER_SOURCE);
  if (!fs.existsSync(source)) return null;
  const { text, rows } = slimListingLedger(fs.readFileSync(source, "utf8"));
  fs.writeFileSync(path.join(PUBLIC_DIR, LISTING_LEDGER_DEST), text);
  return `${LISTING_LEDGER_DEST} (${rows} listings)`;
}

function main() {
  fs.mkdirSync(PUBLIC_DIR, { recursive: true });
  const copied = ASSETS.map(copyAsset);
  const ledger = publishListingLedger();
  if (ledger) copied.push(ledger);
  // eslint-disable-next-line no-console
  console.log(`Synced ${copied.length} public assets: ${copied.join(", ")}`);
}

module.exports = { slimListingLedger, LISTING_LEDGER_COLUMNS };

if (require.main === module) main();
