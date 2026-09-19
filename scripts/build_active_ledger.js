#!/usr/bin/env node
"use strict";

// Maintains redfin_active_ledger.csv: one row per MLS# ever seen in the Redfin
// actives feed, with the first and last day it was seen, its first and last
// asking price, and the last day it was seen genuinely ACTIVE (not under
// contract). This is the first-class replacement for mining git history for
// daily snapshots of the enriched CSV: it is small, tracked, and works in a
// shallow clone or CI where `git log` has no depth.
//
// Consumers: backfill_list_from_active_snapshots.js reads it to give each
// REDFIN_SOLD row its list-at-pending (last active ask), pending date (last day
// seen active), DOM and list date. The Overview's "what changed" feed can read
// firstAsk vs lastAsk for genuine price-cut history.
//
// Two ways rows get in:
//   --seed-from-git   walk the daily commits of the enriched CSV on --ref
//                     (default main, since --since) and apply every
//                     REDFIN_ACTIVE row as an observation dated by the commit.
//                     Run once to bootstrap; harmless to rerun (idempotent).
//   default           upsert today's redfin_active_listings.csv (the actives
//                     fetch output), dated by its fetchedAt in Pacific time.
//                     This is what the daily job runs after merge:actives.
//
// Upsert rules (observations are applied in date order):
//   new MLS#  -> firstSeen = lastSeen = date, firstAsk = lastAsk = ask
//   seen      -> lastSeen/lastAsk/lastDom/lastCdom/lastStatus follow the
//                newest observation; firstSeen/firstAsk the oldest
//   ACTIVE-ish status (Active, Coming Soon, First Look) also advances
//                lastSeenActive / lastActiveAsk; "Active Under Contract" does
//                not, so a listing's pending date is its last genuinely
//                active day, not its last day in the feed.
//   identifiers (propertyId, listingId, url, address, zip, type, listDate)
//                are filled when blank and otherwise left alone.

const fs = require("fs");
const path = require("path");
const { readCsvText, writeCsv, num } = require("./csv_util.js");
const { readSnapshotsFromGit } = require("./backfill_list_from_active_snapshots.js");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_LEDGER = path.join(PROJECT_DIR, "redfin_active_ledger.csv");
const DEFAULT_ACTIVES = path.join(PROJECT_DIR, "redfin_active_listings.csv");
const DEFAULT_REPORT = path.join(PROJECT_DIR, "redfin_active_ledger_report.json");
const DEFAULT_ENRICHED_REL = "public_sales_proxy_mls_enriched_last12mo.csv";
const DEFAULT_REF = "main";
const DEFAULT_SNAPSHOT_SINCE = "2026-06-01";

const LEDGER_COLUMNS = [
  "mlsNumber", "redfinPropertyId", "redfinListingId", "url", "address", "zip", "propertyType",
  "firstSeen", "lastSeen", "lastSeenActive", "firstAsk", "lastAsk", "lastActiveAsk",
  "listDate", "lastDom", "lastCdom", "lastStatus",
  // Ask-change history (requested by the frontend for days-to-first-cut):
  // the day lastAsk last moved, the day it FIRST moved, and how many times.
  "lastAskChangeDate", "firstAskChangeDate", "askChangeCount",
  // "true" when the listing was first seen within 3 days of its list date,
  // so its first recorded ask change really is its first cut.
  "trackedFromListing",
];

// The actives fetch used to append the unit to a street line that already
// carried it ("2727 Fairview Ave E #4 #4"). Fixed at the source; this repairs
// rows already in the ledger and any older feed file replayed into it.
// Handles every prefix form the feed has produced: "#4 #4", "Unit A Unit A",
// "Apt 3B Apt 3B", "Ste 200 Ste 200", and mixed case. Idempotent.
const UNIT_PHRASE = "(?:#\\s*\\S+|(?:unit|apt|ste|suite)\\s+\\S+)";
const DOUBLED_UNIT_RE = new RegExp(`^(.*?)\\s*(${UNIT_PHRASE})\\s+(${UNIT_PHRASE})$`, "i");
function dedupeUnitSuffix(address) {
  let s = String(address || "").trim().replace(/\s+/g, " ");
  const norm = (t) => t.replace(/^(#|unit|apt|ste|suite)\s*/i, "").replace(/\s+/g, "").toUpperCase();
  // Collapse one repetition per pass; an old snapshot held a TRIPLED unit.
  for (let guard = 0; guard < 5; guard += 1) {
    const m = s.match(DOUBLED_UNIT_RE);
    if (!m || norm(m[2]) !== norm(m[3])) break;
    s = `${m[1]} ${m[2]}`.trim();
  }
  return s;
}

// A listing whose first sighting is within this many days of its list date
// has been tracked from the start, so its first ask change is its FIRST cut.
// Listings already on the market when tracking began (Jun 8) are not.
const TRACKED_FROM_LISTING_DAYS = 3;
function trackedFromListing(entry) {
  const listDate = String(entry.listDate || "").trim();
  const firstSeen = String(entry.firstSeen || "").trim();
  if (!listDate || !firstSeen) return "";
  const gap = (Date.parse(firstSeen) - Date.parse(listDate)) / 86400000;
  if (!Number.isFinite(gap)) return "";
  return gap <= TRACKED_FROM_LISTING_DAYS ? "true" : "false";
}
const ACTIVE_STATUSES = new Set(["ACTIVE", "COMING SOON", "FIRST LOOK"]);

function isActiveStatus(status) {
  return ACTIVE_STATUSES.has(String(status || "").trim().toUpperCase());
}

// "2026-09-19T13:39:44.613Z" -> "2026-09-19" in Seattle, so a 06:30 fetch is
// dated the morning it ran, not the UTC day.
function toPacificDate(isoTimestamp) {
  const t = Date.parse(isoTimestamp || "");
  if (!Number.isFinite(t)) return "";
  return new Date(t).toLocaleDateString("en-CA", { timeZone: "America/Los_Angeles" });
}

function shiftDate(isoDate, days) {
  const t = Date.parse(isoDate);
  if (!Number.isFinite(t)) return "";
  return new Date(t + days * 86400000).toISOString().slice(0, 10);
}

// Observations from the raw actives fetch (redfin_active_listings.csv).
function observationsFromActives(rows, fallbackDate) {
  const out = [];
  for (const r of rows) {
    const mls = String(r.mlsListingNumber || "").trim();
    const ask = num(r.listPrice);
    if (!mls || !(ask > 0)) continue;
    const date = toPacificDate(r.fetchedAt) || fallbackDate || "";
    if (!date) continue;
    const dom = String(r.domDays ?? "").trim();
    out.push({
      mlsNumber: mls,
      date,
      ask,
      status: String(r.mlsStatus || "Active").trim(),
      dom,
      cdom: dom,
      listDate: dom !== "" && num(dom) >= 0 ? shiftDate(date, -Math.round(num(dom))) : "",
      redfinPropertyId: String(r.redfinPropertyId || "").trim(),
      redfinListingId: String(r.redfinListingId || "").trim(),
      url: String(r.redfinUrl || "").trim(),
      address: String(r.address || "").trim(),
      zip: String(r.zip || "").trim(),
      propertyType: String(r.uiPropertyType || r.propertyType || "").trim(),
    });
  }
  return out;
}

// Observations from one historical snapshot of the enriched CSV (its
// REDFIN_ACTIVE rows), dated by the commit that holds it.
function observationsFromEnrichedSnapshot(text, date) {
  const { rows } = readCsvText(text);
  const out = [];
  for (const r of rows) {
    if (r.mlsJoinMethod !== "REDFIN_ACTIVE") continue;
    const mls = String(r.mlsListingNumber || "").trim();
    const ask = num(r.mlsListingPrice) || num(r.listPriceAtPending);
    if (!mls || !(ask > 0)) continue;
    const listingId = (String(r.id || "").match(/^redfin-active-(\d+)$/) || [])[1] || "";
    out.push({
      mlsNumber: mls,
      date,
      ask,
      status: "Active", // merge_redfin_actives.js labels every feed row Active
      dom: String(r.mlsDOM ?? "").trim(),
      cdom: String(r.mlsCDOM ?? "").trim(),
      listDate: String(r.mlsListDate || r.listDate || "").trim(),
      redfinPropertyId: "",
      redfinListingId: listingId,
      url: "",
      address: String(r.address || "").trim(),
      zip: String(r.zip || "").trim(),
      propertyType: String(r.type || "").trim(),
    });
  }
  return out;
}

function ledgerRowsToMap(rows) {
  const map = new Map();
  for (const r of rows) {
    const mls = String(r.mlsNumber || "").trim();
    if (!mls) continue;
    map.set(mls, {
      ...r,
      mlsNumber: mls,
      address: dedupeUnitSuffix(r.address || ""),
      // Older ledgers predate the ask-change columns.
      lastAskChangeDate: r.lastAskChangeDate || "",
      firstAskChangeDate: r.firstAskChangeDate || "",
      askChangeCount: String(num(r.askChangeCount) || 0),
    });
  }
  return map;
}

const FILL_IF_BLANK = ["redfinPropertyId", "redfinListingId", "url", "address", "zip", "propertyType", "listDate"];

// Applies observations to the ledger map in date order. Returns counts.
function upsertObservations(map, observations) {
  const sorted = [...observations].sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  const counts = { inserted: 0, updated: 0 };
  for (const o of sorted) {
    const active = isActiveStatus(o.status);
    const ask = String(o.ask);
    const existing = map.get(o.mlsNumber);
    if (!existing) {
      map.set(o.mlsNumber, {
        mlsNumber: o.mlsNumber,
        redfinPropertyId: o.redfinPropertyId || "",
        redfinListingId: o.redfinListingId || "",
        url: o.url || "",
        address: dedupeUnitSuffix(o.address || ""),
        zip: o.zip || "",
        propertyType: o.propertyType || "",
        firstSeen: o.date,
        lastSeen: o.date,
        lastSeenActive: active ? o.date : "",
        firstAsk: ask,
        lastAsk: ask,
        lastActiveAsk: active ? ask : "",
        listDate: o.listDate || "",
        lastDom: o.dom || "",
        lastCdom: o.cdom || "",
        lastStatus: o.status || "",
        lastAskChangeDate: "",
        firstAskChangeDate: "",
        askChangeCount: "0",
      });
      counts.inserted += 1;
      continue;
    }
    if (!existing.firstSeen || o.date < existing.firstSeen) {
      existing.firstSeen = o.date;
      existing.firstAsk = ask;
    }
    if (!existing.lastSeen || o.date >= existing.lastSeen) {
      // A different ask on a later day is a price change (cut or raise);
      // record when it first and last happened and how often. Same-day
      // replays never count.
      if (o.date > existing.lastSeen && String(existing.lastAsk) !== ask) {
        existing.lastAskChangeDate = o.date;
        if (!existing.firstAskChangeDate) existing.firstAskChangeDate = o.date;
        existing.askChangeCount = String(num(existing.askChangeCount) + 1);
      }
      existing.lastSeen = o.date;
      existing.lastAsk = ask;
      existing.lastDom = o.dom || "";
      existing.lastCdom = o.cdom || "";
      existing.lastStatus = o.status || "";
    }
    if (active && (!existing.lastSeenActive || o.date >= existing.lastSeenActive)) {
      existing.lastSeenActive = o.date;
      existing.lastActiveAsk = ask;
    }
    for (const key of FILL_IF_BLANK) {
      if (!String(existing[key] || "").trim() && o[key]) existing[key] = key === "address" ? dedupeUnitSuffix(o[key]) : o[key];
    }
    counts.updated += 1;
  }
  return counts;
}

function ledgerMapToRows(map) {
  for (const e of map.values()) e.trackedFromListing = trackedFromListing(e);
  return [...map.values()].sort((a, b) => {
    if (a.lastSeen !== b.lastSeen) return a.lastSeen < b.lastSeen ? 1 : -1; // newest first
    return a.mlsNumber < b.mlsNumber ? -1 : a.mlsNumber > b.mlsNumber ? 1 : 0;
  });
}

function parseArgs(argv) {
  const opts = {
    ledger: DEFAULT_LEDGER, actives: DEFAULT_ACTIVES, report: DEFAULT_REPORT,
    seedFromGit: false, ref: DEFAULT_REF, since: DEFAULT_SNAPSHOT_SINCE, dryRun: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--ledger") { opts.ledger = next; i += 1; }
    else if (a === "--actives") { opts.actives = next; i += 1; }
    else if (a === "--report") { opts.report = next; i += 1; }
    else if (a === "--seed-from-git") { opts.seedFromGit = true; }
    else if (a === "--ref") { opts.ref = next; i += 1; }
    else if (a === "--since") { opts.since = next; i += 1; }
    else if (a === "--dry-run") { opts.dryRun = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/build_active_ledger.js [options]",
    "",
    "Upserts the Redfin actives feed into redfin_active_ledger.csv (one row per MLS#:",
    "first/last seen, first/last ask, last day seen genuinely active, DOM, list date).",
    "",
    "Options:",
    `  --actives FILE    Actives fetch CSV to upsert (default: ${path.relative(PROJECT_DIR, DEFAULT_ACTIVES)}; skipped if missing)`,
    `  --ledger FILE     Ledger CSV to update in place (default: ${path.relative(PROJECT_DIR, DEFAULT_LEDGER)})`,
    "  --seed-from-git   Also replay the daily REDFIN_ACTIVE snapshots from git history first",
    `  --ref REF         Git ref for --seed-from-git (default: ${DEFAULT_REF})`,
    `  --since DATE      Ignore snapshots committed before this date (default: ${DEFAULT_SNAPSHOT_SINCE})`,
    `  --report FILE     Report JSON (default: ${path.relative(PROJECT_DIR, DEFAULT_REPORT)})`,
    "  --dry-run         Compute and report, but do not write the ledger",
  ].join("\n"));
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }
  const ledgerPath = path.resolve(opts.ledger);
  const existed = fs.existsSync(ledgerPath);
  const map = existed ? ledgerRowsToMap(readCsvText(fs.readFileSync(ledgerPath, "utf8")).rows) : new Map();
  const before = map.size;
  const report = { generatedAt: new Date().toISOString(), ledger: path.relative(PROJECT_DIR, ledgerPath), rowsBefore: before, dryRun: opts.dryRun };

  if (opts.seedFromGit) {
    const snapshots = readSnapshotsFromGit(opts.ref, DEFAULT_ENRICHED_REL, opts.since);
    let observations = 0;
    let counts = { inserted: 0, updated: 0 };
    for (const snap of snapshots) {
      const obs = observationsFromEnrichedSnapshot(snap.text, snap.date);
      observations += obs.length;
      const c = upsertObservations(map, obs);
      counts = { inserted: counts.inserted + c.inserted, updated: counts.updated + c.updated };
    }
    report.seed = { ref: opts.ref, snapshots: snapshots.length, range: snapshots.length ? { min: snapshots[0].date, max: snapshots[snapshots.length - 1].date } : null, observations, ...counts };
    console.log(`Seeded from git: ${snapshots.length} snapshots, ${observations} observations, ${counts.inserted} new MLS#, ${counts.updated} updates.`);
  }

  const activesPath = path.resolve(opts.actives);
  if (fs.existsSync(activesPath)) {
    const { rows } = readCsvText(fs.readFileSync(activesPath, "utf8"));
    const obs = observationsFromActives(rows, toPacificDate(new Date().toISOString()));
    const counts = upsertObservations(map, obs);
    const dates = [...new Set(obs.map((o) => o.date))].sort();
    report.actives = { file: path.relative(PROJECT_DIR, activesPath), rows: rows.length, observations: obs.length, dates, ...counts };
    console.log(`Upserted ${obs.length} actives (${dates.join(", ") || "no date"}): ${counts.inserted} new MLS#, ${counts.updated} updates.`);
  } else {
    report.actives = { file: path.relative(PROJECT_DIR, activesPath), skipped: "missing" };
    console.log(`No actives file at ${path.relative(PROJECT_DIR, activesPath)}; nothing upserted.`);
  }

  const rows = ledgerMapToRows(map);
  report.rowsAfter = rows.length;
  report.lastSeenRange = rows.length ? { min: rows[rows.length - 1].lastSeen, max: rows[0].lastSeen } : null;
  fs.writeFileSync(path.resolve(opts.report), `${JSON.stringify(report, null, 2)}\n`);
  if (opts.dryRun) { console.log(`Dry run: ledger would hold ${rows.length} rows (was ${before}).`); return; }
  writeCsv(ledgerPath, LEDGER_COLUMNS, rows);
  console.log(`Wrote ${path.relative(PROJECT_DIR, ledgerPath)}: ${rows.length} rows (was ${before}), lastSeen ${report.lastSeenRange?.min} -> ${report.lastSeenRange?.max}.`);
}

module.exports = {
  LEDGER_COLUMNS, ACTIVE_STATUSES, isActiveStatus, toPacificDate, dedupeUnitSuffix, trackedFromListing,
  observationsFromActives, observationsFromEnrichedSnapshot,
  ledgerRowsToMap, upsertObservations, ledgerMapToRows,
};

if (require.main === module) main();
