#!/usr/bin/env node
"use strict";

// Direct download of King County Assessor mainframe extract zips.
//
// KC's data download portal (https://info.kingcounty.gov/assessor/datadownload/
// default.aspx) hides links behind a disclaimer checkbox postback, but once
// the page is rendered, the actual zips are at stable URLs on
// aqua.kingcounty.gov. They serve to any client that sends the right Referer
// header — no session cookies, no Playwright, no DOM clicks needed.
//
// Mapping our 5 expected EXTR_*.csv files to KC's published archives:
//   EXTR_RPSale       <- Real Property Sales.zip
//   EXTR_RPAcct_NoName<- Real Property Account.zip   (no-name variant — see below)
//   EXTR_ResBldg      <- Residential Building.zip
//   EXTR_LookUp       <- Lookup.zip
//   EXTR_Parcel       <- Parcel.zip
//
// Note: KC's "Real Property Account.zip" archive contains EXTR_RPAcct.csv
// (with names). The repo expects EXTR_RPAcct_NoName.csv (no owner names —
// likely a separate KC drop). If only EXTR_RPAcct.csv lands, copy it over
// the _NoName.csv file (the build script reads only address/parcel fields,
// not names).
//
// Usage:
//   node scripts/download_kc_zips.js [--dest ~/Downloads]

const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_DEST = path.join(process.env.HOME || "/tmp", "Downloads");
const REFERER = "https://info.kingcounty.gov/assessor/datadownload/default.aspx";
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

// Map repo-expected filenames to the KC archive name + URL.
const TARGETS = [
  { repo: "EXTR_RPSale.zip",        archive: "Real Property Sales.zip" },
  { repo: "EXTR_RPAcct_NoName.zip", archive: "Real Property Account.zip" },
  { repo: "EXTR_ResBldg.zip",       archive: "Residential Building.zip" },
  { repo: "EXTR_LookUp.zip",        archive: "Lookup.zip" },
  { repo: "EXTR_Parcel.zip",        archive: "Parcel.zip" },
];

function urlFor(archive) {
  return `https://aqua.kingcounty.gov/extranet/assessor/${encodeURIComponent(archive).replace(/%20/g, "%20")}`;
}

function parseArgs(argv) {
  const opts = { dest: DEFAULT_DEST };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--dest") { opts.dest = next; i += 1; }
    else if (a === "--list-links") { opts.listLinks = true; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/download_kc_zips.js [--dest DIR] [--list-links]",
    "",
    "Downloads 5 King County Assessor mainframe extract zips directly from",
    "aqua.kingcounty.gov. No Playwright, no browser — just an HTTP fetch with",
    "the proper Referer header.",
    "",
    "Subject to King County's RCW 42.56.070(9) restriction — non-commercial",
    "use only. By running this script you affirm the same disclaimer the KC",
    "portal asks a manual user to check.",
    "",
    "Options:",
    "  --dest DIR       Save downloads here (default: ~/Downloads)",
    "  --list-links     Skip downloads; print the URL Manifest only.",
    "",
    "Then run: npm run refresh:kc",
  ].join("\n"));
}

async function downloadOne(target, dest) {
  const url = urlFor(target.archive);
  const out = path.join(dest, target.repo);
  process.stdout.write(`[${target.repo}] ${url}\n`);
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Referer: REFERER, Accept: "*/*" },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} fetching ${url}`);
  }
  if (!res.body) throw new Error("empty response body");
  await pipeline(res.body, fs.createWriteStream(out));
  const sizeMb = (fs.statSync(out).size / 1024 / 1024).toFixed(1);
  process.stdout.write(`  saved ${sizeMb} MB -> ${out}\n`);
  return out;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }

  if (opts.listLinks) {
    console.log("Manifest of King County extract URLs:");
    for (const t of TARGETS) {
      console.log(`  ${t.repo.padEnd(28)} <- ${urlFor(t.archive)}`);
    }
    return;
  }

  fs.mkdirSync(opts.dest, { recursive: true });
  const downloaded = [];
  const failed = [];
  for (const target of TARGETS) {
    try {
      await downloadOne(target, opts.dest);
      downloaded.push(target.repo);
    } catch (err) {
      console.warn(`  FAILED: ${err.message}`);
      failed.push({ name: target.repo, error: err.message });
    }
  }
  console.log("");
  console.log(`Downloaded: ${downloaded.length}/${TARGETS.length} (${downloaded.join(", ") || "none"})`);
  if (failed.length) {
    console.log(`Failed:     ${failed.length}`);
    for (const f of failed) console.log(`  - ${f.name}: ${f.error}`);
  }
  console.log(`Location:   ${opts.dest}`);
  if (downloaded.length) console.log("\nNext: npm run refresh:kc");
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
