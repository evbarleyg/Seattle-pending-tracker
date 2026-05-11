#!/usr/bin/env node
"use strict";

// Browser-driven download of King County Assessor mainframe extract zips.
// KC's portal renders download links via JS / session state — there's no
// stable curl-able URL — so we drive a real browser via Playwright.
//
// First-time setup (once per machine):
//   npm install playwright --save-dev
//   npx playwright install chromium
//
// Usage:
//   node scripts/download_kc_zips.js [--dest ~/Downloads] [--show-browser]
//
// After this finishes, run `npm run refresh:kc` to ingest the zips into the
// pipeline.

const fs = require("fs");
const path = require("path");

const PROJECT_DIR = path.resolve(__dirname, "..");
const DEFAULT_DEST = path.join(process.env.HOME || "/tmp", "Downloads");

const KC_DATA_DOWNLOAD_PAGE = "https://info.kingcounty.gov/assessor/datadownload/default.aspx";

// Files we want — Playwright matches link text or href substring case-insensitively.
const TARGET_FILES = [
  "EXTR_RPSale.zip",
  "EXTR_RPAcct_NoName.zip",
  "EXTR_ResBldg.zip",
  "EXTR_LookUp.zip",
  "EXTR_Parcel.zip",
];

function parseArgs(argv) {
  const opts = { dest: DEFAULT_DEST, headless: true };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--dest") { opts.dest = next; i += 1; }
    else if (a === "--show-browser") { opts.headless = false; }
    else if (a === "--help" || a === "-h") { opts.help = true; }
  }
  return opts;
}

function printHelp() {
  console.log([
    "Usage: node scripts/download_kc_zips.js [--dest DIR] [--show-browser]",
    "",
    "First-time setup:",
    "  npm install playwright --save-dev",
    "  npx playwright install chromium",
    "",
    "Then:",
    "  node scripts/download_kc_zips.js",
    "  npm run refresh:kc          # ingests the downloaded zips",
    "",
    "Options:",
    "  --dest DIR        Save downloads here (default: ~/Downloads)",
    "  --show-browser    Run with a visible browser window (default: headless)",
  ].join("\n"));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.help) { printHelp(); return; }

  let chromium;
  try {
    ({ chromium } = require("playwright"));
  } catch (err) {
    console.error("Playwright is not installed. Run:");
    console.error("  npm install playwright --save-dev");
    console.error("  npx playwright install chromium");
    process.exit(1);
  }

  fs.mkdirSync(opts.dest, { recursive: true });

  console.log(`Launching ${opts.headless ? "headless" : "windowed"} Chromium...`);
  const browser = await chromium.launch({ headless: opts.headless });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  console.log(`Navigating to ${KC_DATA_DOWNLOAD_PAGE}`);
  await page.goto(KC_DATA_DOWNLOAD_PAGE, { waitUntil: "domcontentloaded", timeout: 30000 });

  // The data-download portal links to "Assessment Mainframe File Extracts"
  // which is a separate page (the modern URL is unstable; navigate via the
  // visible link text rather than guessing).
  const mfLink = page.locator("a", { hasText: /Mainframe File Extracts/i }).first();
  if (await mfLink.count()) {
    console.log("Following 'Mainframe File Extracts' link...");
    const [popup] = await Promise.all([
      page.waitForEvent("popup").catch(() => null),
      mfLink.click(),
    ]);
    if (popup) await popup.waitForLoadState("domcontentloaded").catch(() => {});
    // Some clicks open same-window; wait for nav settling either way.
    await page.waitForLoadState("domcontentloaded", { timeout: 15000 }).catch(() => {});
  } else {
    console.warn("'Mainframe File Extracts' link not found on data-download page.");
  }

  // Now we should be on (or have a popup of) the mainframe-extracts page.
  // Try both contexts when looking for zip links.
  const pages = context.pages();
  const downloaded = [];
  const skipped = [];

  for (const target of TARGET_FILES) {
    let didDownload = false;
    for (const p of pages) {
      const linkSelectors = [
        `a:has-text("${target}")`,
        `a[href*="${target}" i]`,
        `a[href*="${target.replace(/\.zip$/i, "")}" i][href$=".zip" i]`,
      ];
      for (const sel of linkSelectors) {
        const link = p.locator(sel).first();
        if (!(await link.count())) continue;
        try {
          console.log(`Downloading ${target}...`);
          const [download] = await Promise.all([
            p.waitForEvent("download", { timeout: 60000 }),
            link.click({ timeout: 5000 }),
          ]);
          const out = path.join(opts.dest, target);
          await download.saveAs(out);
          const stats = fs.statSync(out);
          console.log(`  saved ${target} (${(stats.size / 1024 / 1024).toFixed(1)} MB)`);
          downloaded.push(target);
          didDownload = true;
          break;
        } catch (err) {
          console.warn(`  failed via ${sel}: ${err.message}`);
        }
      }
      if (didDownload) break;
    }
    if (!didDownload) {
      console.warn(`  ${target}: no download link found`);
      skipped.push(target);
    }
  }

  await browser.close();

  console.log("");
  console.log(`Downloaded: ${downloaded.length} (${downloaded.join(", ") || "none"})`);
  if (skipped.length) console.log(`Skipped:    ${skipped.length} (${skipped.join(", ")})`);
  console.log(`Location:   ${opts.dest}`);
  if (downloaded.length) {
    console.log("");
    console.log("Next: npm run refresh:kc");
  }
}

if (require.main === module) {
  main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
}
