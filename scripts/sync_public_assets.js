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

function main() {
  fs.mkdirSync(PUBLIC_DIR, { recursive: true });
  const copied = ASSETS.map(copyAsset);
  // eslint-disable-next-line no-console
  console.log(`Synced ${copied.length} public assets: ${copied.join(", ")}`);
}

main();
