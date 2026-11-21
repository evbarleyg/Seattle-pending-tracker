"use strict";

const { test } = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { OUTPUT_COLUMNS } = require("../scripts/fetch_redfin_actives.js");

const MERGE_SCRIPT = path.join(__dirname, "..", "scripts", "merge_redfin_actives.js");
const ENRICHED = [
  "address,zip,mlsJoinMethod,mlsListingNumber,mlsStatus",
  "1 Main St,98107,REDFIN_ACTIVE,111,Active",
  "2 Main St,98107,MLS_SOLD_NOT_IN_COUNTY,222,Sold",
  "",
].join("\n");

function runMerge(extraArgs = []) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "merge-actives-"));
  const enriched = path.join(dir, "enriched.csv");
  const redfin = path.join(dir, "actives.csv");
  const report = path.join(dir, "merge_report.json");
  fs.writeFileSync(enriched, ENRICHED);
  fs.writeFileSync(redfin, `${OUTPUT_COLUMNS.join(",")}\n`); // header-only, as on 2026-11-20
  const result = spawnSync(process.execPath, [MERGE_SCRIPT, "--enriched", enriched, "--redfin", redfin, "--report", report, ...extraArgs], { encoding: "utf8" });
  return { result, enriched, report };
}

test("merge keeps the existing REDFIN_ACTIVE rows when the actives CSV has 0 rows", () => {
  const { result, enriched, report } = runMerge();
  assert.strictEqual(result.status, 0, result.stderr);
  assert.match(result.stderr, /0 rows/);
  assert.strictEqual(fs.readFileSync(enriched, "utf8"), ENRICHED, "enriched CSV untouched");
  assert.strictEqual(fs.existsSync(report), false, "no merge report for a skipped merge");
});

test("merge --allow-empty strips the REDFIN_ACTIVE rows on purpose", () => {
  const { result, enriched } = runMerge(["--allow-empty"]);
  assert.strictEqual(result.status, 0, result.stderr);
  const text = fs.readFileSync(enriched, "utf8");
  assert.doesNotMatch(text, /REDFIN_ACTIVE/);
  assert.match(text, /2 Main St/);
});
