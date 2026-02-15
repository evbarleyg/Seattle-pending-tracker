#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const PROJECT_DIR = path.resolve(__dirname, "..");
const REPORT_FILE = path.join(PROJECT_DIR, "data_refresh_report.json");

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: PROJECT_DIR,
    stdio: "inherit",
    shell: false,
  });
  if (result.status !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(" ")}`);
  }
}

function parseArgs(argv) {
  const flags = new Set(argv.slice(2));
  return {
    skipPublic: flags.has("--skip-public"),
    skipMls: flags.has("--skip-mls"),
    reportOnly: flags.has("--report-only"),
    push: flags.has("--push"),
  };
}

function readReportCounts() {
  if (!fs.existsSync(REPORT_FILE)) return {};
  const report = JSON.parse(fs.readFileSync(REPORT_FILE, "utf8"));
  return report.validation?.counts || report.counts || {};
}

function commitMessageFromReport() {
  const counts = readReportCounts();
  const ts = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  const rows = Number(counts.outputRows || 0);
  const mls = Number(counts.mlsRows || 0);
  const active = Number(counts.mlsActiveRows || 0);
  return `data refresh ${ts} rows=${rows} mls=${mls} active=${active}`;
}

function stageAndPush() {
  const stageTargets = [
    "public_sales_proxy_all_prices_last12mo.csv",
    "public_sales_proxy_mls_enriched_last12mo.csv",
    "data_refresh_report.json",
    "index.html",
    "README.md",
    "DATA_SCHEMA.md",
    "scripts",
  ];

  run("git", ["add", ...stageTargets]);
  const diffCheck = spawnSync("git", ["diff", "--cached", "--quiet"], {
    cwd: PROJECT_DIR,
    stdio: "inherit",
    shell: false,
  });
  if (diffCheck.status === 0) {
    // eslint-disable-next-line no-console
    console.log("No staged changes to commit.");
    return;
  }

  const message = commitMessageFromReport();
  run("git", ["commit", "-m", message]);
  run("git", ["push", "origin", "main"]);
}

function main() {
  const opts = parseArgs(process.argv);

  if (!opts.reportOnly) {
    if (!opts.skipPublic) {
      run("node", ["scripts/build_public_proxy_csv.js"]);
    }
    if (!opts.skipMls) {
      run("node", ["scripts/build_mls_enriched_dataset.js"]);
    }
  }

  run("node", ["scripts/validate_data_refresh.js"]);

  if (opts.push) {
    stageAndPush();
  }
}

try {
  main();
} catch (err) {
  // eslint-disable-next-line no-console
  console.error(err.message || String(err));
  process.exit(1);
}
