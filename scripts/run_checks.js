#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const os = require("os");
const { spawnSync } = require("child_process");

const PROJECT_DIR = path.resolve(__dirname, "..");

function fail(message) {
  // eslint-disable-next-line no-console
  console.error(message);
  process.exit(1);
}

function runNodeCheck(filePath) {
  const result = spawnSync(process.execPath, ["--check", filePath], {
    cwd: PROJECT_DIR,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    const rel = path.relative(PROJECT_DIR, filePath);
    fail(`Syntax check failed for ${rel}\n${result.stderr || result.stdout || "Unknown syntax error"}`);
  }
}

function extractInlineScript() {
  const indexPath = path.join(PROJECT_DIR, "index.html");
  const html = fs.readFileSync(indexPath, "utf8");
  const start = html.indexOf("<script>");
  const end = html.lastIndexOf("</script>");
  if (start < 0 || end < 0 || end <= start) {
    fail("Could not locate inline <script> block in index.html");
  }
  const js = html.slice(start + "<script>".length, end);
  const tmpPath = path.join(os.tmpdir(), "seattle_buyer_lens_inline_check.js");
  fs.writeFileSync(tmpPath, js);
  return { html, tmpPath };
}

function checkUiContract(html) {
  const requiredPairs = [
    ["tab-overview", "view-overview"],
    ["tab-charts", "view-charts"],
    ["tab-heat", "view-heat"],
    ["tab-bids", "view-bids"],
    ["tab-geo", "view-geo"],
    ["tab-records", "view-records"],
    ["tab-data", "view-data"],
  ];

  requiredPairs.forEach(([tabId, panelId]) => {
    if (!new RegExp(`id=\"${tabId}\"[^>]*aria-controls=\"${panelId}\"`).test(html)) {
      fail(`Missing tab aria-controls mapping: ${tabId} -> ${panelId}`);
    }
    if (!new RegExp(`id=\"${panelId}\"[^>]*role=\"tabpanel\"[^>]*aria-labelledby=\"${tabId}\"`).test(html)) {
      fail(`Missing tabpanel mapping: ${panelId} <- ${tabId}`);
    }
  });

  ["0.90x", "1.00x", "1.10x", "1.20x"].forEach((label) => {
    if (!html.includes(label)) {
      fail(`Missing fixed geo legend label: ${label}`);
    }
  });

  if (!html.includes("id=\"manualBidSource\"")) {
    fail("Missing manual active-listing selector (manualBidSource)");
  }

  if (!html.includes("data-use-active-bid")) {
    fail("Missing active-listing quick-use action (data-use-active-bid)");
  }
}

function runSyntaxChecks() {
  const scriptsDir = path.join(PROJECT_DIR, "scripts");
  const scriptFiles = fs
    .readdirSync(scriptsDir)
    .filter((name) => name.endsWith(".js"))
    .map((name) => path.join(scriptsDir, name));

  scriptFiles.forEach(runNodeCheck);
  const { html, tmpPath } = extractInlineScript();
  runNodeCheck(tmpPath);
  checkUiContract(html);
}

function runBuildValidation() {
  const result = spawnSync(process.execPath, ["scripts/validate_data_refresh.js"], {
    cwd: PROJECT_DIR,
    stdio: "inherit",
  });
  if (result.status !== 0) {
    fail("Data validation failed. See logs above.");
  }
}

function main() {
  const mode = String(process.argv[2] || "lint").toLowerCase();
  if (!["lint", "typecheck", "build"].includes(mode)) {
    fail(`Unsupported mode: ${mode}`);
  }

  runSyntaxChecks();

  if (mode === "build") {
    runBuildValidation();
  }

  // eslint-disable-next-line no-console
  console.log(`Checks passed (${mode}).`);
}

main();
