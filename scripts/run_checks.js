#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
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

function listFiles(dirPath, predicate) {
  if (!fs.existsSync(dirPath)) return [];
  return fs.readdirSync(dirPath, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dirPath, entry.name);
    if (entry.isDirectory()) return listFiles(full, predicate);
    return predicate(full) ? [full] : [];
  });
}

function readUiSource() {
  const indexPath = path.join(PROJECT_DIR, "index.html");
  const html = fs.readFileSync(indexPath, "utf8");
  const src = listFiles(path.join(PROJECT_DIR, "src"), (filePath) => /\.(mjs|js|css)$/.test(filePath))
    .map((filePath) => fs.readFileSync(filePath, "utf8"))
    .join("\n");
  return { html, source: `${html}\n${src}` };
}

function checkUiContract(html, source) {
  const hasAnchor = (id) => source.includes(`id="${id}"`) || source.includes(`"${id}"`);
  const requiredPairs = [
    ["tab-overview", "view-overview"],
    ["tab-pulse", "view-pulse"],
    ["tab-charts", "view-charts"],
    ["tab-heat", "view-heat"],
    ["tab-bids", "view-bids"],
    ["tab-geo", "view-geo"],
    ["tab-records", "view-records"],
    ["tab-data", "view-data"],
  ];

  requiredPairs.forEach(([tabId, panelId]) => {
    if (!new RegExp(`id=\"${tabId}\"[\\s\\S]*?aria-controls=\"${panelId}\"`).test(source)) {
      fail(`Missing tab aria-controls mapping: ${tabId} -> ${panelId}`);
    }
    if (!new RegExp(`id=\"${panelId}\"[\\s\\S]*?role=\"tabpanel\"[\\s\\S]*?aria-labelledby=\"${tabId}\"`).test(source)) {
      fail(`Missing tabpanel mapping: ${panelId} <- ${tabId}`);
    }
  });

  ["0.90x", "1.00x", "1.10x", "1.20x"].forEach((label) => {
    if (!source.includes(label)) {
      fail(`Missing fixed geo legend label: ${label}`);
    }
  });

  if (!source.includes("id=\"manualBidSource\"")) {
    fail("Missing manual active-listing selector (manualBidSource)");
  }

  if (!source.includes("data-use-active-bid")) {
    fail("Missing active-listing quick-use action (data-use-active-bid)");
  }

  [
    "pulseGroupPills",
    "pulseModeToggles",
    "pulseRecentGrid",
    "pulseMicroBreakout",
    "pulseChartHotShare",
    "pulseTrajectory",
  ].forEach((id) => {
    if (!hasAnchor(id)) {
      fail(`Missing Pulse UI anchor: ${id}`);
    }
  });

  [
    "buyerProfileStatus",
    "buyerProfileName",
    "buyerProfileToggle",
    "buyerProfileInsights",
    "micromarketProfiles",
  ].forEach((id) => {
    if (!hasAnchor(id)) {
      fail(`Missing buyer profile UI anchor: ${id}`);
    }
  });

  if (!hasAnchor("csvFile")) {
    fail("Missing CSV upload input (csvFile)");
  }

  if (!source.includes("data-buyer-profile-toggle=\"1\"")) {
    fail("Missing buyer profile toggle control");
  }

  if (!html.includes("type=\"module\"") || !html.includes("/src/main.mjs")) {
    fail("index.html must load the Vite module entrypoint");
  }

  if (/unpkg\.com\/leaflet|cdn\.jsdelivr\.net\/npm\/leaflet/i.test(source)) {
    fail("Leaflet must be loaded from the local dependency, not a CDN.");
  }
}

function runSyntaxChecks() {
  const scriptsDir = path.join(PROJECT_DIR, "scripts");
  const scriptFiles = fs
    .readdirSync(scriptsDir)
    .filter((name) => name.endsWith(".js"))
    .map((name) => path.join(scriptsDir, name));
  scriptFiles.push(path.join(PROJECT_DIR, "pulse_metrics.js"));
  scriptFiles.push(path.join(PROJECT_DIR, "buyer_profile.js"));
  scriptFiles.push(path.join(PROJECT_DIR, "vite.config.mjs"));
  scriptFiles.push(...listFiles(path.join(PROJECT_DIR, "src"), (filePath) => filePath.endsWith(".mjs")));

  scriptFiles.forEach(runNodeCheck);
  const { html, source } = readUiSource();
  checkUiContract(html, source);
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
