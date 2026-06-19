"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

const indexPath = path.resolve(__dirname, "..", "index.html");
const html = fs.readFileSync(indexPath, "utf8");
const source = [
  html,
  fs.readFileSync(path.resolve(__dirname, "..", "src", "main.mjs"), "utf8"),
  fs.readFileSync(path.resolve(__dirname, "..", "src", "domain", "selectors.mjs"), "utf8"),
].join("\n");

test("tabs and tabpanels are mapped for accessibility", () => {
  const pairs = [
    ["tab-overview", "view-overview"],
    ["tab-pulse", "view-pulse"],
    ["tab-bids", "view-bids"],
    ["tab-geo", "view-geo"],
    ["tab-records", "view-records"],
    ["tab-data", "view-data"],
  ];

  pairs.forEach(([tabId, panelId]) => {
    assert.match(
      source,
      new RegExp(`id=\\"${tabId}\\"[\\s\\S]*?aria-controls=\\"${panelId}\\"`),
      `Expected ${tabId} to control ${panelId}`
    );
    assert.match(
      source,
      new RegExp(`id=\\"${panelId}\\"[\\s\\S]*?role=\\"tabpanel\\"[\\s\\S]*?aria-labelledby=\\"${tabId}\\"`),
      `Expected ${panelId} to be labelled by ${tabId}`
    );
  });
});

test("moderate merge removes charts and heat as top-level tabs", () => {
  assert.doesNotMatch(source, /id=\"tab-charts\"|id=\"view-charts\"/);
  assert.doesNotMatch(source, /id=\"tab-heat\"|id=\"view-heat\"/);
  assert.match(source, /id=\"pulseSliceTrends\"/);
  assert.match(source, /id=\"pulseCompetitionPockets\"/);
});

test("overview owns the command center and decision brief", () => {
  assert.match(source, /function renderOverviewView[\s\S]*?commandCenterSectionHtml/);
  assert.match(source, /id=\"decisionBrief\"/);
  assert.match(source, /What this means for the next offer/);
  assert.match(source, /Start with stance, then inspect the proof/);
});

test("geo legend uses fixed numeric range labels", () => {
  ["0.90x", "1.00x", "1.10x", "1.20x"].forEach((label) => {
    assert.match(source, new RegExp(label.replace(".", "\\.")));
  });
  assert.match(source, /Sale-to-list color legend/);
  assert.match(source, /Color = sale\/list pressure; grey = no list price on record/);
  // Grey "no list price" bucket must be documented in the legend (county +
  // sold-only rows are the dominant, uncolored population).
  assert.match(source, /No list price/);
});

test("geo map lifecycle handles rerenders without stale Leaflet containers", () => {
  assert.match(source, /getContainer\(\) !== mapEl/);
  assert.match(source, /state\.geo\.map\.remove\(\)/);
  assert.match(source, /popupPropertyKey/);
  assert.match(source, /marker\.openPopup\(\)/);
  assert.match(source, /ensureGeoShell/);
  assert.match(source, /updateGeoShell/);
});

test("manual bids can load active listing from bids table", () => {
  assert.match(source, /id=\"manualBidSource\"/);
  assert.match(source, /data-use-active-bid/);
});

test("pulse tab exposes local controls and visualization anchors", () => {
  [
    "pulseGroupPills",
    "pulseModeToggles",
    "pulseStatus",
    "pulseReadout",
    "pulseRecentGrid",
    "pulseMicroBreakout",
    "pulseChartHotShare",
    "pulseChartMedianDom",
    "pulseChartSaleToList",
    "pulseChartBidUp",
    "pulseChartClosePrice",
    "pulseTrajectory",
    "pulseSliceTrends",
    "pulseCompetitionPockets",
    "chartVolume",
    "chartClose",
    "chartRatio",
  ].forEach((id) => {
    assert.match(source, new RegExp(`id=\\"${id}\\"`));
  });

  [
    "primary",
    "Ballard",
    "Fremont / Green Lake / Woodland Park",
    "Queen Anne",
    "Magnolia",
  ].forEach((group) => {
    const escaped = group.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    assert.match(source, new RegExp(`data-pulse-group=\\\"\\$\\{esc\\(group\\.id\\)\\}|data-pulse-group=\\\"${escaped}\\\"`));
  });

  assert.match(source, /from "\.\/domain\/pulseMetrics\.mjs"/);
  assert.match(source, /data-pulse-mode=\"compare\"/);
  assert.match(source, /data-pulse-mode=\"combined\"/);
});

test("overview exposes buyer profile and micromarket anchors", () => {
  [
    "buyerProfileStatus",
    "buyerProfileMemory",
    "buyerProfileToggle",
    "buyerProfileName",
    "buyerProfileSummary",
    "buyerProfileTraits",
    "buyerProfileInsights",
    "micromarketIntro",
    "micromarketProfiles",
  ].forEach((id) => {
    assert.match(source, new RegExp(`id=\\"${id}\\"`));
  });

  assert.match(source, /Micromarket Profiles/);
  assert.match(source, /from "\.\/domain\/buyerProfile\.mjs"/);
  assert.match(source, /data-buyer-profile-toggle=\"1\"/);
});

test("pulse charts wire month interactions through SVG points", () => {
  assert.match(source, /function pulseChartSvg[\s\S]*?data-set-interaction=\"month\"/);
  assert.match(source, /function pulseTrajectoryCards[\s\S]*?data-set-interaction=\"month\"/);
});

test("charts expose readable axes and data labels", () => {
  [
    "chart-summary",
    "chart-guide",
    "legend-swatch",
    "chart-y-tick",
    "chart-x-tick",
    "axis-title",
    "chart-value-label",
  ].forEach((token) => {
    assert.match(source, new RegExp(token));
  });
  assert.match(source, /Y-axis shows/);
  assert.match(source, /X-axis shows sale month/);
  assert.match(source, /Blue line: monthly value/);
  assert.match(source, /Green line: 3-month average/);
  assert.match(source, /What this tells you/);
  assert.match(source, /Fast-Sale Share/);
});

test("records filters expose MLS special-sale control and coverage cue", () => {
  assert.match(source, /id=\"fSpecialSale\"/);
  assert.match(source, /MLS-only extras are neighborhood-scoped/i);
});

test("property surfaces keep Zillow links available", () => {
  [
    "propertyAddressLink",
    "Open Zillow for",
    "mrow-address-link",
    "mini-record-link",
    "map-popup-link",
    "KC Parcel",
  ].forEach((token) => {
    assert.match(source, new RegExp(token));
  });
});

test("geo selection is separate from dashboard map filtering", () => {
  assert.match(source, /selectedPropertyKeys/);
  assert.match(source, /filterPropertyKeys/);
  assert.match(source, /id=\"geoApplySelection\"/);
  assert.match(source, /applyMapSelectionFilter/);
  assert.match(source, /Marker clicks inspect properties only/);
});

test("bids includes a comp finder for prospective listings", () => {
  [
    "Comp Finder",
    "Find Comps",
    "computeManualComps",
    "computeBidCompTiers",
    "manualCompRun",
  ].forEach((token) => {
    assert.match(source, new RegExp(token));
  });
});

test("bids table headers expose sortable controls", () => {
  [
    "address",
    "neighborhood",
    "type",
    "suggestedBid",
    "confidence",
    "compCount",
  ].forEach((key) => {
    assert.match(source, new RegExp(`data-bid-sort=\\\"\\$\\{esc\\(key\\)\\}|data-bid-sort=\\\"${key}\\\"`));
  });
});

test("data tab exposes refresh metadata placeholders", () => {
  [
    "dataDatasetName",
    "dataDatasetRows",
    "dataValidationStatus",
    "dataValidationTime",
    "dataOutputRows",
    "dataRealtorFileCount",
  ].forEach((id) => {
    assert.match(source, new RegExp(`id=\\"\\$\\{esc\\(id\\)\\}|id=\\"${id}\\"`));
  });
});

test("vite shell and public assets are wired", () => {
  assert.match(html, /<div id=\"app\"><\/div>/);
  assert.match(html, /<script type=\"module\" src=\"\/src\/main\.mjs\"><\/script>/);
  assert.doesNotMatch(source, /unpkg\.com\/leaflet|cdn\.jsdelivr\.net\/npm\/leaflet/i);
  assert.match(source, /import\("leaflet"\)/);
  assert.match(source, /new Worker\(new URL\("\.\/workers\/dataWorker\.mjs"/);
  assert.match(source, /id=\"csvFile\"/);
});
