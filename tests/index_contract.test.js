"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

const indexPath = path.resolve(__dirname, "..", "index.html");
const html = fs.readFileSync(indexPath, "utf8");

test("tabs and tabpanels are mapped for accessibility", () => {
  const pairs = [
    ["tab-overview", "view-overview"],
    ["tab-pulse", "view-pulse"],
    ["tab-charts", "view-charts"],
    ["tab-heat", "view-heat"],
    ["tab-bids", "view-bids"],
    ["tab-geo", "view-geo"],
    ["tab-records", "view-records"],
    ["tab-data", "view-data"],
  ];

  pairs.forEach(([tabId, panelId]) => {
    assert.match(
      html,
      new RegExp(`id=\\"${tabId}\\"[^>]*aria-controls=\\"${panelId}\\"`),
      `Expected ${tabId} to control ${panelId}`
    );
    assert.match(
      html,
      new RegExp(`id=\\"${panelId}\\"[^>]*role=\\"tabpanel\\"[^>]*aria-labelledby=\\"${tabId}\\"`),
      `Expected ${panelId} to be labelled by ${tabId}`
    );
  });
});

test("geo legend uses fixed numeric range labels", () => {
  ["0.90x", "1.00x", "1.10x", "1.20x"].forEach((label) => {
    assert.match(html, new RegExp(label.replace(".", "\\.")));
  });
});

test("manual bids can load active listing from bids table", () => {
  assert.match(html, /id=\"manualBidSource\"/);
  assert.match(html, /data-use-active-bid/);
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
  ].forEach((id) => {
    assert.match(html, new RegExp(`id=\\"${id}\\"`));
  });

  [
    "primary",
    "Ballard",
    "Fremont / Green Lake / Woodland Park",
    "Queen Anne",
    "Magnolia",
  ].forEach((group) => {
    const escaped = group.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    assert.match(html, new RegExp(`data-pulse-group=\\\"${escaped}\\\"`));
  });

  assert.match(html, /<script src=\"\.\/pulse_metrics\.js\"><\/script>/);
  assert.match(html, /data-pulse-mode=\"compare\"/);
  assert.match(html, /data-pulse-mode=\"combined\"/);
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
    assert.match(html, new RegExp(`id=\\"${id}\\"`));
  });

  assert.match(html, /Micromarket Profiles/);
  assert.match(html, /<script src=\"\.\/buyer_profile\.js\"><\/script>/);
  assert.match(html, /data-buyer-profile-toggle=\"1\"/);
});

test("pulse charts wire month interactions through SVG points", () => {
  assert.match(html, /function pulseChartSvg[\s\S]*?data-set-interaction=\"month\"/);
  assert.match(html, /function pulseTrajectorySvg[\s\S]*?data-set-interaction=\"month\"/);
});

test("records filters expose MLS special-sale control and coverage cue", () => {
  assert.match(html, /id=\"fSpecialSale\"/);
  assert.match(html, /MLS-only extras are neighborhood-scoped/i);
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
    assert.match(html, new RegExp(`data-bid-sort=\\\"${key}\\\"`));
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
    assert.match(html, new RegExp(`id=\\"${id}\\"`));
  });
});
