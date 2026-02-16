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
