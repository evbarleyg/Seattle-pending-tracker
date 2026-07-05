"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

// Synthetic glossary entry, no real data.
const SAMPLE_METRIC = {
  id: "overAskShare",
  label: "Over-ask share",
  definition: "How often winners paid more than the asking price.",
  formula: "Count closed comps that sold above list, divided by comps with a real list price.",
  source: "Manual MLS exports matched to KC assessor sales.",
  cadence: "MLS exports are pulled by hand, usually weekly.",
  caveats: ["Only comps with a real list price count.", "County-only rows are excluded."],
};

test("renderUniverseCaption formats count, label, and window", async () => {
  const { renderUniverseCaption } = await importModule("src/ui/explain.mjs");
  const html = renderUniverseCaption({
    count: 2179,
    universeLabel: "closed comps in your slice",
    windowLabel: "last 12 mo",
  });
  assert.match(html, /class="universe-caption"/);
  assert.match(html, /of 2,179 closed comps in your slice \(last 12 mo\)/);
});

test("renderUniverseCaption degrades when pieces are missing", async () => {
  const { renderUniverseCaption } = await importModule("src/ui/explain.mjs");
  assert.equal(renderUniverseCaption(), "");
  assert.equal(renderUniverseCaption({ count: null, universeLabel: "" }), "");
  const noCount = renderUniverseCaption({ universeLabel: "closed comps" });
  assert.match(noCount, /closed comps/);
  assert.doesNotMatch(noCount, /\bof\b/);
  const noWindow = renderUniverseCaption({ count: 12, universeLabel: "comps" });
  assert.match(noWindow, /of 12 comps/);
  assert.doesNotMatch(noWindow, /\(/);
});

test("renderUniverseCaption escapes HTML in labels", async () => {
  const { renderUniverseCaption } = await importModule("src/ui/explain.mjs");
  const html = renderUniverseCaption({ count: 5, universeLabel: '<b>"comps"</b>' });
  assert.doesNotMatch(html, /<b>/);
  assert.match(html, /&lt;b&gt;/);
  assert.match(html, /&quot;comps&quot;/);
});

test("renderExplainButton emits an accessible trigger tied to the popover id", async () => {
  const { renderExplainButton, explainPopId } = await importModule("src/ui/explain.mjs");
  const html = renderExplainButton("medianClose");
  assert.match(html, /<button type="button"/);
  assert.match(html, /class="explain-trigger"/);
  assert.match(html, /data-explain-trigger="medianClose"/);
  assert.match(html, /aria-expanded="false"/);
  assert.match(html, /aria-haspopup="dialog"/);
  assert.match(html, new RegExp(`aria-controls="${explainPopId("medianClose")}"`));
  assert.match(html, /what is this\?/);
  assert.equal(renderExplainButton(""), "");
  assert.equal(renderExplainButton(null), "");
});

test("explainPopId sanitizes ids so markup stays valid", async () => {
  const { explainPopId, renderExplainButton } = await importModule("src/ui/explain.mjs");
  assert.equal(explainPopId("medianClose"), "explain-pop-medianClose");
  assert.equal(explainPopId('a"b<c> d'), "explain-pop-a-b-c--d");
  const html = renderExplainButton('a"b');
  assert.match(html, /aria-controls="explain-pop-a-b"/);
  assert.doesNotMatch(html, /aria-controls="[^"]*"[^ ]*b"/);
});

test("renderExplainPopover renders definition, formula, source, cadence, caveats", async () => {
  const { renderExplainPopover, explainPopId } = await importModule("src/ui/explain.mjs");
  const html = renderExplainPopover(SAMPLE_METRIC);
  assert.match(html, /role="dialog"/);
  assert.match(html, /tabindex="-1"/);
  assert.match(html, new RegExp(`id="${explainPopId("overAskShare")}"`));
  assert.match(html, /aria-label="About Over-ask share"/);
  assert.match(html, /data-explain-close/);
  assert.match(html, /How often winners paid more than the asking price\./);
  assert.match(html, /How it is computed/);
  assert.match(html, /sold above list/);
  assert.match(html, /Where the data comes from/);
  assert.match(html, /Manual MLS exports matched to KC assessor sales\./);
  assert.match(html, /usually weekly/);
  assert.match(html, /Keep in mind/);
  const caveatCount = (html.match(/<li>/g) || []).length;
  assert.equal(caveatCount, 2);
});

test("renderExplainPopover weaves in runtime context and skips empty sections", async () => {
  const { renderExplainPopover } = await importModule("src/ui/explain.mjs");
  const withContext = renderExplainPopover(SAMPLE_METRIC, {
    note: "612 of 2,179 comps carry a real list price.",
    freshness: "Newest recorded sale: 2026-06-28.",
  });
  assert.match(withContext, /class="explain-live"/);
  assert.match(withContext, /612 of 2,179 comps carry a real list price\./);
  assert.match(withContext, /Newest recorded sale: 2026-06-28\./);

  const sparse = renderExplainPopover({ id: "x", label: "Sparse", definition: "Just words." });
  assert.doesNotMatch(sparse, /How it is computed/);
  assert.doesNotMatch(sparse, /Where the data comes from/);
  assert.doesNotMatch(sparse, /Keep in mind/);
  assert.doesNotMatch(sparse, /<ul/);

  assert.equal(renderExplainPopover(null), "");
  assert.equal(renderExplainPopover(undefined), "");
});

test("renderExplainPopover escapes HTML coming from glossary text", async () => {
  const { renderExplainPopover } = await importModule("src/ui/explain.mjs");
  const html = renderExplainPopover({
    id: "evil",
    label: '<img src=x onerror="alert(1)">',
    definition: "<script>alert(1)</script>",
    caveats: ['"quoted" & <tagged>'],
  });
  assert.doesNotMatch(html, /<script>/);
  assert.doesNotMatch(html, /<img/);
  assert.match(html, /&lt;script&gt;/);
  assert.match(html, /&quot;quoted&quot; &amp; &lt;tagged&gt;/);
});

test("renderExplainPopover accepts glossary field aliases", async () => {
  const { renderExplainPopover } = await importModule("src/ui/explain.mjs");
  const html = renderExplainPopover({
    id: "alias",
    title: "Alias metric",
    plain: "Alias definition.",
    formulaInWords: "Alias formula.",
    freshness: "Alias cadence.",
    notes: ["Alias caveat."],
  });
  assert.match(html, /Alias metric/);
  assert.match(html, /Alias definition\./);
  assert.match(html, /Alias formula\./);
  assert.match(html, /Alias cadence\./);
  assert.match(html, /Alias caveat\./);
});

test("initExplainLayer is exported and refuses to run without a root or DOM", async () => {
  const { initExplainLayer } = await importModule("src/ui/explain.mjs");
  assert.equal(typeof initExplainLayer, "function");
  // Node has no document; the guard must return null instead of throwing.
  assert.equal(initExplainLayer(null), null);
  assert.equal(initExplainLayer({}, { getMetric: () => null }), null);
});
