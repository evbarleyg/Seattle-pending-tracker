"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { buildSources } = require("../scripts/stamp_report_sources.js");

test("buildSources writes the actives fetch time the app reads, plus the optional feeds", () => {
  const sources = buildSources({
    activesReport: { fetchedAt: "2026-09-19T13:39:44.613Z", totalRows: 1468 },
    soldReport: { fetchedAt: "2026-09-19T20:30:00.000Z", totalRows: 378, soldWithinDays: 30, soldDateRange: { min: "2026-08-20", max: "2026-09-18" } },
    soldCumulative: { rows: 5116, max: "2026-09-18" },
    activeLedger: { rows: 3824, max: "2026-09-19" },
    enrichmentLedger: { rows: 12192, max: "2026-09-18" },
    enrichmentReport: { generatedAt: "2026-09-19T21:00:00.000Z", apply: { applied: 1700 } },
    now: "2026-09-19T21:30:00.000Z",
  });
  assert.strictEqual(sources.redfinActives.fetchedAt, "2026-09-19T13:39:44.613Z");
  assert.strictEqual(sources.redfinActives.rows, 1468);
  assert.deepStrictEqual(sources.redfinSold, { fetchedAt: "2026-09-19T20:30:00.000Z", rowsFetched: 378, windowDays: 30, newestSaleDate: "2026-09-18", cumulativeRows: 5116, cumulativeNewestSaleDate: "2026-09-18" });
  assert.deepStrictEqual(sources.activeLedger, { rows: 3824, lastSeen: "2026-09-19" });
  assert.deepStrictEqual(sources.soldEnrichmentLedger, { rows: 12192, newestSaleDate: "2026-09-18", appliedAt: "2026-09-19T21:00:00.000Z", appliedRows: 1700 });
  assert.strictEqual(sources.stampedAt, "2026-09-19T21:30:00.000Z");
});

test("buildSources leaves out feeds it has no report for, and never throws on missing inputs", () => {
  const sources = buildSources({ activesReport: null, soldReport: null, soldCumulative: null, activeLedger: null, enrichmentLedger: null, enrichmentReport: null, now: "2026-09-19T21:30:00.000Z" });
  assert.deepStrictEqual(Object.keys(sources), ["stampedAt"]);
  const partial = buildSources({ activesReport: { fetchedAt: "2026-09-19T13:39:44.613Z" }, now: "x" });
  assert.strictEqual(partial.redfinActives.rows, 0);
  assert.strictEqual(partial.redfinSold, undefined);
});
