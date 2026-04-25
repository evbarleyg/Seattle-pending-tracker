"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  extractPendingContext,
  toIsoDate,
  writeOutputs,
} = require("../scripts/export_2026_pending_context.js");

test("toIsoDate handles Realtor timestamp exports", () => {
  assert.equal(toIsoDate("1/9/2026 12:00:00 AM"), "2026-01-09");
  assert.equal(toIsoDate("2026-03-20"), "2026-03-20");
  assert.equal(toIsoDate(""), "");
});

test("extractPendingContext exports 2026 sold rows and a missing-context queue", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pending-context-"));
  const sourceDir = path.join(dir, "realtor_exports");
  fs.mkdirSync(sourceDir, { recursive: true });
  fs.writeFileSync(path.join(sourceDir, "NW Seattle Sale Stats.csv"), [
    "Sold And Stats",
    "Listing Number,Street Number,Street Direction,Street Name,Street Suffix,Street Post Direction,Unit,City,State,Zip Code,APN,Bathrooms,Bedrooms,Lot Square Footage ,Listing Date,Listing Number,Listing Price,Pending Date,Selling Price,Square Footage,Contractual Date,Status,Style Code,Year Built,Subdivision,CDOM,Selling Date,DOM,Original Price",
    "2500001,123,NW,Market,St,,A,Seattle,WA,98107,2767601234,2,3,1200,1/2/2026 12:00:00 AM,2500001,1000000,1/12/2026 12:00:00 AM,1100000,1800,1/12/2026 12:00:00 AM,Sold,18 - 2 Stories,1920,Ballard,10,2/10/2026 12:00:00 AM,9,990000",
    "2500002,456,NW,65th,St,,,Seattle,WA,98117,2767605678,1.75,2,900,1/3/2026 12:00:00 AM,2500002,950000,,960000,1300,1/18/2026 12:00:00 AM,Sold,16 - 1 Story,1942,Crown Hill,12,2/11/2026 12:00:00 AM,11,970000",
    "2400001,789,NW,70th,St,,,Seattle,WA,98117,2767609999,1,2,900,1/3/2025 12:00:00 AM,2400001,800000,1/18/2025 12:00:00 AM,840000,1300,1/18/2025 12:00:00 AM,Sold,16 - 1 Story,1942,Crown Hill,12,2/11/2025 12:00:00 AM,11,820000",
  ].join("\n"));

  const result = extractPendingContext({ sourceDir, year: "2026" });
  assert.equal(result.report.soldRows, 2);
  assert.equal(result.report.rowsWithPendingContext, 1);
  assert.equal(result.report.rowsMissingPendingContext, 1);
  assert.equal(result.rows[0].region, "NW Seattle");
  assert.match(result.rows[0].kingCountyUrl, /blue\.kingcounty\.com/);
  assert.equal(result.missingRows[0].missingReasons, "missing_pending_date");

  const outputs = writeOutputs(result, {
    year: "2026",
    outFile: path.join(dir, "context.csv"),
    missingOutFile: path.join(dir, "missing.csv"),
    reportFile: path.join(dir, "report.json"),
  });
  assert.match(fs.readFileSync(outputs.outFile, "utf8"), /hasPendingContext/);
  assert.match(fs.readFileSync(outputs.missingOutFile, "utf8"), /missing_pending_date/);
  assert.equal(JSON.parse(fs.readFileSync(outputs.reportFile, "utf8")).soldRows, 2);
});
