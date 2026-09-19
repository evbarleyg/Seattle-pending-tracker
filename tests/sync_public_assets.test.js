"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { slimListingLedger, LISTING_LEDGER_COLUMNS } = require("../scripts/sync_public_assets.js");

test("slimListingLedger keeps the app's 12 columns in order and drops url and the Redfin ids", () => {
  const text = [
    "mlsNumber,redfinPropertyId,redfinListingId,url,address,zip,propertyType,firstSeen,lastSeen,lastSeenActive,firstAsk,lastAsk,lastActiveAsk,listDate,lastDom,lastCdom,lastStatus",
    "2400001,288413,223584238,https://r/x,\"10037 15th Ave NW, Unit 2\",98177,Single Family,2026-07-17,2026-07-30,2026-07-30,1150000,1100000,1100000,2026-07-17,13,13,Active",
    ",,,,No MLS St,98103,Single Family,2026-07-17,2026-07-20,2026-07-20,900000,900000,900000,,,,Active",
  ].join("\n");
  const { text: out, rows } = slimListingLedger(text);
  const lines = out.trim().split("\n");
  assert.strictEqual(lines[0], LISTING_LEDGER_COLUMNS.join(","));
  assert.deepStrictEqual(LISTING_LEDGER_COLUMNS, ["mlsNumber", "address", "zip", "propertyType", "firstSeen", "lastSeen", "lastSeenActive", "firstAsk", "lastAsk", "listDate", "lastDom", "lastStatus"]);
  assert.strictEqual(rows, 1, "rows without an MLS# are dropped");
  assert.strictEqual(lines[1], "2400001,\"10037 15th Ave NW, Unit 2\",98177,Single Family,2026-07-17,2026-07-30,2026-07-30,1150000,1100000,2026-07-17,13,Active");
  assert.ok(!out.includes("https://r/x") && !out.includes("288413"), "url and ids must not be published");
});

test("slimListingLedger tolerates a missing column and an empty ledger", () => {
  const { text, rows } = slimListingLedger("mlsNumber,address\n2400001,1 Main St\n");
  assert.strictEqual(rows, 1);
  assert.strictEqual(text.trim().split("\n")[1], "2400001,1 Main St,,,,,,,,,,");
  const empty = slimListingLedger("");
  assert.strictEqual(empty.rows, 0);
  assert.strictEqual(empty.text.trim(), LISTING_LEDGER_COLUMNS.join(","));
});
