"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { joinStreetAndUnit, homeToRow } = require("../scripts/fetch_redfin_actives.js");

test("joinStreetAndUnit does not double a unit the street line already carries", () => {
  assert.strictEqual(joinStreetAndUnit("2727 Fairview Ave E #4", "#4"), "2727 Fairview Ave E #4");
  assert.strictEqual(joinStreetAndUnit("9057 Greenwood Ave N Unit 306", "#306"), "9057 Greenwood Ave N Unit 306");
  assert.strictEqual(joinStreetAndUnit("708 N 102nd St #2", "2"), "708 N 102nd St #2");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW Unit A", "Unit A"), "4032 53rd Ave SW Unit A", "the whole unit phrase already present");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW Unit A", "A"), "4032 53rd Ave SW Unit A");
  assert.strictEqual(joinStreetAndUnit("1080 W Ewing Pl Unit 0-P", "Unit 0-P"), "1080 W Ewing Pl Unit 0-P");
  assert.strictEqual(joinStreetAndUnit("4032 53rd Ave SW", "Unit A"), "4032 53rd Ave SW Unit A", "a missing unit phrase is appended once");
  assert.strictEqual(joinStreetAndUnit("2727 Fairview Ave E", "#4"), "2727 Fairview Ave E #4", "a missing unit is still appended");
  assert.strictEqual(joinStreetAndUnit("100 4th Ave", "#4"), "100 4th Ave #4", "a street word that happens to match the unit is not the unit");
  assert.strictEqual(joinStreetAndUnit("1 Main St", ""), "1 Main St");
  assert.strictEqual(joinStreetAndUnit("", "#4"), "#4");
});

test("homeToRow builds the address through joinStreetAndUnit", () => {
  const home = { streetLine: { value: "2727 Fairview Ave E #4" }, unitNumber: { value: "#4" }, propertyId: 1, listingId: 2, mlsId: { value: "2517214" }, mlsStatus: "Active", price: { value: 999000 } };
  const row = homeToRow(home, { label: "Eastlake" }, "2026-09-19T13:39:44.613Z");
  assert.strictEqual(row.address, "2727 Fairview Ave E #4");
  assert.strictEqual(row.unitNumber, "#4");
  assert.strictEqual(row.mlsListingNumber, "2517214");
});

// ---------------------------------------------------------------------------
// Fetch health gate. A fetch that comes back empty, or far below the last good
// fetch, is a network/Redfin failure. Writing it lets merge:actives strip every
// REDFIN_ACTIVE row (2026-11-20: all 26 queries "fetch failed", 0 rows pushed).

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { assessFetchHealth, lastGoodSnapshotRowCount } = require("../scripts/fetch_redfin_actives.js");

const FETCH_SCRIPT = path.join(__dirname, "..", "scripts", "fetch_redfin_actives.js");
const okStat = (n) => ({ label: `Q${n}`, homesReturned: 10 });
const errStat = (n) => ({ label: `Q${n}`, error: "fetch failed" });

test("assessFetchHealth rejects a fetch where every query errored", () => {
  const verdict = assessFetchHealth({ queryStats: [errStat(1), errStat(2)], rowCount: 0, baselineRowCount: 1211 });
  assert.strictEqual(verdict.ok, false);
  assert.match(verdict.reason, /all 2 queries failed/);
});

test("assessFetchHealth rejects zero rows even when the queries answered", () => {
  const verdict = assessFetchHealth({ queryStats: [okStat(1)], rowCount: 0, baselineRowCount: 0 });
  assert.strictEqual(verdict.ok, false);
  assert.match(verdict.reason, /0 rows/);
});

test("assessFetchHealth rejects a count far below the last good fetch (2026-09-27: 152 vs 1462)", () => {
  const verdict = assessFetchHealth({ queryStats: [errStat(1), okStat(2)], rowCount: 152, baselineRowCount: 1462 });
  assert.strictEqual(verdict.ok, false);
  assert.match(verdict.reason, /152 rows/);
  assert.match(verdict.reason, /1462/);
});

test("assessFetchHealth accepts normal day-to-day movement and a first run with no baseline", () => {
  assert.strictEqual(assessFetchHealth({ queryStats: [okStat(1)], rowCount: 1198, baselineRowCount: 1211 }).ok, true);
  assert.strictEqual(assessFetchHealth({ queryStats: [okStat(1), errStat(2)], rowCount: 700, baselineRowCount: 1211 }).ok, true, "a single failed query above the floor is kept");
  assert.strictEqual(assessFetchHealth({ queryStats: [okStat(1)], rowCount: 40, baselineRowCount: 0 }).ok, true);
  assert.strictEqual(assessFetchHealth({ queryStats: [okStat(1)], rowCount: 700, baselineRowCount: 1211, minRatio: 0.8 }).ok, false, "minRatio is configurable");
});

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "fetch-actives-"));
}

function writeSnapshot(dir, stamp, rowCount) {
  fs.mkdirSync(dir, { recursive: true });
  const listingIds = Array.from({ length: rowCount }, (_, i) => String(i + 1));
  fs.writeFileSync(path.join(dir, `${stamp}.json`), `${JSON.stringify({ fetchedAt: stamp, listingIds, rowCount })}\n`);
}

test("lastGoodSnapshotRowCount skips empty and unreadable snapshots", () => {
  const dir = path.join(tmpDir(), "snaps");
  assert.strictEqual(lastGoodSnapshotRowCount(dir), 0, "missing dir");
  writeSnapshot(dir, "2026-11-19T13-30-00-000Z", 1211);
  writeSnapshot(dir, "2026-11-20T13-34-05-107Z", 0);
  fs.writeFileSync(path.join(dir, "2026-11-21T13-30-00-000Z.json"), "{not json");
  assert.strictEqual(lastGoodSnapshotRowCount(dir), 1211);
});

// Runs the real CLI with global fetch replaced by a preload, so no network.
function runFetchCli({ mode, homeCount = 0, queryCount = 2, snapshots = [], extraArgs = [] }) {
  const dir = tmpDir();
  const paths = {
    config: path.join(dir, "searches.json"),
    out: path.join(dir, "actives.csv"),
    report: path.join(dir, "report.json"),
    urlIndex: path.join(dir, "url_index.json"),
    snapshotDir: path.join(dir, "snaps"),
    stub: path.join(dir, "fetch_stub.js"),
  };
  fs.writeFileSync(paths.config, JSON.stringify({
    throttleMs: 0,
    emptyRetries: 0,
    retryBackoffMs: 0,
    queries: Array.from({ length: queryCount }, (_, i) => ({ label: `Q${i + 1}`, regionId: i + 1, regionType: 6 })),
  }));
  fs.writeFileSync(paths.stub, mode === "fail"
    ? "globalThis.fetch = async () => { throw new TypeError(\"fetch failed\"); };\n"
    : `const homes = Array.from({ length: ${homeCount} }, (_, i) => ({ listingId: i + 1, propertyId: i + 1, mlsStatus: "Active", streetLine: { value: (i + 1) + " Main St" }, zip: "98107", price: { value: 900000 }, uiPropertyType: 1 }));
globalThis.fetch = async () => ({ ok: true, status: 200, text: async () => "{}&&" + JSON.stringify({ resultCode: 0, payload: { homes } }) });\n`);
  // Yesterday's good outputs, which a rejected fetch must leave alone.
  fs.writeFileSync(paths.out, "previous,good,csv\n");
  fs.writeFileSync(paths.report, "{\"previous\":true}\n");
  for (const [stamp, rows] of snapshots) writeSnapshot(paths.snapshotDir, stamp, rows);
  const snapshotsBefore = fs.existsSync(paths.snapshotDir) ? fs.readdirSync(paths.snapshotDir).sort() : [];
  const result = spawnSync(process.execPath, [
    "--require", paths.stub, FETCH_SCRIPT,
    "--config", paths.config, "--out", paths.out, "--report", paths.report,
    "--url-index", paths.urlIndex, "--snapshot-dir", paths.snapshotDir,
    ...extraArgs,
  ], { encoding: "utf8" });
  return { result, paths, snapshotsBefore };
}

function assertOutputsUntouched({ paths, snapshotsBefore }) {
  assert.strictEqual(fs.readFileSync(paths.out, "utf8"), "previous,good,csv\n", "actives CSV kept");
  assert.strictEqual(fs.readFileSync(paths.report, "utf8"), "{\"previous\":true}\n", "fetch report kept");
  assert.strictEqual(fs.existsSync(paths.urlIndex), false, "URL index not written");
  const after = fs.existsSync(paths.snapshotDir) ? fs.readdirSync(paths.snapshotDir).sort() : [];
  assert.deepStrictEqual(after, snapshotsBefore, "no new snapshot");
}

test("CLI: a fetch where every query fails exits 2 and writes nothing", () => {
  const run = runFetchCli({ mode: "fail", snapshots: [["2026-11-19T13-30-00-000Z", 1211]] });
  assert.strictEqual(run.result.status, 2, run.result.stderr);
  assert.match(run.result.stderr, /REJECTED/);
  assertOutputsUntouched(run);
});

test("CLI: stops after the first 3 queries fail with nothing fetched", () => {
  const run = runFetchCli({ mode: "fail", queryCount: 5 });
  assert.strictEqual(run.result.status, 2, run.result.stderr);
  assert.match(run.result.stderr, /\[3\/5\] Q3: ERROR fetch failed/);
  assert.doesNotMatch(run.result.stderr, /\[4\/5\]/);
  assertOutputsUntouched(run);
});

test("CLI: a count below half the last good snapshot is rejected; an empty snapshot is not the baseline", () => {
  const run = runFetchCli({
    mode: "ok",
    homeCount: 100,
    snapshots: [["2026-11-19T13-30-00-000Z", 1000], ["2026-11-20T13-34-05-107Z", 0]],
  });
  assert.strictEqual(run.result.status, 2, run.result.stderr);
  assert.match(run.result.stderr, /100 rows/);
  assertOutputsUntouched(run);
});

test("CLI: --force writes a low-count fetch anyway", () => {
  const run = runFetchCli({ mode: "ok", homeCount: 100, snapshots: [["2026-11-19T13-30-00-000Z", 1000]], extraArgs: ["--force"] });
  assert.strictEqual(run.result.status, 0, run.result.stderr);
  assert.strictEqual(fs.readFileSync(run.paths.out, "utf8").trim().split("\n").length, 101);
});

test("CLI: a healthy fetch writes the CSV, report, URL index, and a snapshot", () => {
  const run = runFetchCli({ mode: "ok", homeCount: 10, snapshots: [["2026-11-19T13-30-00-000Z", 10]] });
  assert.strictEqual(run.result.status, 0, run.result.stderr);
  assert.strictEqual(fs.readFileSync(run.paths.out, "utf8").trim().split("\n").length, 11);
  assert.strictEqual(JSON.parse(fs.readFileSync(run.paths.report, "utf8")).totalRows, 10);
  assert.strictEqual(fs.existsSync(run.paths.urlIndex), true);
  assert.strictEqual(fs.readdirSync(run.paths.snapshotDir).length, 2);
});
