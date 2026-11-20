"use strict";

// Drives scripts/refresh_actives_daily.sh against a scratch project dir with a
// fake npm that records which steps ran. The fake build fails, so the script
// never reaches its git commit/push block.

const { test } = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const SCRIPT = path.join(__dirname, "..", "scripts", "refresh_actives_daily.sh");

function runDaily(fetchExit) {
  // Without these overrides the script runs the real pipeline in the main
  // checkout, which commits and pushes. Refuse to spawn it in that case.
  const source = fs.readFileSync(SCRIPT, "utf8");
  assert.match(source, /PROJECT_DIR="\$\{REFRESH_PROJECT_DIR:-/, "script must honor REFRESH_PROJECT_DIR before this test runs it");
  assert.match(source, /NPM="\$\{REFRESH_NPM:-/, "script must honor REFRESH_NPM before this test runs it");
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "refresh-daily-"));
  const calls = path.join(dir, "calls.txt");
  const fakeNpm = path.join(dir, "fake-npm");
  fs.writeFileSync(fakeNpm, [
    "#!/bin/bash",
    "echo \"$2\" >> \"$FAKE_NPM_CALLS\"",
    "case \"$2\" in",
    "  fetch:actives) exit \"$FAKE_FETCH_EXIT\" ;;",
    "  build) exit 1 ;;",
    "esac",
    "exit 0",
    "",
  ].join("\n"), { mode: 0o755 });
  const result = spawnSync("bash", [SCRIPT], {
    encoding: "utf8",
    env: {
      ...process.env,
      REFRESH_PROJECT_DIR: dir,
      REFRESH_NPM: fakeNpm,
      ACTIVES_RETRY_WAIT_S: "0",
      FAKE_NPM_CALLS: calls,
      FAKE_FETCH_EXIT: String(fetchExit),
    },
  });
  return {
    result,
    steps: fs.readFileSync(calls, "utf8").trim().split("\n"),
    log: fs.readFileSync(path.join(dir, "tmp", "refresh_actives_daily.log"), "utf8"),
  };
}

test("daily refresh: a rejected actives fetch is retried, then the actives steps are skipped", () => {
  const { result, steps, log } = runDaily(2);
  assert.strictEqual(result.status, 0, log);
  assert.strictEqual(steps.filter((s) => s === "fetch:actives").length, 3, "three fetch attempts");
  for (const skipped of ["merge:actives", "ledger:upsert", "detect:transitions"]) {
    assert.ok(!steps.includes(skipped), `${skipped} must not run after a failed fetch`);
  }
  for (const kept of ["refresh:sold-daily", "enrichment:apply", "sync:public", "build"]) {
    assert.ok(steps.includes(kept), `${kept} still runs`);
  }
  assert.match(log, /fetch:actives FAILED after 3 attempts/);
});

test("daily refresh: a good actives fetch runs every step once", () => {
  const { result, steps, log } = runDaily(0);
  assert.strictEqual(result.status, 0, log);
  assert.deepStrictEqual(steps.slice(0, 3), ["fetch:actives", "merge:actives", "ledger:upsert"]);
  assert.ok(steps.includes("detect:transitions"));
  assert.doesNotMatch(log, /FAILED after/);
});
