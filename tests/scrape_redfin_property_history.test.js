"use strict";

const { test } = require("node:test");
const assert = require("node:assert");

const { isWafChallenge } = require("../scripts/scrape_redfin_property_history.js");

test("isWafChallenge: the tiny awswaf interstitial is a challenge, a real property page is not", () => {
  const challenge = "<!DOCTYPE HTML><html><head><script>window.awswaf = {}; /* challenge */</script></head><body></body></html>";
  assert.strictEqual(isWafChallenge(challenge), true);

  const realPage = `<html><body>${"x".repeat(30000)}<div id="propertyHistoryTabPanels">Sold | $1,070,000 | NWMLS #2400001</div><script>awswaf telemetry</script></body></html>`;
  assert.strictEqual(isWafChallenge(realPage), false, "a large page that carries the history strip is real even if it mentions awswaf");

  const smallReal = "<html><body><div id=\"propertyHistoryTabPanels\">Listed | $1,100,000</div></body></html>";
  assert.strictEqual(isWafChallenge(smallReal), false, "the history anchor always wins");

  assert.strictEqual(isWafChallenge(""), false);
  assert.strictEqual(isWafChallenge(null), false);
});
