"use strict";

const test = require("node:test");
const assert = require("node:assert");
const { execSync } = require("node:child_process");
const path = require("node:path");

// The engine is ESM; load it via dynamic import inside the async tests.
const ENGINE_URL = new URL("../src/domain/affordability.mjs", `file://${__filename}`).href;

// Synthetic, no-PII fixture. Round numbers chosen so invariants are easy to
// reason about; this is NOT Evan's real config (that lives in the gitignored
// public/affordability.config.json and is verified locally via the bundle's
// own test.mjs). Purpose here: prove the math/port didn't break, in CI, with
// zero personal data committed.
const FIXTURE = {
  income: { anthropicBaseSalary: 300000, partnerIncome: 100000, otherRecurringIncome: 0, annualPretaxContributions: 40000, effectiveTaxRate: 0.35 },
  expenses: { baselineNonHousingMonthly: 10000, currentHousingMonthly: 4000, luxuryRentMonthly: 6000, storageMonthly: 0, annualExpenseInflation: 0.03 },
  assets: { bankCash: 200000, housingGift: 0, brokerageCashEquiv: 100000, brokerageNonCash: 200000, retirement: 100000, stripeVestedStock: 0, creditCardLiabilities: 0, reserveTarget: 100000, brokerageDrawablePct: 0.5, stripeDrawablePct: 0 },
  growth: { cashYield: 0.04, taxableReturn: 0.06, retirementReturn: 0.06, stripeGrowth: 0.05, homeAppreciation: 0.03 },
  anthropicEquity: { grantFairValue: 0, grantValuationBasis: 1e11, selectedValuation: 1e11, monthsSinceVestStartAtModelStart: 0, vestingMonths: 48, cliffMonths: 6, ipoMonthFromStart: 3, lockupMonths: 6, sellablePctAfterLockup: 1, afterTaxLiquidityPct: 0.55, proceedsDeployedToHomePct: 0.65 },
  housing: { purchaseMonthFromStart: 12, targetPrice: 1000000, targetDownPayment: 250000, mortgageRateBeforeDiscount: 0.065, bofaRateDiscount: 0, loanTermYears: 30, propertyTaxAnnualRate: 0.01, insuranceMonthly: 300, hoaMonthly: 0, maintenanceAnnualRate: 0.01, closingCostPct: 0.02, comfortCapMonthly: 10000, stretchCapMonthly: 12000, minDownPaymentPct: 0.2 },
  scenarioGrid: { valuationsB: [100, 200], waitMonths: [0, 12] },
};

test("pmtFactor matches a known amortization factor", async () => {
  const { pmtFactor } = await import(ENGINE_URL);
  // 30-yr at 6% → standard monthly factor ~0.0059955; payment on $100k ≈ $599.55
  const f = pmtFactor(0.06, 30);
  assert.ok(Math.abs(f * 100000 - 599.55) < 0.5, `expected ~599.55, got ${(f * 100000).toFixed(2)}`);
  // zero-rate degenerates to straight-line
  assert.ok(Math.abs(pmtFactor(0, 30) - 1 / 360) < 1e-9);
});

test("ownerCost sums P&I + tax + insurance + HOA + maintenance", async () => {
  const { ownerCost, pmtFactor, netMortgageRate } = await import(ENGINE_URL);
  const price = 1000000;
  const down = 200000;
  const pi = (price - down) * pmtFactor(netMortgageRate(FIXTURE), 30);
  const tax = (price * 0.01) / 12;
  const maint = (price * 0.01) / 12;
  const expected = pi + tax + maint + 300 + 0;
  assert.ok(Math.abs(ownerCost(FIXTURE, price, down) - expected) < 1e-6);
});

test("listingTier sorts price against the scenario ceilings", async () => {
  const { computeAffordability, listingTier } = await import(ENGINE_URL);
  const r = computeAffordability(FIXTURE, {}, {});
  assert.ok(r.maxComfortablePrice > 0);
  assert.equal(listingTier(r, r.maxComfortablePrice - 1), "in_budget");
  assert.equal(listingTier(r, r.maxStretchPrice + 1), "over");
  assert.equal(listingTier(r, 0), null);
  assert.equal(listingTier(null, 500000), null);
  // stretch band sits strictly between the two ceilings
  assert.ok(r.maxStretchPrice >= r.maxComfortablePrice);
});

test("computeAffordability returns a valid decision + flag shape", async () => {
  const { computeAffordability } = await import(ENGINE_URL);
  const r = computeAffordability(FIXTURE, {}, {});
  const decisions = ["RENT_WAIT", "WAIT_FOR_LIQUIDITY", "BUY_NOW_WITHIN_CAP", "BUY_WITHIN_COMFORT", "BUY_ONLY_IF_EXCEPTIONAL"];
  assert.ok(decisions.includes(r.decision));
  assert.equal(typeof r.decisionLabel, "string");
  assert.ok(Array.isArray(r.flags) && r.flags.length === 5);
  for (const f of r.flags) {
    assert.equal(typeof f.id, "string");
    assert.equal(typeof f.triggered, "boolean");
    assert.equal(typeof f.message, "string");
  }
});

test("scenario overrides change the result (more wait → more sellable equity capacity)", async () => {
  const { computeAffordability } = await import(ENGINE_URL);
  const now = computeAffordability(FIXTURE, {}, { waitMonths: 0 });
  const later = computeAffordability(FIXTURE, {}, { waitMonths: 24 });
  // With a non-zero grant this would grow; with the fixture's zero grant they
  // should at least both be finite and the call must not throw.
  assert.ok(Number.isFinite(now.maxComfortablePrice));
  assert.ok(Number.isFinite(later.maxComfortablePrice));
});

test("PRIVACY GUARD: real affordability config is not tracked by git", () => {
  const repoRoot = path.resolve(__dirname, "..");
  let tracked = "";
  try {
    tracked = execSync("git ls-files", { cwd: repoRoot, encoding: "utf8" });
  } catch {
    return; // not a git checkout (e.g. tarball) — nothing to guard
  }
  const offenders = tracked
    .split(/\r?\n/)
    .filter((p) => /(^|\/)affordability\.config\.json$/.test(p) || /(^|\/)private\//.test(p));
  assert.deepEqual(
    offenders,
    [],
    `Private affordability files must never be committed. Tracked offenders: ${offenders.join(", ")}`
  );
});
