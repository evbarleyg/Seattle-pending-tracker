/**
 * affordability.mjs
 * Framework-agnostic affordability engine for the rent-vs-buy model.
 * Mirrors the math in claude_cowork_model_v2.xlsx (Inputs + Scenario_Model + Cash_Flow_Monthly).
 *
 * Direct JS (ESM) port of the provided affordability.ts — type annotations
 * stripped, every formula preserved. Pure functions, no dependencies. Feed it
 * the config (assumptions) plus optional live balances and per-call scenario
 * overrides; get back max comfortable/stretch price, monthly carry, free cash
 * flow, warning flags, a one-line decision, and listingTier() to tag listings.
 *
 * Verified equivalence to the workbook at the default scenario
 * (965B valuation, 12-month wait, $2.0M target, $800k down, 75% brokerage draw):
 *   max comfortable $1,865,444 | max stretch $2,127,942 | carry $10,636/mo
 *   free cash flow -$2,125/mo | post-close liquidity $121,385 | decision RENT_WAIT
 */

// ----------------------------- Math -----------------------------

/** Monthly mortgage payment factor for a fully-amortizing loan. */
export function pmtFactor(annualRate, termYears) {
  if (annualRate === 0) return 1 / (termYears * 12);
  const m = annualRate / 12;
  return m / (1 - Math.pow(1 + m, -termYears * 12));
}

export function netMortgageRate(c) {
  return c.housing.mortgageRateBeforeDiscount - c.housing.bofaRateDiscount;
}

/** All-in monthly ownership cost (P&I + property tax + insurance + HOA + maintenance). */
export function ownerCost(c, price, downPayment) {
  const pf = pmtFactor(netMortgageRate(c), c.housing.loanTermYears);
  const pi = Math.max(0, price - downPayment) * pf;
  const tax = (price * c.housing.propertyTaxAnnualRate) / 12;
  const maint = (price * c.housing.maintenanceAnnualRate) / 12;
  return pi + tax + c.housing.insuranceMonthly + c.housing.hoaMonthly + maint;
}

function afterTaxMonthlyIncome(c, live) {
  const base = live.anthropicBaseSalary ?? c.income.anthropicBaseSalary;
  const partner = live.partnerIncome ?? c.income.partnerIncome;
  const gross = base + partner + c.income.otherRecurringIncome - c.income.annualPretaxContributions;
  return (gross * (1 - c.income.effectiveTaxRate)) / 12;
}

/** Anthropic equity that is vested, sellable (post lockup) and after-tax, at a given wait month. */
export function sellableEquity(c, valuation, waitMonths) {
  const e = c.anthropicEquity;
  const vestedMonths = e.monthsSinceVestStartAtModelStart + waitMonths;
  const vestedPct = vestedMonths < e.cliffMonths ? 0 : Math.min(vestedMonths / e.vestingMonths, 1);
  const gross = (e.grantFairValue * valuation) / e.grantValuationBasis * vestedPct;
  const liqOpen = waitMonths >= e.ipoMonthFromStart + e.lockupMonths;
  return liqOpen ? gross * e.afterTaxLiquidityPct * e.sellablePctAfterLockup : 0;
}

function baseDeployable(c, live, brokerageDraw) {
  const a = c.assets;
  const bank = live.bankCash ?? a.bankCash;
  const bcash = live.brokerageCashEquiv ?? a.brokerageCashEquiv;
  const bnoncash = live.brokerageNonCash ?? a.brokerageNonCash;
  const stripe = live.stripeVestedStock ?? a.stripeVestedStock;
  return Math.max(
    0,
    bank + a.housingGift + bcash - a.reserveTarget + bnoncash * brokerageDraw + stripe * a.stripeDrawablePct
  );
}

/** Max home price whose all-in carry stays at or below `cap`, given deployable down-payment funds. */
function maxHomeForCap(c, cap, totalDeployable) {
  const pf = pmtFactor(netMortgageRate(c), c.housing.loanTermYears);
  const h = c.housing;
  const dpConstrained = totalDeployable / (h.minDownPaymentPct + h.closingCostPct);
  const fixed = h.insuranceMonthly + h.hoaMonthly;
  const denom = pf * (1 + h.closingCostPct) + h.propertyTaxAnnualRate / 12 + h.maintenanceAnnualRate / 12;
  const monthlyConstrained = (cap - fixed + pf * totalDeployable) / denom;
  return Math.max(0, Math.min(dpConstrained, monthlyConstrained));
}

// ----------------------------- Main entry -----------------------------

export function computeAffordability(config, live = {}, overrides = {}) {
  const c = config;
  const valuation = overrides.valuation ?? c.anthropicEquity.selectedValuation;
  const waitMonths = overrides.waitMonths ?? c.housing.purchaseMonthFromStart;
  const targetPrice = overrides.targetPrice ?? c.housing.targetPrice;
  const downPayment = overrides.downPayment ?? c.housing.targetDownPayment;
  const brokerageDraw = overrides.brokerageDrawablePct ?? c.assets.brokerageDrawablePct;

  const sellable = sellableEquity(c, valuation, waitMonths);
  const anthToDP = sellable * c.anthropicEquity.proceedsDeployedToHomePct;
  const baseDep = baseDeployable(c, live, brokerageDraw);
  const totalDep = baseDep + anthToDP;
  const dpCapacity = baseDep + sellable;
  const liqOpen = waitMonths >= c.anthropicEquity.ipoMonthFromStart + c.anthropicEquity.lockupMonths;

  const maxComfort = maxHomeForCap(c, c.housing.comfortCapMonthly, totalDep);
  const maxStretch = maxHomeForCap(c, c.housing.stretchCapMonthly, totalDep);

  const carry = ownerCost(c, targetPrice, downPayment);
  const income = afterTaxMonthlyIncome(c, live);
  const infl = Math.pow(1 + c.expenses.annualExpenseInflation, waitMonths / 12);
  const nonHousing = c.expenses.baselineNonHousingMonthly * infl;
  const fcf = income - (nonHousing + carry);

  // Post-close liquidity: exact month-by-month roll matching the workbook's Cash_Flow P column.
  // Gross deployable cash (no reserve subtraction) grows at cash yield; rent FCF accrues until purchase;
  // at the purchase month, equity proceeds are added and (down payment + closing) are subtracted.
  const a = c.assets;
  const grossCash =
    (live.bankCash ?? a.bankCash) +
    a.housingGift +
    (live.brokerageCashEquiv ?? a.brokerageCashEquiv) +
    (live.brokerageNonCash ?? a.brokerageNonCash) * brokerageDraw +
    (live.stripeVestedStock ?? a.stripeVestedStock) * a.stripeDrawablePct;
  const closing = targetPrice * c.housing.closingCostPct;
  const monthCarry = (m) =>
    m < waitMonths
      ? (c.expenses.luxuryRentMonthly + c.expenses.storageMonthly) *
        Math.pow(1 + c.expenses.annualExpenseInflation, m / 12)
      : carry;
  const monthFcf = (m) =>
    income - c.expenses.baselineNonHousingMonthly * Math.pow(1 + c.expenses.annualExpenseInflation, m / 12) - monthCarry(m);
  let liq = 0;
  for (let m = 0; m <= waitMonths; m++) {
    const inflow = m === waitMonths ? anthToDP : 0;
    const outflow = m === waitMonths ? downPayment + closing : 0;
    liq = m === 0 ? grossCash + monthFcf(0) + inflow - outflow : liq * (1 + c.growth.cashYield / 12) + monthFcf(m) + inflow - outflow;
  }
  const postCloseLiquidity = liq;

  // ----------------------------- Flags -----------------------------
  const flags = [
    {
      id: "reserve",
      triggered: postCloseLiquidity < c.assets.reserveTarget,
      message: `Post-close liquidity below ${fmt(c.assets.reserveTarget)} reserve target`,
    },
    { id: "fcf", triggered: fcf < 0, message: "Monthly free cash flow is negative at this target" },
    {
      id: "carry",
      triggered: carry > c.housing.comfortCapMonthly,
      message:
        carry > c.housing.stretchCapMonthly
          ? "Owner cost above STRETCH cap"
          : "Owner cost above comfort cap (within stretch)",
    },
    {
      id: "equityBeforeLiquidity",
      triggered: downPayment > baseDep && !liqOpen,
      message: "Down payment relies on equity that is not yet sellable",
    },
    {
      id: "dpExceedsAssets",
      triggered: downPayment > dpCapacity,
      message: "Down payment exceeds deployable cash + sellable equity",
    },
  ];

  // ----------------------------- Decision -----------------------------
  const h = c.housing;
  let decision;
  if (carry > h.stretchCapMonthly || fcf < 0) decision = "RENT_WAIT";
  else if (downPayment > dpCapacity) decision = "RENT_WAIT";
  else if (downPayment > baseDep && !liqOpen) decision = "WAIT_FOR_LIQUIDITY";
  else if (downPayment <= baseDep && carry <= h.comfortCapMonthly && postCloseLiquidity >= c.assets.reserveTarget)
    decision = "BUY_NOW_WITHIN_CAP";
  else if (carry <= h.comfortCapMonthly) decision = "BUY_WITHIN_COMFORT";
  else decision = "BUY_ONLY_IF_EXCEPTIONAL";

  const labels = {
    RENT_WAIT: "Rent / wait",
    WAIT_FOR_LIQUIDITY: "Wait for liquidity, then buy",
    BUY_NOW_WITHIN_CAP: "Buy now within cap",
    BUY_WITHIN_COMFORT: "Buy within comfort (verify reserve)",
    BUY_ONLY_IF_EXCEPTIONAL: "Buy only if exceptional home",
  };

  const rationale =
    `Owner cost ${fmt(carry)}/mo vs comfort ${fmt(h.comfortCapMonthly)}; ` +
    `post-close liquidity ${fmt(postCloseLiquidity)} vs reserve ${fmt(c.assets.reserveTarget)}; ` +
    `liquidity event ${liqOpen ? "OPEN" : "NOT open"} by month ${waitMonths}.`;

  return {
    maxComfortablePrice: maxComfort,
    maxStretchPrice: maxStretch,
    ownerCostMonthly: carry,
    monthlyFreeCashFlow: fcf,
    afterTaxMonthlyIncome: income,
    baseDeployableCash: baseDep,
    anthropicToDownPayment: anthToDP,
    totalDeployable: totalDep,
    dpFundingCapacity: dpCapacity,
    liquidityEventOpen: liqOpen,
    postCloseLiquidity,
    decision,
    decisionLabel: labels[decision],
    rationale,
    flags,
    inputs: { valuation, waitMonths, targetPrice, downPayment },
  };
}

// ----------------------------- Helpers for the UI -----------------------------

/** Tag a listing price against the current scenario's comfortable/stretch ceilings. */
export function listingTier(result, price) {
  if (!result || !Number.isFinite(price) || price <= 0) return null;
  if (price <= result.maxComfortablePrice) return "in_budget";
  if (price <= result.maxStretchPrice) return "stretch";
  return "over";
}

/** Build the valuation x wait grid of max comfortable prices (for a heatmap / "when can we move up"). */
export function affordabilityGrid(config, live, valuationsB, waitMonths) {
  const out = [];
  for (const vB of valuationsB) {
    for (const w of waitMonths) {
      const r = computeAffordability(config, live, { valuation: vB * 1e9, waitMonths: w });
      out.push({ valuationB: vB, waitMonths: w, maxComfortable: r.maxComfortablePrice, maxStretch: r.maxStretchPrice });
    }
  }
  return out;
}

function fmt(n) {
  return "$" + Math.round(n).toLocaleString("en-US");
}
