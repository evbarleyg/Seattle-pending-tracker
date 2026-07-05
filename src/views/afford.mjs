// Afford view, extracted from src/main.mjs.
//
// Renders the affordability decision panel, scenario controls, watch-out
// flags, and the valuation-by-wait grid. All numbers come from the private
// local config (never committed); when it is absent the view explains how to
// enable it and stays inert.
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs, miniMetric, buttonIcon, refreshIcons }
//
// Intelligibility treatment (see docs/superpowers/specs/2026-07-05-answer-first-overview-design.md):
// this tab's numbers are the only ones in the app that come from a private
// config file instead of market comps, so every headline number gets a
// universe caption saying so, the decision label gets an explain trigger,
// and the raw rate/tax/insurance/HOA/maintenance/loan-term assumptions are
// spelled out in one plain-words line. Glossary ids used: affordDecision,
// affordMaxComfortable, affordMaxStretch, affordCarry, affordFreeCashFlow,
// affordPostCloseLiquidity, affordDeployable (all from src/domain/glossary.mjs,
// universeId "affordScenario" for every one of them). There is no glossary
// entry for an individual assumption (rate, tax, insurance, HOA, maintenance
// on their own) so the assumptions line uses plain inline text for those
// values and points its one explain trigger at affordCarry, the glossary
// entry whose formula and caveats are grounded in exactly this combination.
import {
  esc,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatPct,
} from "../domain/format.mjs";
import { computeAffordability, affordabilityGrid, netMortgageRate } from "../domain/affordability.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

function affordScenarioValue(key, fallback) {
  const v = ctx.state.affordability.scenario?.[key];
  return v === undefined || v === null || v === "" ? fallback : v;
}

// Local stand-in for the shared miniMetric() helper (kept in main.mjs), which
// escapes its label and so cannot carry an inline explain-trigger button.
// Same "mini-metric" markup and CSS, with an optional explain trigger.
function metricItem(label, value, explainId = "") {
  const trigger = explainId ? ` ${renderExplainButton(explainId)}` : "";
  return `<div class="mini-metric"><span>${esc(label)}${trigger}</span><strong>${esc(value)}</strong></div>`;
}

// Maps each computeAffordability() warning-flag id to the glossary entry that
// best explains the number behind it, so a triggered flag is inspectable, not
// just a scary line of text.
const FLAG_EXPLAIN_ID = {
  reserve: "affordPostCloseLiquidity",
  fcf: "affordFreeCashFlow",
  carry: "affordCarry",
  equityBeforeLiquidity: "affordDeployable",
  dpExceedsAssets: "affordDeployable",
};

// Plain-words sentence spelling out every input baked into the carry
// calculation, so "what can I afford" never hides behind an opaque number.
// Pure and exported for testing; used for the assumptions line in the view.
export function affordAssumptionsSentence(config) {
  const h = config?.housing || {};
  const netRate = netMortgageRate(config);
  const parts = [
    `a ${formatPct(netRate)} mortgage rate (${formatPct(h.mortgageRateBeforeDiscount)} base rate minus a ${formatPct(h.bofaRateDiscount)} lender discount)`,
    `${formatPct(h.propertyTaxAnnualRate)} property tax`,
    `${formatMoneyCompact(h.insuranceMonthly)} a month for insurance`,
    `${formatMoneyCompact(h.hoaMonthly)} a month for HOA dues`,
    `${formatPct(h.maintenanceAnnualRate)} of price a year set aside for maintenance`,
  ];
  const list = parts.length > 1
    ? `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`
    : parts.join("");
  return `This decision assumes ${list}, on a ${h.loanTermYears || 30}-year loan.`;
}

export function renderAffordView(deps) {
  ctx = deps;
  // miniMetric is still passed by main.mjs (see affordDeps()) but this view no
  // longer uses it: miniMetric() escapes its label and so cannot carry an
  // inline explain-trigger button, so metricItem() below replaces it locally.
  const { state, qs, buttonIcon, refreshIcons } = ctx;
  const wrap = qs("#view-afford");
  if (!wrap) return;

  if (!state.affordability.ready) {
    wrap.innerHTML = `
      <div class="view-band">
        <section class="section-head">
          <div><p class="eyebrow">Afford</p><h2>What can I actually afford?</h2></div>
        </section>
        <section class="section-block">
          <p class="note">Affordability is not configured on this device. To enable it, copy
          <code>affordability.config.sample.json</code> to <code>public/affordability.config.json</code>
          and fill in your numbers. That file is <strong>gitignored</strong>, so it stays local, is never
          committed, and is never served on the public site. Reload after adding it.</p>
          <p class="note">This is a planning model, not tax or lender advice.</p>
        </section>
      </div>
    `;
    return;
  }

  const r = state.affordability.result || computeAffordability(state.affordability.config, {}, state.affordability.scenario || {});
  const c = state.affordability.config;
  const sc = state.affordability.scenario || {};
  const waitMonths = affordScenarioValue("waitMonths", c.housing.purchaseMonthFromStart);
  const valuationB = Math.round((affordScenarioValue("valuation", c.anthropicEquity.selectedValuation)) / 1e9);
  const targetPrice = affordScenarioValue("targetPrice", c.housing.targetPrice);
  const downPayment = affordScenarioValue("downPayment", c.housing.targetDownPayment);
  const decisionTone = r.decision === "RENT_WAIT" ? "cool" : (r.decision.startsWith("BUY") ? "hot" : "warm");
  const firedFlags = r.flags.filter((f) => f.triggered);
  const grid = affordabilityGrid(c, {}, c.scenarioGrid?.valuationsB || [380, 700, 965, 1500, 2000, 3000], c.scenarioGrid?.waitMonths || [0, 6, 12, 18, 24, 36]);
  const gridWaits = c.scenarioGrid?.waitMonths || [0, 6, 12, 18, 24, 36];
  const gridVals = c.scenarioGrid?.valuationsB || [380, 700, 965, 1500, 2000, 3000];
  const gridCell = (vB, w) => {
    const cell = grid.find((g) => g.valuationB === vB && g.waitMonths === w);
    return cell ? formatMoneyCompact(cell.maxComfortable) : "—";
  };

  const assumptionsSentence = affordAssumptionsSentence(c);
  const scenarioCaption = renderUniverseCaption({
    universeLabel: "your private affordability scenario, not built from home listings or market comps",
  });

  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head">
        <div><p class="eyebrow">Afford</p><h2>What can I actually afford?</h2></div>
        <span class="note">Planning model, not tax or lender advice.</span>
      </section>

      <section class="state-panel heat-${decisionTone}" id="affordDecision">
        <div class="panel-kicker">Decision ${renderExplainButton("affordDecision")}</div>
        <h2>${esc(r.decisionLabel)}</h2>
        <p>${esc(r.rationale)}</p>
        ${scenarioCaption}
      </section>

      <section class="section-block">
        <div class="section-head compact"><div><p class="eyebrow">Assumptions</p><h3>What this decision assumes</h3></div></div>
        <p class="note">${esc(assumptionsSentence)} ${renderExplainButton("affordCarry")}</p>
        <p class="note">These come from your private config file, not from any specific listing. Change them by editing <code>public/affordability.config.json</code>.</p>
      </section>

      <div class="metric-row">
        ${metricItem("Max comfortable", formatMoneyCompact(r.maxComfortablePrice), "affordMaxComfortable")}
        ${metricItem("Max stretch", formatMoneyCompact(r.maxStretchPrice), "affordMaxStretch")}
        ${metricItem("All-in carry / mo", `${formatMoneyOrNa(r.ownerCostMonthly)} vs ${formatMoneyCompact(c.housing.comfortCapMonthly)} cap`, "affordCarry")}
        ${metricItem("Free cash flow / mo", formatMoneyOrNa(r.monthlyFreeCashFlow), "affordFreeCashFlow")}
        ${metricItem("Post-close liquidity", `${formatMoneyCompact(r.postCloseLiquidity)} vs ${formatMoneyCompact(c.assets.reserveTarget)} reserve`, "affordPostCloseLiquidity")}
        ${metricItem("Deployable for down payment", formatMoneyCompact(r.dpFundingCapacity), "affordDeployable")}
      </div>
      ${scenarioCaption}

      ${firedFlags.length ? `
      <section class="section-block">
        <div class="section-head compact"><div><p class="eyebrow">Watch-outs</p><h3>${firedFlags.length} flag${firedFlags.length === 1 ? "" : "s"} triggered</h3></div></div>
        <ul class="afford-flags">
          ${firedFlags.map((f) => `<li class="afford-flag">${esc(f.message)} ${renderExplainButton(FLAG_EXPLAIN_ID[f.id] || "")}</li>`).join("")}
        </ul>
      </section>` : `<p class="note">No flags triggered at this scenario.</p>`}

      <section class="section-block">
        <div class="section-head compact"><div><p class="eyebrow">Scenario</p><h3>Adjust assumptions</h3></div></div>
        <div class="controls-grid">
          <div class="field">
            <label for="affWaitMonths">Wait before buying (months)</label>
            <input id="affWaitMonths" type="number" min="0" max="60" step="1" value="${esc(waitMonths)}" />
          </div>
          <div class="field">
            <label for="affValuation">Anthropic valuation ($B)</label>
            <input id="affValuation" type="number" min="0" step="5" value="${esc(valuationB)}" />
          </div>
          <div class="field">
            <label for="affTargetPrice">Target home price ($)</label>
            <input id="affTargetPrice" type="number" min="0" step="25000" value="${esc(targetPrice)}" />
          </div>
          <div class="field">
            <label for="affDownPayment">Down payment ($)</label>
            <input id="affDownPayment" type="number" min="0" step="25000" value="${esc(downPayment)}" />
          </div>
          <div class="filter-actions">
            ${buttonIcon("Reset scenario", "refresh-ccw", "id=\"affResetBtn\"", "alt")}
          </div>
        </div>
        <p class="note">Carry, max prices, liquidity and listing badges all recompute live from these. Saved to this browser only.</p>
      </section>

      <section class="section-block">
        <div class="section-head compact"><div><p class="eyebrow">When can we move up</p><h3>Max comfortable price by valuation × wait ${renderExplainButton("affordMaxComfortable")}</h3></div></div>
        <div class="table-wrap">
          <table class="afford-grid">
            <thead>
              <tr><th>Valuation \\ wait</th>${gridWaits.map((w) => `<th>${w}mo</th>`).join("")}</tr>
            </thead>
            <tbody>
              ${gridVals.map((vB) => `
                <tr><th>$${vB}B</th>${gridWaits.map((w) => `<td>${gridCell(vB, w)}</td>`).join("")}</tr>
              `).join("")}
            </tbody>
          </table>
        </div>
        ${renderUniverseCaption({ universeLabel: "each cell recomputed from your config at that valuation and wait, independent of the scenario controls above" })}
        <p class="note">Each cell is the most expensive home whose all-in carry stays within your comfort cap, after the equity that is vested + sellable + after-tax at that wait.</p>
      </section>
    </div>
  `;
  refreshIcons();
}
