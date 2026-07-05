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
import {
  esc,
  formatMoneyCompact,
  formatMoneyOrNa,
} from "../domain/format.mjs";
import { computeAffordability, affordabilityGrid } from "../domain/affordability.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

function affordScenarioValue(key, fallback) {
  const v = ctx.state.affordability.scenario?.[key];
  return v === undefined || v === null || v === "" ? fallback : v;
}

export function renderAffordView(deps) {
  ctx = deps;
  const { state, qs, miniMetric, buttonIcon, refreshIcons } = ctx;
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
          and fill in your numbers. That file is <strong>gitignored</strong> — it stays local, is never
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

  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head">
        <div><p class="eyebrow">Afford</p><h2>What can I actually afford?</h2></div>
        <span class="note">Planning model — not tax or lender advice.</span>
      </section>

      <section class="state-panel heat-${decisionTone}" id="affordDecision">
        <div class="panel-kicker">Decision</div>
        <h2>${esc(r.decisionLabel)}</h2>
        <p>${esc(r.rationale)}</p>
      </section>

      <div class="metric-row">
        ${miniMetric("Max comfortable", formatMoneyCompact(r.maxComfortablePrice))}
        ${miniMetric("Max stretch", formatMoneyCompact(r.maxStretchPrice))}
        ${miniMetric("All-in carry / mo", `${formatMoneyOrNa(r.ownerCostMonthly)} vs ${formatMoneyCompact(c.housing.comfortCapMonthly)} cap`)}
        ${miniMetric("Free cash flow / mo", formatMoneyOrNa(r.monthlyFreeCashFlow))}
        ${miniMetric("Post-close liquidity", `${formatMoneyCompact(r.postCloseLiquidity)} vs ${formatMoneyCompact(c.assets.reserveTarget)} reserve`)}
        ${miniMetric("Deployable for DP", formatMoneyCompact(r.dpFundingCapacity))}
      </div>

      ${firedFlags.length ? `
      <section class="section-block">
        <div class="section-head compact"><div><p class="eyebrow">Watch-outs</p><h3>${firedFlags.length} flag${firedFlags.length === 1 ? "" : "s"} triggered</h3></div></div>
        <ul class="afford-flags">
          ${firedFlags.map((f) => `<li class="afford-flag">${esc(f.message)}</li>`).join("")}
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
        <div class="section-head compact"><div><p class="eyebrow">When can we move up</p><h3>Max comfortable price by valuation × wait</h3></div></div>
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
        <p class="note">Each cell is the most expensive home whose all-in carry stays within your comfort cap, after the equity that is vested + sellable + after-tax at that wait.</p>
      </section>
    </div>
  `;
  refreshIcons();
}
