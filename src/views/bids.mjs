// Bids view (Offer Lab), extracted from src/main.mjs.
//
// Renders the strategy pills, the manual comp finder / bid estimator panel,
// the active-listing stats row, and the paged card/table listing queue.
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs, miniMetric, buttonIcon, optionHtml, propertyAddressLink,
//     affordTierBadge, currentPageSize, paginationControls }
//
// Intelligibility treatment (same pattern as the answer-first Overview, see
// docs/superpowers/specs/2026-07-05-answer-first-overview-design.md): every
// suggested-bid card and the manual bid panel carry "what is this?" explain
// triggers on the suggested bid and the confidence score, a plain-language
// caption naming the comp basis ("of 28 nearby single family comps"), and
// cost-to-win-style over/at/under-ask wording instead of raw ratio jargon
// like "S/List".
import {
  esc,
  formatMoney,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatRatio,
  formatWholeNumber,
  num,
} from "../domain/format.mjs";
import { domMetric } from "../domain/data.mjs";
import {
  BID_STRATEGIES,
  bidTierLabel,
  computeBidCompTiers,
  getBidSortValue,
  paginateRows,
  scoreBidForRow,
  sortRows,
} from "../domain/selectors.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

// ---------------------------------------------------------------------------
// Shared small helpers. The shared miniMetric() passed in through deps
// escapes its whole label, so it cannot carry an explain trigger; these
// local variants mirror src/views/overview.mjs's statItem/captionRow so the
// two tabs read the same way.

function metricWithExplain(label, value, explainId = "") {
  return `<div class="mini-metric"><span>${esc(label)}${explainId ? ` ${renderExplainButton(explainId)}` : ""}</span><strong>${esc(value)}</strong></div>`;
}

function captionRow(captionHtml, explainId = "") {
  const button = explainId ? renderExplainButton(explainId) : "";
  if (!captionHtml && !button) return "";
  return `<div class="caption-row">${captionHtml}${button}</div>`;
}

// Plain-language scope word for which comp pool backed a suggested bid,
// mirroring bidTierLabel's tiers (computeBidCompTiers in selectors.mjs).
export function compScopeLabel(tier) {
  if (tier === "T1_NEIGHBORHOOD_TYPE") return "nearby";
  if (tier === "T2_ZIP_TYPE") return "same-zip";
  if (tier === "T3_CITY_TYPE") return "citywide";
  return "";
}

// "of 28 nearby single family comps" style caption for a scored bid, reusing
// the same shared universe-caption component the Overview uses so both tabs
// state their comp basis the same way.
export function compBasisCaption(compCount, tier, typeLabel) {
  if (!compCount) return "";
  const scope = compScopeLabel(tier);
  const typeWord = String(typeLabel || "home").toLowerCase();
  const universeLabel = [scope, `${typeWord} comps`].filter(Boolean).join(" ");
  return renderUniverseCaption({ count: compCount, universeLabel });
}

// Cost-to-win-style wording for how a suggested bid compares with the asking
// price, matching the Overview's over-ask/at-ask/under-ask phrasing instead
// of a raw "+X% vs ask" delta.
export function overAskPhrase(overAskPct) {
  if (overAskPct === null || overAskPct === undefined) return "";
  if (overAskPct > 0) return `${overAskPct}% over ask`;
  if (overAskPct < 0) return `${Math.abs(overAskPct)}% under ask`;
  return "at ask";
}

export function renderBidsView(deps) {
  ctx = deps;
  const { state, qs, miniMetric, currentPageSize, paginationControls } = ctx;
  const wrap = qs("#view-bids");
  if (!wrap || !state.derived) return;
  let baseRows = state.derived.bidRows || [];
  if (state.bid.watchedOnly) {
    baseRows = baseRows.filter((row) => state.watched.has(row.id));
  }
  const sorted = sortRows(baseRows, state.bidSort, getBidSortValue);
  const pageSize = currentPageSize();
  const page = paginateRows(sorted, state.bidsPage, pageSize);
  state.bidsPage = page.page;
  const stats = state.derived.bidStatsView;
  const watchedCount = state.watched.size;
  const useCards = state.bid.viewMode !== "table";
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head">
        <div>
          <p class="eyebrow">Bids</p>
          <h2>Offer Lab: what should I bid?</h2>
        </div>
        <div class="segmented">
          ${Object.entries(BID_STRATEGIES).map(([key, strategy]) => `<button type="button" class="scope-pill ${state.bid.strategy === key ? "active" : ""}" data-bid-strategy="${esc(key)}">${esc(strategy.label)}</button>`).join("")}
        </div>
      </section>
      <p class="note">Load an active listing or enter a prospective address to find comps first, then estimate a bid range from the same comp pool.</p>
      ${renderManualBidPanel()}
      <div class="metric-row">
        ${miniMetric("Active listings", formatWholeNumber(stats.activeCount))}
        ${miniMetric("Scored", formatWholeNumber(stats.scoredCount))}
        ${metricWithExplain("High confidence", formatWholeNumber(stats.highConfidenceCount), "bidConfidence")}
        ${miniMetric("Watched", formatWholeNumber(watchedCount))}
        ${metricWithExplain("Median over ask", `${(stats.medianOverAskPct || 0).toFixed(1)}%`, "bidMedianOverAsk")}
      </div>
      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Active listing queue</p>
            <h3>Listings ready for scenario review</h3>
          </div>
          <div class="bid-toolbar">
            <label class="check inline"><input type="checkbox" id="bidHighConfidenceOnly" ${state.bid.highConfidenceOnly ? "checked" : ""} /> High confidence only</label>
            <label class="check inline"><input type="checkbox" id="bidWatchedOnly" ${state.bid.watchedOnly ? "checked" : ""} /> Watched only</label>
            <div class="segmented small">
              <button type="button" class="scope-pill ${useCards ? "active" : ""}" data-bid-view-mode="cards">Cards</button>
              <button type="button" class="scope-pill ${!useCards ? "active" : ""}" data-bid-view-mode="table">Table</button>
            </div>
          </div>
        </div>
      <div class="table-head">
        <p class="note">Showing ${page.start}-${page.end} of ${page.total}. Sorting uses the full filtered active-listing set.</p>
        ${paginationControls("bids", page)}
      </div>
      ${useCards ? `
        <div class="bid-card-grid" id="bidCards">
          ${page.rows.map(bidCardHtml).join("") || `<p class="empty-state">No active listings match the current filters.</p>`}
        </div>
      ` : `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                ${bidTh("address", "Address")}
                ${bidTh("neighborhood", "Neighborhood")}
                ${bidTh("type", "Type")}
                ${bidTh("originalListPrice", "Original List")}
                ${bidTh("pendingListPrice", "List@Pending")}
                ${bidTh("dom", "DOM/CDOM")}
                ${bidTh("suggestedBid", "Suggested Bid", "suggestedBid")}
                ${bidTh("bidRange", "Bid Range", "bidRange")}
                ${bidTh("ratio", "Bid vs Ask", "suggestedSaleList")}
                ${bidTh("confidence", "Confidence", "bidConfidence")}
                ${bidTh("compCount", "Comp Count", "bidCompBasis")}
                ${bidTh("compTier", "Comp Tier")}
              </tr>
            </thead>
            <tbody id="bidRows">${page.rows.map(bidRowHtml).join("") || `<tr><td colspan="12">No active listings match the current filters.</td></tr>`}</tbody>
          </table>
        </div>
        <div class="mobile-card-list" id="bidMobileList">${page.rows.map(bidMobileCard).join("")}</div>
      `}
      </section>
    </div>
  `;
}

function bidCardHtml(row) {
  const { state, propertyAddressLink, affordTierBadge } = ctx;
  const isWatched = state.watched.has(row.id);
  const scored = row.bidStatus === "SCored";
  const suggested = scored ? formatMoneyCompact(row.bidSuggested) : "n/a";
  const range = scored ? `${formatMoneyCompact(row.bidLow)} – ${formatMoneyCompact(row.bidHigh)}` : "Insufficient comps";
  const ratio = scored && row.bidRatio > 0 ? `${row.bidRatio.toFixed(2)}x` : "—";
  const confTone = (row.bidConfidenceLabel || "").toLowerCase();
  const dom = domMetric(row);
  const overAsk = scored && row.pendingListPrice > 0
    ? Math.round(((row.bidSuggested - row.pendingListPrice) / row.pendingListPrice) * 100)
    : null;
  const overAskBadge = overAsk === null
    ? ""
    : `<span class="bid-over-ask ${overAsk > 0 ? "up" : overAsk < 0 ? "down" : "flat"}">${esc(overAskPhrase(overAsk))}</span>`;
  const compCaption = scored ? compBasisCaption(row.bidCompCount, row.bidCompTier, row.typeLabel) : "";
  return `
    <article class="bid-card ${isWatched ? "watched" : ""}" data-row-id="${esc(row.id)}">
      <header class="bid-card-head">
        <button type="button" class="watch-star ${isWatched ? "active" : ""}" data-toggle-watch="${esc(row.id)}" aria-label="${isWatched ? "Unwatch" : "Watch"} ${esc(row.address || "listing")}" title="${isWatched ? "Unwatch" : "Watch"}">★</button>
        <div class="bid-card-address">
          ${propertyAddressLink(row, "bid-card-address-link")}
          <span class="bid-card-meta">${esc(row.neighborhoodLabel || "")}${row.typeLabel ? ` · ${esc(row.typeLabel)}` : ""}</span>
        </div>
        ${affordTierBadge(row)}
      </header>
      <div class="bid-card-bid">
        <div class="bid-suggested-block">
          <span class="bid-suggested-label">Suggested bid ${renderExplainButton("suggestedBid")}</span>
          <strong class="bid-suggested-value">${suggested}</strong>
          ${overAskBadge}
        </div>
        <div class="bid-confidence-inline">
          <span class="conf-pill ${esc(confTone)}">${esc(row.bidConfidenceLabel || "n/a")} (${row.bidConfidence || 0})</span>
          ${renderExplainButton("bidConfidence")}
        </div>
      </div>
      ${scored ? captionRow(compCaption, "bidCompBasis") : ""}
      <div class="bid-card-grid-meta">
        <div><span>Range</span><strong>${range}</strong></div>
        <div><span>Ask</span><strong>${formatMoneyOrNa(row.pendingListPrice)}</strong></div>
        <div><span>Original list</span><strong>${formatMoneyOrNa(row.originalListPrice)}</strong></div>
        <div><span>DOM</span><strong>${dom ?? "n/a"}</strong></div>
        <div><span>Bid vs ask price</span><strong>${ratio}</strong></div>
        <div><span>Comps</span><strong>${row.bidCompCount || 0} (${esc(bidTierLabel(row.bidCompTier))})</strong></div>
      </div>
      <footer class="bid-card-footer">
        <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use in scenario</button>
      </footer>
    </article>
  `;
}

export function renderManualBidPanel() {
  const { state, buttonIcon, optionHtml } = ctx;
  const result = state.bid.manualEnabled ? computeManualBid() : null;
  const compLookup = (state.bid.compSearchEnabled || state.bid.manualEnabled) ? computeManualComps() : null;
  return `
    <section class="manual-bid-wrap">
      <div class="section-head compact">
        <div><p class="eyebrow">Comp Finder</p><h3>Prospective listing comps ${renderExplainButton("bidCompBasis")}</h3></div>
        <span class="note" id="manualBidStatus">${result?.status || compLookup?.status || "Load a listing or enter an address to find comps."}</span>
      </div>
      <div class="manual-bid-form">
        <div class="field"><label for="manualBidSource">Load Active Listing</label><select id="manualBidSource"><option value="">Manual entry</option>${state.derived.bidRows.map((row) => `<option value="${esc(row.mapPropertyKey)}" ${state.bid.manualSourceKey === row.mapPropertyKey ? "selected" : ""}>${esc(row.address || "Address unavailable")}</option>`).join("")}</select></div>
        <div class="field"><label for="manualBidAddress">Address</label><input id="manualBidAddress" type="text" value="${esc(state.manualBid.address)}" placeholder="123 Example St" /></div>
        <div class="field"><label for="manualBidListPrice">Ask price</label><input id="manualBidListPrice" type="number" min="0" step="1000" value="${esc(state.manualBid.pendingListPrice)}" placeholder="1250000" /></div>
        <div class="field"><label for="manualBidNeighborhood">Neighborhood</label><select id="manualBidNeighborhood"><option value="">Auto / Seattle</option>${state.options.neighborhoods.map((name) => `<option value="${esc(name)}" ${state.manualBid.neighborhoodLabel === name ? "selected" : ""}>${esc(name)}</option>`).join("")}</select></div>
        <div class="field"><label for="manualBidType">Property type</label><select id="manualBidType">${optionHtml(state.options.types.filter((type) => type !== "All"), state.manualBid.typeLabel || state.filters.type)}</select></div>
        <div class="field"><label for="manualBidZip">ZIP</label><input id="manualBidZip" type="text" maxlength="5" value="${esc(state.manualBid.zip)}" placeholder="98117" /></div>
        <div class="field"><label for="manualBidDom">DOM</label><input id="manualBidDom" type="number" min="0" step="1" value="${esc(state.manualBid.dom)}" placeholder="7" /></div>
        <div class="field"><label for="manualBidCdom">CDOM</label><input id="manualBidCdom" type="number" min="0" step="1" value="${esc(state.manualBid.cdom)}" placeholder="9" /></div>
      </div>
      <div class="manual-bid-actions">
        ${buttonIcon("Find Comps", "search", "id=\"manualCompRun\"", "alt")}
        ${buttonIcon("Estimate Bid", "target", "id=\"manualBidRun\"")}
        ${buttonIcon("Clear", "refresh-ccw", "id=\"manualBidClear\"", "alt")}
      </div>
      <div class="manual-bid-result" id="manualBidResult">
        ${result?.html || ""}
      </div>
      <div class="table-wrap manual-bid-table-wrap">
        <table class="manual-bid-table">
          <thead><tr><th>Comp</th><th>Neighborhood</th><th>Close price</th><th>Sale vs ask</th><th>DOM</th><th>Over/under ask</th></tr></thead>
          <tbody id="manualBidCompRows">${compLookup?.rowsHtml || result?.compRows || `<tr><td colspan="6">${esc(compLookup?.status || "Find comps to populate this list.")}</td></tr>`}</tbody>
        </table>
      </div>
    </section>
  `;
}

function zipFromText(text) {
  const match = String(text || "").match(/\b(98\d{3})\b/);
  return match ? match[1] : "";
}

function manualScenarioRow() {
  const { state } = ctx;
  const domValue = num(state.manualBid.cdom || state.manualBid.dom);
  return {
    address: state.manualBid.address,
    pendingListPrice: num(state.manualBid.pendingListPrice),
    neighborhoodLabel: state.manualBid.neighborhoodLabel || "Seattle",
    typeLabel: state.manualBid.typeLabel || state.filters.type,
    zip: state.manualBid.zip || zipFromText(state.manualBid.address),
    isHotMarket: domValue > 0 && domValue <= 10,
    isUltraHot: domValue > 0 && domValue <= 5,
    hasMlsDomValue: state.manualBid.dom !== "",
    hasMlsCdomValue: state.manualBid.cdom !== "",
    mlsDOM: num(state.manualBid.dom),
    mlsCDOM: num(state.manualBid.cdom),
  };
}

function compRowsHtml(rows) {
  const { propertyAddressLink } = ctx;
  return (rows || []).map((comp) => `
    <tr>
      <td>${propertyAddressLink(comp, "comp-link")}</td>
      <td>${esc(comp.neighborhoodLabel)}</td>
      <td>${formatMoneyCompact(comp.closePrice)}</td>
      <td>${formatRatio(comp.saleToList)}</td>
      <td>${domMetric(comp) ?? "n/a"}</td>
      <td>${formatMoneyCompact(comp.delta)}</td>
    </tr>
  `).join("");
}

function computeManualComps() {
  const { state } = ctx;
  const row = manualScenarioRow();
  if (!row.address && !row.zip && (!row.neighborhoodLabel || row.neighborhoodLabel === "Seattle")) {
    return { status: "Enter an address plus ZIP or neighborhood, or load an active listing.", rowsHtml: "" };
  }
  const tier = computeBidCompTiers(row, state.derived.compPool);
  const rows = tier.rows.slice().sort((a, b) => String(b.saleDate || "").localeCompare(String(a.saleDate || ""))).slice(0, 12);
  if (!rows.length) {
    return { status: "No comparable recent sales found for this type and location.", rowsHtml: "" };
  }
  return {
    status: `Showing ${rows.length} comps from ${bidTierLabel(tier.tier)}.`,
    rowsHtml: compRowsHtml(rows),
  };
}

function computeManualBid() {
  const { state } = ctx;
  const row = manualScenarioRow();
  if (!row.pendingListPrice) return { status: "Enter Ask Price to estimate bid.", html: "", compRows: "" };
  const scored = scoreBidForRow(row, state.derived.compPool, state.bid.strategy, true);
  if (scored.bidStatus !== "SCored") {
    return {
      status: `Insufficient comps (${scored.bidCompCount}) for ${bidTierLabel(scored.bidCompTier)}.`,
      html: `<div class="empty-state">Need more comparable recent sales for this scenario.</div>`,
      compRows: "",
    };
  }
  return {
    status: `Estimated using ${scored.bidCompCount} comps (${bidTierLabel(scored.bidCompTier)}).`,
    html: `
      ${metricWithExplain("Suggested bid", formatMoney(scored.bidSuggested), "suggestedBid")}
      ${metricWithExplain("Range", `${formatMoney(scored.bidLow)} - ${formatMoney(scored.bidHigh)}`, "bidRange")}
      ${metricWithExplain("Bid vs ask price", `${scored.bidRatio.toFixed(2)}x`, "suggestedSaleList")}
      ${metricWithExplain("Confidence", `${scored.bidConfidenceLabel} (${scored.bidConfidence})`, "bidConfidence")}
      ${metricWithExplain("Comp basis", `${scored.bidCompCount} comps · ${bidTierLabel(scored.bidCompTier)}`, "bidCompBasis")}
    `,
    compRows: compRowsHtml(scored.bidCompRows || []),
  };
}

function bidTh(key, label, explainId = "") {
  const { state } = ctx;
  const active = state.bidSort.key === key;
  const arrow = active ? (state.bidSort.dir === "asc" ? "up" : "down") : "both";
  const explain = explainId ? renderExplainButton(explainId) : "";
  return `<th><span class="th-cell"><button class="th-sort ${active ? "active" : ""}" type="button" data-bid-sort="${esc(key)}">${esc(label)} <span class="sort-ind" data-bid-sort-ind="${esc(key)}">${arrow === "up" ? "↑" : arrow === "down" ? "↓" : "↕"}</span></button>${explain}</span></th>`;
}

function bidRowHtml(row) {
  const { propertyAddressLink } = ctx;
  const suggested = row.bidStatus === "SCored" ? formatMoneyCompact(row.bidSuggested) : "n/a";
  const range = row.bidStatus === "SCored" ? `${formatMoneyCompact(row.bidLow)} - ${formatMoneyCompact(row.bidHigh)}` : "n/a";
  const ratio = row.bidStatus === "SCored" && row.bidRatio > 0 ? `${row.bidRatio.toFixed(2)}x` : "n/a";
  return `
    <tr>
      <td>${propertyAddressLink(row)}<button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use</button></td>
      <td>${esc(row.neighborhoodLabel)}</td>
      <td>${esc(row.typeLabel)}</td>
      <td>${formatMoneyOrNa(row.originalListPrice)}</td>
      <td>${formatMoneyOrNa(row.pendingListPrice)}</td>
      <td>${domMetric(row) ?? "n/a"}</td>
      <td>${suggested}</td>
      <td>${range}</td>
      <td>${ratio}</td>
      <td><span class="conf-pill ${esc((row.bidConfidenceLabel || "").toLowerCase())}">${esc(row.bidConfidenceLabel)} (${row.bidConfidence || 0})</span></td>
      <td>${row.bidCompCount || 0}</td>
      <td>${esc(bidTierLabel(row.bidCompTier))}</td>
    </tr>
  `;
}

function bidMobileCard(row) {
  const { propertyAddressLink } = ctx;
  const scored = row.bidStatus === "SCored";
  return `
    <article class="mrow">
      <div class="mrow-head-static">
        ${propertyAddressLink(row, "mrow-address-link")}
        <span class="mrow-bid-figure">${scored ? formatMoneyCompact(row.bidSuggested) : "n/a"}${renderExplainButton("suggestedBid")}</span>
      </div>
      <div class="mrow-grid">
        <div><span>Neighborhood</span><strong>${esc(row.neighborhoodLabel)}</strong></div>
        <div><span>Ask</span><strong>${formatMoneyCompact(row.pendingListPrice)}</strong></div>
        <div><span>Range</span><strong>${scored ? `${formatMoneyCompact(row.bidLow)} - ${formatMoneyCompact(row.bidHigh)}` : "n/a"}</strong></div>
        <div><span>Confidence ${renderExplainButton("bidConfidence")}</span><strong>${esc(row.bidConfidenceLabel)} (${row.bidConfidence || 0})</strong></div>
      </div>
      <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use In Manual Scenario</button>
    </article>
  `;
}
