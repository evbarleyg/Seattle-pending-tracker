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
  formatDateShort,
  formatMoney,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatRatio,
  formatWholeNumber,
  monthLabelCompact,
  num,
} from "../domain/format.mjs";
import { domMetric } from "../domain/data.mjs";
import {
  BID_MIN_COMPS,
  BID_STRATEGIES,
  bidTierLabel,
  getBidSortValue,
  paginateRows,
  scoreBidForRow,
  sortRows,
} from "../domain/selectors.mjs";
import { selectBidComps, topBidComps } from "../domain/bidComps.mjs";
import { LISTING_STAGES, ratioVsAskPhrase, stageMeta } from "../domain/listingStage.mjs";
import { OUTLOOK_HORIZON_DAYS } from "../domain/listingOutlook.mjs";
import { formatAge } from "../domain/freshness.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

// When nothing in the queue could be scored, say why once, in terms of what a
// suggestion needs, instead of printing "n/a" on every card. The usual cause is
// stale sold data: comps must have closed inside the window with a real asking
// price, and that only arrives with a sold pull plus its history backfill.
export function noSuggestionsNoticeHtml(stats, pricedSalesSource) {
  if (!stats || stats.activeCount === 0 || stats.scoredCount > 0) return "";
  const newest = pricedSalesSource?.date
    ? ` The newest sale with a known asking price in this data closed ${formatDateShort(pricedSalesSource.date)} (${formatAge(pricedSalesSource.ageDays)}).`
    : "";
  const remedy = pricedSalesSource?.tone === "behind"
    ? " Suggestions come back once the sold data and its asking-price history are refreshed."
    : " Clearing neighborhood or lot-size filters can bring more comps into range.";
  return `
    <div class="empty-state" id="bidNoSuggestions" role="status">
      <strong>No bid suggestions right now.</strong>
      A suggestion needs the equivalent of ${BID_MIN_COMPS} closely comparable homes that sold in the last 12 months with a known asking price, and none of the ${formatWholeNumber(stats.activeCount)} active listings here has that many.${esc(newest)}${remedy}
      You can still browse the listings below and load any of them into the comp finder.
    </div>`;
}

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

// Plain-language scope word for where the weight behind a suggested bid sits,
// mirroring bidTierLabel (selectBidComps in bidComps.mjs).
export function compScopeLabel(tier) {
  return tier === "T1_NEIGHBORHOOD_TYPE" || tier === "T3_CITY_TYPE" ? bidTierLabel(tier).toLowerCase() : "";
}

// Which stretch of the market the comps were drawn from, in the buyer's terms.
// Week one is pooled; after that comps count by how close their days to pending
// were to this listing's days listed.
export function compTimingWords(stageId, dom = null, domMatch = "near") {
  const stage = stageMeta(stageId);
  if (!stage || domMatch === "none") return "";
  if (stage.id === "fresh") return `that went pending in ${stage.range}`;
  // Too few sales took about this long, so every sale past week one was used.
  if (domMatch === "side") return "that went pending after their first week";
  return Number.isFinite(dom) ? `that went pending around day ${Math.round(dom)}` : `that went pending in ${stage.range}`;
}

// "of 28 mostly nearby single family comps" style caption for a scored bid, reusing
// the same shared universe-caption component the Overview uses so both tabs
// state their comp basis the same way.
export function compBasisCaption(compCount, tier, typeLabel, stageId = "", dom = null, { domMatch = "near", newerBuild = false } = {}) {
  if (!compCount) return "";
  const scope = compScopeLabel(tier);
  const typeWord = String(typeLabel || "home").toLowerCase();
  // Worth saying: it is why a newer home's bid sits nearer its ask than its neighbours'.
  const era = newerBuild ? "newer-build" : "";
  const universeLabel = [scope, era, `${typeWord} comps`, compTimingWords(stageId, dom, domMatch)].filter(Boolean).join(" ");
  return renderUniverseCaption({ count: compCount, universeLabel });
}

// Why a scored bid could not use same-stage comps, in the buyer's terms.
export function unstagedReason(scored) {
  return scored.bidStage
    ? `any stage: too few ${String(scored.bidStageLabel || "").toLowerCase()} comps`
    : "any stage: enter DOM for a sharper read";
}

// Stage pill. Tone follows who holds the leverage: the first week is the
// seller's (hot), a sitting or stale listing is the buyer's (cool).
const STAGE_TONE = { fresh: "hot", past: "warm", sitting: "cool", stale: "cool" };

export function stagePillHtml(row) {
  const stage = stageMeta(row?.bidStage);
  if (!stage) return `<span class="stage-pill unknown" title="No days-on-market reading for this listing">Stage unknown</span>`;
  return `<span class="stage-pill ${STAGE_TONE[stage.id]}" title="${esc(stage.range)} on the market">${esc(stage.label)}</span>`;
}

// What the listing's own history says: how far the ask has come down from the
// opening price, and whether the advertised clock was reset by a relist.
export function listingHistoryFacts(row) {
  const ask = Number(row?.pendingListPrice) || 0;
  const original = Number(row?.originalListPrice) || 0;
  const cutUsd = original > ask && ask > 0 ? original - ask : 0;
  const ownDays = row?.hasMlsDomValue ? Number(row.mlsDOM) : null;
  const totalDays = domMetric(row);
  const relisted = ownDays !== null && totalDays !== null && totalDays > ownDays;
  const priorPrice = Number(row?.priorSalePrice) || 0;
  return {
    priorSaleDate: priorPrice > 0 ? String(row.priorSaleDate || "") : "",
    priorSalePrice: priorPrice,
    askVsPriorPct: priorPrice > 0 && ask > 0 ? ask / priorPrice - 1 : null,
    cutUsd,
    cutPct: cutUsd > 0 ? cutUsd / original : 0,
    totalDays,
    ownDays,
    relisted,
  };
}

export function daysListedText(facts) {
  if (facts.totalDays === null) return "n/a";
  return facts.relisted ? `${facts.totalDays} (${facts.ownDays} since relist)` : String(facts.totalDays);
}

function historyNoteHtml(facts) {
  const bits = [];
  if (facts.cutUsd > 0) bits.push(`Cut ${formatMoneyCompact(facts.cutUsd)} (${(facts.cutPct * 100).toFixed(1)}%) from its first ask`);
  if (facts.relisted) bits.push("relisted under a new MLS number");
  if (facts.priorSalePrice > 0) {
    const change = Math.round(facts.askVsPriorPct * 100);
    const versus = change === 0 ? "the same as the ask" : `ask is ${Math.abs(change)}% ${change > 0 ? "higher" : "lower"}`;
    bits.push(`last sold ${monthLabelCompact(facts.priorSaleDate.slice(0, 7))} for ${formatMoneyCompact(facts.priorSalePrice)} (${versus})`);
  }
  if (!bits.length) return "";
  const text = bits.join(" · ");
  return `<p class="bid-history-note">${esc(`${text[0].toUpperCase()}${text.slice(1)}`)}</p>`;
}

// "Next 14 days: 33% cut the price · 8% go under contract", led by a chip saying which
// way those odds lean. Empty when the ledger has no read.
export function outlookNoteHtml(outlook) {
  if (!outlook || outlook.cutChance === null) return "";
  const pct = (share) => `${Math.round(share * 100)}%`;
  const bits = [`${pct(outlook.cutChance)} cut the price`];
  if (outlook.saleChance !== null) bits.push(`${pct(outlook.saleChance)} go under contract`);
  const chip = outlook.lean ? `<span class="outlook-chip ${esc(outlook.lean.tone)}">${esc(outlook.lean.label)}</span>` : "";
  return `<p class="bid-outlook">${chip}<span>Next ${OUTLOOK_HORIZON_DAYS} days, listings like this: ${esc(bits.join(" · "))} ${renderExplainButton("listingOutlook")}</span></p>`;
}

// How the queue splits by stage, so the page opens with where the leverage is.
function stageSummaryHtml(rows) {
  const counts = Object.fromEntries(LISTING_STAGES.map((stage) => [stage.id, 0]));
  let unknown = 0;
  for (const row of rows) {
    if (counts[row.bidStage] === undefined) unknown += 1;
    else counts[row.bidStage] += 1;
  }
  if (!rows.length) return "";
  const chips = LISTING_STAGES.map((stage) => `<span class="stage-count"><span class="stage-pill ${STAGE_TONE[stage.id]}">${esc(stage.label)}</span><b>${formatWholeNumber(counts[stage.id])}</b><span class="stage-range">${esc(stage.range)}</span></span>`);
  if (unknown) chips.push(`<span class="stage-count"><span class="stage-pill unknown">Stage unknown</span><b>${formatWholeNumber(unknown)}</b></span>`);
  return `<div class="stage-summary" id="bidStageSummary"><span class="mini-label">By stage ${renderExplainButton("bidStage")}</span>${chips.join("")}</div>`;
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
      <p class="note">Each suggestion is built from homes that sold after a similar time on the market, because a week-one listing and a week-eight listing are different negotiations. Load a listing or enter an address to see the comps behind a number.</p>
      ${renderManualBidPanel()}
      <div class="metric-row">
        ${miniMetric("Active listings", formatWholeNumber(stats.activeCount))}
        ${miniMetric("Scored", formatWholeNumber(stats.scoredCount))}
        ${metricWithExplain("High confidence", formatWholeNumber(stats.highConfidenceCount), "bidConfidence")}
        ${miniMetric("Watched", formatWholeNumber(watchedCount))}
        ${metricWithExplain("Median over ask", stats.scoredCount ? `${(stats.medianOverAskPct || 0).toFixed(1)}%` : "n/a", "bidMedianOverAsk")}
      </div>
      ${stageSummaryHtml(baseRows)}
      ${noSuggestionsNoticeHtml(stats, state.dataSource.freshness?.items?.find((item) => item.id === "pricedSales"))}
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
                ${bidTh("pendingListPrice", "List at pending")}
                ${bidTh("dom", "Days on market")}
                <th><span class="th-cell">Stage ${renderExplainButton("bidStage")}</span></th>
                ${bidTh("suggestedBid", "Suggested Bid", "suggestedBid")}
                ${bidTh("bidRange", "Bid Range", "bidRange")}
                ${bidTh("ratio", "Bid vs Ask", "suggestedSaleList")}
                ${bidTh("confidence", "Confidence", "bidConfidence")}
                ${bidTh("compCount", "Comp Count", "bidCompBasis")}
                ${bidTh("compTier", "Comp basis")}
              </tr>
            </thead>
            <tbody id="bidRows">${page.rows.map(bidRowHtml).join("") || `<tr><td colspan="13">No active listings match the current filters.</td></tr>`}</tbody>
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
  const facts = listingHistoryFacts(row);
  const head = `
      <header class="bid-card-head">
        <button type="button" class="watch-star ${isWatched ? "active" : ""}" data-toggle-watch="${esc(row.id)}" aria-label="${isWatched ? "Unwatch" : "Watch"} ${esc(row.address || "listing")}" title="${isWatched ? "Unwatch" : "Watch"}">★</button>
        <div class="bid-card-address">
          ${propertyAddressLink(row, "bid-card-address-link")}
          <span class="bid-card-meta">${esc(row.neighborhoodLabel || "")}${row.typeLabel ? ` · ${esc(row.typeLabel)}` : ""}</span>
          ${stagePillHtml(row)}
        </div>
        ${affordTierBadge(row)}
      </header>`;
  // No suggestion: lead with the facts the listing does have and say, once and
  // small, why there is no number, rather than a large "n/a" and rows of dashes.
  if (!scored) {
    const compCount = row.bidCompCount || 0;
    return `
    <article class="bid-card unscored ${isWatched ? "watched" : ""}" data-row-id="${esc(row.id)}">
      ${head}
      <div class="bid-card-grid-meta">
        <div><span>Ask</span><strong>${formatMoneyOrNa(row.pendingListPrice)}</strong></div>
        <div><span>First listed at</span><strong>${formatMoneyOrNa(row.originalListPrice)}</strong></div>
        <div><span>Days listed</span><strong>${esc(daysListedText(facts))}</strong></div>
      </div>
      ${historyNoteHtml(facts)}
      ${outlookNoteHtml(row.outlook)}
      <p class="bid-unscored-note">No suggestion: the comps add up to ${compCount} of the ${BID_MIN_COMPS} needed ${renderExplainButton("bidCompBasis")}</p>
      <footer class="bid-card-footer">
        <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use in scenario</button>
      </footer>
    </article>
  `;
  }
  const suggested = formatMoneyCompact(row.bidSuggested);
  const range = `${formatMoneyCompact(row.bidLow)} – ${formatMoneyCompact(row.bidHigh)}`;
  const outcome = `${formatMoneyCompact(row.bidOutcomeLow)} – ${formatMoneyCompact(row.bidOutcomeHigh)}`;
  const confTone = (row.bidConfidenceLabel || "").toLowerCase();
  const overAsk = row.pendingListPrice > 0
    ? Math.round(((row.bidSuggested - row.pendingListPrice) / row.pendingListPrice) * 100)
    : null;
  const overAskBadge = overAsk === null
    ? ""
    : `<span class="bid-over-ask ${overAsk > 0 ? "up" : overAsk < 0 ? "down" : "flat"}">${esc(overAskPhrase(overAsk))}</span>`;
  const compCaption = compBasisCaption(row.bidCompCount, row.bidCompTier, row.typeLabel, row.bidStage, facts.totalDays, { domMatch: row.bidDomMatch, newerBuild: row.bidNewerBuild });
  return `
    <article class="bid-card ${isWatched ? "watched" : ""}" data-row-id="${esc(row.id)}">
      ${head}
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
      ${captionRow(compCaption, "bidCompBasis")}
      <div class="bid-card-grid-meta">
        <div><span>Bid range</span><strong>${range}</strong></div>
        <div><span>Likely sale ${renderExplainButton("bidOutcomeRange")}</span><strong>${outcome}</strong></div>
        <div><span>Ask</span><strong>${formatMoneyOrNa(row.pendingListPrice)}</strong></div>
        <div><span>First listed at</span><strong>${formatMoneyOrNa(row.originalListPrice)}</strong></div>
        <div><span>Days listed</span><strong>${esc(daysListedText(facts))}</strong></div>
        <div><span>Comps</span><strong>${row.bidCompCount || 0}</strong></div>
      </div>
      ${historyNoteHtml(facts)}
      ${outlookNoteHtml(row.outlook)}
      <footer class="bid-card-footer">
        <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use in scenario</button>
      </footer>
    </article>
  `;
}

// The three parts of the panel that depend on what has been typed.
function manualBidOutputs() {
  const { state } = ctx;
  const result = state.bid.manualEnabled ? computeManualBid() : null;
  // An estimate already carries its comps; the lookup is for "Find Comps" with no ask.
  const compLookup = !result?.compRows && (state.bid.compSearchEnabled || state.bid.manualEnabled) ? computeManualComps() : null;
  return {
    status: result?.status || compLookup?.status || "Load a listing or enter an address to find comps.",
    resultHtml: result?.html || "",
    compRowsHtml: result?.compRows || compLookup?.rowsHtml || `<tr><td colspan="8">${esc(compLookup?.status || "Find comps to populate this list.")}</td></tr>`,
  };
}

// After a field changes, only the outputs are redrawn. Re-rendering the inputs too
// replaced the one the buyer had just tabbed into: its text was no longer selected,
// so typing appended to the old value ("1948" became "19481948") instead of
// replacing it. Nothing outside the panel depends on these fields.
export function refreshManualBidOutputs(deps) {
  ctx = deps;
  const { qs } = ctx;
  const [status, result, compRows] = [qs("#manualBidStatus"), qs("#manualBidResult"), qs("#manualBidCompRows")];
  if (!status || !result || !compRows) return false;
  const outputs = manualBidOutputs();
  status.textContent = outputs.status;
  result.innerHTML = outputs.resultHtml;
  compRows.innerHTML = outputs.compRowsHtml;
  return true;
}

export function renderManualBidPanel() {
  const { state, buttonIcon, optionHtml } = ctx;
  const outputs = manualBidOutputs();
  return `
    <section class="manual-bid-wrap">
      <div class="section-head compact">
        <div><p class="eyebrow">Comp Finder</p><h3>Prospective listing comps ${renderExplainButton("bidCompBasis")}</h3></div>
        <span class="note" id="manualBidStatus">${esc(outputs.status)}</span>
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
        <div class="field"><label for="manualBidYearBuilt">Year built</label><input id="manualBidYearBuilt" type="number" min="1850" max="2100" step="1" value="${esc(state.manualBid.yearBuilt)}" placeholder="1948" /></div>
      </div>
      <div class="manual-bid-actions">
        ${buttonIcon("Find Comps", "search", "id=\"manualCompRun\"", "alt")}
        ${buttonIcon("Estimate Bid", "target", "id=\"manualBidRun\"")}
        ${buttonIcon("Clear", "refresh-ccw", "id=\"manualBidClear\"", "alt")}
      </div>
      <div class="manual-bid-result" id="manualBidResult">${outputs.resultHtml}</div>
      <div class="table-wrap manual-bid-table-wrap">
        <table class="manual-bid-table">
          <thead><tr><th>Comp</th><th>Neighborhood</th><th>Closed</th><th>Close price</th><th>Sale vs ask</th><th>DOM</th><th>Over/under ask</th><th><span class="th-cell">Counts for ${renderExplainButton("bidCompBasis")}</span></th></tr></thead>
          <tbody id="manualBidCompRows">${outputs.compRowsHtml}</tbody>
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
    yearBuilt: num(state.manualBid.yearBuilt),
  };
}

// A comp's share of all the weight behind the bid.
export function weightShareText(share) {
  if (!Number.isFinite(share) || share <= 0) return "n/a";
  return share < 0.001 ? "<0.1%" : `${(share * 100).toFixed(1)}%`;
}

// "the 12 sales that count most (41% of the weight)"
function topCompsPhrase(rows) {
  const share = rows.reduce((sum, comp) => sum + (comp.bidCompWeightShare || 0), 0);
  return `the ${rows.length} sales that count most (${Math.round(share * 100)}% of the weight)`;
}

function compRowsHtml(rows) {
  const { propertyAddressLink } = ctx;
  return (rows || []).map((comp) => `
    <tr>
      <td>${propertyAddressLink(comp, "comp-link")}</td>
      <td>${esc(comp.neighborhoodLabel)}</td>
      <td>${esc(formatDateShort(comp.saleDate))}</td>
      <td>${formatMoneyCompact(comp.closePrice)}</td>
      <td>${formatRatio(comp.saleToList)}</td>
      <td>${domMetric(comp) ?? "n/a"}</td>
      <td>${formatMoneyCompact(comp.delta)}</td>
      <td>${weightShareText(comp.bidCompWeightShare)}</td>
    </tr>
  `).join("");
}

function computeManualComps() {
  const { state } = ctx;
  const row = manualScenarioRow();
  if (!row.address && !row.zip && (!row.neighborhoodLabel || row.neighborhoodLabel === "Seattle")) {
    return { status: "Enter an address plus ZIP or neighborhood, or load an active listing.", rowsHtml: "" };
  }
  const picked = selectBidComps(row, state.derived.compPool);
  const rows = topBidComps(picked);
  if (!rows.length) {
    return { status: "No comparable sales found for this home type in the last 12 months.", rowsHtml: "" };
  }
  const timing = picked.stageMatched ? ` ${compTimingWords(picked.stage, domMetric(row), picked.domMatch)}` : "";
  return {
    status: `Showing ${topCompsPhrase(rows)}${timing}.`,
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
      status: `Too few comparable sales: they add up to ${scored.bidCompCount} of the ${BID_MIN_COMPS} needed.`,
      html: `<div class="empty-state">Need more comparable recent sales for this scenario.</div>`,
      compRows: compRowsHtml(scored.bidCompRows || []),
    };
  }
  return {
    status: `Estimated from the equivalent of ${scored.bidCompCount} comps (${bidTierLabel(scored.bidCompTier).toLowerCase()}, ${scored.bidStageMatched ? compTimingWords(scored.bidStage, domMetric(row), scored.bidDomMatch).replace(/^that went /, "") : unstagedReason(scored)}).`,
    html: `
      ${metricWithExplain("Suggested bid", formatMoney(scored.bidSuggested), "suggestedBid")}
      ${metricWithExplain("Bid range", `${formatMoney(scored.bidLow)} - ${formatMoney(scored.bidHigh)}`, "bidRange")}
      ${metricWithExplain("Likely sale", `${formatMoney(scored.bidOutcomeLow)} - ${formatMoney(scored.bidOutcomeHigh)}`, "bidOutcomeRange")}
      ${metricWithExplain("Bid vs ask price", `${scored.bidRatio.toFixed(2)}x`, "suggestedSaleList")}
      ${metricWithExplain("Confidence", `${scored.bidConfidenceLabel} (${scored.bidConfidence})`, "bidConfidence")}
      ${metricWithExplain("Comp basis", `${scored.bidCompCount} comps · ${bidTierLabel(scored.bidCompTier)}`, "bidCompBasis")}
      ${metricWithExplain("Listing stage", scored.bidStageLabel || "Unknown", "bidStage")}
      ${bidLadderHtml(scored)}
    `,
    compRows: compRowsHtml(scored.bidCompRows || []),
  };
}

// What each step up in price buys: the bid that would have met or beaten the
// winning price on that share of comparable sales.
export function bidLadderHtml(scored) {
  const steps = scored?.bidLadder || [];
  const ask = Number(scored?.pendingListPrice) || 0;
  if (!steps.length || !(ask > 0)) return "";
  const cells = steps.map((step) => {
    return `<li><span class="ladder-chance">${Math.round(step.chance * 100)}%</span><strong>${esc(formatMoney(step.bid))}</strong><span class="ladder-vs-ask">${esc(ratioVsAskPhrase(step.bid / ask))}</span></li>`;
  }).join("");
  return `
    <div class="bid-ladder" id="manualBidLadder">
      <span class="mini-label">Chance of winning at each price ${renderExplainButton("bidLadder")}</span>
      <ol>${cells}</ol>
    </div>`;
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
      <td>${esc(daysListedText(listingHistoryFacts(row)))}</td>
      <td>${stagePillHtml(row)}</td>
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
        <div><span>Days listed</span><strong>${esc(daysListedText(listingHistoryFacts(row)))}</strong></div>
        <div><span>Stage</span><strong>${esc(row.bidStageLabel || "Unknown")}</strong></div>
        <div><span>Range</span><strong>${scored ? `${formatMoneyCompact(row.bidLow)} - ${formatMoneyCompact(row.bidHigh)}` : "n/a"}</strong></div>
        <div><span>Confidence ${renderExplainButton("bidConfidence")}</span><strong>${esc(row.bidConfidenceLabel)} (${row.bidConfidence || 0})</strong></div>
      </div>
      <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use In Manual Scenario</button>
    </article>
  `;
}
