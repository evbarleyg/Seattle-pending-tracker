// Records view, extracted from src/main.mjs.
//
// Renders the sortable, paged comps table (with Zillow / KC parcel links)
// plus the mobile card list.
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs, propertyAddressLink, affordTierBadge, currentPageSize,
//     paginationControls }
//
// Intelligibility pass: column headers that map to a glossary entry get a
// quiet "what is this?" trigger (data.mjs / normalizeRow already grounds a
// row's lineage in a single field, row.dataMode, which is either
// "PUBLIC_PROXY" (county recorded sale) or "MLS_ENRICHED" (MLS export or
// Redfin folded in, the pipeline does not keep those two separate past that
// point) -- so the Source column and its badge read straight off that field,
// no new derived state. Popovers are resolved globally by
// src/views/overview.mjs's getExplainEntry (wired once in main.mjs), so
// dropping a renderExplainButton(id) call here is enough to make it live.
import {
  esc,
  formatDateShort,
  formatLot,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatPricePerSqft,
  formatRatio,
} from "../domain/format.mjs";
import { countyRecordUrl, domMetric, hotCategory } from "../domain/data.mjs";
import {
  getRecordSortValue,
  paginateRows,
  recordViewLabel,
  sortRows,
} from "../domain/selectors.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

export function renderRecordsView(deps) {
  ctx = deps;
  const { state, qs, currentPageSize, paginationControls } = ctx;
  const wrap = qs("#view-records");
  if (!wrap || !state.derived) return;
  const sorted = sortRows(state.derived.viewRows, state.recordSort, getRecordSortValue);
  const pageSize = currentPageSize();
  const page = paginateRows(sorted, state.recordsPage, pageSize);
  state.recordsPage = page.page;
  const emptyMessage = state.derived.slices.emptyMessage;
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head">
        <div><p class="eyebrow">Records</p><h2>Comps and property links</h2></div>
        <div class="caption-row">${renderUniverseCaption({
          count: page.total,
          universeLabel: "rows in this table",
          windowLabel: recordViewLabel(state.filters.recordView),
        })}${renderExplainButton("recordRows")}</div>
        <p class="note" id="recordsStatus">${page.total ? `Showing ${page.total} comps/properties in ${recordViewLabel(state.filters.recordView)}. Zillow and KC parcel links stay available for property-level review; blank MLS fields mean unavailable or unknown for that export.${state.flags.projection ? ` Rows marked Projected are modeled estimates, not recorded sales.${renderExplainButton("projectedClose")}` : ""}` : emptyMessage}</p>
      </section>
      <div class="table-head">
        <p class="note">Showing ${page.start}-${page.end} of ${page.total}. Sorting and export use the full filtered dataset.</p>
        ${paginationControls("records", page)}
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              ${recordTh("address", "Address")}
              ${recordTh("dataMode", "Source", "recordSource")}
              ${recordTh("neighborhood", "Neighborhood")}
              ${recordTh("type", "Type")}
              ${recordTh("beds", "Beds")}
              ${recordTh("baths", "Baths")}
              ${recordTh("sqft", "SqFt")}
              ${recordTh("psf", "$/SqFt")}
              ${recordTh("lotSize", "Lot Size")}
              ${recordTh("yearBuilt", "Built")}
              ${recordTh("saleDate", "Sale Dt")}
              ${recordTh("closePrice", "Close Price")}
              ${recordTh("originalListPrice", "Original List")}
              ${recordTh("pendingListPrice", "Ask", "recordAsk")}
              ${recordTh("domMetric", "DOM", "recordDom")}
              ${recordTh("hotCategory", "Hot", "recordHot")}
              ${recordTh("saleToList", "S/List", "recordSaleToList")}
              ${recordTh("delta", "Bid-Up", "recordBidUp")}
            </tr>
          </thead>
          <tbody id="recordRows">${page.rows.map(recordRowHtml).join("") || `<tr><td colspan="18">${esc(emptyMessage)}</td></tr>`}</tbody>
        </table>
      </div>
      <div class="mobile-card-list" id="recordMobileList">${page.rows.map(recordMobileCard).join("")}</div>
    </div>
  `;
}

function recordTh(key, label, explainId = "") {
  const { state } = ctx;
  const active = state.recordSort.key === key;
  const arrow = active ? (state.recordSort.dir === "asc" ? "up" : "down") : "both";
  const explain = explainId ? renderExplainButton(explainId) : "";
  return `<th><span class="th-cell"><button class="th-sort ${active ? "active" : ""}" type="button" data-record-sort="${esc(key)}">${esc(label)} <span class="sort-ind" data-sort-ind="${esc(key)}">${arrow === "up" ? "↑" : arrow === "down" ? "↓" : "↕"}</span></button>${explain}</span></th>`;
}

// Plain-language row lineage, read straight off row.dataMode (see
// domain/data.mjs normalizeRow/inferMode). The pipeline folds manual MLS
// exports and Redfin into the same "MLS_ENRICHED" kind, so this is a county
// vs. MLS-enriched split, not a three-way county/MLS/Redfin split -- there is
// no field that tells the two enrichment sources apart on a given row.
function recordSourceBadge(row) {
  const isMlsEnriched = row.dataMode === "MLS_ENRICHED";
  return `<span class="lineage-pill ${isMlsEnriched ? "mls" : "county"}">${isMlsEnriched ? "MLS-enriched" : "County record"}</span>`;
}

function hotBadge(row) {
  if (row.isUltraHot) return `<span class="hot-pill ultra">Ultra Hot</span>`;
  if (row.isHotMarket) return `<span class="hot-pill hot">Hot</span>`;
  return `<span class="hot-pill">Normal</span>`;
}

function recordRowHtml(row) {
  const { propertyAddressLink, affordTierBadge } = ctx;
  const countyUrl = countyRecordUrl(row);
  return `
    <tr>
      <td>${propertyAddressLink(row)}${row.isProjectionRow ? `<span class="proj-pill">Projected</span>` : ""}${countyUrl ? `<a class="sub-link" href="${esc(countyUrl)}" target="_blank" rel="noopener noreferrer">KC Parcel</a>` : ""}${affordTierBadge(row)}</td>
      <td>${recordSourceBadge(row)}</td>
      <td><button class="link-button" data-set-interaction="neighborhood" data-set-value="${esc(row.neighborhoodLabel)}">${esc(row.neighborhoodLabel)}</button></td>
      <td><button class="link-button" data-set-interaction="type" data-set-value="${esc(row.typeLabel)}">${esc(row.typeLabel)}</button></td>
      <td>${row.beds > 0 ? row.beds : "n/a"}</td>
      <td>${row.baths > 0 ? row.baths.toFixed(2) : "n/a"}</td>
      <td>${row.sqft > 0 ? row.sqft.toLocaleString("en-US") : "n/a"}</td>
      <td>${formatPricePerSqft(row.pricePerSqft)}</td>
      <td>${formatLot(row.lotSize)}</td>
      <td>${row.yearBuilt > 0 ? row.yearBuilt : "n/a"}</td>
      <td>${formatDateShort(row.saleDate || row.pendingDate)}</td>
      <td>${row.isProjectionRow ? `~${formatMoneyOrNa(row.projectedClosePrice)} (est.)` : formatMoneyOrNa(row.closePrice)}</td>
      <td>${formatMoneyOrNa(row.originalListPrice)}</td>
      <td>${formatMoneyOrNa(row.pendingListPrice)}</td>
      <td>${recordDomCell(row)}</td>
      <td>${hotBadge(row)}</td>
      <td>${recordSaleListCell(row)}</td>
      <td>${recordBidUpCell(row)}</td>
    </tr>
  `;
}

function recordDomCell(row) {
  const primary = domMetric(row);
  if (primary === null || primary === undefined) return "n/a";
  const usingCdom = !!(row?.hasMlsCdomValue && Number.isFinite(row.mlsCDOM) && row.mlsCDOM >= 0);
  let secondaryVal = null;
  let secondaryLabel = "";
  if (usingCdom) {
    if (row?.hasMlsDomValue && Number.isFinite(row.mlsDOM) && row.mlsDOM >= 0 && row.mlsDOM !== primary) {
      secondaryVal = row.mlsDOM;
      secondaryLabel = "DOM";
    }
  } else if (row?.hasMlsCdomValue && Number.isFinite(row.mlsCDOM) && row.mlsCDOM >= 0 && row.mlsCDOM !== primary) {
    secondaryVal = row.mlsCDOM;
    secondaryLabel = "CDOM";
  }
  const sub = secondaryVal !== null ? `<span class="cell-sub">${secondaryLabel} ${secondaryVal}</span>` : "";
  return `${primary}${sub}`;
}

function recordSaleListCell(row) {
  const primary = formatRatio(row.saleToList);
  const orig = Number(row.saleToOriginalList || 0);
  const list = Number(row.saleToList || 0);
  const sub = orig > 0 && Math.abs(orig - list) >= 0.005 ? `<span class="cell-sub">orig ${formatRatio(orig)}</span>` : "";
  return `${primary}${sub}`;
}

function recordBidUpCell(row) {
  if (!row.hasMarketListPrice) return "n/a";
  const primary = formatMoneyCompact(row.delta);
  const delta = Number(row.delta || 0);
  const showPct = Math.abs(delta) >= 1;
  const pct = Number(row.deltaPct || 0);
  const sub = showPct ? `<span class="cell-sub">${pct >= 0 ? "+" : ""}${(pct * 100).toFixed(1)}%</span>` : "";
  return `${primary}${sub}`;
}

function recordMobileCard(row) {
  const { propertyAddressLink } = ctx;
  return `
    <article class="mrow">
      <div class="mrow-head-static">
        ${propertyAddressLink(row, "mrow-address-link")}${row.isProjectionRow ? `<span class="proj-pill">Projected</span>` : ""}
        <span>${row.isProjectionRow ? `~${formatMoneyCompact(row.projectedClosePrice)} (est.)` : formatMoneyCompact(row.closePrice || row.pendingListPrice)}</span>
      </div>
      <div class="mrow-grid">
        <div><span>Source</span><strong>${recordSourceBadge(row)}</strong></div>
        <div><span>Neighborhood</span><strong>${esc(row.neighborhoodLabel)}</strong></div>
        <div><span>Type</span><strong>${esc(row.typeLabel)}</strong></div>
        <div><span>Beds</span><strong>${row.beds > 0 ? row.beds : "n/a"}</strong></div>
        <div><span>Baths</span><strong>${row.baths > 0 ? row.baths.toFixed(2) : "n/a"}</strong></div>
        <div><span>SqFt</span><strong>${row.sqft > 0 ? row.sqft.toLocaleString("en-US") : "n/a"}</strong></div>
        <div><span>$/SqFt</span><strong>${formatPricePerSqft(row.pricePerSqft)}</strong></div>
        <div><span>Built</span><strong>${row.yearBuilt > 0 ? row.yearBuilt : "n/a"}</strong></div>
        <div><span>Sale/List</span><strong>${recordSaleListCell(row)}</strong></div>
        <div><span>DOM</span><strong>${recordDomCell(row)}</strong></div>
        <div><span>Bid-Up</span><strong>${recordBidUpCell(row)}</strong></div>
        <div><span>Status</span><strong>${esc(hotCategory(row))}</strong></div>
      </div>
    </article>
  `;
}
