// Geo view, extracted from src/main.mjs.
//
// Renders the Leaflet map (loaded lazily the first time the tab opens), the
// marker layer with pressure-colored dots, the legend, and the selected-
// property side list. Marker clicks toggle an inspection selection; applying
// or clearing that selection as a dashboard filter stays in main.mjs (the
// buttons are bound in bindEvents there).
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs, buttonIcon, refreshIcons, markDirty, propertyAddressText,
//     propertyAddressLink, affordTierMeta }
import {
  esc,
  formatDateShort,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatRatio,
  formatWholeNumber,
} from "../domain/format.mjs";
import { countyRecordUrl, domMetric, zillowUrl } from "../domain/data.mjs";
import { recordViewLabel } from "../domain/selectors.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

const RATIO_NEUTRAL_COLOR = "#9ca3af";
const ACTIVE_LISTING_COLOR = "#7c3aed";

export async function renderGeoView(deps) {
  ctx = deps;
  const { state, qs, refreshIcons } = ctx;
  const wrap = qs("#view-geo");
  if (!wrap || !state.derived) return;
  const rows = geoMappableRows();
  ensureGeoShell(wrap);
  updateGeoShell(rows);
  refreshIcons();
  if (!state.geo.leaflet) {
    const [{ default: L }] = await Promise.all([
      import("leaflet"),
      import("leaflet/dist/leaflet.css"),
    ]);
    state.geo.leaflet = L;
  }
  mountOrRefreshMap(rows);
}

function ensureGeoShell(wrap) {
  const { qs, buttonIcon } = ctx;
  if (qs("#map", wrap)) return;
  wrap.innerHTML = `
    <div class="view-band geo-view">
      <section class="section-head">
        <div>
          <p class="eyebrow">Geo</p>
          <h2>Where is pressure located?</h2>
          <p class="note geo-pressure-line">Pressure means how much buyers paid over or under the asking price to win a home. Each dot's color shows that for one property. ${renderExplainButton("geoPressure")}</p>
        </div>
        <div class="geo-actions">
          <label class="check inline"><input type="checkbox" id="geoViewportFilter" /> Filter to viewport</label>
          <label class="check inline"><input type="checkbox" id="geoShowActive" checked /> Active listings</label>
          ${buttonIcon("Filter dashboard to selected", "target", "id=\"geoApplySelection\"")}
          ${buttonIcon("Clear selection", "refresh-ccw", "id=\"geoClearSelection\"", "alt")}
          ${buttonIcon("Clear map filter", "search", "id=\"geoClearFilter\"", "alt")}
        </div>
      </section>
      <div class="geo-layout">
        <div id="map" class="map-surface" aria-label="Seattle sales map"></div>
        <aside class="geo-side">
          <div id="geoStatus" class="note" aria-live="polite"></div>
          <div id="geoStatusCaption" class="caption-row"></div>
          ${geoLegendHtml()}
          <div id="geoSelectedRows" aria-live="polite"></div>
        </aside>
      </div>
    </div>
  `;
}

function updateGeoShell(rows) {
  const { state, qs } = ctx;
  const viewport = qs("#geoViewportFilter");
  if (viewport) viewport.checked = !!state.geo.viewportFilter;
  const showActive = qs("#geoShowActive");
  if (showActive) showActive.checked = !state.geo.hideActive;
  const applySelection = qs("#geoApplySelection");
  if (applySelection) applySelection.disabled = !state.geo.selectedPropertyKeys.length;
  const clearFilter = qs("#geoClearFilter");
  if (clearFilter) clearFilter.disabled = !state.geo.filterPropertyKeys.length;
  const status = qs("#geoStatus");
  const drawnCount = Math.min(rows.length, 1200);
  if (status) {
    const applied = state.geo.filterPropertyKeys.length ? ` ${formatWholeNumber(state.geo.filterPropertyKeys.length)} map-selected properties are filtering the dashboard.` : " Marker clicks inspect properties only until you apply them as a filter.";
    const activeCount = rows.reduce((n, row) => n + (isActiveRow(row) ? 1 : 0), 0);
    status.textContent = `${formatWholeNumber(rows.length)} mapped properties in ${recordViewLabel(state.filters.recordView)}${activeCount ? ` (${formatWholeNumber(activeCount)} active)` : ""}. ${drawnCount < rows.length ? `Drawing ${formatWholeNumber(drawnCount)} of ${formatWholeNumber(rows.length)} for speed (all active listings shown).` : "All visible points are drawn."} Color = sale/list pressure; grey = sold, no list price; violet ring = active listing.${applied}`;
  }
  const statusCaption = qs("#geoStatusCaption");
  if (statusCaption) {
    statusCaption.innerHTML = `${renderUniverseCaption({ count: rows.length, universeLabel: `mapped properties in ${recordViewLabel(state.filters.recordView)}` })}${renderExplainButton("recordRows")}`;
  }
  const legend = qs(".geo-legend");
  if (legend) legend.outerHTML = geoLegendHtml();
  renderGeoSelectedRows();
}

// Recency line for a property card: shows the dates we genuinely have, omitting
// gracefully. Pending is gated on hasGenuinePendingDate (county rows fake
// pendingDate=saleDate); list on hasGenuineListDate. Returns "" when no date.
function geoDatesLineHtml(row) {
  const parts = [
    row.saleDate ? `Sold ${formatDateShort(row.saleDate)}` : "",
    row.hasGenuineListDate ? `Listed ${formatDateShort(row.listDate)}` : "",
    row.hasGenuinePendingDate ? `Pending ${formatDateShort(row.pendingDate)}` : "",
  ].filter(Boolean);
  if (!parts.length) return "";
  return `<span class="geo-popup-row">${esc(parts.join(" · "))}</span>`;
}

function propertyPopupLink(row) {
  const kc = countyRecordUrl(row);
  const zillowLink = `<a class="map-popup-link" href="${esc(zillowUrl(row))}" target="_blank" rel="noopener noreferrer">Zillow</a>`;
  const kcLink = kc ? `<a class="map-popup-link" href="${esc(kc)}" target="_blank" rel="noopener noreferrer">KC parcel</a>` : "";
  return `<div class="map-popup-actions">${zillowLink}${kcLink}</div>`;
}

function geoCardHtml(row, { withLinks = false } = {}) {
  const { state, propertyAddressText, affordTierMeta } = ctx;
  const priceText = row.isProjectionRow
    ? `~${formatMoneyOrNa(row.projectedClosePrice || row.closePrice)} (est. close)`
    : formatMoneyOrNa(row.closePrice || row.pendingListPrice);
  const ratioText = row.saleToList > 0 ? `${formatRatio(row.saleToList)} sale/list` : "";
  const dom = domMetric(row);
  const domText = dom !== null && dom !== undefined ? `${dom}d DOM` : "";
  const facts = [priceText, ratioText, domText].filter(Boolean).join(" · ");
  const datesLine = geoDatesLineHtml(row);

  // For active rows with a scored bid suggestion: project bid-up over ask.
  // For sold rows: show actual bid-up if available.
  const isActive = isActiveRow(row);
  let bidLine = "";
  if (isActive && row.bidStatus === "SCored" && row.pendingListPrice > 0) {
    const overAsk = row.bidSuggested - row.pendingListPrice;
    const overPct = (overAsk / row.pendingListPrice) * 100;
    const sign = overAsk > 0 ? "+" : overAsk < 0 ? "" : "";
    const cls = overAsk > 0 ? "bid-up-tooltip up" : overAsk < 0 ? "bid-up-tooltip down" : "bid-up-tooltip flat";
    bidLine = `<br><span class="${cls}">Suggested bid: ${formatMoneyOrNa(row.bidSuggested)} (${sign}${formatMoneyCompact(overAsk)} / ${sign}${overPct.toFixed(1)}%)</span>`;
  } else if (row.hasActualClose && row.hasMarketListPrice && Number.isFinite(row.delta) && row.delta !== 0) {
    const cls = row.delta > 0 ? "bid-up-tooltip up" : "bid-up-tooltip down";
    const pct = row.pendingListPrice > 0 ? (row.delta / row.pendingListPrice) * 100 : 0;
    const sign = row.delta > 0 ? "+" : "";
    bidLine = `<br><span class="${cls}">Bid-up: ${sign}${formatMoneyCompact(row.delta)} (${sign}${pct.toFixed(1)}%)</span>`;
  } else if (row.hasActualClose && !row.hasMarketListPrice) {
    // County deed only — no MLS/Redfin listing matched, so list price (and
    // therefore bid-up) is genuinely unavailable. Label it so it doesn't
    // read as missing/broken data.
    bidLine = `<br><span class="bid-up-tooltip flat">Bid-up: list price not on record (county sale only)</span>`;
  }

  // Affordability tier line (only when a private config is loaded).
  let affordLine = "";
  if (state.affordability.ready && affordTierMeta[row.affordTier]) {
    const meta = affordTierMeta[row.affordTier];
    affordLine = `<br><span class="afford-pill ${meta.cls}">${meta.label}</span>`;
  }

  const proj = row.isProjectionRow ? `<span class="proj-pill">Projected</span><br>` : "";
  const linkLine = withLinks ? `<br>${propertyPopupLink(row)}` : "";
  return `${proj}<strong>${esc(propertyAddressText(row))}</strong><br>${esc(row.neighborhoodLabel || "")}<br>${facts}${datesLine ? `<br>${datesLine}` : ""}${bidLine}${affordLine}${linkLine}`;
}

function geoLegendHtml() {
  return `
    <div class="geo-legend" aria-label="Property map color legend">
      <span><i style="--legend-color:#047857"></i><strong>Under ask</strong><small>0.90x-0.99x</small></span>
      <span><i style="--legend-color:#2563eb"></i><strong>At ask</strong><small>1.00x-1.02x</small></span>
      <span><i style="--legend-color:#c77700"></i><strong>Over ask</strong><small>1.03x-1.09x</small></span>
      <span><i style="--legend-color:#b91c1c"></i><strong>Hot</strong><small>1.10x-1.20x+</small></span>
      <span><i style="--legend-color:#9ca3af"></i><strong>No list price</strong><small>sold, sale/list n/a</small></span>
      <span><i class="ring" style="--legend-color:#7c3aed"></i><strong>Active listing</strong><small>for sale now, not yet sold</small></span>
    </div>
  `;
}

function geoMappableRows() {
  const { state } = ctx;
  if (!state.derived) return [];
  const mappable = state.derived.viewRows.filter((row) => {
    if (!Number.isFinite(row.mapLat) || !Number.isFinite(row.mapLon)) return false;
    if (state.geo.hideActive && isActiveRow(row)) return false;
    return true;
  });
  // Flip supersede: when a property is both an active listing and a prior sale in
  // the visible set, show only the active marker (the relist supersedes the sale)
  // rather than stacking two dots. Match on normalized street, and require a clean
  // 1:1 — exactly one active and one sale at that street — so multi-unit buildings
  // (several actives/sales sharing a street) are never collapsed onto each other.
  const activeAt = new Map();
  const soldAt = new Map();
  for (const row of mappable) {
    const key = streetSupersedeKey(row);
    if (!key) continue;
    if (isActiveRow(row)) activeAt.set(key, (activeAt.get(key) || 0) + 1);
    else if (row.hasActualClose) soldAt.set(key, (soldAt.get(key) || 0) + 1);
  }
  const superseded = new Set();
  for (const [key, count] of activeAt) {
    if (count === 1 && soldAt.get(key) === 1) superseded.add(key);
  }
  if (!superseded.size) return mappable;
  return mappable.filter((row) => isActiveRow(row) || !superseded.has(streetSupersedeKey(row)));
}

function mountOrRefreshMap(rows = geoMappableRows()) {
  const { state, qs, markDirty } = ctx;
  const L = state.geo.leaflet;
  const mapEl = qs("#map");
  if (!L || !mapEl || !state.derived) return;
  if (state.geo.map && state.geo.map.getContainer() !== mapEl) {
    state.geo.map.off();
    state.geo.map.remove();
    state.geo.map = null;
    state.geo.layer = null;
    state.geo.mapEl = null;
    state.geo.hasFitBounds = false;
  }
  const shouldFitBounds = !state.geo.map || !state.geo.hasFitBounds;
  if (!state.geo.map) {
    state.geo.map = L.map(mapEl, { preferCanvas: true }).setView([47.64, -122.34], 11);
    state.geo.mapEl = mapEl;
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(state.geo.map);
    state.geo.layer = L.layerGroup().addTo(state.geo.map);
    state.geo.map.on("moveend", () => {
      if (!state.geo.viewportFilter) return;
      const b = state.geo.map.getBounds();
      state.geo.mapBounds = { north: b.getNorth(), south: b.getSouth(), east: b.getEast(), west: b.getWest() };
      markDirty("geo");
    });
  } else {
    state.geo.map.invalidateSize();
  }
  state.geo.layer.clearLayers();
  // Always draw active listings (current inventory — small N, high interest) and
  // paint them LAST so they sit on top; fill the rest up to the cap underneath.
  // Without this, the draw cap silently dropped every active row whenever they
  // sorted past position 1,200 in the view.
  const DRAW_CAP = 1200;
  const activeRows = rows.filter(isActiveRow);
  const otherRows = rows.filter((row) => !isActiveRow(row));
  const drawnRows = otherRows.slice(0, Math.max(0, DRAW_CAP - activeRows.length)).concat(activeRows);
  drawnRows.forEach((row) => {
    const selected = state.geo.selectedPropertyKeys.includes(row.mapPropertyKey);
    // Active listings render as a violet hollow ring so "for sale now" reads
    // distinctly from a sold comp (filled, colored by sale/list pressure) and
    // from a list-less sale (filled grey).
    const active = isActiveRow(row);
    const dotColor = active ? ACTIVE_LISTING_COLOR : ratioColor(row.saleToList);
    const marker = L.circleMarker([row.mapLat, row.mapLon], {
      radius: selected ? 7 : (active ? 6 : 5),
      color: selected ? "#111827" : dotColor,
      fillColor: dotColor,
      fillOpacity: active ? (selected ? 0.5 : 0.25) : (selected ? 0.95 : 0.7),
      weight: active ? 2.5 : (selected ? 3 : 1),
      dashArray: row.isProjectionRow ? "3 3" : null,
    });
    marker.bindTooltip(
      geoCardHtml(row, { withLinks: false }),
      { direction: "top", offset: [0, -4], className: "geo-marker-tooltip", sticky: true }
    );
    marker.bindPopup(geoCardHtml(row, { withLinks: true }));
    marker.on("click", () => {
      toggleMapSelection(row.mapPropertyKey);
    });
    marker.addTo(state.geo.layer);
    if (row.mapPropertyKey === state.geo.popupPropertyKey) marker.openPopup();
  });
  state.geo.map.invalidateSize();
  if (shouldFitBounds && drawnRows.length) {
    const bounds = L.latLngBounds(drawnRows.map((row) => [row.mapLat, row.mapLon]));
    state.geo.map.fitBounds(bounds, { padding: [24, 24], maxZoom: 13 });
    state.geo.hasFitBounds = true;
  }
  renderGeoSelectedRows();
}

// A genuinely active listing: an MLS "Active" status with no recorded close.
// (An earlier form OR-ed in `!hasActualClose && pendingListPrice > 0`, which also
// swept in ~5.5k county rows that merely lack a close price — those are not
// listings. Status + no-close keeps it to real for-sale inventory, matching how
// the bid lab scopes active rows.) Shared by the marker, the visibility filter,
// the flip supersede, and the popup card.
function isActiveRow(row) {
  return row.mlsStatusNorm === "ACTIVE" && !row.hasActualClose;
}

// Normalized street identity for flip detection (a home that sold and is now
// relisted). ZIP is intentionally excluded — active-scrape rows can carry a wrong
// zip — while any unit in the address is kept so distinct units don't merge.
// Returns null for placeholder/parcel-only "addresses" that can't identify a home.
function streetSupersedeKey(row) {
  const addr = String(row.address || "").trim().toUpperCase();
  if (!addr) return null;
  const street = addr.split(",")[0].replace(/\s+/g, " ").trim();
  if (!/\d/.test(street) || street.includes("UNAVAILABLE") || street.startsWith("PARCEL")) return null;
  return street;
}

function ratioColor(value) {
  const n = Number(value || 0);
  if (n <= 0) return RATIO_NEUTRAL_COLOR; // no real list price -> unknown pressure
  if (n >= 1.1) return "#b91c1c";
  if (n >= 1.03) return "#c77700";
  if (n >= 1) return "#2563eb";
  return "#047857";
}

export function renderGeoSelectedRows(deps) {
  if (deps) ctx = deps;
  const { state, qs, propertyAddressLink } = ctx;
  const wrap = qs("#geoSelectedRows");
  if (!wrap || !state.derived) return;
  const selected = state.derived.viewRows.filter((row) => state.geo.selectedPropertyKeys.includes(row.mapPropertyKey));
  const filterCount = state.geo.filterPropertyKeys.length;
  const selectedCaption = selected.length
    ? `<div class="caption-row">${renderUniverseCaption({ count: selected.length, universeLabel: "properties picked from the map" })}</div>`
    : "";
  wrap.innerHTML = selected.length
    ? `<div class="geo-selected-status">${formatWholeNumber(selected.length)} selected for inspection${filterCount ? ` · ${formatWholeNumber(filterCount)} applied as dashboard filter` : ""}</div>${selectedCaption}${selected.map((row) => { const dates = geoDatesLineHtml(row); return `<article class="mini-record">${propertyAddressLink(row, "mini-record-link")}<span>${esc(row.neighborhoodLabel)} · ${formatMoneyOrNa(row.closePrice || row.pendingListPrice)}</span>${dates}</article>`; }).join("")}`
    : `<div class="empty-state">Click map points to inspect properties. Use "Filter dashboard to selected" only when you want the rest of the app to narrow.</div>`;
}

function toggleMapSelection(key) {
  const { state, markDirty } = ctx;
  if (!key) return;
  if (state.geo.selectedPropertyKeys.includes(key)) {
    state.geo.selectedPropertyKeys = state.geo.selectedPropertyKeys.filter((item) => item !== key);
    state.geo.popupPropertyKey = "";
  } else {
    state.geo.selectedPropertyKeys = [...state.geo.selectedPropertyKeys, key];
    state.geo.popupPropertyKey = key;
  }
  markDirty("geo");
}
