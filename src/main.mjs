import "./styles.css";
import {
  Activity,
  BarChart3,
  ChevronLeft,
  ChevronRight,
  ClipboardList,
  Database,
  Download,
  ExternalLink,
  FileText,
  Home,
  Info,
  Layers,
  Map as MapIcon,
  Moon,
  RefreshCcw,
  Rows3,
  Search,
  SlidersHorizontal,
  Sun,
  Target,
  Upload,
  Wallet,
  Waves,
  createIcons,
} from "lucide";
import {
  BUYER_PROFILE_FILE,
  DEFAULT_DATASET,
  REFRESH_REPORT_FILE,
  PRICE_SLIDER_CAP,
  PRICE_SLIDER_MIN,
  PRICE_SLIDER_STEP,
  domMetric,
  parseCsv,
  specialSaleFilterLabel,
  zillowUrl,
} from "./domain/data.mjs";
import {
  formatDateTime,
  formatMoney,
  formatMoneyCompact,
  formatPct,
  formatWholeNumber,
  monthLabelCompact,
  esc,
} from "./domain/format.mjs";
import {
  DEFAULT_PROFILE_MEMORY,
  normalizeProfileMemory,
  profileScore,
} from "./domain/buyerProfile.mjs";
import { computeAffordability } from "./domain/affordability.mjs";
import {
  BID_STRATEGIES,
  DESKTOP_PAGE_SIZE,
  MOBILE_PAGE_SIZE,
  applyBidViewInteractions,
  buildBidCompPool,
  buildOptions,
  buildPulseSnapshot,
  computeActiveBidSuggestions,
  computeSlices,
  createDefaultFilters,
  createEmptyInteractions,
  exportRowsToCsv,
  filtersToSummary,
  mergeAffordabilityTier,
  mergeBidFields,
  matchesSharedGlobalFilters,
  normalizeFilters,
  recordViewLabel,
} from "./domain/selectors.mjs";
import { initExplainLayer } from "./ui/explain.mjs";
import { getExplainEntry, renderOverviewView } from "./views/overview.mjs";
import { renderPulseView } from "./views/pulse.mjs";
import { renderBidsView } from "./views/bids.mjs";
import { renderAffordView } from "./views/afford.mjs";
import { renderRecordsView } from "./views/records.mjs";
import { renderGeoView } from "./views/geo.mjs";
import { renderDataView } from "./views/data.mjs";

const LUCIDE_ICONS = {
  Activity,
  BarChart3,
  ChevronLeft,
  ChevronRight,
  ClipboardList,
  Database,
  Download,
  ExternalLink,
  FileText,
  Home,
  Info,
  Layers,
  Map: MapIcon,
  Moon,
  RefreshCcw,
  Rows3,
  Search,
  SlidersHorizontal,
  Sun,
  Target,
  Upload,
  Wallet,
  Waves,
};

const PUBLIC_BASE = import.meta.env.BASE_URL || "./";
const BUYER_PROFILE_TOGGLE_STORAGE_KEY = "buyer_lens_profile_views_enabled";
const THEME_STORAGE_KEY = "buyer_lens_theme";
const WATCHED_STORAGE_KEY = "buyer_lens_watched_ids_v1";

function loadWatchedIds() {
  try {
    const raw = localStorage.getItem(WATCHED_STORAGE_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed.map(String) : []);
  } catch {
    return new Set();
  }
}

function saveWatchedIds(set) {
  try {
    localStorage.setItem(WATCHED_STORAGE_KEY, JSON.stringify([...set]));
  } catch {
    // Ignore storage errors.
  }
}

const AFFORD_CONFIG_FILE = "affordability.config.json";
const AFFORD_SCENARIO_KEY = "buyer_lens_afford_scenario_v1";

function loadAffordScenario() {
  try {
    const raw = localStorage.getItem(AFFORD_SCENARIO_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveAffordScenario(scenario) {
  try {
    localStorage.setItem(AFFORD_SCENARIO_KEY, JSON.stringify(scenario || {}));
  } catch {
    // Ignore storage errors (private browsing).
  }
}

const app = document.getElementById("app");
const state = {
  normalizedRows: [],
  options: {
    neighborhoods: [],
    types: ["All", "Single Family"],
    mlsStatuses: ["All"],
    defaultType: "Single Family",
  },
  filters: createDefaultFilters(["All", "Single Family"]),
  interactions: createEmptyInteractions(),
  flags: {
    projection: false,
    excludeLikelyPresoldNewBuild: false,
    includeOpenMls: true,
  },
  dataSource: {
    datasetName: DEFAULT_DATASET,
    rowCount: 0,
    kind: "default",
    report: null,
    elapsedMs: 0,
    status: "Loading dataset...",
    error: "",
  },
  buyerProfile: {
    memory: normalizeProfileMemory(DEFAULT_PROFILE_MEMORY),
    enabled: true,
    source: "embedded",
    ready: false,
  },
  affordability: {
    config: null,            // private config loaded from local file; null = unconfigured
    result: null,            // computeAffordability() output for the current scenario
    scenario: loadAffordScenario(), // persisted overrides (wait months, valuation, etc.)
    ready: false,            // true once a config has loaded
    source: "",
  },
  bid: {
    strategy: "balanced",
    highConfidenceOnly: false,
    watchedOnly: false,
    viewMode: "cards",
    manualEnabled: false,
    compSearchEnabled: false,
    manualSourceKey: "",
    activeLookup: new Map(),
  },
  watched: loadWatchedIds(),
  manualBid: {
    address: "",
    pendingListPrice: "",
    neighborhoodLabel: "",
    typeLabel: "Single Family",
    zip: "",
    dom: "",
    cdom: "",
  },
  pulseSliceGrain: "month",
  recordSort: { key: "saleDate", dir: "desc" },
  bidSort: { key: "confidence", dir: "desc" },
  recordsPage: 1,
  bidsPage: 1,
  activeView: "overview",
  mountedViews: new Set(["overview"]),
  dirtyViews: new Set(["overview", "pulse", "bids", "afford", "geo", "records", "data"]),
  geo: {
    leaflet: null,
    map: null,
    mapEl: null,
    layer: null,
    selectedPropertyKeys: [],
    filterPropertyKeys: [],
    popupPropertyKey: "",
    viewportFilter: false,
    hideActive: false,
    mapBounds: null,
    hasFitBounds: false,
  },
  derived: null,
  renderQueued: false,
};

function publicUrl(path) {
  return new URL(path, new URL(PUBLIC_BASE, window.location.href)).href;
}

function qs(selector, root = document) {
  return root.querySelector(selector);
}

function qsa(selector, root = document) {
  return [...root.querySelectorAll(selector)];
}

function refreshIcons() {
  createIcons({ icons: LUCIDE_ICONS });
}

function icon(name) {
  return `<i data-lucide="${esc(name)}" aria-hidden="true"></i>`;
}

function optionHtml(values, selectedValue) {
  return (values || []).map((value) => (
    `<option value="${esc(value)}" ${value === selectedValue ? "selected" : ""}>${esc(value)}</option>`
  )).join("");
}

function buttonIcon(label, iconName, attrs = "", extraClass = "") {
  return `<button class="btn ${extraClass}" type="button" ${attrs}>${icon(iconName)}<span>${esc(label)}</span></button>`;
}

function propertyAddressText(row) {
  return row?.address || "Address unavailable";
}

function propertyAddressLink(row, extraClass = "") {
  const label = propertyAddressText(row);
  return `<a class="address-link ${esc(extraClass)}" href="${esc(zillowUrl(row))}" target="_blank" rel="noopener noreferrer" aria-label="Open Zillow for ${esc(label)}"><span class="address-link-text">${esc(label)}</span><span class="external-link-label">Zillow</span>${icon("external-link")}</a>`;
}

function applyTheme(theme) {
  const normalized = theme === "dark" ? "dark" : "light";
  document.body.classList.toggle("dark", normalized === "dark");
  document.body.classList.toggle("light", normalized !== "dark");
  try {
    localStorage.setItem(THEME_STORAGE_KEY, normalized);
  } catch {
    // Ignore storage errors in private browsing.
  }
  const toggle = qs("#themeToggle");
  if (toggle) {
    toggle.innerHTML = `${icon(normalized === "dark" ? "sun" : "moon")}<span>${normalized === "dark" ? "Light" : "Dark"}</span>`;
    refreshIcons();
  }
}

function initTheme() {
  let saved = "light";
  try {
    saved = localStorage.getItem(THEME_STORAGE_KEY) || "light";
  } catch {
    saved = "light";
  }
  applyTheme(saved);
}

function initBuyerProfileToggle() {
  try {
    const saved = localStorage.getItem(BUYER_PROFILE_TOGGLE_STORAGE_KEY);
    if (saved !== null) state.buyerProfile.enabled = saved === "true";
  } catch {
    state.buyerProfile.enabled = true;
  }
}

function setBuyerProfileToggle(enabled) {
  state.buyerProfile.enabled = !!enabled;
  try {
    localStorage.setItem(BUYER_PROFILE_TOGGLE_STORAGE_KEY, String(!!enabled));
  } catch {
    // Ignore storage errors.
  }
  markDirty();
}

function renderShell() {
  app.innerHTML = `
    <div class="app-shell">
      <header class="topbar">
        <a class="brand" href="./" aria-label="Seattle Pending Tracker home">
          <img src="${esc(publicUrl("assets/ebg-icon.svg"))}" alt="" width="34" height="34" />
          <span>
            <strong>Seattle Pending Tracker</strong>
            <small>Buyer command center</small>
          </span>
        </a>
        <div class="topbar-actions">
          ${buttonIcon("Refresh", "refresh-ccw", "id=\"reloadDatasetBtn\"", "ghost")}
          ${buttonIcon("Export", "download", "id=\"exportCsvBtn\"", "ghost")}
          <button class="btn ghost" id="themeToggle" type="button">${icon("moon")}<span>Dark</span></button>
        </div>
      </header>

      <main>
        <section class="control-band" aria-label="Dashboard controls">
          <div class="active-filter-strip">
            <div>
              <span class="mini-label">Active defaults</span>
              <div class="chip-row" id="activeFilterChips"></div>
            </div>
            <div class="dataset-pill" id="datasetStatus" aria-live="polite">Loading dataset...</div>
          </div>
          <details class="filters-details" id="globalFilters">
            <summary>${icon("sliders-horizontal")}<span>Global filters</span></summary>
            <div class="filters-grid" id="filterControls"></div>
          </details>
          <div class="cross-filter-strip" id="crossFilterChips"></div>
        </section>

        <nav class="tabs" role="tablist" aria-label="Dashboard views">
          ${tabButton("overview", "Overview", "home", true)}
          ${tabButton("pulse", "Pulse", "activity")}
          ${tabButton("bids", "Bids", "target")}
          ${tabButton("afford", "Afford", "wallet")}
          ${tabButton("geo", "Geo", "map")}
          ${tabButton("records", "Records", "rows-3")}
          ${tabButton("data", "Data", "database")}
        </nav>

        <section class="view active" id="view-overview" role="tabpanel" aria-labelledby="tab-overview" tabindex="0"></section>
        <section class="view" id="view-pulse" role="tabpanel" aria-labelledby="tab-pulse" tabindex="0" aria-hidden="true"></section>
        <section class="view" id="view-bids" role="tabpanel" aria-labelledby="tab-bids" tabindex="0" aria-hidden="true"></section>
        <section class="view" id="view-afford" role="tabpanel" aria-labelledby="tab-afford" tabindex="0" aria-hidden="true"></section>
        <section class="view" id="view-geo" role="tabpanel" aria-labelledby="tab-geo" tabindex="0" aria-hidden="true"></section>
        <section class="view" id="view-records" role="tabpanel" aria-labelledby="tab-records" tabindex="0" aria-hidden="true"></section>
        <section class="view" id="view-data" role="tabpanel" aria-labelledby="tab-data" tabindex="0" aria-hidden="true"></section>

        <details class="help-panel">
          <summary>${icon("info")}<span>How to use this dashboard</span></summary>
          <div class="help-grid">
            <p>Use the command center first: confirm the active filter band, scan market pressure, then jump to Pulse, Bids, Geo, or Records.</p>
            <p>Filters affect every view. Table sorting and CSV export operate on the full filtered set, while Records and Bids only render one page at a time.</p>
            <p>Geo loads its local Leaflet bundle only after the Geo tab opens.</p>
          </div>
        </details>
      </main>
    </div>
  `;
  refreshIcons();
}

function tabButton(view, label, iconName, active = false) {
  return `
    <button
      class="tab ${active ? "active" : ""}"
      id="tab-${esc(view)}"
      data-view="${esc(view)}"
      type="button"
      role="tab"
      aria-selected="${active ? "true" : "false"}"
      aria-controls="view-${esc(view)}"
      tabindex="${active ? "0" : "-1"}"
    >${icon(iconName)}<span>${esc(label)}</span></button>
  `;
}

function renderLoadingState() {
  const overview = qs("#view-overview");
  if (overview) {
    overview.innerHTML = `
      <section class="command-center" id="commandCenter" aria-label="Buyer command center">
        <div class="hero-copy">
          <p class="eyebrow">MLS-enriched Seattle pending and sold lens</p>
          <h1>Command center for the next offer.</h1>
          <p class="lead">Defaulting to Single Family homes in the $1.1M-$1.4M band, with market pressure, saved-home fit, and direct jumps into Pulse, Bids, Geo, and Records.</p>
        </div>
        <div class="command-grid" id="commandGrid">
          <article class="state-panel loading-panel">
            <div class="panel-kicker">Dataset</div>
            <h2>${esc(state.dataSource.status || "Loading dataset...")}</h2>
            <p>${state.dataSource.error ? esc(state.dataSource.error) : "Parsing and normalizing in a module worker so the interface stays responsive."}</p>
          </article>
        </div>
      </section>
    `;
  }
  const dataset = qs("#datasetStatus");
  if (dataset) dataset.textContent = state.dataSource.status || "Loading dataset...";
}

function recomputeDerived() {
  const filters = normalizeFilters(state.filters, state.options);
  state.filters = filters;
  // Affordability: compute one result for the current scenario, then tag every
  // normalized row with its tier BEFORE slices/filters run (the affordability
  // global filter reads row.affordTier). Null when no private config loaded.
  const affordResult = state.affordability.ready && state.affordability.config
    ? computeAffordability(state.affordability.config, {}, state.affordability.scenario || {})
    : null;
  state.affordability.result = affordResult;
  mergeAffordabilityTier(state.normalizedRows, affordResult);
  const slices = computeSlices(filters, state.normalizedRows, {
    flags: state.flags,
    interactions: state.interactions,
    geo: state.geo,
  });
  const compPool = buildBidCompPool(filters, state.normalizedRows);
  const bidResult = computeActiveBidSuggestions(filters, state.normalizedRows, state.bid.strategy, { compPool });
  let bidRows = applyBidViewInteractions(bidResult.rows, state.interactions, state.geo);
  if (state.bid.highConfidenceOnly) {
    bidRows = bidRows.filter((row) => row.bidStatus === "SCored" && row.bidConfidence >= 75);
  }
  const bidScoredView = bidRows.filter((row) => row.bidStatus === "SCored");
  const bidStatsView = {
    activeCount: bidRows.length,
    scoredCount: bidScoredView.length,
    highConfidenceCount: bidRows.filter((row) => row.bidConfidence >= 75).length,
    insufficientCount: bidRows.filter((row) => row.bidStatus !== "SCored").length,
    medianOverAskPct: bidScoredView.length
      ? bidScoredView.reduce((sum, row) => sum + ((row.bidSuggested - row.pendingListPrice) / row.pendingListPrice) * 100, 0) / bidScoredView.length
      : 0,
  };
  state.bid.activeLookup = new Map(bidRows.map((row) => [row.mapPropertyKey, row]));
  let viewRows = slices.recordGeoRows.map((row) => mergeBidFields(row, bidResult.byKey));
  if (state.geo.filterPropertyKeys.length) {
    const validMapKeys = new Set(viewRows.map((row) => row.mapPropertyKey));
    state.geo.filterPropertyKeys = state.geo.filterPropertyKeys.filter((key) => validMapKeys.has(key));
    viewRows = viewRows.filter((row) => !state.geo.filterPropertyKeys.length || state.geo.filterPropertyKeys.includes(row.mapPropertyKey));
  }

  state.derived = {
    slices,
    compPool,
    bidResult,
    bidRows,
    bidStatsView,
    viewRows,
    sliceMonthlySeries: buildSliceMonthlySeries(slices.closedSlice).slice(-12),
    inventoryMonthlySeries: buildInventoryMonthlySeries(state.normalizedRows, filters),
    latestSaleDate: slices.closedRows.map((row) => row.saleDate).filter(Boolean).sort().slice(-1)[0] || "",
    pulse: buildPulseSnapshot(state.normalizedRows, filters, {
      selectedGroup: state.pulseSelectedGroup || "primary",
      flags: state.flags,
    }),
  };
}

function markDirty(view = null) {
  if (view) state.dirtyViews.add(view);
  else ["overview", "pulse", "bids", "geo", "records", "data"].forEach((name) => state.dirtyViews.add(name));
  if (state.renderQueued) return;
  state.renderQueued = true;
  requestAnimationFrame(() => {
    state.renderQueued = false;
    renderDashboard();
  });
}

function renderDashboard() {
  if (!state.normalizedRows.length) {
    renderFilterControls();
    renderLoadingState();
    renderDataView(dataDeps());
    refreshIcons();
    return;
  }
  recomputeDerived();
  renderFilterControls();
  renderActiveFilterChips();
  renderCrossFilterChips();
  renderView(state.activeView);
  renderDataSourcePill();
  refreshIcons();
}

function renderFilterControls() {
  const wrap = qs("#filterControls");
  if (!wrap) return;
  const f = state.filters;
  const maxValue = f.maxClose === null ? PRICE_SLIDER_CAP : f.maxClose;
  wrap.innerHTML = `
    <div class="field">
      <label for="fType">Property type</label>
      <select id="fType">${optionHtml(state.options.types, f.type)}</select>
      <details class="exclude-types-field" id="fExcludeTypes">
        <summary>${(f.excludeTypes || []).length ? `Excluding ${(f.excludeTypes || []).length}` : "Exclude types..."}</summary>
        <div class="multi-options" id="fExcludeTypesOptions">
          ${(state.options.types || []).filter((t) => t && t !== "All").map((name) => `
            <label class="multi-opt">
              <input type="checkbox" data-exclude-type="${esc(name)}" ${(f.excludeTypes || []).includes(name) ? "checked" : ""} />
              <span>${esc(name)}</span>
            </label>
          `).join("")}
        </div>
      </details>
    </div>
    <div class="field">
      <label for="fMlsStatus">MLS status</label>
      <select id="fMlsStatus">${optionHtml(state.options.mlsStatuses, f.mlsStatus)}</select>
    </div>
    <div class="field">
      <label for="fScope">Competition scope</label>
      <select id="fScope">
        ${optionHtml(["all", "hot10", "ultra5"], f.scope).replace(">all<", ">All rows<").replace(">hot10<", ">Hot <=10 DOM<").replace(">ultra5<", ">Ultra <=5 DOM<")}
      </select>
    </div>
    <div class="field">
      <label for="fSpecialSale">Special sale</label>
      <select id="fSpecialSale">
        <option value="all" ${f.specialSale === "all" ? "selected" : ""}>All</option>
        <option value="exclude" ${f.specialSale === "exclude" ? "selected" : ""}>Exclude special-sale</option>
        <option value="only" ${f.specialSale === "only" ? "selected" : ""}>Special-sale only</option>
      </select>
      <span class="hint">MLS-only extras are neighborhood-scoped.</span>
    </div>
    <div class="field range-field">
      <label for="fMinClose">Min close</label>
      <input id="fMinClose" type="range" min="${PRICE_SLIDER_MIN}" max="${PRICE_SLIDER_CAP}" step="${PRICE_SLIDER_STEP}" value="${esc(f.minClose)}" />
      <output id="fMinCloseValue">${esc(formatMoney(f.minClose))}</output>
    </div>
    <div class="field range-field">
      <label for="fMaxClose">Max close</label>
      <input id="fMaxClose" type="range" min="${PRICE_SLIDER_MIN}" max="${PRICE_SLIDER_CAP}" step="${PRICE_SLIDER_STEP}" value="${esc(maxValue)}" />
      <output id="fMaxCloseValue">${f.maxClose === null ? "No max" : esc(formatMoney(f.maxClose))}</output>
    </div>
    <div class="field">
      <label for="fMinLot">Min lot size (sq ft)</label>
      <input id="fMinLot" type="number" min="0" step="500" value="${esc(f.minLot || "")}" placeholder="0" />
    </div>
    <div class="field">
      <label for="fMaxLot">Max lot size (sq ft)</label>
      <input id="fMaxLot" type="number" min="0" step="500" value="${esc(f.maxLot || "")}" placeholder="No max" />
    </div>
    <div class="field">
      <label for="fAffordability">Affordability</label>
      <select id="fAffordability" ${state.affordability.ready ? "" : "disabled"}>
        <option value="all" ${f.affordability === "all" ? "selected" : ""}>All listings</option>
        <option value="in_budget" ${f.affordability === "in_budget" ? "selected" : ""}>In budget only</option>
        <option value="in_budget_stretch" ${f.affordability === "in_budget_stretch" ? "selected" : ""}>In budget + stretch</option>
      </select>
      <span class="hint">${state.affordability.ready ? "Uses your Afford-tab scenario." : "Add a local affordability config to enable."}</span>
    </div>
    <div class="field">
      <label for="fDateFrom">From sale date</label>
      <input id="fDateFrom" type="date" value="${esc(f.dateFrom)}" />
    </div>
    <div class="field">
      <label for="fDateTo">To sale date</label>
      <input id="fDateTo" type="date" value="${esc(f.dateTo)}" />
    </div>
    <div class="field">
      <label for="fRecordView">Records / map view</label>
      <select id="fRecordView">
        <option value="all" ${f.recordView === "all" ? "selected" : ""}>All visible rows</option>
        <option value="projOnly" ${f.recordView === "projOnly" ? "selected" : ""}>Projected only</option>
        <option value="openOnly" ${f.recordView === "openOnly" ? "selected" : ""}>Open/pending only</option>
        <option value="projAndOpen" ${f.recordView === "projAndOpen" ? "selected" : ""}>Projected + open/pending</option>
      </select>
    </div>
    <div class="field checks-field">
      <label>Feature flags</label>
      <label class="check"><input type="checkbox" id="ffProjection" ${state.flags.projection ? "checked" : ""} /> Pending projection</label>
      <label class="check"><input type="checkbox" id="ffIncludeOpenMls" ${state.flags.includeOpenMls ? "checked" : ""} /> Include open/pending MLS</label>
      <label class="check"><input type="checkbox" id="ffExcludePresold" ${state.flags.excludeLikelyPresoldNewBuild ? "checked" : ""} /> Exclude likely pre-sold new builds</label>
    </div>
    <details class="field neighborhoods-field" id="fNeighborhood">
      <summary id="fNeighborhoodSummary">${state.filters.neighborhoods.length ? `${state.filters.neighborhoods.length} selected` : "All neighborhoods"}</summary>
      <div class="multi-actions">
        <button class="mini-btn" type="button" id="fNeighborhoodSelectAll">Select all</button>
        <button class="mini-btn" type="button" id="fNeighborhoodClear">Clear</button>
      </div>
      <div class="multi-options" id="fNeighborhoodOptions">
        ${state.options.neighborhoods.map((name) => `
          <label class="multi-opt">
            <input type="checkbox" value="${esc(name)}" ${state.filters.neighborhoods.includes(name) ? "checked" : ""} />
            <span>${esc(name)}</span>
          </label>
        `).join("") || `<div class="note">No neighborhoods loaded yet.</div>`}
      </div>
    </details>
    <div class="field upload-field">
      <label for="csvFile">Upload CSV</label>
      <input id="csvFile" type="file" accept=".csv,text/csv" />
      <span class="hint" id="uploadStatus">${esc(state.dataSource.status || "")}</span>
    </div>
    <div class="filter-actions">
      ${buttonIcon("Reset", "refresh-ccw", "id=\"resetBtn\"", "alt")}
      ${buttonIcon("Clear cross-filters", "search", "id=\"clearCrossBtn\"", "alt")}
    </div>
  `;
  refreshIcons();
}

function renderActiveFilterChips() {
  const wrap = qs("#activeFilterChips");
  if (!wrap) return;
  wrap.innerHTML = filtersToSummary(state.filters)
    .map((item) => `<span class="chip">${esc(item)}</span>`)
    .join("");
}

function renderCrossFilterChips() {
  const wrap = qs("#crossFilterChips");
  if (!wrap || !state.derived) return;
  const chips = [];
  Object.entries(state.interactions).forEach(([key, value]) => {
    if (value) chips.push(`<span class="chip strong">${esc(crossFilterLabel(key, value))}<button type="button" data-clear-interaction="${esc(key)}">x</button></span>`);
  });
  if (state.geo.filterPropertyKeys.length) chips.push(`<span class="chip strong">Map filter: ${state.geo.filterPropertyKeys.length} properties<button type="button" data-clear-map-filter="1">x</button></span>`);
  if (state.geo.viewportFilter) chips.push(`<span class="chip strong">Map viewport<button type="button" data-clear-viewport-filter="1">x</button></span>`);
  if (state.flags.projection) chips.push(`<span class="chip">Pending projection<button type="button" data-clear-flag="projection">x</button></span>`);
  if (state.flags.includeOpenMls) chips.push(`<span class="chip">Open/Pending MLS<button type="button" data-clear-flag="includeOpenMls">x</button></span>`);
  if (state.flags.excludeLikelyPresoldNewBuild) chips.push(`<span class="chip">Exclude likely pre-sold<button type="button" data-clear-flag="excludePresold">x</button></span>`);
  if (state.filters.recordView !== "all") chips.push(`<span class="chip">Map/Records: ${esc(recordViewLabel(state.filters.recordView))}<button type="button" data-clear-record-view="1">x</button></span>`);
  if (state.filters.specialSale !== "all") chips.push(`<span class="chip">Special Sale: ${esc(specialSaleFilterLabel(state.filters.specialSale))}<button type="button" data-clear-special-sale="1">x</button></span>`);
  if (state.bid.strategy !== "balanced") chips.push(`<span class="chip">Bid Strategy: ${esc(BID_STRATEGIES[state.bid.strategy]?.label || state.bid.strategy)}<button type="button" data-clear-bid-strategy="1">x</button></span>`);
  if (state.bid.highConfidenceOnly) chips.push(`<span class="chip">Bids: High Confidence Only<button type="button" data-clear-bid-highconf="1">x</button></span>`);
  wrap.innerHTML = chips.length ? chips.join("") : `<span class="note">No cross-filters applied.</span>`;
}

function crossFilterLabel(key, value) {
  const labels = {
    month: "Month",
    neighborhood: "Neighborhood",
    type: "Type",
    ratioBucket: "Sale/List",
    hotMarket: "Heat",
    mode: "Mode",
    closeTier: "Close",
    psfTier: "Price/SF",
  };
  return `${labels[key] || key}: ${value}`;
}

function renderDataSourcePill() {
  const el = qs("#datasetStatus");
  if (!el) return;
  const report = state.dataSource.report;
  const refreshed = report?.generatedAt || report?.timestamp || "";
  el.textContent = `${state.dataSource.kind === "upload" ? "Uploaded" : "Loaded"} ${state.dataSource.datasetName} · ${formatWholeNumber(state.dataSource.rowCount)} rows${refreshed ? ` · refreshed ${formatDateTime(refreshed)}` : ""}`;
}

function renderView(view) {
  if (!state.mountedViews.has(view)) state.mountedViews.add(view);
  if (!state.dirtyViews.has(view) && view !== "geo") return;
  if (view === "overview") renderOverviewView(overviewDeps());
  if (view === "pulse") renderPulseView(pulseDeps());
  if (view === "bids") renderBidsView(bidsDeps());
  if (view === "afford") renderAffordView(affordDeps());
  if (view === "records") renderRecordsView(recordsDeps());
  if (view === "data") renderDataView(dataDeps());
  if (view === "geo") {
    renderGeoView(geoDeps());
  }
  state.dirtyViews.delete(view);
}

// Every tab renderer lives in src/views/*.mjs; main.mjs stays the owner of
// app state and the shared helpers, and hands them to each view through a
// per-view deps object on every render.
function overviewDeps() {
  return {
    state,
    qs,
    icon,
    buttonIcon,
    miniMetric,
    sparklineSvg,
    chartMetricLabel,
    pulseMetricConfig,
    medianValue,
    minTileComps: MIN_TILE_COMPS,
  };
}

function pulseDeps() {
  return {
    state,
    qs,
    chartData,
    chartDomain,
    chartMetricLabel,
    formatChartValue,
    pulseMetricConfig,
    medianValue,
    groupRows,
    buildSliceMonthlySeries,
    minTileComps: MIN_TILE_COMPS,
  };
}

function bidsDeps() {
  return {
    state,
    qs,
    miniMetric,
    buttonIcon,
    optionHtml,
    propertyAddressLink,
    affordTierBadge,
    currentPageSize,
    paginationControls,
  };
}

function affordDeps() {
  return {
    state,
    qs,
    miniMetric,
    buttonIcon,
    refreshIcons,
  };
}

function recordsDeps() {
  return {
    state,
    qs,
    propertyAddressLink,
    affordTierBadge,
    currentPageSize,
    paginationControls,
  };
}

function geoDeps() {
  return {
    state,
    qs,
    buttonIcon,
    refreshIcons,
    markDirty,
    propertyAddressText,
    propertyAddressLink,
    affordTierMeta: AFFORD_TIER_META,
  };
}

function dataDeps() {
  return {
    state,
    qs,
  };
}

function miniMetric(label, value) {
  return `<div class="mini-metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
}

function pulseMetricConfig(key) {
  if (key === "salesCount") return { label: "Sales Count", format: (value) => formatWholeNumber(value || 0), delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}` };
  if (key === "activeInventory") return { label: "Active + Pending", format: (value) => formatWholeNumber(value || 0), delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}` };
  if (key === "hotShare") return { label: "Fast-Sale Share", format: (value) => value === null ? "n/a" : formatPct(value), delta: (value) => `${value >= 0 ? "+" : ""}${((value || 0) * 100).toFixed(1)} pts` };
  if (key === "overAskShare") return { label: "Share Sold Over Ask", format: (value) => value === null ? "n/a" : formatPct(value), delta: (value) => `${value >= 0 ? "+" : ""}${((value || 0) * 100).toFixed(1)} pts` };
  if (key === "medianDom") return { label: "Median DOM", format: (value) => value === null ? "n/a" : `${Math.round(value)}d`, delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}d` };
  if (key === "medianSaleToList") return { label: "Median Sale/List", format: (value) => value === null ? "n/a" : `${value.toFixed(2)}x`, delta: (value) => `${value >= 0 ? "+" : ""}${(value || 0).toFixed(3)}x` };
  if (key === "medianBidUp") return { label: "Median Bid-Up", format: (value) => value === null ? "n/a" : formatMoneyCompact(value), delta: (value) => `${value >= 0 ? "+" : ""}${formatMoneyCompact(value || 0)}` };
  if (key === "medianPsf") return { label: "Median $/Sqft", format: (value) => value === null ? "n/a" : `$${Math.round(value)}`, delta: (value) => `${value >= 0 ? "+" : ""}$${Math.round(value || 0)}` };
  return { label: "Median Close", format: (value) => value === null ? "n/a" : formatMoneyCompact(value), delta: (value) => `${value >= 0 ? "+" : ""}${formatMoneyCompact(value || 0)}` };
}

function chartMetricLabel(metricKey) {
  return pulseMetricConfig(metricKey).label;
}

function formatChartValue(metricKey, value) {
  return pulseMetricConfig(metricKey).format(value);
}

// Default the per-point confidence count to the metric's real coverage where
// it differs from total sales: sale/list ratio is only meaningful on rows that
// carry a genuine list price, so the badge/gating must read ratioSampleSize,
// not salesCount. Callers may override via options.sampleField.
function defaultSampleField(metricKey) {
  if (metricKey === "medianSaleToList") return "ratioSampleSize";
  if (metricKey === "overAskShare") return "ratioSampleSize";
  if (metricKey === "medianBidUp") return "bidUpSampleSize";
  return undefined;
}

function chartData(series, metricKey, options = {}) {
  const sampleField = options.sampleField || defaultSampleField(metricKey);
  return (series || [])
    .map((entry) => {
      const raw = entry[metricKey];
      const value = raw === null || raw === undefined || raw === "" ? Number.NaN : Number(raw);
      const count = sampleField && entry[sampleField] !== undefined
        ? Number(entry[sampleField]) || 0
        : Number(entry.sampleSize ?? entry.salesCount) || 0;
      return { month: entry.month, value, count, label: entry.label || "", isPartial: !!entry.isPartial };
    })
    .filter((entry) => Number.isFinite(entry.value));
}

function chartDomain(data, metricKey, includeZero = false) {
  let min = includeZero ? 0 : Math.min(...data.map((entry) => entry.value));
  let max = Math.max(...data.map((entry) => entry.value));
  if (!Number.isFinite(min)) min = 0;
  if (!Number.isFinite(max)) max = 1;
  const span = Math.max(max - min, 0.0001);
  min = includeZero ? 0 : min - span * 0.08;
  max += span * 0.08;
  if (metricKey === "hotShare") {
    min = Math.max(0, min);
    max = Math.min(1, Math.max(max, 0.01));
  }
  if (metricKey === "medianSaleToList") min = Math.max(0, min);
  if (min === max) max = min + 1;
  return { min, max };
}

const MIN_TILE_COMPS = 5;

function sparklineSvg(series, metricKey, options = {}) {
  const sampleField = options.sampleField || defaultSampleField(metricKey);
  const gated = (series || []).filter((entry) => ((sampleField ? entry[sampleField] : undefined) ?? entry.sampleSize ?? entry.salesCount ?? Infinity) >= MIN_TILE_COMPS);
  const data = chartData(gated, metricKey, { sampleField });
  const w = options.width || 124;
  const h = options.height || 34;
  const big = w >= 200;
  const pad = 3;
  const label = chartMetricLabel(metricKey);
  if (data.length < 2) {
    // Speak the gated-empty case rather than hiding it: a tile that softens to
    // "no plottable trend" is itself a signal a screen-reader user should hear.
    return `<svg class="sparkline" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(`${label}: not enough recent comps to plot a trend`)}"><title>${esc(`${label}: not enough recent comps (n<${MIN_TILE_COMPS}) to plot`)}</title></svg>`;
  }
  const domain = chartDomain(data, metricKey);
  const span = Math.max(domain.max - domain.min, 0.0001);
  const first = data[0];
  const last = data[data.length - 1];
  const firstStr = formatChartValue(metricKey, first.value);
  const lastStr = formatChartValue(metricKey, last.value);
  // Reserve room on the right for the endpoint value label so it isn't clipped.
  // Scale the reserve to the label's character count (bold tabular-nums) so long
  // values like "$1.29M" don't overrun the viewBox on narrow mobile cards.
  const rightPad = big
    ? Math.max(48, Math.ceil(lastStr.length * 8) + 10)
    : Math.max(34, Math.ceil(lastStr.length * 7) + 6);
  const xAt = (index) => pad + (index / (data.length - 1)) * (w - pad - rightPad);
  const yAt = (value) => pad + (h - pad * 2) - ((value - domain.min) / span) * (h - pad * 2);
  const pts = data.map((entry, index) => `${index ? "L" : "M"}${xAt(index).toFixed(1)},${yAt(entry.value).toFixed(1)}`).join(" ");
  const lastX = xAt(data.length - 1);
  const lastY = yAt(last.value);
  const months = data.length;
  const dir = last.value > first.value ? "up" : last.value < first.value ? "down" : "flat";
  const pctChange = first.value ? ((last.value - first.value) / Math.abs(first.value)) * 100 : 0;
  const pctStr = Number.isFinite(pctChange) && Math.abs(pctChange) >= 0.05 ? ` (${pctChange >= 0 ? "+" : ""}${pctChange.toFixed(1)}%)` : "";
  const summary = `${label}: ${firstStr} → ${lastStr} over ${months} mo${pctStr}`;
  const ariaTrend = `${label}: ${dir === "up" ? "up" : dir === "down" ? "down" : "flat"} from ${firstStr} to ${lastStr} over ${months} months`;
  const labelClass = big ? "spark-endpoint-label big" : "spark-endpoint-label";
  return `
    <svg class="sparkline" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(ariaTrend)}">
      <title>${esc(summary)}</title>
      <path class="spark-line" d="${esc(pts)}" fill="none" />
      <circle class="spark-dot" cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="2" />
      <text class="${labelClass}" x="${(lastX + 4).toFixed(1)}" y="${Math.min(h - 2, Math.max(8, lastY + 3)).toFixed(1)}" text-anchor="start">${esc(lastStr)}</text>
      ${big ? `<text class="spark-endpoint-label muted" x="${pad}" y="${(h - 2).toFixed(1)}" text-anchor="start">${esc(monthLabelCompact(first.month))}</text><text class="spark-endpoint-label muted" x="${(lastX).toFixed(1)}" y="${(h - 2).toFixed(1)}" text-anchor="end">${esc(monthLabelCompact(last.month))}</text>` : ""}
    </svg>`;
}

function buildSliceMonthlySeries(rows) {
  const byMonth = groupRows(rows, (row) => row.saleDate ? row.saleDate.slice(0, 7) : "Unknown");
  return Object.entries(byMonth).sort((a, b) => a[0].localeCompare(b[0])).map(([month, monthRows]) => ({
    month,
    salesCount: monthRows.length,
    sampleSize: monthRows.length,
    medianClosePrice: medianValue(monthRows.map((row) => row.closePrice)),
    medianSaleToList: medianValue(monthRows.map((row) => row.saleToList).filter((value) => value > 0)),
    // Share of real-list-price sales that closed above ask; the ratio rows
    // (saleToList > 0) are the denominator, so the monthly trend matches the
    // cost-to-win block's eligibility rule.
    overAskShare: (() => {
      const ratioRows = monthRows.filter((row) => row.saleToList > 0);
      return ratioRows.length ? ratioRows.filter((row) => row.saleToList > 1).length / ratioRows.length : null;
    })(),
    medianPsf: medianValue(monthRows.map((row) => row.pricePerSqft).filter((value) => value > 0)),
    hotShare: monthRows.length ? monthRows.filter((row) => row.isHotMarket).length / monthRows.length : null,
    medianDom: medianValue(monthRows.map((row) => domMetric(row)).filter((value) => value !== null && value !== undefined)),
    medianBidUp: (() => { const b = monthRows.filter((row) => row.hasMarketListPrice && Number.isFinite(row.delta)); return b.length ? medianValue(b.map((row) => row.delta)) : null; })(),
    bidUpSampleSize: monthRows.filter((row) => row.hasMarketListPrice && Number.isFinite(row.delta)).length,
    ratioSampleSize: monthRows.filter((row) => row.saleToList > 0).length,
  }));
}

function buildInventoryMonthlySeries(normalizedRows, filterState) {
  // NOTE: counts listings by the month of effectiveDate; this is a per-month observed-active
  // proxy, NOT a true month-end standing-inventory snapshot (dataset is sale-centric).
  const candidates = (normalizedRows || [])
    .filter((row) => row.dataMode === "MLS_ENRICHED" && !row.hasActualClose && row.pendingListPrice > 0)
    .filter((row) => matchesSharedGlobalFilters(row, filterState, { priceField: "pendingListPrice", dateField: "effectiveDate" }));
  const byMonth = groupRows(candidates, (row) => (row.effectiveDate ? row.effectiveDate.slice(0, 7) : "Unknown"));
  return Object.entries(byMonth)
    .filter(([month]) => /^\d{4}-\d{2}$/.test(month))
    .sort((a, b) => a[0].localeCompare(b[0]))
    .slice(-12)
    .map(([month, monthRows]) => ({ month, activeInventory: monthRows.length, sampleSize: monthRows.length }));
}

function medianValue(values) {
  const nums = values.filter((value) => Number.isFinite(value));
  if (!nums.length) return null;
  nums.sort((a, b) => a - b);
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

function groupRows(rows, keyFn) {
  return (rows || []).reduce((acc, row) => {
    const key = keyFn(row);
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});
}

const AFFORD_TIER_META = {
  in_budget: { label: "In budget", cls: "afford-in" },
  stretch: { label: "Stretch", cls: "afford-stretch" },
  over: { label: "Over budget", cls: "afford-over" },
};

/** Small affordability pill for a row; empty string when unconfigured/untiered. */
function affordTierBadge(row) {
  if (!state.affordability.ready) return "";
  const meta = AFFORD_TIER_META[row?.affordTier];
  if (!meta) return "";
  return `<span class="afford-pill ${meta.cls}" title="vs your affordability scenario">${meta.label}</span>`;
}

function currentPageSize() {
  return window.matchMedia("(max-width: 760px)").matches ? MOBILE_PAGE_SIZE : DESKTOP_PAGE_SIZE;
}

function paginationControls(kind, page) {
  return `
    <div class="pagination" data-pagination="${esc(kind)}">
      <button class="icon-btn" type="button" data-page-kind="${esc(kind)}" data-page-target="${page.page - 1}" ${page.page <= 1 ? "disabled" : ""} aria-label="Previous page">${icon("chevron-left")}</button>
      <span>Page ${page.page} of ${page.pageCount}</span>
      <button class="icon-btn" type="button" data-page-kind="${esc(kind)}" data-page-target="${page.page + 1}" ${page.page >= page.pageCount ? "disabled" : ""} aria-label="Next page">${icon("chevron-right")}</button>
    </div>
  `;
}

function clearMapSelection() {
  state.geo.selectedPropertyKeys = [];
  state.geo.popupPropertyKey = "";
  markDirty("geo");
}

function applyMapSelectionFilter() {
  state.geo.filterPropertyKeys = state.geo.selectedPropertyKeys.slice();
  state.recordsPage = 1;
  state.bidsPage = 1;
  markDirty();
}

function clearMapSelectionFilter() {
  state.geo.filterPropertyKeys = [];
  state.recordsPage = 1;
  state.bidsPage = 1;
  markDirty();
}

function setActiveView(view, moveFocus = false) {
  const next = view || "overview";
  state.activeView = next;
  qsa(".tab").forEach((tab) => {
    const active = tab.dataset.view === next;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", active ? "true" : "false");
    tab.setAttribute("tabindex", active ? "0" : "-1");
    if (moveFocus && active) tab.focus();
  });
  qsa(".view").forEach((panel) => {
    const active = panel.id === `view-${next}`;
    panel.classList.toggle("active", active);
    panel.setAttribute("aria-hidden", active ? "false" : "true");
  });
  state.dirtyViews.add(next);
  renderDashboard();
}

function setSort(kind, key) {
  const target = kind === "bids" ? state.bidSort : state.recordSort;
  const textKeys = kind === "bids"
    ? new Set(["address", "neighborhood", "type"])
    : new Set(["address", "neighborhood", "type", "dataMode", "hotCategory"]);
  if (target.key === key) {
    target.dir = target.dir === "asc" ? "desc" : "asc";
  } else {
    target.key = key;
    target.dir = textKeys.has(key) ? "asc" : "desc";
  }
  if (kind === "bids") state.bidsPage = 1;
  if (kind === "records") state.recordsPage = 1;
  markDirty(kind === "bids" ? "bids" : "records");
}

function updateManualBidFromInputs() {
  state.manualBid = {
    address: qs("#manualBidAddress")?.value || "",
    pendingListPrice: qs("#manualBidListPrice")?.value || "",
    neighborhoodLabel: qs("#manualBidNeighborhood")?.value || "",
    typeLabel: qs("#manualBidType")?.value || state.filters.type,
    zip: qs("#manualBidZip")?.value || "",
    dom: qs("#manualBidDom")?.value || "",
    cdom: qs("#manualBidCdom")?.value || "",
  };
}

function applyManualScenarioFromActiveRow(row) {
  state.manualBid = {
    address: row.address || "",
    pendingListPrice: row.pendingListPrice > 0 ? String(Math.round(row.pendingListPrice)) : "",
    neighborhoodLabel: row.neighborhoodLabel || "",
    typeLabel: row.typeLabel || state.filters.type,
    zip: row.zip || "",
    dom: row.hasMlsDomValue ? String(Math.round(row.mlsDOM)) : "",
    cdom: row.hasMlsCdomValue ? String(Math.round(row.mlsCDOM)) : "",
  };
  state.bid.manualEnabled = true;
  state.bid.compSearchEnabled = true;
  state.bid.manualSourceKey = row.mapPropertyKey || "";
}

function bindEvents() {
  app.addEventListener("change", (event) => {
    const target = event.target;
    if (!target) return;
    if (target.id === "fType") state.filters.type = target.value;
    if (target.id === "fMlsStatus") {
      state.filters.mlsStatus = target.value;
      if (["ACTIVE", "PENDING", "PENDING INSPECTION", "PENDING BU REQUESTED", "CONTINGENT"].includes(String(target.value || "").toUpperCase())) {
        state.flags.includeOpenMls = true;
      }
    }
    if (target.id === "fScope") state.filters.scope = target.value;
    if (target.id === "fSpecialSale") state.filters.specialSale = target.value;
    if (target.id === "fRecordView") {
      state.filters.recordView = target.value;
      if (target.value === "projOnly" || target.value === "projAndOpen") state.flags.projection = true;
      if (target.value === "openOnly" || target.value === "projAndOpen") state.flags.includeOpenMls = true;
    }
    if (target.id === "fDateFrom") state.filters.dateFrom = target.value;
    if (target.id === "fDateTo") state.filters.dateTo = target.value;
    if (target.id === "fMinLot") state.filters.minLot = Math.max(0, Number(target.value) || 0);
    if (target.id === "fMaxLot") state.filters.maxLot = Math.max(0, Number(target.value) || 0);
    if (target.id === "fAffordability") state.filters.affordability = target.value;
    if (target.id === "affWaitMonths" || target.id === "affValuation" || target.id === "affTargetPrice" || target.id === "affDownPayment") {
      const scenario = { ...(state.affordability.scenario || {}) };
      if (target.id === "affWaitMonths") scenario.waitMonths = Math.max(0, Number(target.value) || 0);
      if (target.id === "affValuation") scenario.valuation = Math.max(0, Number(target.value) || 0) * 1e9;
      if (target.id === "affTargetPrice") scenario.targetPrice = Math.max(0, Number(target.value) || 0);
      if (target.id === "affDownPayment") scenario.downPayment = Math.max(0, Number(target.value) || 0);
      state.affordability.scenario = scenario;
      saveAffordScenario(scenario);
      markDirty(); // affects badges everywhere + the Afford tab
    }
    if (target.dataset?.excludeType !== undefined) {
      const t = target.dataset.excludeType;
      const set = new Set(state.filters.excludeTypes || []);
      if (target.checked) set.add(t); else set.delete(t);
      state.filters.excludeTypes = [...set];
    }
    if (target.id === "ffProjection") state.flags.projection = target.checked;
    if (target.id === "ffIncludeOpenMls") state.flags.includeOpenMls = target.checked;
    if (target.id === "ffExcludePresold") state.flags.excludeLikelyPresoldNewBuild = target.checked;
    if (target.id === "bidHighConfidenceOnly") state.bid.highConfidenceOnly = target.checked;
    if (target.id === "bidWatchedOnly") {
      state.bid.watchedOnly = target.checked;
      state.bidsPage = 1;
      markDirty("bids");
    }
    if (target.id === "geoViewportFilter") {
      state.geo.viewportFilter = target.checked;
      if (target.checked && state.geo.map) {
        const b = state.geo.map.getBounds();
        state.geo.mapBounds = { north: b.getNorth(), south: b.getSouth(), east: b.getEast(), west: b.getWest() };
      }
    }
    if (target.id === "geoShowActive") {
      state.geo.hideActive = !target.checked;
    }
    if (target.closest("#fNeighborhoodOptions") && target.matches("input[type='checkbox']")) {
      state.filters.neighborhoods = qsa("#fNeighborhoodOptions input[type='checkbox']:checked").map((input) => input.value);
    }
    if (target.id === "manualBidSource") {
      const row = state.bid.activeLookup.get(target.value);
      if (row) applyManualScenarioFromActiveRow(row);
      else state.bid.manualSourceKey = "";
    }
    if (target.id?.startsWith("manualBid")) updateManualBidFromInputs();
    if (target.id === "csvFile") handleCsvUpload(target.files?.[0]);

    state.recordsPage = 1;
    state.bidsPage = 1;
    markDirty();
  });

  app.addEventListener("input", (event) => {
    const target = event.target;
    if (target.id === "fMinClose") {
      state.filters.minClose = Number(target.value || 0);
      const out = qs("#fMinCloseValue");
      if (out) out.textContent = formatMoney(state.filters.minClose);
      markDirty();
    }
    if (target.id === "fMaxClose") {
      const value = Number(target.value || PRICE_SLIDER_CAP);
      state.filters.maxClose = value >= PRICE_SLIDER_CAP ? null : value;
      const out = qs("#fMaxCloseValue");
      if (out) out.textContent = state.filters.maxClose === null ? "No max" : formatMoney(state.filters.maxClose);
      markDirty();
    }
  });

  app.addEventListener("click", (event) => {
    const target = event.target;
    // Explain popovers are handled by the document-level listener in
    // src/ui/explain.mjs; without this guard a trigger inside an insight tile
    // would also fire the tile's data-switch-view action.
    if (target.closest?.(".explain-trigger, .explain-pop")) return;
    const switchView = target.closest("[data-switch-view]");
    if (switchView) return setActiveView(switchView.dataset.switchView);

    const tab = target.closest(".tab");
    if (tab) return setActiveView(tab.dataset.view);

    if (target.closest("#themeToggle")) {
      return applyTheme(document.body.classList.contains("dark") ? "light" : "dark");
    }
    if (target.closest("#reloadDatasetBtn")) return loadDefaultDataset();
    if (target.closest("#exportCsvBtn")) return exportCurrentCsv();
    if (target.closest("#resetBtn")) return resetFilters();
    if (target.closest("#clearCrossBtn")) return clearCrossFilters();
    if (target.closest("#fNeighborhoodSelectAll")) {
      state.filters.neighborhoods = state.options.neighborhoods.slice();
      return markDirty();
    }
    if (target.closest("#fNeighborhoodClear")) {
      state.filters.neighborhoods = [];
      return markDirty();
    }
    if (target.closest("#geoClearSelection")) return clearMapSelection();
    if (target.closest("#geoApplySelection")) return applyMapSelectionFilter();
    if (target.closest("#geoClearFilter")) return clearMapSelectionFilter();
    if (target.closest("#manualCompRun")) {
      updateManualBidFromInputs();
      state.bid.compSearchEnabled = true;
      return markDirty("bids");
    }
    if (target.closest("#manualBidRun")) {
      updateManualBidFromInputs();
      state.bid.manualEnabled = true;
      state.bid.compSearchEnabled = true;
      return markDirty("bids");
    }
    if (target.closest("#manualBidClear")) {
      state.bid.manualEnabled = false;
      state.bid.compSearchEnabled = false;
      state.bid.manualSourceKey = "";
      state.manualBid = {
        address: "",
        pendingListPrice: "",
        neighborhoodLabel: "",
        typeLabel: state.options.defaultType,
        zip: "",
        dom: "",
        cdom: "",
      };
      return markDirty("bids");
    }
    const bidStrategy = target.closest("[data-bid-strategy]");
    if (bidStrategy) {
      state.bid.strategy = bidStrategy.dataset.bidStrategy;
      return markDirty();
    }
    const bidViewMode = target.closest("[data-bid-view-mode]");
    if (bidViewMode) {
      state.bid.viewMode = bidViewMode.dataset.bidViewMode === "table" ? "table" : "cards";
      return markDirty("bids");
    }
    const watchToggle = target.closest("[data-toggle-watch]");
    if (watchToggle) {
      const id = watchToggle.dataset.toggleWatch;
      if (state.watched.has(id)) state.watched.delete(id);
      else state.watched.add(id);
      saveWatchedIds(state.watched);
      return markDirty("bids");
    }
    if (target.closest("#affResetBtn")) {
      state.affordability.scenario = {};
      saveAffordScenario({});
      return markDirty();
    }
    const profileToggle = target.closest("[data-buyer-profile-toggle]");
    if (profileToggle) return setBuyerProfileToggle(!state.buyerProfile.enabled);

    const pulseGroup = target.closest("[data-pulse-group]");
    if (pulseGroup) {
      state.pulseSelectedGroup = pulseGroup.dataset.pulseGroup;
      return markDirty("pulse");
    }
    const pulseMode = target.closest("[data-pulse-mode]");
    if (pulseMode) {
      state.pulseTimelineMode = pulseMode.dataset.pulseMode;
      return markDirty("pulse");
    }
    const pulseGrain = target.closest("[data-pulse-grain]");
    if (pulseGrain) {
      state.pulseSliceGrain = pulseGrain.dataset.pulseGrain === "week" ? "week" : "month";
      return markDirty("pulse");
    }
    const recordSort = target.closest("[data-record-sort]");
    if (recordSort) return setSort("records", recordSort.dataset.recordSort);
    const bidSort = target.closest("[data-bid-sort]");
    if (bidSort) return setSort("bids", bidSort.dataset.bidSort);
    const pageBtn = target.closest("[data-page-kind]");
    if (pageBtn) {
      const page = Number(pageBtn.dataset.pageTarget || 1);
      if (pageBtn.dataset.pageKind === "records") {
        state.recordsPage = page;
        return markDirty("records");
      }
      state.bidsPage = page;
      return markDirty("bids");
    }
    const useBid = target.closest("[data-use-active-bid]");
    if (useBid) {
      const row = state.bid.activeLookup.get(useBid.dataset.useActiveBid);
      if (row) applyManualScenarioFromActiveRow(row);
      setActiveView("bids");
      return markDirty("bids");
    }
    const setInteraction = target.closest("[data-set-interaction]");
    if (setInteraction) {
      const key = setInteraction.dataset.setInteraction;
      const value = setInteraction.dataset.setValue;
      state.interactions[key] = state.interactions[key] === value ? null : value;
      return markDirty();
    }
    const clearInteraction = target.closest("[data-clear-interaction]");
    if (clearInteraction) {
      state.interactions[clearInteraction.dataset.clearInteraction] = null;
      return markDirty();
    }
    if (target.closest("[data-clear-map-selection]")) return clearMapSelection();
    if (target.closest("[data-clear-map-filter]")) return clearMapSelectionFilter();
    if (target.closest("[data-clear-viewport-filter]")) {
      state.geo.viewportFilter = false;
      state.geo.mapBounds = null;
      return markDirty();
    }
    const clearFlag = target.closest("[data-clear-flag]");
    if (clearFlag) {
      const key = clearFlag.dataset.clearFlag;
      if (key === "excludePresold") state.flags.excludeLikelyPresoldNewBuild = false;
      else state.flags[key] = false;
      return markDirty();
    }
    if (target.closest("[data-clear-record-view]")) {
      state.filters.recordView = "all";
      return markDirty();
    }
    if (target.closest("[data-clear-special-sale]")) {
      state.filters.specialSale = "all";
      return markDirty();
    }
    if (target.closest("[data-clear-bid-strategy]")) {
      state.bid.strategy = "balanced";
      return markDirty();
    }
    if (target.closest("[data-clear-bid-highconf]")) {
      state.bid.highConfidenceOnly = false;
      return markDirty();
    }
  });

  app.addEventListener("keydown", (event) => {
    // Enter/Space activate keyboard-focusable role=button tiles and clickable
    // chart points/bars. el.click() re-enters the click delegation above, so
    // activation logic lives in exactly one place.
    if (event.key === "Enter" || event.key === " " || event.key === "Spacebar") {
      // Explain triggers are real buttons: let the browser produce their click
      // instead of synthesizing one on the surrounding tile.
      if (event.target.closest?.(".explain-trigger, .explain-pop")) return;
      const activatable = event.target.closest('[role="button"][tabindex], .chart-point, .insight-tile');
      if (activatable && !activatable.closest(".tab")) {
        event.preventDefault();
        if (typeof activatable.click === "function") {
          activatable.click();
        } else {
          // SVG <g> elements may not have a click() that dispatches delegation;
          // synthesize a bubbling click so the app-level handler runs.
          activatable.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        }
        return;
      }
    }
  });

  app.addEventListener("keydown", (event) => {
    const tabs = qsa(".tab");
    const current = event.target.closest(".tab");
    if (!current || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const index = tabs.indexOf(current);
    let nextIndex = index;
    if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
    if (event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = tabs.length - 1;
    setActiveView(tabs[nextIndex]?.dataset.view || "overview", true);
  });
}

function resetFilters() {
  state.filters = createDefaultFilters(state.options.types);
  state.flags = {
    projection: false,
    excludeLikelyPresoldNewBuild: false,
    includeOpenMls: true,
  };
  clearCrossFilters(false);
  markDirty();
}

function clearCrossFilters(render = true) {
  state.interactions = createEmptyInteractions();
  state.geo.selectedPropertyKeys = [];
  state.geo.filterPropertyKeys = [];
  state.geo.popupPropertyKey = "";
  state.geo.viewportFilter = false;
  state.geo.mapBounds = null;
  if (render) markDirty();
}

function exportCurrentCsv() {
  if (!state.derived) recomputeDerived();
  const rows = state.derived?.viewRows || [];
  if (!rows.length) return;
  const blob = new Blob([exportRowsToCsv(rows)], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "seattle_buyer_lens_filtered.csv";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function handleCsvUpload(file) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    state.dataSource.status = `Loading ${file.name}...`;
    renderLoadingState();
    dataWorker.postMessage({ type: "load-text", text: String(reader.result || ""), datasetName: file.name });
  };
  reader.readAsText(file);
}

function applyLoadedRows(message, kind = "default") {
  state.normalizedRows = message.normalizedRows || [];
  state.options = buildOptions(state.normalizedRows);
  state.filters = createDefaultFilters(state.options.types);
  state.manualBid.typeLabel = state.options.defaultType;
  state.dataSource = {
    ...state.dataSource,
    datasetName: message.datasetName || DEFAULT_DATASET,
    rowCount: message.rowCount || state.normalizedRows.length,
    kind,
    elapsedMs: message.elapsedMs || 0,
    status: `Loaded ${message.datasetName || DEFAULT_DATASET}.`,
    error: "",
  };
  clearCrossFilters(false);
  state.recordsPage = 1;
  state.bidsPage = 1;
  markDirty();
}

function loadDefaultDataset() {
  state.dataSource.status = `Loading ${DEFAULT_DATASET}...`;
  state.dataSource.error = "";
  renderLoadingState();
  dataWorker.postMessage({
    type: "load-url",
    url: publicUrl(DEFAULT_DATASET),
    datasetName: DEFAULT_DATASET,
  });
}

async function loadRefreshReport() {
  try {
    const response = await fetch(publicUrl(REFRESH_REPORT_FILE), { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state.dataSource.report = await response.json();
  } catch {
    state.dataSource.report = null;
  }
  markDirty("data");
}

async function loadBuyerProfileMemory() {
  try {
    const response = await fetch(publicUrl(BUYER_PROFILE_FILE), { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    state.buyerProfile.memory = normalizeProfileMemory(await response.json());
    state.buyerProfile.source = BUYER_PROFILE_FILE;
    state.buyerProfile.ready = true;
  } catch {
    state.buyerProfile.memory = normalizeProfileMemory(DEFAULT_PROFILE_MEMORY);
    state.buyerProfile.source = "embedded";
    state.buyerProfile.ready = false;
  }
  markDirty("overview");
}

// Private affordability config. Served only from a gitignored local file, so on
// the public deploy this fetch 404s and the feature stays inert (ready=false).
// A config with all-zero balances (the sample) is treated as unconfigured.
async function loadAffordabilityConfig() {
  try {
    const response = await fetch(publicUrl(AFFORD_CONFIG_FILE), { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const config = await response.json();
    const hasRealNumbers = config?.income?.anthropicBaseSalary > 0 || config?.assets?.bankCash > 0;
    if (!config?.housing || !hasRealNumbers) throw new Error("config present but unconfigured");
    state.affordability.config = config;
    state.affordability.source = AFFORD_CONFIG_FILE;
    state.affordability.ready = true;
  } catch {
    state.affordability.config = null;
    state.affordability.ready = false;
    state.affordability.source = "";
  }
  markDirty();
}

const dataWorker = new Worker(new URL("./workers/dataWorker.mjs", import.meta.url), { type: "module" });
dataWorker.addEventListener("message", (event) => {
  const message = event.data || {};
  if (message.type === "status") {
    state.dataSource.status = message.message || state.dataSource.status;
    renderLoadingState();
  }
  if (message.type === "loaded") {
    const kind = message.datasetName === DEFAULT_DATASET ? "default" : "upload";
    applyLoadedRows(message, kind);
  }
  if (message.type === "error") {
    state.normalizedRows = [];
    state.dataSource.status = "Dataset load failed.";
    state.dataSource.error = message.message || "Unknown dataset error";
    renderDashboard();
  }
});

function init() {
  renderShell();
  initTheme();
  initBuyerProfileToggle();
  bindEvents();
  initExplainLayer(app, { getMetric: (metricId) => getExplainEntry(metricId, state) });
  loadRefreshReport();
  loadBuyerProfileMemory();
  loadAffordabilityConfig();
  loadDefaultDataset();
}

init();

// Tiny smoke hook for contract tests without forcing a browser runtime.
export const appContract = {
  parseCsv,
  profileScore,
};

export const uiStaticContract = `
id="tab-overview" aria-controls="view-overview"
id="view-overview" role="tabpanel" aria-labelledby="tab-overview"
id="tab-pulse" aria-controls="view-pulse"
id="view-pulse" role="tabpanel" aria-labelledby="tab-pulse"
id="tab-bids" aria-controls="view-bids"
id="view-bids" role="tabpanel" aria-labelledby="tab-bids"
id="tab-geo" aria-controls="view-geo"
id="view-geo" role="tabpanel" aria-labelledby="tab-geo"
id="tab-records" aria-controls="view-records"
id="view-records" role="tabpanel" aria-labelledby="tab-records"
id="tab-data" aria-controls="view-data"
id="view-data" role="tabpanel" aria-labelledby="tab-data"
data-pulse-group="primary"
data-pulse-group="Ballard"
data-pulse-group="Fremont / Green Lake / Woodland Park"
data-pulse-group="Queen Anne"
data-pulse-group="Magnolia"
id="pulseGroupPills"
id="pulseModeToggles"
id="pulseStatus"
id="pulseReadout"
id="pulseRecentGrid"
id="pulseMicroBreakout"
id="pulseChartHotShare"
id="pulseChartMedianDom"
id="pulseChartSaleToList"
id="pulseChartBidUp"
id="pulseChartClosePrice"
id="pulseTrajectory"
id="pulseSliceTrends"
id="pulseCompetitionPockets"
id="chartVolume"
id="chartClose"
id="chartRatio"
id="geoApplySelection"
id="buyerProfileStatus"
id="buyerProfileMemory"
id="buyerProfileToggle"
id="buyerProfileName"
id="buyerProfileSummary"
id="buyerProfileTraits"
id="buyerProfileInsights"
id="micromarketIntro"
id="micromarketProfiles"
data-bid-sort="address"
data-bid-sort="neighborhood"
data-bid-sort="type"
data-bid-sort="suggestedBid"
data-bid-sort="confidence"
data-bid-sort="compCount"
id="dataDatasetName"
id="dataDatasetRows"
id="dataValidationStatus"
id="dataValidationTime"
id="dataOutputRows"
id="dataRealtorFileCount"
`;
