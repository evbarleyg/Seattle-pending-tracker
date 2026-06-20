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
  countyRecordUrl,
  domMetric,
  hotCategory,
  parseCsv,
  specialSaleFilterLabel,
  zillowUrl,
} from "./domain/data.mjs";
import {
  daysAgo,
  formatDateShort,
  formatDateTime,
  formatLot,
  formatMoney,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatPct,
  formatPricePerSqft,
  formatRatio,
  formatWholeNumber,
  monthLabelCompact,
  num,
  esc,
  toIso,
} from "./domain/format.mjs";
import {
  PULSE_GROUPS,
  competitiveDelta,
  metricDirection,
  rollingAverage,
} from "./domain/pulseMetrics.mjs";
import {
  DEFAULT_PROFILE_MEMORY,
  buildMicromarketProfiles,
  buildProfileCohort,
  normalizeProfileMemory,
  profileScore,
} from "./domain/buyerProfile.mjs";
import { computeAffordability, affordabilityGrid } from "./domain/affordability.mjs";
import {
  BID_STRATEGIES,
  DESKTOP_PAGE_SIZE,
  MOBILE_PAGE_SIZE,
  applyBidViewInteractions,
  bidTierLabel,
  buildBidCompPool,
  computeBidCompTiers,
  buildOptions,
  buildPulseSnapshot,
  computeActiveBidSuggestions,
  computeSlices,
  createDefaultFilters,
  createEmptyInteractions,
  exportRowsToCsv,
  filtersToSummary,
  getBidSortValue,
  getRecordSortValue,
  mergeAffordabilityTier,
  mergeBidFields,
  matchesSharedGlobalFilters,
  normalizeFilters,
  paginateRows,
  recordViewLabel,
  scoreBidForRow,
  sortRows,
} from "./domain/selectors.mjs";

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
const LAST_VISIT_STORAGE_KEY = "buyer_lens_last_visit_iso";
const SESSION_FLAG_STORAGE_KEY = "buyer_lens_session_started";
const WATCHED_STORAGE_KEY = "buyer_lens_watched_ids_v1";
const FRESHNESS_STALE_DAYS = 7;

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

function initVisitBaseline() {
  let baseline = null;
  try {
    if (!sessionStorage.getItem(SESSION_FLAG_STORAGE_KEY)) {
      const previous = localStorage.getItem(LAST_VISIT_STORAGE_KEY);
      baseline = previous ? new Date(previous) : null;
      if (baseline && Number.isNaN(baseline.getTime())) baseline = null;
      localStorage.setItem(LAST_VISIT_STORAGE_KEY, new Date().toISOString());
      sessionStorage.setItem(SESSION_FLAG_STORAGE_KEY, "1");
    } else {
      const previous = localStorage.getItem(LAST_VISIT_STORAGE_KEY);
      baseline = previous ? new Date(previous) : null;
      if (baseline && Number.isNaN(baseline.getTime())) baseline = null;
    }
  } catch {
    baseline = null;
  }
  return baseline;
}

const VISIT_BASELINE = initVisitBaseline();

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

function propertyPopupLink(row) {
  const kc = countyRecordUrl(row);
  const zillowLink = `<a class="map-popup-link" href="${esc(zillowUrl(row))}" target="_blank" rel="noopener noreferrer">Zillow</a>`;
  const kcLink = kc ? `<a class="map-popup-link" href="${esc(kc)}" target="_blank" rel="noopener noreferrer">KC parcel</a>` : "";
  return `<div class="map-popup-actions">${zillowLink}${kcLink}</div>`;
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
    renderDataView();
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

// Coverage caption for the over-ask tile: county + REDFIN_SOLD rows carry no
// genuine list price, so the median sale/list can rest on a small minority of
// the displayed comps. Surface N of M so the precision isn't read as universal.
function ratioCoverageSub(stats) {
  const n = Number(stats.ratioSampleSize || 0);
  const m = Number(stats.sampleSize || 0);
  if (!m) return "";
  if (!n) return "no comps with a real list price in this slice";
  const base = `based on ${formatWholeNumber(n)} of ${formatWholeNumber(m)} comps with a real list`;
  return n < MIN_TILE_COMPS ? `${base} — thin coverage` : base;
}

function insightTileHtml({ label, value, metricKey, series, switchView = "pulse", note = "", sub = "" }) {
  const d = tileDelta(series, metricKey);
  return `
    <article class="insight-tile ${d.tone}" role="button" tabindex="0" data-switch-view="${esc(switchView)}"${note ? ` title="${esc(note)}"` : ""}>
      <div class="tile-head"><span class="panel-kicker">${esc(label)}</span></div>
      <div class="tile-figure">
        <strong>${esc(value)}</strong>
        <span class="tile-delta ${d.tone}">${esc(d.arrow)} ${esc(d.deltaLabel)}</span>
      </div>
      ${d.secondaryLabel ? `<span class="tile-delta tile-delta-secondary ${d.secondaryTone}" title="3-month median vs prior 3-month median (trend context)">${esc(d.secondaryArrow)} ${esc(d.secondaryLabel)}</span>` : ""}
      ${sub ? `<span class="tile-sub">${esc(sub)}</span>` : ""}
      ${sparklineSvg(series, metricKey)}
      ${d.signal ? `<span class="tile-signal">${esc(d.signal)}</span>` : ""}
    </article>`;
}

function commandCenterCardsHtml() {
  if (!state.derived) {
    return `
      <article class="state-panel loading-panel">
        <span class="skeleton wide"></span>
        <span class="skeleton"></span>
        <span class="skeleton short"></span>
      </article>
    `;
  }
  const { slices, bidStatsView } = state.derived;
  const stats = slices.stats;
  const profile = state.buyerProfile.memory;
  const cohort = buildProfileCohort(slices.closedSlice, profile);
  const report = state.dataSource.report;
  const validationStatus = report?.status || report?.validationStatus || "not reported";
  const latestSale = state.derived.latestSaleDate || "";
  const staleDays = latestSale ? daysAgo(latestSale) : null;
  const savedStatus = state.buyerProfile.enabled
    ? `${cohort.summary.count} saved-home matches`
    : "Saved-home lens paused";
  const sliceSeries = state.derived.sliceMonthlySeries || [];
  const invSeries = state.derived.inventoryMonthlySeries || [];
  const tiles = [
    insightTileHtml({ label: "Median close", value: formatMoneyOrNa(stats.medianClose), metricKey: "medianClosePrice", series: sliceSeries }),
    insightTileHtml({ label: "Median $/sqft", value: stats.medianPsf ? `$${Math.round(stats.medianPsf)}` : "n/a", metricKey: "medianPsf", series: sliceSeries }),
    insightTileHtml({ label: "Over-ask ratio", value: formatRatio(stats.medianSaleToList), metricKey: "medianSaleToList", series: sliceSeries, sub: ratioCoverageSub(stats) }),
    insightTileHtml({ label: "Median DOM", value: stats.medianDom === null ? "n/a" : `${Math.round(stats.medianDom)}d`, metricKey: "medianDom", series: sliceSeries }),
    insightTileHtml({ label: "Fast-sale share", value: stats.hotShare === null ? "n/a" : formatPct(stats.hotShare), metricKey: "hotShare", series: sliceSeries }),
    insightTileHtml({ label: "Active + pending (MLS only)", value: formatWholeNumber(slices.openRows.length + slices.projectedRows.length), metricKey: "activeInventory", series: invSeries, note: "MLS/Redfin listings only — a flow proxy, not standing inventory (county rows excluded)." }),
  ].join("");

  return `
    <div class="insight-tile-grid">${tiles}</div>
    <p class="note tone-legend"><span class="tone-chip cooler">●</span> Cooling — better for buyers &nbsp; <span class="tone-chip hotter">●</span> Heating — tougher for buyers</p>
    <div class="command-grid">
      <article class="state-panel${staleDays !== null && staleDays > FRESHNESS_STALE_DAYS ? " stale" : ""}">
        <div class="panel-kicker">Freshness</div>
        <h2>${esc(latestSale ? formatDateShort(latestSale) : "n/a")}</h2>
        <p>${latestSale ? `Newest sale ${staleDays}d ago` : "No sale dates"} · ${esc(validationStatus)} · ${formatWholeNumber(state.dataSource.rowCount)} rows</p>
      </article>
      <article class="state-panel">
        <div class="panel-kicker">Saved-home lens</div>
        <h2>${esc(savedStatus)}</h2>
        <p>${esc(profile.name)} · ${cohort.summary.topMicromarket || "no top micromarket yet"}</p>
      </article>
      <article class="state-panel">
        <div class="panel-kicker">Bid queue</div>
        <h2>${formatWholeNumber(bidStatsView.activeCount)} active</h2>
        <p>${formatWholeNumber(bidStatsView.scoredCount)} scored · ${formatWholeNumber(bidStatsView.highConfidenceCount)} high confidence</p>
      </article>
    </div>
    <div class="quick-actions">
      ${buttonIcon("Open Pulse", "activity", "data-switch-view=\"pulse\"")}
      ${buttonIcon("Open Bids", "target", "data-switch-view=\"bids\"")}
      ${buttonIcon("Open Geo", "map", "data-switch-view=\"geo\"")}
      ${buttonIcon("Open Records", "rows-3", "data-switch-view=\"records\"")}
    </div>
  `;
}

function commandCenterSectionHtml() {
  return `
    <section class="command-center compact" id="commandCenter" aria-label="Buyer command center">
      <div class="hero-strip">
        <p class="eyebrow">Buyer command center</p>
        <h2 class="hero-line">${esc(filtersToSummary(state.filters).join(" + "))} · ${formatWholeNumber((state.derived?.slices?.closedSlice || []).length)} comps in slice</h2>
      </div>
      <div class="command-stack" id="commandGrid">
        ${commandCenterCardsHtml()}
      </div>
    </section>
  `;
}

function marketTone(stats) {
  if ((stats.hotShare || 0) >= 0.55 || (stats.medianSaleToList || 0) >= 1.05) return "hot";
  if ((stats.hotShare || 0) >= 0.35 || (stats.medianSaleToList || 0) >= 1.01) return "warm";
  return "cool";
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
  if (view === "overview") renderOverviewView();
  if (view === "pulse") renderPulseView();
  if (view === "bids") renderBidsView();
  if (view === "afford") renderAffordView();
  if (view === "records") renderRecordsView();
  if (view === "data") renderDataView();
  if (view === "geo") {
    renderGeoView();
  }
  state.dirtyViews.delete(view);
}

function computeRecentChanges(rows, baseline) {
  if (!baseline) return null;
  const baseTime = baseline.getTime();
  const now = Date.now();
  const minutes = Math.max(0, Math.round((now - baseTime) / 60000));
  const result = {
    baseline,
    minutesSince: minutes,
    newActives: 0,
    wentPending: 0,
    newSold: 0,
    sampleActive: null,
  };
  const list = Array.isArray(rows) ? rows : [];
  for (const row of list) {
    if (row?.dataMode !== "MLS_ENRICHED") continue;
    const isActive = String(row.mlsStatusNorm || row.mlsStatus || "").toUpperCase() === "ACTIVE";
    if (isActive) {
      const listDate = row.listDate || row.mlsListDate;
      const listTime = listDate ? new Date(listDate).getTime() : NaN;
      if (Number.isFinite(listTime) && listTime >= baseTime) {
        result.newActives += 1;
        if (!result.sampleActive) result.sampleActive = row;
      }
    }
    const pendingDate = row.pendingDate || row.mlsPendingDate;
    if (pendingDate) {
      const pendingTime = new Date(pendingDate).getTime();
      if (Number.isFinite(pendingTime) && pendingTime >= baseTime) result.wentPending += 1;
    }
    const saleDate = row.saleDate;
    if (saleDate && Number(row.closePrice) > 0) {
      const saleTime = new Date(saleDate).getTime();
      if (Number.isFinite(saleTime) && saleTime >= baseTime) result.newSold += 1;
    }
  }
  return result;
}

function sinceLastVisitSectionHtml(changes) {
  if (!changes) {
    return `
      <section class="section-block since-visit">
        <p class="eyebrow">Since you last looked</p>
        <p class="note">First visit on this device — we'll start tracking new actives, pendings, and sales for next time.</p>
      </section>
    `;
  }
  if (changes.minutesSince < 360) {
    const label = changes.minutesSince < 1 ? "moments ago" : changes.minutesSince < 60 ? `${changes.minutesSince} min ago` : `${Math.round(changes.minutesSince / 60)}h ago`;
    return `
      <section class="section-block since-visit">
        <p class="eyebrow">Since you last looked</p>
        <p class="note">Last looked ${esc(label)} — wait a bit and check back to see new movement.</p>
      </section>
    `;
  }
  const sample = changes.sampleActive
    ? `Newest: ${esc(changes.sampleActive.address || "address unavailable")} at ${esc(formatMoneyOrNa(Number(changes.sampleActive.pendingListPrice || changes.sampleActive.listPriceAtPending || 0)))}`
    : "";
  const baselineLabel = changes.minutesSince < 1440
    ? `${Math.round(changes.minutesSince / 60)}h ago`
    : `${Math.round(changes.minutesSince / 1440)}d ago`;
  return `
    <section class="section-block since-visit">
      <div class="section-head compact">
        <div>
          <p class="eyebrow">Since you last looked (${esc(baselineLabel)})</p>
          <h3>${formatWholeNumber(changes.newActives)} new active · ${formatWholeNumber(changes.wentPending)} went pending · ${formatWholeNumber(changes.newSold)} newly sold</h3>
        </div>
      </div>
      <p class="note">Filtered to your current slice. ${esc(sample)}</p>
    </section>
  `;
}

function renderOverviewView() {
  const wrap = qs("#view-overview");
  if (!wrap || !state.derived) return;
  const { slices } = state.derived;
  const gatedSlice = (state.derived.sliceMonthlySeries || []).filter((e) => (e.sampleSize ?? e.salesCount ?? 0) >= MIN_TILE_COMPS);
  const gatedCounts = gatedSlice.map((e) => e.sampleSize ?? e.salesCount ?? 0).slice().sort((a, b) => a - b);
  const medGatedCount = gatedCounts.length ? gatedCounts[Math.floor(gatedCounts.length / 2)] : 0;
  const latestGated = gatedSlice[gatedSlice.length - 1] || null;
  const latestGatedCount = latestGated ? (latestGated.sampleSize ?? latestGated.salesCount ?? 0) : 0;
  const latestPartial = !!latestGated && medGatedCount > 0 && latestGatedCount < 0.5 * medGatedCount;
  const freshnessLine = latestGated
    ? `Charts show data through ${monthLabelCompact(latestGated.month)}${latestPartial ? ` (partial — only ${latestGatedCount} comps so far; treat its swing as noise)` : ""}.`
    : "No qualifying months in this slice yet.";
  const profile = state.buyerProfile.memory;
  const cohort = buildProfileCohort(slices.closedSlice, profile);
  const profiles = buildMicromarketProfiles(slices.closedSlice, profile, new Date());
  const allSliceRows = [...(slices.closedSlice || []), ...(slices.openRows || []), ...(slices.projectedRows || [])];
  const recentChanges = computeRecentChanges(allSliceRows, VISIT_BASELINE);
  const stats = slices.stats;
  const pulse90 = state.derived.pulse.recentComparisons.find((entry) => entry.windowDays === 90);
  const fastSaleText = pulse90?.current?.hotShare !== null && pulse90?.current?.hotShare !== undefined
    ? formatPct(pulse90.current.hotShare)
    : "n/a";
  const stance = `${fastSaleText} fast-sale share · ${formatMoneyOrNa(stats.medianClose)} median close · ${formatMoneyOrNa(stats.medianBidUp)} median bid-up · ${stats.medianDom === null ? "n/a" : `${Math.round(stats.medianDom)}d`} median DOM`;
  wrap.innerHTML = `
    <div class="view-band">
      ${marketDirectionBannerHtml()}
      ${commandCenterSectionHtml()}

      ${sinceLastVisitSectionHtml(recentChanges)}

      <section class="section-block decision-brief" id="decisionBrief">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">What this means for the next offer</p>
            <h2>Start with stance, then inspect the proof.</h2>
          </div>
        </div>
        <div class="decision-grid">
          <article class="decision-card">
            <span>Current lens</span>
            <strong>${esc(filtersToSummary(state.filters).join(" + "))}</strong>
            <p>${formatWholeNumber(slices.closedSlice.length)} closed comps are shaping the read.</p>
          </article>
          <article class="decision-card">
            <span>Market stance</span>
            <strong>${esc(stance)}</strong>
            <p>Fast-sale share is DOM-based pressure; sale/list and bid-up are price pressure.</p>
          </article>
          <button class="decision-action" type="button" data-switch-view="pulse">
            ${icon("activity")}
            <strong>Review Pulse</strong>
            <span>See whether watchlist pressure is changing.</span>
          </button>
          <button class="decision-action" type="button" data-switch-view="bids">
            ${icon("target")}
            <strong>Estimate a bid</strong>
            <span>Load a listing or enter an address to find comps.</span>
          </button>
          <button class="decision-action" type="button" data-switch-view="geo">
            ${icon("map")}
            <strong>Inspect map pockets</strong>
            <span>Compare where pressure clusters before applying map filters.</span>
          </button>
          <button class="decision-action" type="button" data-switch-view="records">
            ${icon("rows-3")}
            <strong>Open comp records</strong>
            <span>Use Zillow and KC links for property-level review.</span>
          </button>
        </div>
      </section>

      <section class="section-head">
        <div>
          <p class="eyebrow">Overview</p>
          <h2>Buyer profile and current slice</h2>
        </div>
        <button class="btn alt" type="button" data-buyer-profile-toggle="1" id="buyerProfileToggle">
          ${icon(state.buyerProfile.enabled ? "target" : "search")}
          <span>${state.buyerProfile.enabled ? "Pause lens" : "Enable lens"}</span>
        </button>
      </section>

      <div class="overview-grid">
        <article class="panel" id="buyerProfileMemory">
          <div class="panel-kicker" id="buyerProfileStatus">${state.buyerProfile.ready ? `Profile loaded from ${esc(state.buyerProfile.source)}` : "Using embedded profile"}</div>
          <h3 id="buyerProfileName">${esc(profile.name)}</h3>
          <p id="buyerProfileSummary">${esc(profile.summary)}</p>
          <div class="chip-row" id="buyerProfileTraits">${profile.traits.map((trait) => `<span class="chip">${esc(trait)}</span>`).join("")}</div>
          <div id="buyerProfileInsights" class="insight-list">
            <p>${esc(state.buyerProfile.enabled ? `${cohort.summary.count} homes in this slice match your saved-home profile.` : "Saved-home cohorting is paused for this browser.")}</p>
            <p>${esc(cohort.summary.topMicromarket ? `${cohort.summary.topMicromarket} is the strongest matching micromarket right now.` : "No dominant matching micromarket in this slice yet.")}</p>
          </div>
        </article>

        <article class="panel">
          <div class="panel-kicker">Slice status</div>
          <div class="metric-list">
            ${miniMetric("Closed rows", formatWholeNumber(slices.closedSlice.length))}
            ${miniMetric("Projected rows", formatWholeNumber(slices.projectedRows.length))}
            ${miniMetric("Open/pending rows", formatWholeNumber(slices.openRows.length))}
            ${miniMetric("Record view", recordViewLabel(slices.filterState.recordView))}
          </div>
        </article>
      </div>

      <section class="section-block" id="marketTrendsStrip">
        <div class="section-head compact">
          <div><p class="eyebrow">Market trends</p><h3>Trailing 12 months in this slice</h3></div>
        </div>
        <p class="note">${esc(freshnessLine)} Months with fewer than ${MIN_TILE_COMPS} comps are omitted.</p>
        <div class="trend-strip">
          ${["medianPsf", "medianSaleToList", "medianBidUp"].map((k) => `
            <article class="trend-mini"><div class="chart-title">${esc(k === "medianBidUp" ? "Over-ask premium" : chartMetricLabel(k))}</div>${sparklineSvg(state.derived.sliceMonthlySeries, k, { width: 240, height: 64, sampleField: k === "medianBidUp" ? "bidUpSampleSize" : undefined })}</article>
          `).join("")}
          <article class="trend-mini"><div class="chart-title">${esc(chartMetricLabel("activeInventory"))}</div>${sparklineSvg(state.derived.inventoryMonthlySeries, "activeInventory", { width: 240, height: 64 })}</article>
        </div>
      </section>

      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Micromarket Profiles</p>
            <h3>Saved-home fit by watchlist pocket</h3>
          </div>
        </div>
        <p id="micromarketIntro" class="note">Recent MLS-enriched sales are scored against the saved-home profile.</p>
        <div class="profile-grid" id="micromarketProfiles">
          ${profiles.map((entry) => `
            <article class="micro-card">
              <div class="micro-card-head">
                <strong>${esc(entry.group)}</strong>
                <span>${esc(entry.fitLabel)}</span>
              </div>
              <p>${esc(entry.descriptor)}</p>
              <div class="micro-list">
                <span>${formatWholeNumber(entry.summary.salesCount)} recent sales</span>
                <span>${formatMoneyOrNa(entry.summary.medianClosePrice)}</span>
                <span>${formatRatio(entry.summary.medianSaleToList)}</span>
              </div>
            </article>
          `).join("")}
        </div>
      </section>
    </div>
  `;
}

function miniMetric(label, value) {
  return `<div class="mini-metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
}

function renderPulseView() {
  const wrap = qs("#view-pulse");
  if (!wrap || !state.derived) return;
  const snapshot = state.derived.pulse;
  const recent90 = snapshot.recentComparisons.find((entry) => entry.windowDays === 90);
  const sliceRows = state.derived.slices.closedSlice;
  const sliceSeries = buildSliceMonthlySeries(sliceRows);
  const weekly = state.pulseSliceGrain === "week";
  const weeklySeries = weekly ? buildSliceWeeklySeries(sliceRows) : [];
  // Volume + median price/$sqft support weekly grain (saleDate is reliable);
  // sale/list does NOT (ratio coverage collapses to single digits weekly).
  const volSeries = weekly ? weeklySeries : sliceSeries;
  const priceSeries = weekly ? weeklySeries : sliceSeries;
  const grainOpts = weekly ? { xAxisTitle: "Sale week" } : {};
  const pockets = competitionPocketEntries(sliceRows);
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head">
        <div>
          <p class="eyebrow">Pulse</p>
          <h2>Is my watchlist heating up?</h2>
        </div>
        <div class="segmented" id="pulseModeToggles">
          <button type="button" class="scope-pill ${state.pulseTimelineMode !== "combined" ? "active" : ""}" data-pulse-mode="compare">Compare</button>
          <button type="button" class="scope-pill ${state.pulseTimelineMode === "combined" ? "active" : ""}" data-pulse-mode="combined">Combined</button>
        </div>
      </section>
      <div class="scope-row" id="pulseGroupPills">
        ${PULSE_GROUPS.map((group) => `<button class="scope-pill ${snapshot.selectedGroup === group.id ? "active" : ""}" type="button" data-pulse-group="${esc(group.id)}">${esc(group.label)}</button>`).join("")}
      </div>
      <div id="pulseStatus" class="note">Showing ${esc(snapshot.selectedLabel)} across ${formatWholeNumber(snapshot.selectedRows.length)} MLS-enriched closed rows.</div>
      <div class="pulse-grid" id="pulseRecentGrid">
        ${["salesCount", "hotShare", "medianDom", "medianSaleToList", "medianBidUp", "medianClosePrice"].map((key) => pulseMetricCard(key, recent90)).join("")}
      </div>
      <div id="pulseReadout" class="readout">${pulseReadout(snapshot)}</div>
      <div class="chart-grid">
        ${chartPanel("Fast-sale share", "pulseChartHotShare", pulseChartSvg("hotShare", snapshot), chartSummary(snapshot.selectedMonthlySeries, "hotShare"), chartGuide("hotShare", "line"), chartInsight("hotShare"))}
        ${chartPanel("Median DOM", "pulseChartMedianDom", pulseChartSvg("medianDom", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianDom"), chartGuide("medianDom", "line"), chartInsight("medianDom"))}
        ${chartPanel("Sale/List price pressure", "pulseChartSaleToList", pulseChartSvg("medianSaleToList", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianSaleToList"), chartGuide("medianSaleToList", "line"), chartInsight("medianSaleToList"))}
        ${chartPanel("Bid-up price pressure", "pulseChartBidUp", pulseChartSvg("medianBidUp", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianBidUp"), chartGuide("medianBidUp", "line"), chartInsight("medianBidUp"))}
        ${chartPanel("Close price band", "pulseChartClosePrice", pulseChartSvg("medianClosePrice", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianClosePrice"), chartGuide("medianClosePrice", "line"), chartInsight("medianClosePrice"))}
      </div>
      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Watchlist comparison</p>
            <h3>Sale/List trajectory by pocket</h3>
          </div>
        </div>
        <p class="note">Use this to separate broad buyer pressure from a single neighborhood spike.</p>
        <div id="pulseTrajectory" class="trajectory-grid">
          ${pulseTrajectoryCards(snapshot)}
        </div>
      </section>
      <section class="section-block" id="pulseSliceTrends">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Whole-slice trends</p>
            <h3>How the current filter band is moving</h3>
          </div>
        </div>
        <div class="trends-controls">
          <div class="segmented" id="pulseGrainToggles" role="group" aria-label="Trend grain">
            <button type="button" class="scope-pill ${!weekly ? "active" : ""}" data-pulse-grain="month">Monthly</button>
            <button type="button" class="scope-pill ${weekly ? "active" : ""}" data-pulse-grain="week">Weekly (rolling 28d)</button>
          </div>
        </div>
        <p class="note">${weekly
          ? "Weekly grain medians a trailing 28-day window keyed on sale date, advancing every 7 days — it surfaces softening weeks before a calendar month closes. The trailing partial week is de-weighted; only volume and median price/$sqft are shown weekly (sale/list ratio coverage is too thin to trust at this grain)."
          : "These are the old Charts view, kept here so the context sits beside the watchlist pulse."}</p>
        <div class="chart-grid">
          ${chartPanel(weekly ? "Weekly volume (rolling 28d)" : "Monthly volume", "chartVolume", barSvg(volSeries, "salesCount", grainOpts), chartSummary(volSeries, "salesCount"), chartGuide("salesCount", "bar"), chartInsight("salesCount"))}
          ${chartPanel("Median close", "chartClose", lineSvg(priceSeries, "medianClosePrice", grainOpts), chartSummary(priceSeries, "medianClosePrice"), chartGuide("medianClosePrice", "line"), chartInsight("medianClosePrice"))}
          ${weekly
            ? `<article class="chart-panel"><div class="chart-title">Median sale/list</div><p class="chart-insight">Sale/list ratio stays on monthly grain: only a small minority of recent weekly sales carry a genuine list price, so a weekly ratio would jump on n&lt;5. Switch to Monthly to read it.</p>${lineSvg(sliceSeries, "medianSaleToList")}</article>`
            : chartPanel("Median sale/list", "chartRatio", lineSvg(sliceSeries, "medianSaleToList"), chartSummary(sliceSeries, "medianSaleToList"), chartGuide("medianSaleToList", "line"), chartInsight("medianSaleToList"))}
          ${chartPanel("Median $/sqft", "chartPsf", lineSvg(priceSeries, "medianPsf", grainOpts), chartSummary(priceSeries, "medianPsf"), chartGuide("medianPsf", "line"), chartInsight("medianPsf"))}
        </div>
      </section>
      <section class="section-block" id="pulseCompetitionPockets">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Competition pockets</p>
            <h3>Where fast sales concentrate</h3>
          </div>
        </div>
        <p class="note">These rows are clickable neighborhood cross-filters. Fast-sale share is DOM-based; sale/list shows price pressure.</p>
        <div class="heat-list">
          ${heatListHtml(pockets)}
        </div>
      </section>
      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Micro breakout</p>
            <h3>Watchlist neighborhoods</h3>
          </div>
        </div>
        <div id="pulseMicroBreakout" class="profile-grid">
          ${snapshot.microBreakout.map((group) => `
            <article class="micro-card">
              <div class="micro-card-head"><strong>${esc(group.group)}</strong><span>${formatWholeNumber(group.totalSalesCount)} sales</span></div>
              ${group.neighborhoods.slice(0, 4).map((entry) => `
                <div class="mini-metric">
                  <button class="link-button" data-set-interaction="neighborhood" data-set-value="${esc(entry.neighborhoodLabel)}">${esc(entry.neighborhoodLabel)}</button>
                  <strong>${formatPct(entry.current.hotShare || 0)} fast-sale</strong>
                </div>
              `).join("")}
            </article>
          `).join("") || `<p class="note">No watchlist pulse rows in this slice.</p>`}
        </div>
      </section>
    </div>
  `;
}

function pulseMetricCard(key, recent) {
  const current = recent?.current || {};
  const previous = recent?.previous || {};
  const config = pulseMetricConfig(key);
  const direction = metricDirection(key, current[key], previous[key]);
  const delta = competitiveDelta(key, current[key], previous[key]);
  const tone = direction > 0 ? "hotter" : direction < 0 ? "cooler" : "flat";
  let deltaLabel;
  if (delta === null) deltaLabel = "n/a vs prior";
  else if (Math.abs(delta) < 1e-9) deltaLabel = "no change vs prior";
  else deltaLabel = `${esc(config.delta(delta))} vs prior`;
  return `
    <article class="metric-card ${tone}">
      <span>${esc(config.label)}</span>
      <strong>${esc(config.format(current[key]))}</strong>
      <small>${deltaLabel}</small>
    </article>
  `;
}

function tileDelta(series, metricKey, sampleField) {
  const sampleOf = (e) => ((sampleField ? e[sampleField] : undefined) ?? e.sampleSize ?? e.salesCount ?? 0);
  let gated = (series || []).filter((entry) => ((sampleField ? entry[sampleField] : undefined) ?? entry.sampleSize ?? entry.salesCount ?? Infinity) >= MIN_TILE_COMPS && Number.isFinite(Number(entry[metricKey])));
  // Drop a thin/partial trailing month (the stale current month) so month-over-month compares two COMPLETE months.
  if (gated.length >= 2 && sampleOf(gated[gated.length - 1]) < 0.5 * sampleOf(gated[gated.length - 2])) gated = gated.slice(0, -1);
  if (gated.length < 2) return { tone: "flat", arrow: "", deltaLabel: "insufficient history", signal: "", secondaryArrow: "", secondaryTone: "flat", secondaryLabel: "" };
  const cfg = pulseMetricConfig(metricKey);
  const toneFor = (dir) => (dir > 0 ? "hotter" : dir < 0 ? "cooler" : "flat");
  const arrowFor = (dir) => (dir > 0 ? "↑" : dir < 0 ? "↓" : "→");
  const signalFor = (dir) => ({
    medianClosePrice: dir > 0 ? "prices rising" : dir < 0 ? "prices easing" : "prices flat",
    medianPsf: dir > 0 ? "$/sqft climbing" : dir < 0 ? "$/sqft softening" : "$/sqft steady",
    medianSaleToList: dir > 0 ? "buyers bidding over ask" : dir < 0 ? "buyers no longer over ask" : "clearing at ask",
    medianDom: dir > 0 ? "selling faster" : dir < 0 ? "sitting longer" : "steady pace",
    hotShare: dir > 0 ? "heating up" : dir < 0 ? "cooling" : "holding",
    activeInventory: dir < 0 ? "more choice" : dir > 0 ? "tightening supply" : "flat supply",
  })[metricKey] || "";
  // PRIMARY: month-over-month between the two most recent COMPLETE months (well-sampled in this slice).
  const momDir = metricDirection(metricKey, gated[gated.length - 1][metricKey], gated[gated.length - 2][metricKey]);
  const momDelta = competitiveDelta(metricKey, gated[gated.length - 1][metricKey], gated[gated.length - 2][metricKey]);
  // SECONDARY: 3-month vs prior-3-month median for trend context.
  let secondary = { secondaryArrow: "", secondaryTone: "flat", secondaryLabel: "" };
  if (gated.length >= 6) {
    const windowMedian = (entries) => medianValue(entries.map((e) => Number(e[metricKey])));
    const tDir = metricDirection(metricKey, windowMedian(gated.slice(-3)), windowMedian(gated.slice(-6, -3)));
    const tDelta = competitiveDelta(metricKey, windowMedian(gated.slice(-3)), windowMedian(gated.slice(-6, -3)));
    if (tDelta !== null) secondary = { secondaryArrow: arrowFor(tDir), secondaryTone: toneFor(tDir), secondaryLabel: `${cfg.delta(tDelta)} vs prior 3mo` };
  }
  return {
    tone: toneFor(momDir), arrow: arrowFor(momDir), deltaLabel: momDelta === null ? "n/a" : `${cfg.delta(momDelta)} MoM`, signal: signalFor(momDir),
    ...secondary,
  };
}

function marketDirectionVerdict(series) {
  const metrics = ["medianClosePrice", "medianPsf", "medianSaleToList", "medianDom", "hotShare"];
  const reads = metrics.map((k) => tileDelta(series, k)).filter((d) => d.tone === "hotter" || d.tone === "cooler");
  if (reads.length < 2) return { tone: "flat", headline: "Direction unclear", detail: "Not enough recent comps in this slice to call a trend — widen the slice or compare to the whole market." };
  const cooler = reads.filter((d) => d.tone === "cooler").length;
  const hotter = reads.filter((d) => d.tone === "hotter").length;
  if (cooler - hotter >= 2) return { tone: "cooler", headline: "Cooling — tilting toward buyers", detail: `${cooler} of ${reads.length} signals eased vs the prior month (more room to negotiate). Last complete month; the partial current month is excluded.` };
  if (hotter - cooler >= 2) return { tone: "hotter", headline: "Heating up — tougher for buyers", detail: `${hotter} of ${reads.length} signals tightened vs the prior month (expect more competition). Last complete month; the partial current month is excluded.` };
  return { tone: "flat", headline: "Mixed signals", detail: `Vs the prior complete month, ${cooler} signal(s) eased and ${hotter} tightened — no single direction. See the per-tile 3-mo trend below.` };
}

function marketDirectionBannerHtml() {
  const v = marketDirectionVerdict(state.derived?.sliceMonthlySeries || []);
  const latest = state.derived?.latestSaleDate || "";
  const asOf = latest ? ` As of ${formatDateShort(latest)} in this slice.` : "";
  return `
    <section class="section-block market-verdict ${v.tone}" aria-label="Market direction verdict">
      <p class="eyebrow">Market direction</p>
      <h2 class="hero-line">${esc(v.headline)}</h2>
      <p class="note">${esc(v.detail)}${esc(asOf)}</p>
    </section>`;
}

function pulseMetricConfig(key) {
  if (key === "salesCount") return { label: "Sales Count", format: (value) => formatWholeNumber(value || 0), delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}` };
  if (key === "activeInventory") return { label: "Active + Pending", format: (value) => formatWholeNumber(value || 0), delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}` };
  if (key === "hotShare") return { label: "Fast-Sale Share", format: (value) => value === null ? "n/a" : formatPct(value), delta: (value) => `${value >= 0 ? "+" : ""}${((value || 0) * 100).toFixed(1)} pts` };
  if (key === "medianDom") return { label: "Median DOM", format: (value) => value === null ? "n/a" : `${Math.round(value)}d`, delta: (value) => `${value >= 0 ? "+" : ""}${Math.round(value || 0)}d` };
  if (key === "medianSaleToList") return { label: "Median Sale/List", format: (value) => value === null ? "n/a" : `${value.toFixed(2)}x`, delta: (value) => `${value >= 0 ? "+" : ""}${(value || 0).toFixed(3)}x` };
  if (key === "medianBidUp") return { label: "Median Bid-Up", format: (value) => value === null ? "n/a" : formatMoneyCompact(value), delta: (value) => `${value >= 0 ? "+" : ""}${formatMoneyCompact(value || 0)}` };
  if (key === "medianPsf") return { label: "Median $/Sqft", format: (value) => value === null ? "n/a" : `$${Math.round(value)}`, delta: (value) => `${value >= 0 ? "+" : ""}$${Math.round(value || 0)}` };
  return { label: "Median Close", format: (value) => value === null ? "n/a" : formatMoneyCompact(value), delta: (value) => `${value >= 0 ? "+" : ""}${formatMoneyCompact(value || 0)}` };
}

function chartPanel(title, id, content, summary = "", guide = "", insight = "") {
  return `<article class="chart-panel"><div class="chart-title">${esc(title)}</div>${insight}${summary}${guide}<div id="${esc(id)}">${content}</div></article>`;
}

function chartInsight(metricKey) {
  const copy = {
    salesCount: "What this tells you: whether the slice has enough recent activity to trust the read.",
    hotShare: "What this tells you: how often homes are moving quickly enough to compress your decision window.",
    medianDom: "What this tells you: how much time you may have before a strong listing gets claimed.",
    medianSaleToList: "What this tells you: whether accepted prices are clearing above, at, or below ask.",
    medianBidUp: "What this tells you: how many dollars buyers are adding over the pending ask price.",
    medianClosePrice: "What this tells you: whether the target band is drifting away from your budget.",
    medianPsf: "What this tells you: price intensity normalized for size — rising $/sqft means you pay more per foot even if list prices look flat.",
    activeInventory: "What this tells you: a flow proxy for MLS/Redfin listings observed each month (county rows excluded) — directional competing-supply signal, not a true standing-inventory snapshot.",
  };
  return `<p class="chart-insight">${esc(copy[metricKey] || "What this tells you: how the market is moving inside this slice.")}</p>`;
}

function pulseChartSvg(metricKey, snapshot) {
  const series = snapshot.selectedMonthlySeries || [];
  return lineSvg(series, metricKey, { pointAttr: 'data-set-interaction="month"' });
}

function pulseTrajectoryCards(snapshot) {
  const combined = state.pulseTimelineMode === "combined";
  if (combined) {
    return `
      <article class="chart-panel wide">
        <div class="chart-title">${esc(snapshot.selectedLabel)}</div>
        ${chartSummary(snapshot.selectedMonthlySeries, "medianSaleToList")}
        ${chartGuide("medianSaleToList", "line")}
        ${lineSvg(snapshot.selectedMonthlySeries, "medianSaleToList", { pointAttr: 'data-set-interaction="month"' })}
      </article>
    `;
  }
  return snapshot.activeGroups.map((group) => `
    <article class="chart-panel">
      <div class="chart-title">${esc(group)}</div>
      ${chartSummary(snapshot.monthlyByGroup[group] || [], "medianSaleToList")}
      ${chartGuide("medianSaleToList", "line")}
      ${lineSvg(snapshot.monthlyByGroup[group] || [], "medianSaleToList", { pointAttr: 'data-set-interaction="month"' })}
    </article>
  `).join("");
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

// X-axis label: weekly series carry an explicit short label (e.g. "6/8");
// monthly series fall back to the YYYY-MM compact month label.
function pointLabel(point) {
  return point.label || monthLabelCompact(point.month);
}

function chartSummary(series, metricKey, options = {}) {
  const data = chartData(series, metricKey, options);
  if (!data.length) return `<div class="chart-summary"><span>No plotted data</span></div>`;
  const latest = data[data.length - 1];
  const high = data.reduce((best, entry) => entry.value > best.value ? entry : best, data[0]);
  const low = data.reduce((best, entry) => entry.value < best.value ? entry : best, data[0]);
  return `
    <div class="chart-summary" aria-label="${esc(chartMetricLabel(metricKey))} chart data summary">
      <span><strong>Latest</strong> ${esc(monthLabelCompact(latest.month))}: ${esc(formatChartValue(metricKey, latest.value))}${Number(latest.count || 0) > 0 && Number(latest.count) < MIN_TILE_COMPS ? ` (n=${latest.count})` : ""}</span>
      <span><strong>High</strong> ${esc(monthLabelCompact(high.month))}: ${esc(formatChartValue(metricKey, high.value))}</span>
      <span><strong>Low</strong> ${esc(monthLabelCompact(low.month))}: ${esc(formatChartValue(metricKey, low.value))}</span>
    </div>
  `;
}

function chartGuide(metricKey, kind = "line") {
  const label = chartMetricLabel(metricKey);
  if (kind === "bar") {
    return `
      <div class="chart-guide">
        <span><i class="legend-swatch bar"></i>Each bar is one sale month</span>
        <span>Y-axis: ${esc(label)}</span>
      </div>
    `;
  }
  return `
    <div class="chart-guide">
      <span><i class="legend-swatch monthly"></i>Blue line: monthly value</span>
      <span><i class="legend-swatch average"></i>Green line: 3-month average</span>
      <span>Dots: clickable months</span>
      <span>Y-axis: ${esc(label)}</span>
    </div>
  `;
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

function chartTicks(min, max, count = 4) {
  const safeCount = Math.max(2, count);
  return Array.from({ length: safeCount }, (_, index) => max - ((max - min) * index) / (safeCount - 1));
}

function xTickIndexes(length) {
  if (length <= 1) return [0];
  if (length <= 6) return Array.from({ length }, (_, index) => index);
  const indexes = new Set([0, length - 1]);
  const step = Math.ceil((length - 1) / 4);
  for (let index = step; index < length - 1; index += step) indexes.add(index);
  return [...indexes].sort((a, b) => a - b);
}

function shouldLabelPoint(point, index, points) {
  if (points.length <= 6) return true;
  const values = points.map((entry) => entry.value);
  return index === points.length - 1 || point.value === Math.max(...values) || point.value === Math.min(...values);
}

function lineSvg(series, metricKey, options = {}) {
  const data = chartData(series, metricKey, options);
  const width = 460;
  const height = 230;
  const padLeft = 62;
  const padRight = 18;
  const padTop = 24;
  const padBottom = 48;
  if (!data.length) return `<div class="empty-state">No chart data.</div>`;
  const domain = chartDomain(data, metricKey);
  const span = Math.max(domain.max - domain.min, 0.0001);
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const points = data.map((entry, index) => {
    const x = data.length === 1 ? padLeft + plotWidth / 2 : padLeft + (index / Math.max(data.length - 1, 1)) * plotWidth;
    const y = padTop + plotHeight - ((entry.value - domain.min) / span) * plotHeight;
    return { ...entry, x, y };
  });
  const path = points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const smooth = rollingAverage(data.map((entry) => entry.value), 3);
  const smoothPoints = smooth.map((value, index) => {
    if (value === null) return "";
    const x = smooth.length === 1 ? padLeft + plotWidth / 2 : padLeft + (index / Math.max(smooth.length - 1, 1)) * plotWidth;
    const y = padTop + plotHeight - ((value - domain.min) / span) * plotHeight;
    return `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).filter(Boolean).join(" ");
  const ticks = chartTicks(domain.min, domain.max);
  const xIndexes = xTickIndexes(data.length);
  return `
    <svg class="line-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(chartMetricLabel(metricKey))} monthly chart">
      <title>${esc(chartMetricLabel(metricKey))} by month</title>
      <desc>Y-axis shows ${esc(chartMetricLabel(metricKey))}; X-axis shows sale month.</desc>
      <path class="axis-line" d="M${padLeft},${padTop} V${padTop + plotHeight} H${width - padRight}" />
      ${ticks.map((tick) => {
        const y = padTop + plotHeight - ((tick - domain.min) / span) * plotHeight;
        return `
          <g class="chart-y-tick">
            <path class="gridline" d="M${padLeft},${y.toFixed(1)} H${width - padRight}" />
            <text x="${padLeft - 8}" y="${y.toFixed(1)}" text-anchor="end">${esc(formatChartValue(metricKey, tick))}</text>
          </g>
        `;
      }).join("")}
      ${xIndexes.map((index) => {
        const point = points[index];
        return `
          <g class="chart-x-tick">
            <path class="tickline" d="M${point.x.toFixed(1)},${padTop + plotHeight} V${padTop + plotHeight + 5}" />
            <text x="${point.x.toFixed(1)}" y="${height - 14}" text-anchor="middle">${esc(pointLabel(point))}</text>
          </g>
        `;
      }).join("")}
      <text class="axis-title" x="${padLeft}" y="13">${esc(chartMetricLabel(metricKey))}</text>
      <text class="axis-title" x="${width - padRight}" y="${height - 2}" text-anchor="end">${esc(options.xAxisTitle || "Month")}</text>
      <path class="trend" d="${esc(path)}" />
      ${smoothPoints ? `<path class="trend smooth" d="${esc(smoothPoints)}" />` : ""}
      ${points.map((point, index) => {
        const lowSample = Number(point.count || 0) > 0 && Number(point.count) < MIN_TILE_COMPS;
        const partial = !!point.isPartial;
        const note = `${lowSample ? ` (n=${point.count})` : ""}${partial ? " · partial — still filling" : ""}`;
        return `
        <g class="chart-point${lowSample ? " low-sample" : ""}${partial ? " partial" : ""}" role="button" tabindex="0" aria-label="${esc(`${pointLabel(point)} ${formatChartValue(metricKey, point.value)}${note}`)}" ${options.pointAttr || ""} data-set-value="${esc(point.month)}">
          <circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="4"><title>${esc(`${pointLabel(point)}: ${formatChartValue(metricKey, point.value)}${note}`)}</title></circle>
          ${!lowSample && !partial && shouldLabelPoint(point, index, points) ? `<text class="chart-value-label" x="${point.x.toFixed(1)}" y="${Math.max(12, point.y - 9).toFixed(1)}" text-anchor="middle">${esc(formatChartValue(metricKey, point.value))}</text>` : ""}
        </g>`;
      }).join("")}
    </svg>
  `;
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

function pulseReadout(snapshot) {
  const recent90 = snapshot.recentComparisons.find((entry) => entry.windowDays === 90);
  if (!recent90 || !recent90.current.salesCount) return `<p>No recent watchlist sales in this slice.</p>`;
  const bullets = [];
  bullets.push(`${snapshot.selectedLabel} has ${formatWholeNumber(recent90.current.salesCount)} sales in the last 90-day pulse window.`);
  if (recent90.current.medianBidUp !== null) bullets.push(`Median bid-up is ${formatMoney(recent90.current.medianBidUp)}.`);
  if (recent90.current.medianDom !== null) bullets.push(`Median DOM is ${Math.round(recent90.current.medianDom)} days.`);
  return bullets.map((text) => `<p>${esc(text)}</p>`).join("");
}

function buildSliceMonthlySeries(rows) {
  const byMonth = groupRows(rows, (row) => row.saleDate ? row.saleDate.slice(0, 7) : "Unknown");
  return Object.entries(byMonth).sort((a, b) => a[0].localeCompare(b[0])).map(([month, monthRows]) => ({
    month,
    salesCount: monthRows.length,
    sampleSize: monthRows.length,
    medianClosePrice: medianValue(monthRows.map((row) => row.closePrice)),
    medianSaleToList: medianValue(monthRows.map((row) => row.saleToList).filter((value) => value > 0)),
    medianPsf: medianValue(monthRows.map((row) => row.pricePerSqft).filter((value) => value > 0)),
    hotShare: monthRows.length ? monthRows.filter((row) => row.isHotMarket).length / monthRows.length : null,
    medianDom: medianValue(monthRows.map((row) => domMetric(row)).filter((value) => value !== null && value !== undefined)),
    medianBidUp: (() => { const b = monthRows.filter((row) => row.hasMarketListPrice && Number.isFinite(row.delta)); return b.length ? medianValue(b.map((row) => row.delta)) : null; })(),
    bidUpSampleSize: monthRows.filter((row) => row.hasMarketListPrice && Number.isFinite(row.delta)).length,
    ratioSampleSize: monthRows.filter((row) => row.saleToList > 0).length,
  }));
}

// Weekly rolling-window grain, keyed STRICTLY on saleDate (reliable on
// REDFIN_SOLD / MLS rows; county pendingDate is faked). For each week-ending
// anchor over the last `weeks` weeks we median over a trailing `windowDays`
// window so sparse weekly n is smoothed while the read still advances every 7
// days — surfacing softening weeks before a calendar month closes. Only VOLUME
// and MEDIAN PRICE / $sqft are exposed at this grain; sale/list, pending and
// DOM collapse to single-digit coverage in the newest weeks and are NOT offered.
function buildSliceWeeklySeries(rows, { weeks = 13, windowDays = 28 } = {}) {
  const dated = (rows || [])
    .filter((row) => row.closePrice > 0 && row.saleDate)
    .map((row) => ({ row, t: new Date(`${row.saleDate}T00:00:00`).getTime() }))
    .filter((entry) => Number.isFinite(entry.t));
  if (!dated.length) return [];
  const latest = dated.reduce((max, entry) => Math.max(max, entry.t), dated[0].t);
  const anchor = new Date(latest);
  anchor.setHours(0, 0, 0, 0);
  const dayMs = 24 * 60 * 60 * 1000;
  const windowMs = windowDays * dayMs;
  const series = [];
  for (let i = weeks - 1; i >= 0; i -= 1) {
    const end = anchor.getTime() - i * 7 * dayMs;
    const start = end - windowMs + dayMs;
    const windowRows = dated.filter((entry) => entry.t >= start && entry.t <= end).map((entry) => entry.row);
    const endDate = new Date(end);
    series.push({
      month: toIso(endDate),
      label: `${endDate.getMonth() + 1}/${endDate.getDate()}`,
      salesCount: windowRows.length,
      sampleSize: windowRows.length,
      isPartial: i === 0,
      medianClosePrice: medianValue(windowRows.map((row) => row.closePrice)),
      medianPsf: medianValue(windowRows.map((row) => row.pricePerSqft).filter((value) => value > 0)),
    });
  }
  return series;
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

function barSvg(series, metricKey, options = {}) {
  const data = chartData(series, metricKey, options);
  if (!data.length) return `<div class="empty-state">No chart data.</div>`;
  const width = 460;
  const height = 230;
  const padLeft = 62;
  const padRight = 18;
  const padTop = 24;
  const padBottom = 48;
  const domain = chartDomain(data, metricKey, true);
  const span = Math.max(domain.max - domain.min, 0.0001);
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const barWidth = plotWidth / data.length;
  const ticks = chartTicks(domain.min, domain.max);
  const xIndexes = xTickIndexes(data.length);
  return `
    <svg class="bar-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(chartMetricLabel(metricKey))} bar chart">
      <title>${esc(chartMetricLabel(metricKey))} by month</title>
      <desc>Y-axis shows ${esc(chartMetricLabel(metricKey))}; X-axis shows sale month.</desc>
      <path class="axis-line" d="M${padLeft},${padTop} V${padTop + plotHeight} H${width - padRight}" />
      ${ticks.map((tick) => {
        const y = padTop + plotHeight - ((tick - domain.min) / span) * plotHeight;
        return `
          <g class="chart-y-tick">
            <path class="gridline" d="M${padLeft},${y.toFixed(1)} H${width - padRight}" />
            <text x="${padLeft - 8}" y="${y.toFixed(1)}" text-anchor="end">${esc(formatChartValue(metricKey, tick))}</text>
          </g>
        `;
      }).join("")}
      ${xIndexes.map((index) => {
        const x = padLeft + index * barWidth + barWidth / 2;
        return `
          <g class="chart-x-tick">
            <path class="tickline" d="M${x.toFixed(1)},${padTop + plotHeight} V${padTop + plotHeight + 5}" />
            <text x="${x.toFixed(1)}" y="${height - 14}" text-anchor="middle">${esc(pointLabel(data[index]))}</text>
          </g>
        `;
      }).join("")}
      <text class="axis-title" x="${padLeft}" y="13">${esc(chartMetricLabel(metricKey))}</text>
      <text class="axis-title" x="${width - padRight}" y="${height - 2}" text-anchor="end">${esc(options.xAxisTitle || "Month")}</text>
      ${data.map((entry, index) => {
        const h = ((entry.value - domain.min) / span) * plotHeight;
        const x = padLeft + index * barWidth + 3;
        const barBodyWidth = Math.max(2, barWidth - 6);
        const y = padTop + plotHeight - h;
        const lowSample = Number(entry.count || 0) > 0 && Number(entry.count) < MIN_TILE_COMPS;
        const partial = !!entry.isPartial;
        const note = `${lowSample ? ` (n=${entry.count}, thin)` : ""}${partial ? " · partial — still filling" : ""}`;
        return `
          <g class="chart-bar${lowSample ? " low-sample" : ""}${partial ? " partial" : ""}" role="button" tabindex="0" aria-label="${esc(`${pointLabel(entry)} ${formatChartValue(metricKey, entry.value)}${note}`)}" ${options.pointAttr || ""} data-set-value="${esc(entry.month)}">
            <rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barBodyWidth.toFixed(1)}" height="${h.toFixed(1)}"><title>${esc(`${pointLabel(entry)}: ${formatChartValue(metricKey, entry.value)}${note}`)}</title></rect>
            ${!lowSample && !partial && data.length <= 12 ? `<text class="chart-value-label" x="${(x + barBodyWidth / 2).toFixed(1)}" y="${Math.max(12, y - 7).toFixed(1)}" text-anchor="middle">${esc(formatChartValue(metricKey, entry.value))}</text>` : ""}
          </g>
        `;
      }).join("")}
    </svg>
  `;
}

function competitionPocketEntries(rows) {
  return Object.entries(groupRows(rows, (row) => row.neighborhoodLabel || "Unknown"))
    .map(([name, list]) => ({
      name,
      count: list.length,
      hotShare: list.length ? list.filter((row) => row.isHotMarket).length / list.length : 0,
      medianRatio: medianValue(list.map((row) => row.saleToList).filter((value) => value > 0)),
      medianClose: medianValue(list.map((row) => row.closePrice)),
    }))
    .sort((a, b) => (b.hotShare - a.hotShare) || (b.count - a.count))
    .slice(0, 30);
}

function heatListHtml(entries) {
  return entries.map((entry) => `
    <button class="heat-row" type="button" data-set-interaction="neighborhood" data-set-value="${esc(entry.name)}">
      <span>${esc(entry.name)}</span>
      <strong>${formatPct(entry.hotShare)} fast-sale</strong>
      <em>${formatWholeNumber(entry.count)} sales · ${formatRatio(entry.medianRatio)} sale/list · ${formatMoneyOrNa(entry.medianClose)}</em>
      <i style="inline-size:${Math.max(6, entry.hotShare * 100).toFixed(1)}%"></i>
    </button>
  `).join("") || `<div class="empty-state">No rows in this slice.</div>`;
}

function groupRows(rows, keyFn) {
  return (rows || []).reduce((acc, row) => {
    const key = keyFn(row);
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});
}

function renderBidsView() {
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
        ${miniMetric("High confidence", formatWholeNumber(stats.highConfidenceCount))}
        ${miniMetric("Watched", formatWholeNumber(watchedCount))}
        ${miniMetric("Median over ask", `${(stats.medianOverAskPct || 0).toFixed(1)}%`)}
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
                ${bidTh("suggestedBid", "Suggested Bid")}
                ${bidTh("bidRange", "Bid Range")}
                ${bidTh("ratio", "Suggested S/List")}
                ${bidTh("confidence", "Confidence")}
                ${bidTh("compCount", "Comp Count")}
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

function bidCardHtml(row) {
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
    : `<span class="bid-over-ask ${overAsk > 0 ? "up" : overAsk < 0 ? "down" : "flat"}">${overAsk > 0 ? "+" : ""}${overAsk}% vs ask</span>`;
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
          <span class="bid-suggested-label">Suggested bid</span>
          <strong class="bid-suggested-value">${suggested}</strong>
          ${overAskBadge}
        </div>
        <span class="conf-pill ${esc(confTone)}">${esc(row.bidConfidenceLabel || "n/a")} (${row.bidConfidence || 0})</span>
      </div>
      <div class="bid-card-grid-meta">
        <div><span>Range</span><strong>${range}</strong></div>
        <div><span>Ask</span><strong>${formatMoneyOrNa(row.pendingListPrice)}</strong></div>
        <div><span>Original list</span><strong>${formatMoneyOrNa(row.originalListPrice)}</strong></div>
        <div><span>DOM</span><strong>${dom ?? "n/a"}</strong></div>
        <div><span>Sugg S/List</span><strong>${ratio}</strong></div>
        <div><span>Comps</span><strong>${row.bidCompCount || 0} (${esc(bidTierLabel(row.bidCompTier))})</strong></div>
      </div>
      <footer class="bid-card-footer">
        <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use in scenario</button>
      </footer>
    </article>
  `;
}

function affordScenarioValue(key, fallback) {
  const v = state.affordability.scenario?.[key];
  return v === undefined || v === null || v === "" ? fallback : v;
}

function renderAffordView() {
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

function renderManualBidPanel() {
  const result = state.bid.manualEnabled ? computeManualBid() : null;
  const compLookup = (state.bid.compSearchEnabled || state.bid.manualEnabled) ? computeManualComps() : null;
  return `
    <section class="manual-bid-wrap">
      <div class="section-head compact">
        <div><p class="eyebrow">Comp Finder</p><h3>Prospective listing comps</h3></div>
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
          <thead><tr><th>Comp</th><th>Neighborhood</th><th>Close</th><th>S/List</th><th>DOM</th><th>Bid-Up</th></tr></thead>
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
      ${miniMetric("Suggested bid", formatMoney(scored.bidSuggested))}
      ${miniMetric("Range", `${formatMoney(scored.bidLow)} - ${formatMoney(scored.bidHigh)}`)}
      ${miniMetric("S/List", `${scored.bidRatio.toFixed(2)}x`)}
      ${miniMetric("Confidence", `${scored.bidConfidenceLabel} (${scored.bidConfidence})`)}
      ${miniMetric("Comp basis", `${scored.bidCompCount} comps · ${bidTierLabel(scored.bidCompTier)}`)}
    `,
    compRows: compRowsHtml(scored.bidCompRows || []),
  };
}

function bidTh(key, label) {
  const active = state.bidSort.key === key;
  const arrow = active ? (state.bidSort.dir === "asc" ? "up" : "down") : "both";
  return `<th><button class="th-sort ${active ? "active" : ""}" type="button" data-bid-sort="${esc(key)}">${esc(label)} <span class="sort-ind" data-bid-sort-ind="${esc(key)}">${arrow === "up" ? "↑" : arrow === "down" ? "↓" : "↕"}</span></button></th>`;
}

function bidRowHtml(row) {
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
  return `
    <article class="mrow">
      <div class="mrow-head-static">
        ${propertyAddressLink(row, "mrow-address-link")}
        <span>${row.bidStatus === "SCored" ? formatMoneyCompact(row.bidSuggested) : "n/a"}</span>
      </div>
      <div class="mrow-grid">
        <div><span>Neighborhood</span><strong>${esc(row.neighborhoodLabel)}</strong></div>
        <div><span>Ask</span><strong>${formatMoneyCompact(row.pendingListPrice)}</strong></div>
        <div><span>Range</span><strong>${row.bidStatus === "SCored" ? `${formatMoneyCompact(row.bidLow)} - ${formatMoneyCompact(row.bidHigh)}` : "n/a"}</strong></div>
        <div><span>Confidence</span><strong>${esc(row.bidConfidenceLabel)} (${row.bidConfidence || 0})</strong></div>
      </div>
      <button type="button" class="mini-btn" data-use-active-bid="${esc(row.mapPropertyKey)}">Use In Manual Scenario</button>
    </article>
  `;
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

function renderRecordsView() {
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
        <p class="note" id="recordsStatus">${page.total ? `Showing ${page.total} comps/properties in ${recordViewLabel(state.filters.recordView)}. Zillow and KC parcel links stay available for property-level review; blank MLS fields mean unavailable or unknown for that export.${state.flags.projection ? " Rows marked Projected are modeled estimates, not recorded sales." : ""}` : emptyMessage}</p>
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
              ${recordTh("pendingListPrice", "Ask")}
              ${recordTh("domMetric", "DOM")}
              ${recordTh("hotCategory", "Hot")}
              ${recordTh("saleToList", "S/List")}
              ${recordTh("delta", "Bid-Up")}
            </tr>
          </thead>
          <tbody id="recordRows">${page.rows.map(recordRowHtml).join("") || `<tr><td colspan="17">${esc(emptyMessage)}</td></tr>`}</tbody>
        </table>
      </div>
      <div class="mobile-card-list" id="recordMobileList">${page.rows.map(recordMobileCard).join("")}</div>
    </div>
  `;
}

function recordTh(key, label) {
  const active = state.recordSort.key === key;
  const arrow = active ? (state.recordSort.dir === "asc" ? "up" : "down") : "both";
  return `<th><button class="th-sort ${active ? "active" : ""}" type="button" data-record-sort="${esc(key)}">${esc(label)} <span class="sort-ind" data-sort-ind="${esc(key)}">${arrow === "up" ? "↑" : arrow === "down" ? "↓" : "↕"}</span></button></th>`;
}

function hotBadge(row) {
  if (row.isUltraHot) return `<span class="hot-pill ultra">Ultra Hot</span>`;
  if (row.isHotMarket) return `<span class="hot-pill hot">Hot</span>`;
  return `<span class="hot-pill">Normal</span>`;
}

function recordRowHtml(row) {
  const countyUrl = countyRecordUrl(row);
  return `
    <tr>
      <td>${propertyAddressLink(row)}${row.isProjectionRow ? `<span class="proj-pill">Projected</span>` : ""}${countyUrl ? `<a class="sub-link" href="${esc(countyUrl)}" target="_blank" rel="noopener noreferrer">KC Parcel</a>` : ""}${affordTierBadge(row)}</td>
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
  return `
    <article class="mrow">
      <div class="mrow-head-static">
        ${propertyAddressLink(row, "mrow-address-link")}${row.isProjectionRow ? `<span class="proj-pill">Projected</span>` : ""}
        <span>${row.isProjectionRow ? `~${formatMoneyCompact(row.projectedClosePrice)} (est.)` : formatMoneyCompact(row.closePrice || row.pendingListPrice)}</span>
      </div>
      <div class="mrow-grid">
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

async function renderGeoView() {
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
  if (qs("#map", wrap)) return;
  wrap.innerHTML = `
    <div class="view-band geo-view">
      <section class="section-head">
        <div><p class="eyebrow">Geo</p><h2>Where is pressure located?</h2></div>
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
          ${geoLegendHtml()}
          <div id="geoSelectedRows" aria-live="polite"></div>
        </aside>
      </div>
    </div>
  `;
}

function updateGeoShell(rows) {
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
    status.textContent = `${formatWholeNumber(rows.length)} mapped properties in ${recordViewLabel(state.filters.recordView)}${activeCount ? ` (${formatWholeNumber(activeCount)} active)` : ""}. ${drawnCount < rows.length ? `Drawing first ${formatWholeNumber(drawnCount)} for speed.` : "All visible points are drawn."} Color = sale/list pressure; grey = sold, no list price; violet ring = active listing.${applied}`;
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

function geoCardHtml(row, { withLinks = false } = {}) {
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
  if (state.affordability.ready && AFFORD_TIER_META[row.affordTier]) {
    const meta = AFFORD_TIER_META[row.affordTier];
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
  const drawnRows = rows.slice(0, 1200);
  drawnRows.forEach((row) => {
    const selected = state.geo.selectedPropertyKeys.includes(row.mapPropertyKey);
    // Active listings render as a violet hollow ring so "for sale now" reads
    // distinctly from a sold comp (filled, colored by sale/list pressure) and
    // from a list-less sale (filled grey).
    const active = isActiveRow(row);
    const dotColor = active ? ACTIVE_LISTING_COLOR : ratioColor(row.saleToList);
    const marker = L.circleMarker([row.mapLat, row.mapLon], {
      radius: selected ? 7 : 5,
      color: selected ? "#111827" : dotColor,
      fillColor: dotColor,
      fillOpacity: active ? (selected ? 0.35 : 0.1) : (selected ? 0.95 : 0.7),
      weight: active ? 2 : (selected ? 3 : 1),
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

const RATIO_NEUTRAL_COLOR = "#9ca3af";
const ACTIVE_LISTING_COLOR = "#7c3aed";

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

function renderGeoSelectedRows() {
  const wrap = qs("#geoSelectedRows");
  if (!wrap || !state.derived) return;
  const selected = state.derived.viewRows.filter((row) => state.geo.selectedPropertyKeys.includes(row.mapPropertyKey));
  const filterCount = state.geo.filterPropertyKeys.length;
  wrap.innerHTML = selected.length
    ? `<div class="geo-selected-status">${formatWholeNumber(selected.length)} selected for inspection${filterCount ? ` · ${formatWholeNumber(filterCount)} applied as dashboard filter` : ""}</div>${selected.map((row) => { const dates = geoDatesLineHtml(row); return `<article class="mini-record">${propertyAddressLink(row, "mini-record-link")}<span>${esc(row.neighborhoodLabel)} · ${formatMoneyOrNa(row.closePrice || row.pendingListPrice)}</span>${dates}</article>`; }).join("")}`
    : `<div class="empty-state">Click map points to inspect properties. Use "Filter dashboard to selected" only when you want the rest of the app to narrow.</div>`;
}

function toggleMapSelection(key) {
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

function renderDataView() {
  const wrap = qs("#view-data");
  if (!wrap) return;
  const report = state.dataSource.report || {};
  const latestSale = state.derived?.latestSaleDate || "";
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head"><div><p class="eyebrow">Data</p><h2>Refresh and dataset health</h2></div></section>
      <div class="data-grid">
        ${dataTile("Dataset", state.dataSource.datasetName, "dataDatasetName")}
        ${dataTile("Rows", formatWholeNumber(state.dataSource.rowCount), "dataDatasetRows")}
        ${dataTile("Validation", report.status || report.validationStatus || state.dataSource.error || "Not loaded", "dataValidationStatus")}
        ${dataTile("Validation time", report.generatedAt ? formatDateTime(report.generatedAt) : (report.timestamp ? formatDateTime(report.timestamp) : "n/a"), "dataValidationTime")}
        ${dataTile("Latest sale date", latestSale ? `${formatDateShort(latestSale)} (${daysAgo(latestSale)}d ago)` : "n/a", "dataLatestSaleDate")}
        ${dataTile("Output rows", formatWholeNumber(report.outputRows || report.output_row_count || state.dataSource.rowCount || 0), "dataOutputRows")}
        ${dataTile("Realtor files", formatWholeNumber(report.realtorFileCount || report.realtor_file_count || 0), "dataRealtorFileCount")}
      </div>
      <section class="section-block">
        <h3>Public assets</h3>
        <div class="mono">dataMode,addressSource,major,minor,parcelNbr,lat,lon,neighborhood,typeCode,zip,listDate,pendingDate,saleDate,originalListPrice,pendingListPrice,listPriceAtPending,closePrice,beds,baths,sqft,yearBuilt,mlsStatus,mlsListingPrice,mlsOriginalPrice,mlsDOM,mlsCDOM,mlsStyleCode,mlsParkingType,mlsParkingCoveredTotal,mlsTaxesAnnual,mlsBuildingCondition,mlsView,mlsBankOwned,mlsThirdPartyApprovalRequired,mlsNewConstructionState,mlsSquareFootageSource,hotMarketTag,saleToListRatio,saleToOriginalListRatio,bidUpAmount,bidUpPct,bidStrategy,bidSuggested,bidLow,bidHigh,bidRatio,bidConfidence,bidConfidenceLabel,bidCompCount,bidCompTier,bidStatus,isLikelyPresoldNewBuild,presoldRuleReason</div>
      </section>
    </div>
  `;
}

function dataTile(label, value, id) {
  return `<article class="data-tile"><span>${esc(label)}</span><strong id="${esc(id)}">${esc(value)}</strong></article>`;
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
