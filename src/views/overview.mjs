// Answer-first Overview view, extracted from src/main.mjs.
//
// The view leads with three plain-language answer blocks:
//   1. "Is now a good time?"            — market-direction verdict + KPI tiles
//   2. "What does winning cost?"        — over/at/under-ask spread (costToWin)
//   3. "What changed since you last looked?" — per-slice delta feed (changesSince)
// followed by the decision brief, buyer profile, trends, and micromarkets.
//
// main.mjs stays the owner of app state and shared chart helpers; it passes
// them in through the deps object on every render:
//   { state, qs, icon, buttonIcon, miniMetric, sparklineSvg, chartMetricLabel,
//     pulseMetricConfig, medianValue, minTileComps }
import {
  esc,
  formatDateShort,
  formatMoneyCompact,
  formatMoneyOrNa,
  formatPct,
  formatRatio,
  formatWholeNumber,
  monthLabelCompact,
} from "../domain/format.mjs";
import { domMetric } from "../domain/data.mjs";
import { filtersToSummary, recordViewLabel } from "../domain/selectors.mjs";
import { buildProfileCohort, buildMicromarketProfiles } from "../domain/buyerProfile.mjs";
import { metricDirection } from "../domain/pulseMetrics.mjs";
import { getMetric, formatCadenceNote } from "../domain/glossary.mjs";
import { computeCostToWin, buildCostToWinVerdict } from "../domain/costToWin.mjs";
import { captureBaseline, diffSinceBaseline, isValidBaseline } from "../domain/changesSince.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

// ---------------------------------------------------------------------------
// "What changed since you last looked?" persistence.
//
// Baselines are keyed by a hash of the current slice identity (filters +
// cross-filter clicks + feature flags + dataset), so each saved search gets
// its own memory. Within one page session the first computed diff per slice
// is cached in memory, then a fresh baseline is captured, so the feed stays
// visible while you browse but the next session diffs against today.
const CHANGES_STORAGE_KEY = "buyer_lens_changes_baselines_v1";
const MAX_STORED_SLICE_BASELINES = 4;
const CHANGES_FEED_CAP = 8;
const sessionDiffs = new Map();

function sliceIdentity(state) {
  const src = JSON.stringify({
    f: state.filters,
    i: state.interactions,
    fl: state.flags,
    d: state.dataSource?.datasetName || "",
  });
  // djb2 hash, base36 — short stable key for localStorage.
  let hash = 5381;
  for (let index = 0; index < src.length; index += 1) {
    hash = ((hash * 33) ^ src.charCodeAt(index)) >>> 0;
  }
  return hash.toString(36);
}

function loadBaselineStore() {
  try {
    const raw = localStorage.getItem(CHANGES_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && typeof parsed === "object" && parsed.slices && typeof parsed.slices === "object") {
      return parsed;
    }
  } catch {
    // Fall through to a fresh store (private browsing, corrupted JSON).
  }
  return { v: 1, slices: {} };
}

function persistBaseline(sliceKey, baseline) {
  try {
    const store = loadBaselineStore();
    store.slices[sliceKey] = baseline;
    // Keep only the freshest few slices so thousands of rows per baseline
    // cannot creep toward the localStorage quota.
    const ranked = Object.keys(store.slices).sort((a, b) =>
      String(store.slices[b]?.capturedAt || "").localeCompare(String(store.slices[a]?.capturedAt || "")));
    for (const key of ranked.slice(MAX_STORED_SLICE_BASELINES)) delete store.slices[key];
    localStorage.setItem(CHANGES_STORAGE_KEY, JSON.stringify(store));
  } catch {
    // Quota exceeded: drop everything else and keep only this slice.
    try {
      localStorage.setItem(CHANGES_STORAGE_KEY, JSON.stringify({ v: 1, slices: { [sliceKey]: baseline } }));
    } catch {
      // Storage unavailable entirely; the feature degrades to first-visit copy.
    }
  }
}

function getSliceChanges(state, universeRows) {
  const sliceKey = sliceIdentity(state);
  const cached = sessionDiffs.get(sliceKey);
  if (cached) return { diff: cached, sliceKey, shouldCapture: false };
  const stored = loadBaselineStore().slices[sliceKey] || null;
  const diff = diffSinceBaseline(isValidBaseline(stored) ? stored : null, universeRows);
  sessionDiffs.set(sliceKey, diff);
  return { diff, sliceKey, shouldCapture: true };
}

// ---------------------------------------------------------------------------
// Shared small helpers

function windowLabel(filters) {
  if (filters?.dateFrom || filters?.dateTo) {
    const from = filters.dateFrom ? formatDateShort(filters.dateFrom) : "start";
    const to = filters.dateTo ? formatDateShort(filters.dateTo) : "today";
    return `${from} to ${to}`;
  }
  return "last 12 mo";
}

function agoLabel(minutes) {
  if (minutes === null || minutes === undefined) return "";
  if (minutes < 1) return "moments ago";
  if (minutes < 60) return `${minutes} min ago`;
  if (minutes < 1440) return `${Math.round(minutes / 60)}h ago`;
  return `${Math.round(minutes / 1440)}d ago`;
}

function captionRow(captionHtml, explainId = "") {
  const button = explainId ? renderExplainButton(explainId) : "";
  if (!captionHtml && !button) return "";
  return `<div class="caption-row">${captionHtml}${button}</div>`;
}

function statItem(label, value, explainId = "") {
  return `<div class="mini-metric"><span>${esc(label)}${explainId ? ` ${renderExplainButton(explainId)}` : ""}</span><strong>${esc(value)}</strong></div>`;
}

// ---------------------------------------------------------------------------
// Explain-layer resolver: maps glossary entries (plus live counts) into the
// shape src/ui/explain.mjs renders. main.mjs wires this into initExplainLayer.
export function getExplainEntry(metricId, state) {
  let metric;
  try {
    metric = getMetric(metricId);
  } catch {
    return null;
  }
  const report = state?.dataSource?.report;
  const dates = {
    latestSaleDate: state?.derived?.latestSaleDate || "",
    generatedAt: report?.generatedAt || report?.timestamp || "",
  };
  return {
    metric: {
      id: metric.id,
      label: metric.label,
      definition: metric.plain,
      formula: metric.formulaWords,
      source: metric.source?.label || "",
      cadence: formatCadenceNote(metric, dates),
      caveats: metric.caveats,
    },
    context: { note: liveCoverageNote(metric, state) },
  };
}

function liveCoverageNote(metric, state) {
  const slices = state?.derived?.slices;
  if (!slices) return "";
  const stats = slices.stats || {};
  const closedCount = (slices.closedSlice || []).length;
  const openCount = (slices.openRows || []).length + (slices.projectedRows || []).length;
  switch (metric.id) {
    case "overAskRatio":
    case "shareOverAsk":
    case "typicalPremiumWhenOver":
    case "p75Premium": {
      const ctw = computeCostToWin(slices.closedSlice || []);
      return `Right now ${formatWholeNumber(ctw.eligibleCount)} of ${formatWholeNumber(ctw.totalCount)} closed comps in your slice carry a real list price.`;
    }
    case "activePending":
    case "actives":
      return `Right now ${formatWholeNumber(openCount)} open or pending listings match your filters.`;
    case "allRows":
    case "freshness":
      return `Right now ${formatWholeNumber(state.dataSource?.rowCount || 0)} rows are loaded from ${state.dataSource?.datasetName || "the default dataset"}.`;
    case "savedMatches":
    case "bidQueue":
      return "";
    default:
      return `Right now ${formatWholeNumber(stats.sampleSize ?? closedCount)} closed sales pass your filters.`;
  }
}

// ---------------------------------------------------------------------------
// Trend deltas (moved from main.mjs). Two sign systems meet here and must not
// be mixed up:
//   - The printed number and arrow show the metric's OWN movement
//     (current - previous), so "Median DOM ↑ +5d MoM" means homes sat 5 more
//     days than the prior month.
//   - The tone color and signal text translate that movement into buyer
//     impact via pulseMetrics.mjs, where positive always means hotter for
//     buyers (DOM and inventory are flipped there).

// Metrics whose denominator is a subset of monthly sales must gate on that
// subset's count, mirroring defaultSampleField in main.mjs so a tile's delta
// and its sparkline always agree on which months are trustworthy.
function tileSampleField(metricKey) {
  if (metricKey === "medianSaleToList" || metricKey === "overAskShare") return "ratioSampleSize";
  if (metricKey === "medianBidUp") return "bidUpSampleSize";
  return undefined;
}

function tileDelta(series, metricKey, sampleFieldOverride) {
  const { pulseMetricConfig, medianValue, minTileComps } = ctx;
  const sampleField = sampleFieldOverride || tileSampleField(metricKey);
  const sampleOf = (e) => ((sampleField ? e[sampleField] : undefined) ?? e.sampleSize ?? e.salesCount ?? 0);
  let gated = (series || []).filter((entry) => ((sampleField ? entry[sampleField] : undefined) ?? entry.sampleSize ?? entry.salesCount ?? Infinity) >= minTileComps && Number.isFinite(Number(entry[metricKey])));
  // Drop a thin/partial trailing month (the stale current month) so month-over-month compares two COMPLETE months.
  if (gated.length >= 2 && sampleOf(gated[gated.length - 1]) < 0.5 * sampleOf(gated[gated.length - 2])) gated = gated.slice(0, -1);
  if (gated.length < 2) return { tone: "flat", arrow: "", deltaLabel: "insufficient history", signal: "", secondaryArrow: "", secondaryTone: "flat", secondaryLabel: "" };
  const cfg = pulseMetricConfig(metricKey);
  const toneFor = (dir) => (dir > 0 ? "hotter" : dir < 0 ? "cooler" : "flat");
  // Raw movement of the metric itself (no buyer-impact sign flip).
  const rawDelta = (current, previous) => (Number.isFinite(Number(current)) && Number.isFinite(Number(previous)) ? Number(current) - Number(previous) : null);
  const rawArrow = (delta) => (delta > 0 ? "↑" : delta < 0 ? "↓" : "→");
  const signalFor = (dir) => ({
    medianClosePrice: dir > 0 ? "prices rising" : dir < 0 ? "prices easing" : "prices flat",
    medianPsf: dir > 0 ? "$/sqft climbing" : dir < 0 ? "$/sqft softening" : "$/sqft steady",
    medianSaleToList: dir > 0 ? "buyers bidding over ask" : dir < 0 ? "buyers no longer over ask" : "clearing at ask",
    overAskShare: dir > 0 ? "more homes going over ask" : dir < 0 ? "fewer homes going over ask" : "over-ask share steady",
    medianDom: dir > 0 ? "selling faster" : dir < 0 ? "sitting longer" : "steady pace",
    hotShare: dir > 0 ? "heating up" : dir < 0 ? "cooling" : "holding",
    activeInventory: dir < 0 ? "more choice" : dir > 0 ? "tightening supply" : "flat supply",
  })[metricKey] || "";
  // PRIMARY: month-over-month between the two most recent COMPLETE months (well-sampled in this slice).
  const momDir = metricDirection(metricKey, gated[gated.length - 1][metricKey], gated[gated.length - 2][metricKey]);
  const momDelta = rawDelta(gated[gated.length - 1][metricKey], gated[gated.length - 2][metricKey]);
  // SECONDARY: 3-month vs prior-3-month median for trend context.
  let secondary = { secondaryArrow: "", secondaryTone: "flat", secondaryLabel: "" };
  if (gated.length >= 6) {
    const windowMedian = (entries) => medianValue(entries.map((e) => Number(e[metricKey])));
    const tDir = metricDirection(metricKey, windowMedian(gated.slice(-3)), windowMedian(gated.slice(-6, -3)));
    const tDelta = rawDelta(windowMedian(gated.slice(-3)), windowMedian(gated.slice(-6, -3)));
    if (tDelta !== null) secondary = { secondaryArrow: rawArrow(tDelta), secondaryTone: toneFor(tDir), secondaryLabel: `${cfg.delta(tDelta)} vs prior 3mo` };
  }
  return {
    tone: toneFor(momDir), arrow: momDelta === null ? "" : rawArrow(momDelta), deltaLabel: momDelta === null ? "n/a" : `${cfg.delta(momDelta)} MoM`, signal: signalFor(momDir),
    ...secondary,
  };
}

// Buyer-phrased market direction verdict: same eased/tightened counting as
// before, with the headline rewritten as an answer to "is now a good time?".
function buyerVerdict(series) {
  const metrics = ["medianClosePrice", "medianPsf", "medianSaleToList", "medianDom", "hotShare"];
  const reads = metrics.map((k) => tileDelta(series, k)).filter((d) => d.tone === "hotter" || d.tone === "cooler");
  if (reads.length < 2) {
    return {
      tone: "flat",
      answer: "Hard to say this month.",
      detail: "Not enough recent comps in this slice to call a trend. Widen the price band or clear some filters for a firmer read.",
    };
  }
  const cooler = reads.filter((d) => d.tone === "cooler").length;
  const hotter = reads.filter((d) => d.tone === "hotter").length;
  if (cooler - hotter >= 2) {
    return {
      tone: "cooler",
      answer: "Conditions are leaning your way.",
      detail: `${cooler} of ${reads.length} pressure signals eased vs the prior complete month, so patient shopping and normal contingencies are more realistic right now.`,
    };
  }
  if (hotter - cooler >= 2) {
    return {
      tone: "hotter",
      answer: "It is a tough moment to buy.",
      detail: `${hotter} of ${reads.length} pressure signals tightened vs the prior complete month, so expect faster sales and more competition on good listings.`,
    };
  }
  return {
    tone: "flat",
    answer: "No clear tilt this month.",
    detail: `Compared with the prior complete month, ${cooler} ${cooler === 1 ? "signal" : "signals"} eased and ${hotter} tightened. Check the tiles below to see which pressures moved.`,
  };
}

function goodTimeBannerHtml() {
  const { state } = ctx;
  const v = buyerVerdict(state.derived?.sliceMonthlySeries || []);
  const latest = state.derived?.latestSaleDate || "";
  const asOf = latest ? ` Based on sales recorded through ${formatDateShort(latest)} in this slice; the partial current month is excluded.` : "";
  return `
    <section class="section-block market-verdict ${v.tone}" aria-label="Is now a good time to buy">
      <p class="eyebrow">Is now a good time?</p>
      <h2 class="hero-line">${esc(v.answer)}</h2>
      <p class="note">${esc(v.detail)}${esc(asOf)}</p>
    </section>`;
}

// ---------------------------------------------------------------------------
// Answer block 1 proof: the six KPI tiles + trust cards.

function insightTileHtml({ label, value, metricKey, series, explainId = "", switchView = "pulse", note = "", sub = "", caption = "" }) {
  const d = tileDelta(series, metricKey);
  const buyerPhrase = d.tone === "cooler" ? "better for you" : d.tone === "hotter" ? "tougher for you" : "";
  const signal = d.signal ? `${d.signal}${buyerPhrase ? `, ${buyerPhrase}` : ""}` : buyerPhrase;
  // The article is a plain container (mouse clicks anywhere still navigate via
  // the delegated data-switch-view handler), while keyboard and screen-reader
  // users get a real button: the label itself. Keeping the explain trigger and
  // its popover outside any role=button element is what makes them reachable
  // by assistive tech.
  return `
    <article class="insight-tile ${d.tone}" data-switch-view="${esc(switchView)}"${note ? ` title="${esc(note)}"` : ""}>
      <div class="tile-head"><button type="button" class="panel-kicker tile-open" data-switch-view="${esc(switchView)}">${esc(label)}</button>${explainId ? renderExplainButton(explainId) : ""}</div>
      <div class="tile-figure">
        <strong>${esc(value)}</strong>
        <span class="tile-delta ${d.tone}">${esc(d.arrow)} ${esc(d.deltaLabel)}</span>
      </div>
      ${d.secondaryLabel ? `<span class="tile-delta tile-delta-secondary ${d.secondaryTone}" title="3-month median vs prior 3-month median (trend context)">${esc(d.secondaryArrow)} ${esc(d.secondaryLabel)}</span>` : ""}
      ${sub ? `<span class="tile-sub">${esc(sub)}</span>` : ""}
      ${caption}
      ${ctx.sparklineSvg(series, metricKey)}
      ${signal ? `<span class="tile-signal">${esc(signal)}</span>` : ""}
    </article>`;
}

function freshnessCardHtml() {
  const { state } = ctx;
  const report = state.dataSource.report;
  const validationStatus = String(report?.validation?.status || report?.status || report?.validationStatus || "").toLowerCase();
  const pipelineFailed = !!validationStatus && validationStatus !== "pass";
  const latestSale = state.derived?.latestSaleDate || "";
  let statusLine;
  if (pipelineFailed) statusLine = "The last data refresh did not pass its checks, so treat these numbers with caution.";
  else if (validationStatus === "pass") statusLine = "The last data refresh passed its checks.";
  else statusLine = "No refresh report loaded yet.";
  const cadenceLine = formatCadenceNote("freshness", {
    latestSaleDate: latestSale,
    generatedAt: report?.generatedAt || report?.timestamp || "",
  });
  return `
    <article class="state-panel${pipelineFailed ? " alert" : ""}">
      <div class="panel-kicker">Freshness ${renderExplainButton("freshness")}</div>
      <h2>${esc(latestSale ? `Newest sale ${formatDateShort(latestSale)}` : "No sale dates")}</h2>
      <p>${esc(statusLine)} ${esc(cadenceLine)}</p>
      ${renderUniverseCaption({ count: state.dataSource.rowCount, universeLabel: "rows loaded before filters" })}
    </article>`;
}

function commandCenterCardsHtml(costToWin) {
  const { state, buttonIcon } = ctx;
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
  const savedStatus = state.buyerProfile.enabled
    ? `${cohort.summary.count} saved-home matches`
    : "Saved-home lens paused";
  const sliceSeries = state.derived.sliceMonthlySeries || [];
  const invSeries = state.derived.inventoryMonthlySeries || [];
  const window = windowLabel(state.filters);
  const closedCaption = renderUniverseCaption({ count: stats.sampleSize, universeLabel: "closed sales in your filters", windowLabel: window });
  // The $/sqft and DOM medians rest on subsets of the closed slice (rows with
  // recorded square footage / a usable days-on-market value), so their
  // captions must count that subset, not the whole slice.
  const closedRows = slices.closedSlice || [];
  const psfCount = closedRows.filter((row) => Number(row.pricePerSqft) > 0).length;
  const domCount = closedRows.filter((row) => domMetric(row) !== null).length;
  const eligibleOfClosed = (count, eligibleLabel) => renderUniverseCaption({
    count,
    universeLabel: `${eligibleLabel}, out of ${formatWholeNumber(stats.sampleSize)} closed in your filters`,
    windowLabel: window,
  });
  const tiles = [
    insightTileHtml({
      label: "Median close",
      value: formatMoneyOrNa(stats.medianClose),
      metricKey: "medianClosePrice",
      series: sliceSeries,
      explainId: "medianClose",
      sub: "The typical final sale price here. Lower or easing is better for you.",
      caption: closedCaption,
    }),
    insightTileHtml({
      label: "Median $/sqft",
      value: stats.medianPsf ? `$${Math.round(stats.medianPsf)}` : "n/a",
      metricKey: "medianPsf",
      series: sliceSeries,
      explainId: "medianPsf",
      sub: "Price per square foot, so different-sized homes compare fairly.",
      caption: eligibleOfClosed(psfCount, "sales with recorded square footage"),
    }),
    insightTileHtml({
      label: "Sold over ask",
      value: costToWin.overShare === null ? "n/a" : formatPct(costToWin.overShare),
      metricKey: "overAskShare",
      series: sliceSeries,
      explainId: "shareOverAsk",
      sub: "The share of winners who paid more than the asking price.",
      caption: renderUniverseCaption({ count: costToWin.eligibleCount, universeLabel: "comps with a real list price", windowLabel: window }),
    }),
    insightTileHtml({
      label: "Median DOM",
      value: stats.medianDom === null ? "n/a" : `${Math.round(stats.medianDom)}d`,
      metricKey: "medianDom",
      series: sliceSeries,
      explainId: "medianDom",
      sub: "Days a typical home sat before going pending. More days is better for you.",
      caption: eligibleOfClosed(domCount, "sales with days-on-market data"),
    }),
    insightTileHtml({
      label: "Fast-sale share",
      value: stats.hotShare === null ? "n/a" : formatPct(stats.hotShare),
      metricKey: "hotShare",
      series: sliceSeries,
      explainId: "fastSaleShare",
      sub: "Share of homes gone in 10 days or less, a competition gauge.",
      caption: closedCaption,
    }),
    insightTileHtml({
      label: "Active + pending (MLS only)",
      value: formatWholeNumber(slices.openRows.length + slices.projectedRows.length),
      metricKey: "activeInventory",
      series: invSeries,
      explainId: "activePending",
      sub: "Homes on the market or under contract right now. More choice helps you.",
      caption: renderUniverseCaption({ universeLabel: "MLS and Redfin listings only, county rows excluded" }),
      note: "MLS/Redfin listings only, a flow proxy, not standing inventory (county rows excluded).",
    }),
  ].join("");

  return `
    <div class="insight-tile-grid">${tiles}</div>
    <p class="note tone-legend"><span class="tone-chip cooler">●</span> Cooling, better for buyers &nbsp; <span class="tone-chip hotter">●</span> Heating, tougher for buyers</p>
    <div class="command-grid">
      ${freshnessCardHtml()}
      <article class="state-panel">
        <div class="panel-kicker">Saved-home lens ${renderExplainButton("savedMatches")}</div>
        <h2>${esc(savedStatus)}</h2>
        <p>${esc(profile.name)} · ${cohort.summary.topMicromarket || "no top micromarket yet"}</p>
        ${renderUniverseCaption({ count: slices.closedSlice.length, universeLabel: "closed comps in your slice", windowLabel: windowLabel(state.filters) })}
      </article>
      <article class="state-panel">
        <div class="panel-kicker">Bid queue ${renderExplainButton("bidQueue")}</div>
        <h2>${formatWholeNumber(bidStatsView.activeCount)} active</h2>
        <p>${formatWholeNumber(bidStatsView.scoredCount)} scored · ${formatWholeNumber(bidStatsView.highConfidenceCount)} high confidence</p>
        ${renderUniverseCaption({ universeLabel: "active MLS listings passing your filters" })}
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

function commandCenterSectionHtml(costToWin) {
  const { state } = ctx;
  const closedCount = (state.derived?.slices?.closedSlice || []).length;
  return `
    <section class="command-center compact" id="commandCenter" aria-label="Buyer command center">
      <div class="hero-strip">
        <p class="eyebrow">Buyer command center</p>
        <h2 class="hero-line">${esc(filtersToSummary(state.filters).join(" + "))} · ${formatWholeNumber(closedCount)} comps in slice</h2>
        ${captionRow(renderUniverseCaption({ count: closedCount, universeLabel: "closed comps in your slice", windowLabel: windowLabel(state.filters) }), "closedSlice")}
      </div>
      <div class="command-stack" id="commandGrid">
        ${commandCenterCardsHtml(costToWin)}
      </div>
    </section>
  `;
}

// ---------------------------------------------------------------------------
// Answer block 2: "What does winning cost?"

function costToWinSectionHtml(ctw) {
  const { state } = ctx;
  const verdict = buildCostToWinVerdict(ctw);
  const caption = renderUniverseCaption({
    count: ctw.eligibleCount,
    universeLabel: `comps with a real list price, out of ${formatWholeNumber(ctw.totalCount)} closed comps in your slice`,
    windowLabel: windowLabel(state.filters),
  });
  let proof = "";
  if (ctw.eligibleCount > 0) {
    const overPct = (ctw.overShare || 0) * 100;
    const atPct = (ctw.atShare || 0) * 100;
    const underPct = (ctw.underShare || 0) * 100;
    const barLabel = `Of ${formatWholeNumber(ctw.eligibleCount)} sales with a real list price: ${formatPct(ctw.overShare)} sold over ask, ${formatPct(ctw.atShare)} at ask, ${formatPct(ctw.underShare)} under ask.`;
    const statItems = [];
    if (ctw.overCount > 0) {
      statItems.push(statItem(
        "Typical premium when over",
        `${formatMoneyCompact(ctw.medianPremiumUsdWhenOver)} (${formatPct(ctw.medianPremiumPctWhenOver)})`,
        "typicalPremiumWhenOver"
      ));
      statItems.push(statItem("1 in 4 over-ask winners paid above", formatPct(ctw.p75PremiumPct), "p75Premium"));
      statItems.push(statItem("1 in 10 over-ask winners paid above", formatPct(ctw.p90PremiumPct)));
    }
    if (ctw.underCount > 0) {
      statItems.push(statItem(
        "Typical discount when under",
        `${formatMoneyCompact(ctw.medianDiscountUsdWhenUnder)} (${formatPct(ctw.medianDiscountPctWhenUnder)})`
      ));
    }
    proof = `
      <div class="cost-bar" role="img" aria-label="${esc(barLabel)}">
        ${overPct > 0 ? `<span class="cost-seg over" style="width:${overPct.toFixed(2)}%"></span>` : ""}
        ${atPct > 0 ? `<span class="cost-seg at" style="width:${atPct.toFixed(2)}%"></span>` : ""}
        ${underPct > 0 ? `<span class="cost-seg under" style="width:${underPct.toFixed(2)}%"></span>` : ""}
      </div>
      <div class="cost-bar-legend">
        <span><i class="cost-swatch over"></i>Over ask ${esc(formatPct(ctw.overShare))} (${formatWholeNumber(ctw.overCount)})</span>
        <span><i class="cost-swatch at"></i>At ask ${esc(formatPct(ctw.atShare))} (${formatWholeNumber(ctw.atCount)})</span>
        <span><i class="cost-swatch under"></i>Under ask ${esc(formatPct(ctw.underShare))} (${formatWholeNumber(ctw.underCount)})</span>
      </div>
      ${statItems.length ? `<div class="metric-list">${statItems.join("")}</div>` : ""}
    `;
  } else {
    proof = `<p class="note">Widen the slice or include more MLS-covered neighborhoods to measure the over-ask spread.</p>`;
  }
  return `
    <section class="section-block" id="costToWin">
      <div class="section-head compact">
        <div>
          <p class="eyebrow">What does winning cost?</p>
          <h3>${esc(verdict)}</h3>
        </div>
      </div>
      ${captionRow(caption, "shareOverAsk")}
      ${proof}
    </section>
  `;
}

// ---------------------------------------------------------------------------
// Answer block 3: "What changed since you last looked?"

function feedItemHtml(entry, extra = "") {
  const address = entry.address || "Address unavailable";
  const link = entry.url
    ? `<a class="changes-link" href="${esc(entry.url)}" target="_blank" rel="noopener noreferrer">${esc(address)}</a>`
    : `<span class="changes-link">${esc(address)}</span>`;
  const bits = [entry.neighborhood, entry.price > 0 ? formatMoneyCompact(entry.price) : ""].filter(Boolean).join(" · ");
  return `<li>${link}${bits ? ` · ${esc(bits)}` : ""}${extra}</li>`;
}

function changesGroupHtml(title, list, renderItem) {
  if (!list.length) return "";
  const items = list.slice(0, CHANGES_FEED_CAP).map(renderItem).join("");
  const more = list.length > CHANGES_FEED_CAP
    ? `<p class="note">+${formatWholeNumber(list.length - CHANGES_FEED_CAP)} more, see the Records tab.</p>`
    : "";
  return `
    <div class="changes-group">
      <h4>${esc(title)}</h4>
      <ul class="changes-list">${items}</ul>
      ${more}
    </div>`;
}

function changesSectionHtml(diff) {
  if (!diff || diff.isFirstVisit) {
    return `
      <section class="section-block since-visit">
        <p class="eyebrow">What changed since you last looked?</p>
        <p class="note">First look at this slice on this device. A snapshot was just saved, so from your next visit this section lists new listings, price cuts, fast pendings, and fresh closings that match these filters.</p>
      </section>
    `;
  }
  const totals = diff.totals || {};
  const total = (totals.newActives || 0) + (totals.priceCuts || 0) + (totals.wentPendingFast || 0) + (totals.newClosed || 0);
  const sinceLabel = agoLabel(diff.minutesSinceBaseline);
  if (!total) {
    return `
      <section class="section-block since-visit">
        <p class="eyebrow">What changed since you last looked? (${esc(sinceLabel)})</p>
        <p class="note">Nothing new in this slice since then. New activity usually lands with the next data refresh.</p>
      </section>
    `;
  }
  const groups = [
    changesGroupHtml("New on the market", diff.newActives, (entry) => feedItemHtml(entry, entry.listDate ? ` · listed ${esc(formatDateShort(entry.listDate))}` : "")),
    changesGroupHtml("Price cuts", diff.priceCuts, (entry) => feedItemHtml(
      { ...entry, price: 0 },
      ` · was ${esc(formatMoneyCompact(entry.oldPrice))}, now ${esc(formatMoneyCompact(entry.newPrice))} (down ${esc(formatMoneyCompact(entry.cutAmount))}, ${esc(formatPct(entry.cutPct))})`
    )),
    changesGroupHtml("Went pending fast", diff.wentPendingFast, (entry) => feedItemHtml(entry, entry.pendingDate ? ` · pending ${esc(formatDateShort(entry.pendingDate))}` : "")),
    changesGroupHtml("Newly closed", diff.newClosed, (entry) => feedItemHtml(entry, entry.saleDate ? ` · closed ${esc(formatDateShort(entry.saleDate))}` : "")),
  ].join("");
  return `
    <section class="section-block since-visit" id="changesFeed">
      <div class="section-head compact">
        <div>
          <p class="eyebrow">What changed since you last looked? (${esc(sinceLabel)})</p>
          <h3>${formatWholeNumber(totals.newActives || 0)} new on market · ${formatWholeNumber(totals.priceCuts || 0)} price cut${(totals.priceCuts || 0) === 1 ? "" : "s"} · ${formatWholeNumber(totals.wentPendingFast || 0)} went pending fast · ${formatWholeNumber(totals.newClosed || 0)} newly closed</h3>
        </div>
      </div>
      <p class="note">Compared against this exact slice, so changing filters starts a new memory.</p>
      <div class="changes-grid">${groups}</div>
    </section>
  `;
}

// ---------------------------------------------------------------------------
// Main render (called by main.mjs renderView with the deps object).

export function renderOverviewView(deps) {
  ctx = deps;
  const { state, qs, icon, miniMetric, sparklineSvg, chartMetricLabel, medianValue, minTileComps } = ctx;
  const wrap = qs("#view-overview");
  if (!wrap || !state.derived) return;
  const { slices } = state.derived;
  const costToWin = computeCostToWin(slices.closedSlice || []);
  const gatedSlice = (state.derived.sliceMonthlySeries || []).filter((e) => (e.sampleSize ?? e.salesCount ?? 0) >= minTileComps);
  const gatedCounts = gatedSlice.map((e) => e.sampleSize ?? e.salesCount ?? 0).slice().sort((a, b) => a - b);
  const medGatedCount = gatedCounts.length ? gatedCounts[Math.floor(gatedCounts.length / 2)] : 0;
  const latestGated = gatedSlice[gatedSlice.length - 1] || null;
  const latestGatedCount = latestGated ? (latestGated.sampleSize ?? latestGated.salesCount ?? 0) : 0;
  const latestPartial = !!latestGated && medGatedCount > 0 && latestGatedCount < 0.5 * medGatedCount;
  const freshnessLine = latestGated
    ? `Charts show data through ${monthLabelCompact(latestGated.month)}${latestPartial ? ` (partial, only ${latestGatedCount} comps so far; treat its swing as noise)` : ""}.`
    : "No qualifying months in this slice yet.";
  const profile = state.buyerProfile.memory;
  const cohort = buildProfileCohort(slices.closedSlice, profile);
  const profiles = buildMicromarketProfiles(slices.closedSlice, profile, new Date());
  const changeUniverse = [...(slices.closedSlice || []), ...(slices.openRows || [])];
  const changes = getSliceChanges(state, changeUniverse);
  const stats = slices.stats;
  const pulse90 = state.derived.pulse.recentComparisons.find((entry) => entry.windowDays === 90);
  const fastSaleText = pulse90?.current?.hotShare !== null && pulse90?.current?.hotShare !== undefined
    ? formatPct(pulse90.current.hotShare)
    : "n/a";
  // Median bid-up only means something on rows with a real list price; the
  // normalizer writes delta=0 for every county row without one, so an
  // unfiltered median (stats.medianBidUp) drags toward a fake $0. This mirrors
  // the hasMarketListPrice filter buildSliceMonthlySeries uses for the trends
  // strip, so the stance line and the charts agree.
  const bidUpRows = (slices.closedSlice || []).filter((row) => row.hasMarketListPrice && Number.isFinite(row.delta));
  const stanceBidUp = bidUpRows.length ? medianValue(bidUpRows.map((row) => row.delta)) : null;
  const stance = `${fastSaleText} fast-sale share · ${formatMoneyOrNa(stats.medianClose)} median close · ${formatMoneyOrNa(stanceBidUp)} median bid-up · ${stats.medianDom === null ? "n/a" : `${Math.round(stats.medianDom)}d`} median days on market`;
  wrap.innerHTML = `
    <div class="view-band">
      ${goodTimeBannerHtml()}
      ${commandCenterSectionHtml(costToWin)}

      ${costToWinSectionHtml(costToWin)}

      ${changesSectionHtml(changes.diff)}

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
            <p>Fast-sale share shows how quickly homes get snapped up; median close and bid-up show what winners actually paid.</p>
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
        <p class="note">${esc(freshnessLine)} Months with fewer than ${minTileComps} comps are omitted.</p>
        <div class="trend-strip">
          ${["medianPsf", "overAskShare", "medianBidUp"].map((k) => `
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
                <span>${formatMoneyOrNa(entry.summary.medianClosePrice)} median close</span>
                <span>${formatRatio(entry.summary.medianSaleToList)} sale vs list</span>
              </div>
            </article>
          `).join("")}
        </div>
      </section>
    </div>
  `;
  // Capture the fresh baseline only after the diff has been rendered, so what
  // the buyer just saw is exactly what the next visit compares against.
  if (changes.shouldCapture) {
    persistBaseline(changes.sliceKey, captureBaseline(changeUniverse));
  }
}
