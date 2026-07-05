// Pulse view, extracted from src/main.mjs.
//
// Renders the watchlist pulse: recent-window metric cards, per-metric monthly
// charts, the sale/list trajectory by pocket, whole-slice trends (with the
// monthly/weekly grain toggle), competition pockets, and the micro breakout.
//
// main.mjs stays the owner of app state and shared chart helpers; it passes
// them in through the deps object on every render:
//   { state, qs, chartData, chartDomain, chartMetricLabel, formatChartValue,
//     pulseMetricConfig, medianValue, groupRows, buildSliceMonthlySeries,
//     minTileComps }
import {
  esc,
  formatMoney,
  formatMoneyOrNa,
  formatPct,
  formatRatio,
  formatWholeNumber,
  monthLabelCompact,
  toIso,
} from "../domain/format.mjs";
import {
  PULSE_GROUPS,
  competitiveDelta,
  metricDirection,
  rollingAverage,
} from "../domain/pulseMetrics.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

// Maps the six recent-window tile keys (and the pulseReadout bullets) to
// their glossary ids, so "what is this?" always opens the right definition.
const PULSE_METRIC_EXPLAIN_IDS = {
  salesCount: "pulseSalesCount",
  hotShare: "pulseFastSaleShare",
  medianDom: "pulseMedianDom",
  medianSaleToList: "pulseMedianSaleToList",
  medianBidUp: "pulseMedianBidUp",
  medianClosePrice: "pulseMedianClose",
};

// Caption + "what is this?" pair under a headline stat, matching the layout
// overview.mjs uses (shares the .caption-row CSS already defined for it).
function captionRow(captionHtml, explainId = "") {
  const button = explainId ? renderExplainButton(explainId) : "";
  if (!captionHtml && !button) return "";
  return `<div class="caption-row">${captionHtml}${button}</div>`;
}

// One-sentence answer to "is my watchlist heating up?", built from the same
// six recent-window signals the tile grid already shows (salesCount,
// hotShare, medianDom, medianSaleToList, medianBidUp, medianClosePrice).
// Mirrors the counting rule the Overview's good-time banner uses: needs at
// least 2 readable signals, and a 2-signal lead to call a direction instead
// of "no clear shift".
export function watchlistVerdict(recent90) {
  const current = recent90?.current || {};
  const previous = recent90?.previous || {};
  if (!recent90 || !current.salesCount) {
    return {
      tone: "flat",
      answer: "Not enough recent watchlist sales to say.",
      detail: "There have been too few closed sales in your watchlist pockets over the last 90 days to call a trend. Try a broader group pill above, or widen your filters.",
    };
  }
  const directions = Object.keys(PULSE_METRIC_EXPLAIN_IDS).map((key) => metricDirection(key, current[key], previous[key]));
  const hotter = directions.filter((direction) => direction > 0).length;
  const cooler = directions.filter((direction) => direction < 0).length;
  const readCount = hotter + cooler;
  if (readCount < 2) {
    return {
      tone: "flat",
      answer: "Hard to say from this window.",
      detail: "Most of the six signals below have no clean comparison against the prior 90 days yet, so a trend read is not reliable here.",
    };
  }
  if (hotter - cooler >= 2) {
    return {
      tone: "hotter",
      answer: "Yes, this watchlist is heating up.",
      detail: `${hotter} of ${readCount} tracked signals moved against buyers over the last 90 days compared with the 90 days before, so expect faster sales and steeper bidding on the pockets below.`,
    };
  }
  if (cooler - hotter >= 2) {
    return {
      tone: "cooler",
      answer: "No, this watchlist is cooling off.",
      detail: `${cooler} of ${readCount} tracked signals eased for buyers over the last 90 days compared with the 90 days before, so there is a little more room to breathe right now.`,
    };
  }
  return {
    tone: "flat",
    answer: "No clear shift in this watchlist right now.",
    detail: `Over the last 90 days compared with the 90 days before, ${cooler} ${cooler === 1 ? "signal" : "signals"} eased and ${hotter} tightened. Check the tiles below to see which ones moved.`,
  };
}

function watchlistVerdictHtml(recent90, snapshot) {
  const v = watchlistVerdict(recent90);
  const caption = renderUniverseCaption({
    count: recent90?.current?.salesCount,
    universeLabel: `closed sales in ${snapshot.selectedLabel} (last 90 days)`,
  });
  return `
    <section class="section-block market-verdict ${v.tone}" aria-label="Is my watchlist heating up">
      <p class="eyebrow">Is my watchlist heating up?</p>
      <h2 class="hero-line">${esc(v.answer)}</h2>
      <p class="note">${esc(v.detail)}</p>
      ${captionRow(caption, "pulseWatchlist")}
    </section>`;
}

export function renderPulseView(deps) {
  ctx = deps;
  const { state, qs, buildSliceMonthlySeries } = ctx;
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
      ${watchlistVerdictHtml(recent90, snapshot)}
      <section class="section-head">
        <div>
          <p class="eyebrow">Pulse</p>
          <h2>Watchlist snapshot</h2>
        </div>
        <div class="segmented" id="pulseModeToggles">
          <button type="button" class="scope-pill ${state.pulseTimelineMode !== "combined" ? "active" : ""}" data-pulse-mode="compare">Compare</button>
          <button type="button" class="scope-pill ${state.pulseTimelineMode === "combined" ? "active" : ""}" data-pulse-mode="combined">Combined</button>
        </div>
      </section>
      <div class="scope-row" id="pulseGroupPills">
        ${PULSE_GROUPS.map((group) => `<button class="scope-pill ${snapshot.selectedGroup === group.id ? "active" : ""}" type="button" data-pulse-group="${esc(group.id)}">${esc(group.label)}</button>`).join("")}
      </div>
      <div id="pulseStatus" class="note">Showing ${esc(snapshot.selectedLabel)} across ${formatWholeNumber(snapshot.selectedRows.length)} MLS-enriched closed rows. ${renderExplainButton("watchlistPockets")}</div>
      <div class="pulse-grid" id="pulseRecentGrid">
        ${["salesCount", "hotShare", "medianDom", "medianSaleToList", "medianBidUp", "medianClosePrice"].map((key) => pulseMetricCard(key, recent90)).join("")}
      </div>
      <div id="pulseReadout" class="readout">${pulseReadout(snapshot)}</div>
      <div class="chart-grid">
        ${chartPanel("Fast-sale share", "pulseChartHotShare", pulseChartSvg("hotShare", snapshot), chartSummary(snapshot.selectedMonthlySeries, "hotShare"), chartGuide("hotShare", "line"), chartInsight("hotShare"), "pulseFastSaleShare")}
        ${chartPanel("Median DOM", "pulseChartMedianDom", pulseChartSvg("medianDom", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianDom"), chartGuide("medianDom", "line"), chartInsight("medianDom"), "pulseMedianDom")}
        ${chartPanel("Sale/List price pressure", "pulseChartSaleToList", pulseChartSvg("medianSaleToList", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianSaleToList"), chartGuide("medianSaleToList", "line"), chartInsight("medianSaleToList"), "pulseMedianSaleToList")}
        ${chartPanel("Bid-up price pressure", "pulseChartBidUp", pulseChartSvg("medianBidUp", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianBidUp"), chartGuide("medianBidUp", "line"), chartInsight("medianBidUp"), "pulseMedianBidUp")}
        ${chartPanel("Close price band", "pulseChartClosePrice", pulseChartSvg("medianClosePrice", snapshot), chartSummary(snapshot.selectedMonthlySeries, "medianClosePrice"), chartGuide("medianClosePrice", "line"), chartInsight("medianClosePrice"), "pulseMedianClose")}
      </div>
      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Watchlist comparison</p>
            <h3>Sale/List trajectory by pocket ${renderExplainButton("pulseMedianSaleToList")}</h3>
          </div>
        </div>
        <p class="note">Use this to separate broad buyer pressure from a single neighborhood spike.</p>
        ${captionRow(renderUniverseCaption({ count: snapshot.selectedRows.length, universeLabel: `watchlist sales in ${snapshot.selectedLabel} (trailing 12 mo)` }))}
        <div id="pulseTrajectory" class="trajectory-grid">
          ${pulseTrajectoryCards(snapshot)}
        </div>
      </section>
      <section class="section-block" id="pulseSliceTrends">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Whole-slice trends</p>
            <h3>How the current filter band is moving ${renderExplainButton("pulseWeeklyGrain")}</h3>
          </div>
        </div>
        <div class="trends-controls">
          <div class="segmented" id="pulseGrainToggles" role="group" aria-label="Trend grain">
            <button type="button" class="scope-pill ${!weekly ? "active" : ""}" data-pulse-grain="month">Monthly</button>
            <button type="button" class="scope-pill ${weekly ? "active" : ""}" data-pulse-grain="week">Weekly</button>
          </div>
        </div>
        <p class="note">${weekly
          ? "Each point is one calendar week keyed on sale date (not a rolling average), so you read genuine week-to-week movement. The newest week is partial and de-weighted; weeks with fewer than 5 sales show volume only — a thin-week median would whipsaw. Sale/list, pending and DOM stay on monthly grain (too sparse weekly)."
          : "These are the old Charts view, kept here so the context sits beside the watchlist pulse."} These four charts cover every closed sale in your current filters, not just the watchlist pockets above.</p>
        ${captionRow(renderUniverseCaption({ count: sliceRows.length, universeLabel: "closed sales in your current filters, citywide not just the watchlist", windowLabel: "trailing 12 mo" }))}
        <div class="chart-grid">
          ${chartPanel(weekly ? "Weekly volume" : "Monthly volume", "chartVolume", barSvg(volSeries, "salesCount", grainOpts), chartSummary(volSeries, "salesCount"), chartGuide("salesCount", "bar", weekly ? "week" : "month"), chartInsight("salesCount"))}
          ${chartPanel("Median close", "chartClose", lineSvg(priceSeries, "medianClosePrice", grainOpts), chartSummary(priceSeries, "medianClosePrice"), chartGuide("medianClosePrice", "line", weekly ? "week" : "month"), chartInsight("medianClosePrice"), "medianClose")}
          ${weekly
            ? `<article class="chart-panel"><div class="chart-title">Median sale/list ${renderExplainButton("overAskRatio")}</div><p class="chart-insight">Sale/list ratio stays on monthly grain: only a small minority of recent weekly sales carry a genuine list price, so a weekly ratio would jump on n&lt;5. Switch to Monthly to read it.</p>${lineSvg(sliceSeries, "medianSaleToList")}</article>`
            : chartPanel("Median sale/list", "chartRatio", lineSvg(sliceSeries, "medianSaleToList"), chartSummary(sliceSeries, "medianSaleToList"), chartGuide("medianSaleToList", "line"), chartInsight("medianSaleToList"), "overAskRatio")}
          ${chartPanel("Median $/sqft", "chartPsf", lineSvg(priceSeries, "medianPsf", grainOpts), chartSummary(priceSeries, "medianPsf"), chartGuide("medianPsf", "line", weekly ? "week" : "month"), chartInsight("medianPsf"), "medianPsf")}
        </div>
      </section>
      <section class="section-block" id="pulseCompetitionPockets">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Competition pockets</p>
            <h3>Where fast sales concentrate ${renderExplainButton("competitionPockets")}</h3>
          </div>
        </div>
        <p class="note">These rows are clickable neighborhood cross-filters. Fast-sale share is DOM-based; sale/list shows price pressure. Ranked across every neighborhood in your filters, not just the watchlist pockets.</p>
        ${captionRow(renderUniverseCaption({ count: sliceRows.length, universeLabel: "closed sales grouped by neighborhood, citywide not just the watchlist", windowLabel: "trailing 12 mo" }))}
        <div class="heat-list">
          ${heatListHtml(pockets)}
        </div>
      </section>
      <section class="section-block">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Micro breakout</p>
            <h3>Watchlist neighborhoods ${renderExplainButton("watchlistPockets")}</h3>
          </div>
        </div>
        <p class="note">Every watchlist pocket, no matter which pill is selected above, so you can compare all four at a glance.</p>
        ${captionRow(renderUniverseCaption({ count: snapshot.microBreakout.reduce((sum, group) => sum + (group.totalSalesCount || 0), 0), universeLabel: "watchlist sales across all four pockets", windowLabel: "last 90 days" }))}
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
  const config = ctx.pulseMetricConfig(key);
  const direction = metricDirection(key, current[key], previous[key]);
  const delta = competitiveDelta(key, current[key], previous[key]);
  const tone = direction > 0 ? "hotter" : direction < 0 ? "cooler" : "flat";
  let deltaLabel;
  if (delta === null) deltaLabel = "n/a vs prior";
  else if (Math.abs(delta) < 1e-9) deltaLabel = "no change vs prior";
  else deltaLabel = `${esc(config.delta(delta))} vs prior`;
  const explainId = PULSE_METRIC_EXPLAIN_IDS[key] || "";
  return `
    <article class="metric-card ${tone}">
      <span>${esc(config.label)}${explainId ? ` ${renderExplainButton(explainId)}` : ""}</span>
      <strong>${esc(config.format(current[key]))}</strong>
      <small>${deltaLabel}</small>
    </article>
  `;
}

function chartPanel(title, id, content, summary = "", guide = "", insight = "", explainId = "") {
  return `<article class="chart-panel"><div class="chart-title">${esc(title)}${explainId ? ` ${renderExplainButton(explainId)}` : ""}</div>${insight}${summary}${guide}<div id="${esc(id)}">${content}</div></article>`;
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
  const combined = ctx.state.pulseTimelineMode === "combined";
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

// X-axis label: weekly series carry an explicit short label (e.g. "6/8");
// monthly series fall back to the YYYY-MM compact month label.
function pointLabel(point) {
  return point.label || monthLabelCompact(point.month);
}

function chartSummary(series, metricKey, options = {}) {
  const { chartData, chartMetricLabel, formatChartValue, minTileComps } = ctx;
  const data = chartData(series, metricKey, options);
  if (!data.length) return `<div class="chart-summary"><span>No plotted data</span></div>`;
  const latest = data[data.length - 1];
  const high = data.reduce((best, entry) => entry.value > best.value ? entry : best, data[0]);
  const low = data.reduce((best, entry) => entry.value < best.value ? entry : best, data[0]);
  return `
    <div class="chart-summary" aria-label="${esc(chartMetricLabel(metricKey))} chart data summary">
      <span><strong>Latest</strong> ${esc(monthLabelCompact(latest.month))}: ${esc(formatChartValue(metricKey, latest.value))}${Number(latest.count || 0) > 0 && Number(latest.count) < minTileComps ? ` (n=${latest.count})` : ""}</span>
      <span><strong>High</strong> ${esc(monthLabelCompact(high.month))}: ${esc(formatChartValue(metricKey, high.value))}</span>
      <span><strong>Low</strong> ${esc(monthLabelCompact(low.month))}: ${esc(formatChartValue(metricKey, low.value))}</span>
    </div>
  `;
}

function chartGuide(metricKey, kind = "line", grain = "month") {
  const label = ctx.chartMetricLabel(metricKey);
  const unit = grain === "week" ? "week" : "month";
  const unitAdj = grain === "week" ? "weekly" : "monthly";
  if (kind === "bar") {
    return `
      <div class="chart-guide">
        <span><i class="legend-swatch bar"></i>Each bar is one sale ${unit}</span>
        <span>Y-axis: ${esc(label)}</span>
      </div>
    `;
  }
  return `
    <div class="chart-guide">
      <span><i class="legend-swatch monthly"></i>Blue line: ${unitAdj} value</span>
      <span><i class="legend-swatch average"></i>Green line: 3-${unit} average</span>
      <span>Dots: clickable ${unit}s</span>
      <span>Y-axis: ${esc(label)}</span>
    </div>
  `;
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
  const { chartData, chartDomain, chartMetricLabel, formatChartValue, minTileComps } = ctx;
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
        const lowSample = Number(point.count || 0) > 0 && Number(point.count) < minTileComps;
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

function pulseReadout(snapshot) {
  const recent90 = snapshot.recentComparisons.find((entry) => entry.windowDays === 90);
  if (!recent90 || !recent90.current.salesCount) return `<p>No recent watchlist sales in this slice.</p>`;
  const bullets = [];
  bullets.push({
    text: `${snapshot.selectedLabel} has ${formatWholeNumber(recent90.current.salesCount)} sales in the last 90-day pulse window.`,
    explainId: "pulseSalesCount",
  });
  if (recent90.current.medianBidUp !== null) {
    bullets.push({
      text: `Median bid-up is ${formatMoney(recent90.current.medianBidUp)}.`,
      explainId: "pulseMedianBidUp",
    });
  }
  if (recent90.current.medianDom !== null) {
    bullets.push({
      text: `Median DOM is ${Math.round(recent90.current.medianDom)} days.`,
      explainId: "pulseMedianDom",
    });
  }
  return bullets.map(({ text, explainId }) => `<p>${esc(text)} ${renderExplainButton(explainId)}</p>`).join("");
}

// Discrete weekly grain: each point is one NON-overlapping 7-day week, keyed
// STRICTLY on saleDate (reliable on REDFIN_SOLD / MLS rows; county pendingDate is
// faked). Anchored to the latest sale and stepping back every 7 days, so you read
// genuine week-to-week movement — unlike the old rolling-28d window, which shared
// 75% of its data with each neighbour and damped the deltas. Volume is reported
// for every week; the MEDIAN price/$sqft is nulled when a week has fewer than
// MIN_TILE_COMPS sales (thin weeks — narrow slices and the freshest, still-
// reporting weeks — so a 2-sale median can't masquerade as a real swing). Those
// nulls drop out of the price lines via chartData but leave the volume bar intact.
// Sale/list, pending and DOM stay on monthly grain (too sparse weekly).
function buildSliceWeeklySeries(rows, { weeks = 16, windowDays = 7 } = {}) {
  const { medianValue, minTileComps } = ctx;
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
    const enoughForMedian = windowRows.length >= minTileComps;
    series.push({
      month: toIso(endDate),
      label: `${endDate.getMonth() + 1}/${endDate.getDate()}`,
      salesCount: windowRows.length,
      sampleSize: windowRows.length,
      isPartial: i === 0,
      medianClosePrice: enoughForMedian ? medianValue(windowRows.map((row) => row.closePrice)) : null,
      medianPsf: enoughForMedian ? medianValue(windowRows.map((row) => row.pricePerSqft).filter((value) => value > 0)) : null,
    });
  }
  return series;
}

function barSvg(series, metricKey, options = {}) {
  const { chartData, chartDomain, chartMetricLabel, formatChartValue, minTileComps } = ctx;
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
        const lowSample = Number(entry.count || 0) > 0 && Number(entry.count) < minTileComps;
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
  const { groupRows, medianValue } = ctx;
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
