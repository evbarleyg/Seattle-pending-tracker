// listingStage — how long a home has been for sale decides who has leverage.
//
// Seattle listings are priced for an offer-review date about a week out. Homes
// that sell inside that window and homes that miss it behave like two different
// markets, and one blended sale-to-list number describes neither. Measured on
// about 2,500 single-family sales listed at $1.0M to $1.7M (Jan 2025 to Nov 2026):
//
//   days to pending   sold over ask   sold under ask   median sale / ask
//   0-7                    65%             10%              1.03x
//   8-14                   24%             50%              1.00x
//   15-45                   6%             80%              0.97x
//   46+                     5%             90%              0.96x
//
// The cliff sits between day 7 and day 14 and holds in every window checked
// (all dates, last 12 months, last 6 months, last 90 days), so the stage cut
// points are fixed here rather than fitted per slice.
//
// Days on market means cumulative days where the pipeline knows them: a home
// cancelled and relisted under a new MLS# keeps its original clock (see
// deriveRelistChains in scripts/build_active_ledger.js).
import { domMetric } from "./data.mjs";
import { formatMoneyCompact } from "./format.mjs";
import { percentile } from "./costToWin.mjs";

export const LISTING_STAGES = [
  { id: "fresh", label: "First week", range: "0-7 days", minDom: 0, maxDom: 7 },
  { id: "past", label: "Missed review", range: "8-14 days", minDom: 8, maxDom: 14 },
  { id: "sitting", label: "Sitting", range: "15-45 days", minDom: 15, maxDom: 45 },
  { id: "stale", label: "Stale", range: "46+ days", minDom: 46, maxDom: Infinity },
];

const STAGE_BY_ID = new Map(LISTING_STAGES.map((stage) => [stage.id, stage]));

// A stage needs this many priced sales before its numbers are shown.
export const MIN_STAGE_SALES = 5;

// null in, null out: an unknown days-on-market reading has no stage. It must
// never default to "fresh", which is the most expensive stage to be wrong about.
export function stageForDom(dom) {
  if (dom === null || dom === undefined || dom === "") return null;
  const days = Number(dom);
  if (!Number.isFinite(days) || days < 0) return null;
  return LISTING_STAGES.find((stage) => days <= stage.maxDom)?.id || null;
}

export function stageForRow(row) {
  return stageForDom(domMetric(row));
}

// A normal week lists well under a tenth of standing inventory, so a set of live
// listings that mostly read exactly 0 days is a broken feed, not a hot market
// (this shipped for seven weeks in autumn 2026). Callers show "n/a" instead.
export function hasPlausibleLiveDom(rows) {
  const readings = (rows || []).map(domMetric).filter((days) => days !== null);
  if (readings.length < 10) return true;
  return readings.filter((days) => days === 0).length / readings.length <= 0.5;
}

export function stageMeta(stageId) {
  return STAGE_BY_ID.get(stageId) || null;
}

// Within a dollar of ask counts as "at ask", matching costToWin.mjs.
const AT_ASK_DOLLARS = 1;

// Sale-versus-ask outcomes by stage. A sale takes part only when it closed, has
// a dollar asking price straight from a listing (never the assessed-value
// fallback) and has a days-on-market reading.
export function computeStageLeverage(rows) {
  const buckets = new Map(LISTING_STAGES.map((stage) => [stage.id, []]));
  let eligibleCount = 0;
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || !row.hasDollarListPrice) continue;
    const close = Number(row.closePrice);
    const ask = Number(row.listPriceAtPending);
    if (!(close > 0) || !(ask > 0)) continue;
    const stageId = stageForRow(row);
    if (!stageId) continue;
    buckets.get(stageId).push({ diff: close - ask, ratio: close / ask });
    eligibleCount += 1;
  }
  const stages = LISTING_STAGES.map((stage) => {
    const sales = buckets.get(stage.id);
    const count = sales.length;
    const over = sales.filter((sale) => sale.diff >= AT_ASK_DOLLARS).length;
    const under = sales.filter((sale) => sale.diff <= -AT_ASK_DOLLARS).length;
    const ratios = sales.map((sale) => sale.ratio);
    return {
      ...stage,
      count,
      shareOfSales: eligibleCount ? count / eligibleCount : null,
      overShare: count ? over / count : null,
      atShare: count ? (count - over - under) / count : null,
      underShare: count ? under / count : null,
      medianRatio: percentile(ratios, 0.5),
      p25Ratio: percentile(ratios, 0.25),
      p75Ratio: percentile(ratios, 0.75),
      medianDiffUsd: percentile(sales.map((sale) => sale.diff), 0.5),
      enough: count >= MIN_STAGE_SALES,
    };
  });
  return { eligibleCount, totalCount: Array.isArray(rows) ? rows.length : 0, stages };
}

function oneIn(share) {
  if (!(share > 0)) return "almost none";
  if (share >= 0.45) return `${Math.round(share * 100)}%`;
  return `1 in ${Math.max(2, Math.round(1 / share))}`;
}

// "2.6% under ask" / "3.0% over ask" / "at ask" for a sale-to-ask ratio.
export function ratioVsAskPhrase(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) return "n/a";
  const pct = (ratio - 1) * 100;
  if (Math.abs(pct) < 0.05) return "at ask";
  return `${Math.abs(pct).toFixed(1)}% ${pct > 0 ? "over" : "under"} ask`;
}

// One sentence answering "when does leverage flip to the buyer?".
export function buildStageVerdict(leverage) {
  const stages = leverage?.stages || [];
  const fresh = stages.find((stage) => stage.id === "fresh");
  const later = stages.filter((stage) => stage.id !== "fresh" && stage.count > 0);
  const laterCount = later.reduce((sum, stage) => sum + stage.count, 0);
  if (!fresh?.enough || laterCount < MIN_STAGE_SALES) {
    return "Not enough sales with both an asking price and days on market to compare fresh and older listings here.";
  }
  const laterOver = later.reduce((sum, stage) => sum + stage.overShare * stage.count, 0) / laterCount;
  const laterUnder = later.reduce((sum, stage) => sum + stage.underShare * stage.count, 0) / laterCount;
  return `Competition lives in the first week: ${Math.round(fresh.overShare * 100)}% of homes that sold by day 7 went over ask. After that only ${oneIn(laterOver)} did, and ${Math.round(laterUnder * 100)}% sold for less than ask.`;
}

// Short per-stage result, for example "$38K under" or "$35K over".
export function stageTypicalResult(stage) {
  if (!stage?.enough || !Number.isFinite(stage.medianDiffUsd)) return "n/a";
  if (Math.abs(stage.medianDiffUsd) < AT_ASK_DOLLARS) return "at ask";
  return `${formatMoneyCompact(Math.abs(stage.medianDiffUsd))} ${stage.medianDiffUsd > 0 ? "over" : "under"}`;
}
