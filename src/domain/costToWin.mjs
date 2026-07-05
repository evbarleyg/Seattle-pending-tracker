import { formatMoneyCompact, formatPct } from "./format.mjs";

// A sale counts as "at list" when the ratio sits within this distance of 1.0.
// Genuine sold-at-list rows divide identical dollar figures and land on exactly
// 1.0, but ratios that arrive pre-computed in the CSV can carry float noise, so
// we absorb anything within a billionth.
const AT_LIST_EPSILON = 1e-9;

// Percentile with linear interpolation between closest ranks (the R-7 method,
// same as Excel PERCENTILE.INC and quantile() in format.mjs). Chosen because it
// is continuous in q, matches the rest of this codebase, and behaves sensibly
// on the small over-ask samples a narrow slice can produce. Returns null when
// no finite values exist, so callers can render "n/a" instead of a fake zero.
export function percentile(values, q) {
  const nums = (values || []).filter((value) => Number.isFinite(value)).slice().sort((a, b) => a - b);
  if (!nums.length) return null;
  const clamped = Math.min(1, Math.max(0, Number(q) || 0));
  const pos = (nums.length - 1) * clamped;
  const base = Math.floor(pos);
  const rest = pos - base;
  const next = nums[base + 1];
  return next === undefined ? nums[base] : nums[base] + rest * (next - nums[base]);
}

// Cost-to-win spread over a set of comps. This replaces the misleading
// "median sale/list" single number: the true distribution straddles list price
// (roughly half over, half under), so its median collapses to 1.00x and reads
// as "no signal" even in a split market.
//
// Eligibility: a row participates only when it is a closed sale (closePrice > 0)
// AND it carries a real, non-imputed list price. The provenance flag is
// hasMarketListPrice (set in data.mjs normalizeRow); county PUBLIC_PROXY rows
// fall back to the tax-assessed value in pendingListPrice, so anything not
// flagged must be excluded or we would chart close-vs-assessed.
//
// Two bases, in order of trust:
// 1. Dollar columns. When normalizeRow's hasDollarListPrice flag says
//    listPriceAtPending came straight from a listing column (not the
//    assessed-value/close-price fallback), classify and measure with real
//    dollars: close minus list. CSV saleToListRatio values can arrive rounded
//    (1.00 for a sale $4k over ask) or contradict the dollar columns, so the
//    dollars win whenever both exist.
// 2. The CSV ratio, for rows flagged hasMarketListPrice without a usable
//    dollar list price. Dollar figures are then reconstructed as
//    impliedList = closePrice / saleToList; we require saleToList > 0 because
//    0 is the normalizer's "unavailable" sentinel.
//
// Shares are fractions of eligibleCount (0..1). Share and median fields are
// null when their basis is empty (no eligible rows, no over-ask rows, no
// under-ask rows) so the UI can say "n/a" rather than show a fake zero.

// Dollar-backed sales within a dollar of list count as "at list": sub-dollar
// differences are rounding noise, matching data.mjs's saleEqualsList rule.
const AT_LIST_DOLLAR_EPSILON = 1;

export function computeCostToWin(rows) {
  const data = Array.isArray(rows) ? rows : [];
  const totalCount = data.length;

  const eligible = [];
  for (const row of data) {
    if (!row || !row.hasMarketListPrice) continue;
    const close = Number(row.closePrice);
    if (!Number.isFinite(close) || close <= 0) continue;
    const dollarList = row.hasDollarListPrice ? Number(row.listPriceAtPending) : 0;
    if (Number.isFinite(dollarList) && dollarList > 0) {
      eligible.push({ close, list: dollarList, dollarBacked: true });
      continue;
    }
    const ratio = Number(row.saleToList);
    if (!Number.isFinite(ratio) || ratio <= 0) continue;
    eligible.push({ close, ratio, dollarBacked: false });
  }

  const overPcts = [];
  const overUsds = [];
  const underPcts = [];
  const underUsds = [];
  let atCount = 0;

  for (const entry of eligible) {
    if (entry.dollarBacked) {
      const diff = entry.close - entry.list;
      if (Math.abs(diff) < AT_LIST_DOLLAR_EPSILON) {
        atCount += 1;
      } else if (diff > 0) {
        overPcts.push(diff / entry.list);
        overUsds.push(diff);
      } else {
        underPcts.push(-diff / entry.list);
        underUsds.push(-diff);
      }
      continue;
    }
    const impliedList = entry.close / entry.ratio;
    if (Math.abs(entry.ratio - 1) <= AT_LIST_EPSILON) {
      atCount += 1;
    } else if (entry.ratio > 1) {
      overPcts.push(entry.ratio - 1);
      overUsds.push(entry.close - impliedList);
    } else {
      underPcts.push(1 - entry.ratio);
      underUsds.push(impliedList - entry.close);
    }
  }

  const eligibleCount = eligible.length;
  const overCount = overPcts.length;
  const underCount = underPcts.length;

  return {
    totalCount,
    eligibleCount,
    overCount,
    atCount,
    underCount,
    overShare: eligibleCount ? overCount / eligibleCount : null,
    atShare: eligibleCount ? atCount / eligibleCount : null,
    underShare: eligibleCount ? underCount / eligibleCount : null,
    medianPremiumPctWhenOver: percentile(overPcts, 0.5),
    medianPremiumUsdWhenOver: percentile(overUsds, 0.5),
    p75PremiumPct: percentile(overPcts, 0.75),
    p90PremiumPct: percentile(overPcts, 0.9),
    medianDiscountPctWhenUnder: percentile(underPcts, 0.5),
    medianDiscountUsdWhenUnder: percentile(underUsds, 0.5),
  };
}

function listOrLessPhrase(share) {
  if (share >= 0.85) return "Nearly all winners";
  if (share >= 0.65) return "Most winners";
  if (share >= 0.55) return "More than half of winners";
  if (share >= 0.45) return "About half of winners";
  if (share >= 0.35) return "Just under half of winners";
  if (share >= 0.15) return "A minority of winners";
  return "Only a few winners";
}

function wholeNumber(value) {
  return Math.round(Number(value) || 0).toLocaleString("en-US");
}

// One plain-English verdict sentence for the "What does winning cost?" block.
// Accepts the stats object from computeCostToWin plus optional formatter
// overrides ({ formatMoneyCompact, formatPct }); defaults to the shared
// formatters from format.mjs so the sentence is usable standalone.
export function buildCostToWinVerdict(stats, formatters = {}) {
  const money = typeof formatters.formatMoneyCompact === "function"
    ? formatters.formatMoneyCompact
    : formatMoneyCompact;
  const pct = typeof formatters.formatPct === "function" ? formatters.formatPct : formatPct;

  const safe = stats || {};
  const totalCount = Number(safe.totalCount) || 0;
  const eligibleCount = Number(safe.eligibleCount) || 0;
  if (!totalCount) {
    return "No closed sales in this slice yet, so cost to win cannot be measured.";
  }
  if (!eligibleCount) {
    return `None of the ${wholeNumber(totalCount)} closed sales here carry a real list price, so cost to win cannot be measured.`;
  }

  const overCount = Number(safe.overCount) || 0;
  const underCount = Number(safe.underCount) || 0;
  const atCount = Number(safe.atCount) || 0;

  if (overCount === 0) {
    if (underCount === 0) {
      return "Every winner in this slice paid exactly list price.";
    }
    return `Every winner in this slice paid list price or less; the typical discount was ${money(safe.medianDiscountUsdWhenUnder)} (${pct(safe.medianDiscountPctWhenUnder)}).`;
  }

  const premiumClause = `the typical premium was ${money(safe.medianPremiumUsdWhenOver)} (${pct(safe.medianPremiumPctWhenOver)})`;
  if (overCount === eligibleCount) {
    return `Every winner in this slice paid over asking; ${premiumClause}.`;
  }

  const listOrLessShare = (atCount + underCount) / eligibleCount;
  return `${listOrLessPhrase(listOrLessShare)} paid list price or less; when buyers went over, ${premiumClause}.`;
}
