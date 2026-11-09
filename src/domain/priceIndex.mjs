// priceIndex — are prices moving, once the mix of homes that sold is held constant?
//
// The median close cannot answer that here. The lens's price band clips it (a $1.1M to
// $1.6M band holds the median near $1.30M whatever the market does), and even unclipped it
// moves with what happened to sell: more small houses in winter reads as falling prices.
//
// This is a time-dummy hedonic index, the standard fix. One regression over every sale of
// the home type:
//
//   ln(close) = size, lot, age, newer build, beds, baths, neighborhood, whose
//               square footage it is  +  a term per month
//
// The month terms are the index: what the same home would have fetched in each month.
//
// Sales are trimmed to a wide price range rather than the lens's band: choosing sales by
// their price is choosing them by the thing being measured. Only listed (MLS or Redfin)
// sales take part; county-only records run months behind. The current calendar month is
// never shown: a third of a month's closings is not the month.
//
// THE HARD PART IS THE SOURCES. Rows built on county records carry the assessor's square
// footage; Redfin's own rows carry Redfin's, which averages 13% larger on the 265 homes
// that have both (finished basements, mostly). And the mix shifts: Redfin rows were a fifth
// of listed sales in Oct 2025 and all of them from Jul 2026. Two things follow.
//
//  1. A term for whose square footage a row carries, or homes seem to grow and prices to
//     fall.
//  2. A source counts in a month only as far as it is a fair sample of that month. Where it
//     supplies a small share it is a leftover: the Redfin rows of late 2025 are the sales
//     that failed to match a county record (34% newer builds against 13%), and they ran
//     2.5% ABOVE county rows like for like, where the Redfin rows of Apr-Jun 2026, half the
//     market, ran 4.9% below. One source term cannot be both. Left in at full weight they
//     pull the year's change from about -1% to -5%, and by a different amount on every
//     re-merge of the data (-1.9% and -5.0% on two builds of the same month). So a source's
//     rows are weighted by its share of the month: nothing at INDEX_SOURCE_SHARE_NONE or
//     less, fully at INDEX_SOURCE_SHARE_FULL, a ramp between so that no handful of rows
//     can switch a month on or off.
//
// What survives is a chain: county rows carry Oct to May, Redfin rows Apr onward, and the
// months both cover tie them together. The tie's uncertainty is in the standard errors,
// which is why a year's change is known to about +/-4% while one inside a single source's
// stretch (Jul to Oct 2026: -3.5%) is known to +/-3%. Single family, Oct 2025 to Oct 2026,
// reads between -2% and +1% across two builds of the data and ramps from 0.2-0.35 to
// 0.3-0.45. So: about level on the year, 7% to 10% below the May peak.
import { clamp, median, monthLabelCompact, toIso } from "./format.mjs";

export const INDEX_MONTHS = 13;
export const INDEX_MIN_MONTH_SALES = 40;
// A month also needs this share of the typical month's sales.
export const INDEX_MIN_MONTH_SHARE = 0.35;
export const INDEX_MIN_MONTHS = 6;
export const INDEX_SOURCE_SHARE_NONE = 0.25;
export const INDEX_SOURCE_SHARE_FULL = 0.4;
// Two sources are tied together through the months both cover, each month counting by the
// lesser of the two weights. Under this much the tie rests on too little, and only the
// newer source's stretch is shown.
export const INDEX_MIN_OVERLAP_MONTHS = 1;
export const INDEX_MIN_PRICE = 400_000;
export const INDEX_MAX_PRICE = 4_000_000;
export const INDEX_MIN_NEIGHBORHOOD_SALES = 15;
export const INDEX_NEWER_BUILD_YEARS = 15;
// A change is called only when it is this many standard errors from zero.
export const INDEX_CALL_Z = 1.96;
// The peak is picked for being the highest of a dozen noisy months, so a fall from it has
// to clear a higher bar. At 1.96 a perfectly flat market showed a "clear" fall from its
// peak in 18% of simulated years; this is the same 5% test spread over 12 comparisons.
export const INDEX_PEAK_Z = 2.87;

// Solves A x = b for symmetric positive-definite A and returns [x, inverse(A)]. Plain
// Gauss-Jordan: the system has about 60 unknowns and is solved once per dataset.
function solveWithInverse(a, b) {
  const n = b.length;
  const m = a.map((row, i) => [...row, ...Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)), b[i]]);
  for (let col = 0; col < n; col += 1) {
    let pivot = col;
    for (let row = col + 1; row < n; row += 1) if (Math.abs(m[row][col]) > Math.abs(m[pivot][col])) pivot = row;
    if (Math.abs(m[pivot][col]) < 1e-10) return null; // collinear columns
    [m[col], m[pivot]] = [m[pivot], m[col]];
    const scale = m[col][col];
    for (let j = col; j <= 2 * n; j += 1) m[col][j] /= scale;
    for (let row = 0; row < n; row += 1) {
      if (row === col) continue;
      const factor = m[row][col];
      if (!factor) continue;
      for (let j = col; j <= 2 * n; j += 1) m[row][j] -= factor * m[col][j];
    }
  }
  return [m.map((row) => row[2 * n]), m.map((row) => row.slice(n, 2 * n))];
}

function shiftMonth(key, by) {
  const at = Number(key.slice(0, 4)) * 12 + Number(key.slice(5, 7)) - 1 + by;
  return `${Math.floor(at / 12)}-${String((at % 12) + 1).padStart(2, "0")}`;
}

const monthOf = (row) => row.saleDate.slice(0, 7);
const isRedfinSized = (row) => row.mlsSquareFootageSource === "Redfin";

function usableSale(row, typeLabel, currentMonth) {
  return row.hasActualClose && row.dataMode === "MLS_ENRICHED" && row.typeLabel === typeLabel
    && !!row.saleDate && monthOf(row) < currentMonth
    && row.closePrice >= INDEX_MIN_PRICE && row.closePrice <= INDEX_MAX_PRICE
    && row.sqft > 300 && row.sqft < 12_000;
}

// The newest unbroken run of months with enough (weighted) sales to place. Thin months at
// the end are skipped; a thin month further back ends the run.
function monthWindow(weighted, months) {
  const totals = new Map();
  for (const { row, weight } of weighted) totals.set(monthOf(row), (totals.get(monthOf(row)) || 0) + weight);
  const floor = Math.max(INDEX_MIN_MONTH_SALES, INDEX_MIN_MONTH_SHARE * median([...totals.values()]));
  const enough = (key) => (totals.get(key) || 0) >= floor;
  const keys = [];
  for (let key = [...totals.keys()].filter(enough).sort().pop() || ""; key && enough(key) && keys.length < months; key = shiftMonth(key, -1)) keys.unshift(key);
  return keys;
}

// Weighs each sale by how fair a sample its source is of its month, picks the months to
// fit, and makes sure the sources inside them can be chained (see the header).
function chooseSales(sales, months) {
  const tally = new Map(); // month -> [other, redfin]
  for (const row of sales) {
    const counts = tally.get(monthOf(row)) || [0, 0];
    counts[isRedfinSized(row) ? 1 : 0] += 1;
    tally.set(monthOf(row), counts);
  }
  const sourceWeight = (month, redfin) => {
    const counts = tally.get(month);
    const share = counts[redfin ? 1 : 0] / (counts[0] + counts[1]);
    return clamp((share - INDEX_SOURCE_SHARE_NONE) / (INDEX_SOURCE_SHARE_FULL - INDEX_SOURCE_SHARE_NONE), 0, 1);
  };
  let weighted = sales.map((row) => ({ row, weight: sourceWeight(monthOf(row), isRedfinSized(row)) })).filter((entry) => entry.weight > 0);
  let monthKeys = monthWindow(weighted, months);
  const present = (redfin) => monthKeys.some((month) => sourceWeight(month, redfin) > 0);
  if (present(true) && present(false)) {
    const overlap = monthKeys.reduce((sum, month) => sum + Math.min(sourceWeight(month, true), sourceWeight(month, false)), 0);
    if (overlap < INDEX_MIN_OVERLAP_MONTHS) {
      // Nothing to tie them with: keep whichever source carries the newest month fitted.
      const newest = monthKeys[monthKeys.length - 1];
      const redfin = sourceWeight(newest, true) >= sourceWeight(newest, false);
      weighted = weighted.filter((entry) => isRedfinSized(entry.row) === redfin);
      monthKeys = monthWindow(weighted, months);
    }
  }
  const kept = new Set(monthKeys);
  return { monthKeys, weighted: weighted.filter((entry) => kept.has(monthOf(entry.row))) };
}

// `rows` are normalized rows. Returns null when there is too little to fit.
export function computePriceIndex(rows, { typeLabel, neighborhoods = [], months = INDEX_MONTHS, today = new Date() } = {}) {
  const currentMonth = toIso(today).slice(0, 7);
  let sales = (rows || []).filter((row) => usableSale(row, typeLabel, currentMonth));
  if (neighborhoods.length) sales = sales.filter((row) => neighborhoods.includes(row.neighborhoodLabel));
  const { monthKeys, weighted } = chooseSales(sales, months);
  if (monthKeys.length < INDEX_MIN_MONTHS) return null;
  const monthAt = new Map(monthKeys.map((key, index) => [key, index]));

  const hoodCounts = new Map();
  for (const { row } of weighted) hoodCounts.set(row.neighborhoodLabel, (hoodCounts.get(row.neighborhoodLabel) || 0) + 1);
  // The largest neighborhood is the reference; small ones share one "other" term.
  const hoods = [...hoodCounts.entries()].filter(([, count]) => count >= INDEX_MIN_NEIGHBORHOOD_SALES).sort((a, b) => b[1] - a[1]).map(([name]) => name);
  const hoodAt = new Map(hoods.slice(1).map((name, index) => [name, index]));
  const hasOther = weighted.some(({ row }) => !hoods.includes(row.neighborhoodLabel));
  const lots = weighted.map(({ row }) => Number(row.lotSize) || 0).filter((lot) => lot > 300);
  const typicalLogLot = lots.length ? Math.log(median(lots)) : 0;

  const BASE = 10; // intercept + 9 characteristics
  const otherAt = BASE + hoodAt.size;
  const monthStart = otherAt + (hasOther ? 1 : 0);
  const width = monthStart + monthKeys.length - 1; // the first month is the reference
  if (weighted.length < width * 5) return null;
  const features = (row) => {
    const x = new Float64Array(width);
    const year = Number(row.yearBuilt);
    const age = year > 1800 ? Math.min(130, Math.max(0, Number(row.saleDate.slice(0, 4)) - year)) : 70;
    const lot = Number(row.lotSize) || 0;
    x[0] = 1;
    x[1] = Math.log(row.sqft);
    x[2] = lot > 300 ? Math.log(Math.min(lot, 40_000)) : typicalLogLot;
    x[3] = lot > 300 ? 0 : 1;
    x[4] = age / 100;
    x[5] = (age / 100) ** 2;
    x[6] = year > 1800 && age <= INDEX_NEWER_BUILD_YEARS ? 1 : 0;
    x[7] = Math.min(6, Math.max(1, Number(row.beds) || 3));
    x[8] = Math.min(5, Math.max(1, Number(row.baths) || 2));
    x[9] = isRedfinSized(row) ? 1 : 0;
    if (hoodAt.has(row.neighborhoodLabel)) x[BASE + hoodAt.get(row.neighborhoodLabel)] = 1;
    else if (row.neighborhoodLabel !== hoods[0]) x[otherAt] = 1;
    const month = monthAt.get(monthOf(row));
    if (month > 0) x[monthStart + month - 1] = 1;
    return x;
  };

  // Weighted least squares through the normal equations.
  const design = weighted.map(({ row }) => features(row));
  const y = weighted.map(({ row }) => Math.log(row.closePrice));
  const xtx = Array.from({ length: width }, () => new Float64Array(width));
  const xty = new Float64Array(width);
  design.forEach((x, i) => {
    const w = weighted[i].weight;
    for (let j = 0; j < width; j += 1) {
      if (!x[j]) continue;
      xty[j] += w * x[j] * y[i];
      for (let k = j; k < width; k += 1) if (x[k]) xtx[j][k] += w * x[j] * x[k];
    }
  });
  // A column that never varies (one source throughout, say) would make the system singular.
  const keep = [];
  for (let j = 0; j < width; j += 1) if (j === 0 || design.some((x) => x[j] !== design[0][j])) keep.push(j);
  const solved = solveWithInverse(keep.map((j) => keep.map((k) => (j <= k ? xtx[j][k] : xtx[k][j]))), keep.map((j) => xty[j]));
  if (!solved) return null;
  const place = new Map(keep.map((j, at) => [j, at]));
  const beta = new Float64Array(width);
  keep.forEach((j, at) => { beta[j] = solved[0][at]; });

  let residual = 0;
  let weightSum = 0;
  design.forEach((x, i) => {
    let fit = 0;
    for (let j = 0; j < width; j += 1) fit += x[j] * beta[j];
    residual += weighted[i].weight * (y[i] - fit) ** 2;
    weightSum += weighted[i].weight;
  });
  const sigma2 = residual / Math.max(1, weightSum - keep.length);

  const counts = monthKeys.map(() => 0);
  for (const { row } of weighted) counts[monthAt.get(monthOf(row))] += 1;
  const points = monthKeys.map((month, index) => ({
    month,
    index: 100 * Math.exp(index === 0 ? 0 : beta[monthStart + index - 1]),
    count: counts[index],
  }));
  // Covariance of the month terms, so any two months can be compared exactly. The first
  // month is the reference: its term is 0 by construction, with no variance of its own.
  const monthCovariance = monthKeys.map((_, a) => monthKeys.map((__, b) => (
    a === 0 || b === 0 ? 0 : sigma2 * solved[1][place.get(monthStart + a - 1)][place.get(monthStart + b - 1)]
  )));
  return { typeLabel, points, salesCount: weighted.length, monthCovariance };
}

// Percent change between two points, and whether it stands clear of the noise.
export function indexChange(index, from, to, z = INDEX_CALL_Z) {
  if (!index || !from || !to) return null;
  const [a, b] = [index.points.indexOf(from), index.points.indexOf(to)];
  const cov = index.monthCovariance;
  const se = Math.sqrt(Math.max(0, cov[a][a] + cov[b][b] - 2 * cov[a][b]));
  return { pct: to.index / from.index - 1, from: from.month, to: to.month, se, clear: Math.abs(Math.log(to.index / from.index)) >= z * se };
}

// The readings a buyer asks for: against a year ago, against the peak, and over the last
// three months.
export function summarizePriceIndex(index) {
  if (!index) return null;
  const { points } = index;
  const last = points[points.length - 1];
  const yearAgo = points.find((point) => point.month === shiftMonth(last.month, -12)) || null;
  const peak = points.reduce((best, point) => (point.index > best.index ? point : best), points[0]);
  return {
    last,
    peak,
    overYear: yearAgo ? indexChange(index, yearAgo, last) : null,
    sincePeak: peak.month !== last.month ? indexChange(index, peak, last, INDEX_PEAK_Z) : null,
    overQuarter: points.length >= 4 ? indexChange(index, points[points.length - 4], last) : null,
  };
}

const wholePct = (pct) => `${Math.abs(Math.round(pct * 100))}%`;
const direction = (pct) => (pct < 0 ? "lower" : "higher");

// Answers "are prices moving?": a headline and the sentences that qualify it, or null.
export function buildPriceIndexVerdict(summary) {
  if (!summary) return null;
  const { overYear, sincePeak, overQuarter, peak, last } = summary;
  const lead = overYear || overQuarter;
  if (!lead) return null;
  const span = overYear ? "a year ago" : `in ${monthLabelCompact(lead.from)}`;
  const headline = lead.clear
    ? `Like for like, prices are ${wholePct(lead.pct)} ${direction(lead.pct)} than ${span}.`
    : `Like for like, prices are about where they were ${span}.`;
  const bits = [];
  if (sincePeak?.clear) {
    // The spring market (March to June) runs above autumn and winter every year, so a
    // fall from one to the other is partly the calendar. Spring against spring is not.
    const monthNumber = (point) => Number(point.month.slice(5, 7));
    const seasonal = monthNumber(peak) >= 3 && monthNumber(peak) <= 6 && (monthNumber(last) >= 9 || monthNumber(last) <= 2);
    bits.push(`They are ${wholePct(sincePeak.pct)} below the ${monthLabelCompact(peak.month)} peak${seasonal ? ", though spring usually runs above autumn and winter, so part of that is the season" : ""}.`);
  }
  if (overYear && overQuarter) {
    bits.push(overQuarter.clear
      ? `Over the last three months they moved ${wholePct(overQuarter.pct)} ${direction(overQuarter.pct)}.`
      : "Over the last three months they have not moved by more than the noise.");
  }
  return { headline, detail: bits.join(" ") };
}
