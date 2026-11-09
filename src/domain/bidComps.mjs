// bidComps — which past sales stand behind a suggested bid, and how much each counts.
//
// Every sale of the same home type from the last 12 months takes part, but not
// equally. A sale counts for more the more it resembles the listing being bid on:
//
//   recency        halves every 60 days
//   days on market week one (0-7 days) is its own market and is pooled; after that
//                  a sale counts by how close its days-to-pending were, on a log
//                  scale (day 20 is near day 30, far from day 90)
//   asking price   by how close the two asks are, on a log scale
//   location       same neighborhood or ZIP counts up to 20x, the more such sales there are
//   build era      homes under 15 years old and older homes barely inform each other
//
// This replaced hard tiers (same stage, then 10 in the neighborhood, else 10 in the
// ZIP, else citywide, 90 days, inside the price band). Hard edges made neighbours
// disagree: day 14 and day 15, the 9th and 10th local sale, an ask just above the
// band floor. Replayed walk-forward on 1,113 single-family sales asked at $1.1M to
// $1.6M (Oct 2025 to Nov 2026), each scored only on sales that had closed before it
// went pending:
//
//                                              hard tiers   weighted
//   quantile loss, mean of 50th/60th/70th         2.135       2.019
//   mean miss, share of price                     4.57%       4.28%
//   newer builds in week one: win rate at the
//   "wins 6 in 10" bid, and mean overpayment    89%, $72K   64%, $25K
//
// Slices it was not tuned on improved too (loss down 4.5% on $0.6-1.1M houses, 2.8%
// on $1.6-3.0M, 4.5% on townhouses, 2.3% on condos).
//
// What did NOT help, so is not here: asking price against a fitted fair value
// (correlation with the sale-to-ask ratio: 0.00), price per square foot, list day of
// week, round or "charm" pricing, size of earlier price cuts, trailing market heat.
// The exact day inside week one does predict (day 5, the usual offer-review date,
// runs hottest) but a buyer looking at a day-2 listing cannot know which day it
// will go pending, so using it would flatter the backtest and mislead the bid.
//
// The loss barely moves across half-lives of 45-90 days, bandwidths of 0.35-0.7 and
// local boosts of 6-40x, so the round numbers below are not finely tuned.
import { domMetric, zip5 } from "./data.mjs";
import { daysBetween, toIso } from "./format.mjs";
import { LISTING_STAGES, stageForDom } from "./listingStage.mjs";

export const BID_COMP_WINDOW_DAYS = 365;
export const BID_HALFLIFE_DAYS = 60;
// In log(1 + days on market). For a day-20 listing, 0.5 gives a sale that went
// pending on day 30 74% weight and one on day 60 10%.
export const BID_DOM_BANDWIDTH = 0.5;
// In log(ask). 0.25 puts a $1.0M ask at 67% weight for a $1.25M listing.
export const BID_ASK_BANDWIDTH = 0.25;
// The most a nearby sale can count for, relative to the same sale elsewhere. The boost
// is earned: with n (effective) nearby comps it is 1 + 19n / (n + BID_LOCAL_HALF_COMPS), so
// one nearby sale counts 3x, eight 10x, fifty 17x. A flat 10x let a single nearby sale take
// 40% of the weight from fifteen citywide ones, while busy areas could have leaned more
// local than they did: in week one the "wins 6 in 10" bid won 51% of the time in Ballard /
// Crown Hill and 66% in Queen Anne, against 55% and 59% now, at the same overall loss.
export const BID_LOCAL_BOOST = 20;
export const BID_LOCAL_HALF_COMPS = 8;
export const BID_NEWER_BUILD_YEARS = 15;
export const BID_OTHER_ERA_WEIGHT = 0.05;
// Smallest effective number of comparable sales (comparableCount) that backs a suggestion.
export const BID_MIN_COMPS = 6;
// At or above this share of the weight, the basis is described as local.
export const BID_LOCAL_SHARE = 0.5;

const FRESH_MAX_DOM = LISTING_STAGES[0].maxDom;

// 1 newer, 0 older, -1 unknown. Age is measured when the home was on the market. A
// year far in the future is a typo, not a very new home (a presale can run a year or
// two ahead).
function buildEra(yearBuilt, marketYear) {
  const year = Number(yearBuilt);
  if (!(year > 1800) || !(marketYear > 1800) || year > marketYear + 3) return -1;
  return marketYear - year <= BID_NEWER_BUILD_YEARS ? 1 : 0;
}

export function isNewerBuild(row, nowValue = new Date()) {
  return buildEra(row?.yearBuilt, Number(toIso(nowValue).slice(0, 4))) === 1;
}

// Per-comp numbers that do not depend on the listing being scored, worked out once
// per pool rather than once per listing (1,400 listings against 5,000 comps).
// Each type's comps are held sorted by sale-to-ask ratio, so a weighted quantile is
// one pass with no per-listing sort. Keyed on the pool array, which main.mjs
// rebuilds on every recompute.
const preparedPools = new WeakMap();

function prepareCompPool(pool, nowValue) {
  const nowIso = toIso(nowValue);
  const cached = preparedPools.get(pool);
  if (cached && cached.nowIso === nowIso) return cached;
  const byType = new Map();
  for (const comp of pool) {
    const raw = daysBetween(comp.saleDate, nowIso);
    if (raw === null || raw > BID_COMP_WINDOW_DAYS || !(comp.saleToList > 0)) continue;
    const list = byType.get(comp.typeLabel);
    if (list) list.push(comp); else byType.set(comp.typeLabel, [comp]);
  }
  for (const [type, comps] of byType) {
    comps.sort((a, b) => a.saleToList - b.saleToList);
    const n = comps.length;
    const typed = {
      comps,
      ratio: new Float64Array(n),
      age: new Float64Array(n),
      recency: new Float64Array(n),
      dom: new Float64Array(n),
      logDom: new Float64Array(n),
      logAsk: new Float64Array(n),
      era: new Int8Array(n),
      zip: comps.map((comp) => zip5(comp.zip)),
    };
    comps.forEach((comp, i) => {
      const age = Math.max(0, daysBetween(comp.saleDate, nowIso));
      const dom = domMetric(comp);
      typed.ratio[i] = comp.saleToList;
      typed.age[i] = age;
      typed.recency[i] = Math.pow(0.5, age / BID_HALFLIFE_DAYS);
      typed.dom[i] = dom === null ? NaN : dom;
      typed.logDom[i] = dom === null ? NaN : Math.log1p(dom);
      typed.logAsk[i] = comp.pendingListPrice > 0 ? Math.log(comp.pendingListPrice) : NaN;
      typed.era[i] = buildEra(comp.yearBuilt, Number(String(comp.saleDate).slice(0, 4)));
    });
    byType.set(type, typed);
  }
  const prepared = { nowIso, byType };
  preparedPools.set(pool, prepared);
  return prepared;
}

// Squared distance, in bandwidths, past which a comp is dropped rather than weighed: at
// 3.5 bandwidths it would count 0.2%. This loop runs listings x comps times per render.
const MAX_SQUARED_DISTANCE = 3.5 ** 2;

// Kish's effective sample size: 300 sales that mostly barely count are worth far fewer
// than 300. Equal weights give back the plain count.
const kish = (sum, sumSquares) => (sumSquares > 0 ? (sum * sum) / sumSquares : 0);

// Labels that mean "nowhere in particular": sharing one does not make two homes neighbours.
const CATCH_ALL_LABELS = new Set(["Seattle", "Seattle (Other)"]);

// `domMatch`: "near" weights comps by how close their days to pending were, "side" only
// keeps them on the listing's side of the week-one wall, "none" ignores days on market.
function weigh(target, typed, nowIso, domMatch) {
  const n = typed.comps.length;
  const weights = new Float64Array(n);
  const dom = domMatch === "none" ? null : domMetric(target);
  const fresh = dom !== null && dom <= FRESH_MAX_DOM;
  const logDom = dom === null ? NaN : Math.log1p(dom);
  const logAsk = target.pendingListPrice > 0 ? Math.log(target.pendingListPrice) : NaN;
  const era = buildEra(target.yearBuilt, Number(nowIso.slice(0, 4)));
  const zip = zip5(target.zip);
  const hood = CATCH_ALL_LABELS.has(target.neighborhoodLabel) ? "" : target.neighborhoodLabel || "";
  const isLocal = new Uint8Array(n);
  let [localSum, localSquares, otherSum, otherSquares, weighed] = [0, 0, 0, 0, 0];
  for (let i = 0; i < n; i += 1) {
    let squared = 0; // both Gaussian kernels, as one exponent
    if (dom !== null) {
      const compDom = typed.dom[i];
      if (Number.isNaN(compDom) || (compDom <= FRESH_MAX_DOM) !== fresh) continue;
      if (!fresh && domMatch === "near") squared += ((typed.logDom[i] - logDom) / BID_DOM_BANDWIDTH) ** 2;
    }
    if (!Number.isNaN(logAsk) && !Number.isNaN(typed.logAsk[i])) squared += ((typed.logAsk[i] - logAsk) / BID_ASK_BANDWIDTH) ** 2;
    if (squared > MAX_SQUARED_DISTANCE) continue;
    let w = typed.recency[i] * Math.exp(-0.5 * squared);
    if (era !== -1 && typed.era[i] !== -1 && typed.era[i] !== era) w *= BID_OTHER_ERA_WEIGHT;
    weights[i] = w;
    weighed += 1;
    if ((hood && typed.comps[i].neighborhoodLabel === hood) || (zip && typed.zip[i] === zip)) {
      isLocal[i] = 1;
      localSum += w;
      localSquares += w * w;
    } else {
      otherSum += w;
      otherSquares += w * w;
    }
  }
  const boost = 1 + ((BID_LOCAL_BOOST - 1) * kish(localSum, localSquares)) / (kish(localSum, localSquares) + BID_LOCAL_HALF_COMPS);
  const sum = localSum * boost + otherSum;
  // "Newest" among the comps that matter: a sale carrying under 1% of an average
  // weighed comp's weight says nothing about how current the basis is.
  const floor = weighed ? (sum / weighed) * 0.01 : 0;
  let newestAge = Infinity;
  for (let i = 0; i < n; i += 1) {
    if (isLocal[i]) weights[i] *= boost;
    if (weights[i] > floor && typed.age[i] < newestAge) newestAge = typed.age[i];
  }
  return {
    weights,
    sum,
    // How many comparable sales there are, wherever they are. This decides whether
    // there is enough to go on: measured after the boost, finding one more sale nearby
    // could lower the count and cost a listing its suggestion.
    comparableCount: kish(localSum + otherSum, localSquares + otherSquares),
    // How many the weighted percentiles really rest on, once nearby sales count extra.
    effectiveCount: kish(sum, localSquares * boost * boost + otherSquares),
    localShare: sum > 0 ? (localSum * boost) / sum : 0,
    newestAge: Number.isFinite(newestAge) ? newestAge : null,
  };
}

// Six equal comps come out as 5.999999999999999 or 6.000000000000001 depending on their date.
const isEnough = (picked) => picked.comparableCount >= BID_MIN_COMPS - 1e-9;

const EMPTY = { comps: [], ratio: new Float64Array(0), age: new Float64Array(0) };

// The weighted comps behind one bid. When too few sales took about as long to sell as
// this listing has been listed (a newer build on day 160, say), the search first widens
// to every sale on the same side of the week-one wall: a stale listing must never be
// priced off bidding wars just because stale comps are scarce. Only a listing with no
// days-on-market reading, or with too few sales even then, uses comps regardless of
// days on market, flagged so confidence can say so.
export function selectBidComps(targetRow, compPool, { nowValue = new Date() } = {}) {
  const prepared = prepareCompPool(compPool || [], nowValue);
  const typed = prepared.byType.get(targetRow.typeLabel) || EMPTY;
  const stage = stageForDom(domMetric(targetRow));
  // Week one is pooled, so "near" and "side" are the same search there.
  const searches = !stage ? ["none"] : stage === LISTING_STAGES[0].id ? ["near", "none"] : ["near", "side", "none"];
  let picked = null;
  let domMatch = "none";
  for (domMatch of searches) {
    picked = weigh(targetRow, typed, prepared.nowIso, domMatch);
    if (isEnough(picked)) break;
  }
  const enough = isEnough(picked);
  return {
    ...picked,
    typed,
    stage,
    domMatch,
    stageMatched: domMatch !== "none",
    tier: !enough ? "NONE" : picked.localShare >= BID_LOCAL_SHARE ? "T1_NEIGHBORHOOD_TYPE" : "T3_CITY_TYPE",
  };
}

// Weighted quantiles of the sale-to-ask ratio, for ascending `quantiles`, in one pass.
export function compRatioQuantiles(selection, quantiles) {
  const { weights, sum, typed } = selection;
  const out = new Array(quantiles.length).fill(null);
  if (!(sum > 0)) return out;
  let acc = 0;
  let next = 0;
  let last = null;
  for (let i = 0; i < weights.length && next < quantiles.length; i += 1) {
    if (!(weights[i] > 0)) continue;
    acc += weights[i];
    last = typed.ratio[i];
    while (next < quantiles.length && acc >= sum * quantiles[next] - 1e-12) {
      out[next] = last;
      next += 1;
    }
  }
  while (next < quantiles.length) {
    out[next] = last;
    next += 1;
  }
  return out;
}

// The sales that count most, heaviest first, each with its share of the total weight.
export function topBidComps(selection, limit = 12) {
  const { weights, sum, typed } = selection;
  const order = [];
  for (let i = 0; i < weights.length; i += 1) if (weights[i] > 0) order.push(i);
  order.sort((a, b) => weights[b] - weights[a] || typed.age[a] - typed.age[b]);
  return order.slice(0, limit).map((i) => ({ ...typed.comps[i], bidCompWeightShare: weights[i] / sum }));
}
