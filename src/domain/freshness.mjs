// Per-source freshness, derived from the rows themselves.
//
// The dataset is stitched from sources that refresh on very different clocks
// (a daily Redfin listings pull, an occasional Redfin sold pull, King County's
// roughly biweekly recorded sales, a history backfill that recovers asking
// prices). One "last refreshed" timestamp hides that: the file can be rebuilt
// this morning while the newest sale with an asking price is months old. So
// this reads the newest date each kind of row actually carries and says how
// old it is, in the buyer's terms.
//
// Tone is "ok", "behind" or "unknown". There is deliberately no red: a source
// resting on its normal cadence is not an alarm, and a red state stays reserved
// for a refresh that failed its checks (see the Data tab health card).
import { daysBetween, toDate, toIso } from "./format.mjs";
import { isActiveListing } from "./data.mjs";

// okDays: how old the newest row can be before the source counts as behind.
// County allows for its publish cadence (~2 weeks) plus recording lag (~2 more).
export const FRESHNESS_SOURCES = [
  {
    id: "listings",
    label: "Active listings",
    shortLabel: "Listings",
    okDays: 3,
    note: "Homes for sale now, pulled from Redfin's public search each morning.",
  },
  {
    id: "sales",
    label: "Closed sales",
    shortLabel: "Sales",
    okDays: 28,
    note: "Newest closed sale from any source. County records trail a closing by two to three weeks; the Redfin sold pull fills that gap when it has been run.",
  },
  {
    id: "pricedSales",
    label: "Sales with an asking price",
    shortLabel: "Sale vs ask",
    okDays: 45,
    note: "Newest sale that also carries a real asking price. Sale-versus-ask stats use only these, and a bid suggestion needs one from the last 90 days.",
  },
  {
    id: "county",
    label: "King County records",
    shortLabel: "County",
    okDays: 45,
    note: "King County publishes recorded sales about every two weeks, and a sale takes about two more weeks to appear, so three to five weeks old is normal.",
  },
];

const ISO_DAY = /^\d{4}-\d{2}-\d{2}/;

// Normalized rows already store dates as YYYY-MM-DD, which sort as strings, so
// the common path is a slice and a compare; anything else falls back to parsing.
function isoDay(value) {
  if (typeof value === "string" && ISO_DAY.test(value)) return value.slice(0, 10);
  return value ? toIso(value) : "";
}

// Local midnight for "now": row dates are whole days and daysBetween rounds,
// so an evening clock would otherwise read today's rows as a day old.
function startOfToday(now) {
  return toDate(toIso(now)) || toDate(toIso(new Date()));
}

function ageInDays(dateIso, today) {
  if (!dateIso) return null;
  const raw = daysBetween(dateIso, today);
  return raw === null ? null : Math.max(0, raw);
}

function toneFor(ageDays, okDays) {
  if (ageDays === null) return "unknown";
  return ageDays <= okDays ? "ok" : "behind";
}

export function computeSourceFreshness(rows, { now = new Date(), report = null } = {}) {
  const dates = { listings: "", sales: "", pricedSales: "", county: "" };
  const keepNewest = (key, value) => {
    const day = isoDay(value);
    if (day && day > dates[key]) dates[key] = day;
  };
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row) continue;
    if (row.hasActualClose) {
      keepNewest("sales", row.saleDate);
      if (row.hasMarketListPrice) keepNewest("pricedSales", row.saleDate);
      if (row.dataMode === "PUBLIC_PROXY") keepNewest("county", row.saleDate);
    } else if (isActiveListing(row)) {
      keepNewest("listings", row.listDate);
    }
  }
  // A feed fetched today with no brand-new listing still has yesterday's newest
  // list date, so when the pipeline reports the fetch time, that is the truth.
  // Callers pass no report for an uploaded file, which the pipeline never saw.
  const fetchedAt = toIso(report?.sources?.redfinActives?.fetchedAt);
  if (fetchedAt && fetchedAt > dates.listings) dates.listings = fetchedAt;

  const today = startOfToday(now);
  const items = FRESHNESS_SOURCES.map((source) => {
    const date = dates[source.id] || "";
    const ageDays = ageInDays(date, today);
    return { ...source, date, ageDays, tone: toneFor(ageDays, source.okDays) };
  });
  return { items };
}

// Whether one particular newest-sale date (for example the newest sale inside
// the buyer's current filters) is within the normal lag, as a sentence. It lives
// beside the threshold it quotes so the wording and the tone cannot drift apart,
// and it counts in days because rounding to weeks can land on the limit itself
// ("4 weeks old, past the usual four weeks").
export function describeSalesLag(dateIso, now = new Date()) {
  const { okDays } = FRESHNESS_SOURCES.find((source) => source.id === "sales");
  const ageDays = ageInDays(isoDay(dateIso), startOfToday(now));
  const tone = toneFor(ageDays, okDays);
  let sentence = "";
  if (tone === "ok") {
    sentence = `That is ${ageDays === 0 ? "today" : `${ageDays} ${ageDays === 1 ? "day" : "days"} ago`}, within the usual ${okDays}-day lag for recorded sales.`;
  } else if (tone === "behind") {
    sentence = `That is ${ageDays} days ago, past the usual ${okDays}-day lag, so the newest shifts may not show yet.`;
  }
  return { ageDays, tone, sentence };
}

// "1 day", "9 days", "5 weeks", "3 months": coarse on purpose, since the reader
// wants the order of magnitude, not a day count to subtract.
export function formatDuration(ageDays) {
  const days = Math.max(0, Math.round(Number(ageDays)));
  if (days === 1) return "1 day";
  if (days < 14) return `${days} days`;
  if (days < 56) return `${Math.round(days / 7)} weeks`;
  return `${Math.round(days / 30)} months`;
}

export function formatAge(ageDays) {
  if (ageDays === null || ageDays === undefined || !Number.isFinite(Number(ageDays))) return "no data";
  return Math.round(Number(ageDays)) <= 0 ? "today" : `${formatDuration(ageDays)} ago`;
}
