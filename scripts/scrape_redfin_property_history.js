#!/usr/bin/env node
"use strict";

// Fetches a single Redfin property page and extracts the price-history strip.
// Exposes parsePropertyHistory(html) for reuse + a CLI for one-off lookups.

const fs = require("fs");
const path = require("path");

const REDFIN_ORIGIN = "https://www.redfin.com";
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

function cleanText(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/g, " ")
    .replace(/<style[\s\S]*?<\/style>/g, " ")
    .replace(/<[^>]+>/g, " | ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ");
}

function isoFromMonthDay(value) {
  if (!value) return null;
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).toISOString().slice(0, 10);
}

function parsePropertyHistory(html) {
  const idx = html.indexOf("propertyHistoryTabPanels");
  if (idx < 0) return [];
  const slice = html.slice(idx, idx + 12000);
  const text = cleanText(slice);
  const stopIdx = (() => {
    const a = text.indexOf("Property tax");
    const b = text.indexOf("Show more");
    const c = text.indexOf("Public facts");
    return Math.min(a > 0 ? a : Infinity, b > 0 ? b : Infinity, c > 0 ? c : Infinity);
  })();
  const trimmed = stopIdx === Infinity ? text : text.slice(0, stopIdx);
  const monthRe = /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s*\d{4}\b/g;
  const dates = [];
  let m;
  while ((m = monthRe.exec(trimmed))) {
    dates.push({ raw: m[0], idx: m.index });
  }
  const events = [];
  for (let i = 0; i < dates.length; i += 1) {
    const start = dates[i].idx + dates[i].raw.length;
    const end = i + 1 < dates.length ? dates[i + 1].idx : trimmed.length;
    const segment = trimmed.slice(start, end);
    const eventMatch = segment.match(/\|\s*([^|$]+?)(?=\s*\|\s*(?:NWMLS|MLS Grid|\$|—))/);
    const eventLabel = eventMatch ? eventMatch[1].trim() : segment.slice(0, 50).trim();
    const priceMatch = segment.match(/\$\s?([\d,]+)/);
    const mlsMatch = segment.match(/#(\d{5,})/);
    events.push({
      date: dates[i].raw,
      dateIso: isoFromMonthDay(dates[i].raw),
      event: eventLabel.replace(/\s*\|\s*/g, " ").trim(),
      price: priceMatch ? Number(priceMatch[1].replace(/,/g, "")) : null,
      mlsNumber: mlsMatch ? mlsMatch[1] : null,
    });
  }
  return events;
}

function summarizeMostRecentSale(events) {
  if (!events?.length) return null;
  const dated = events.map((e) => ({ ...e, t: e.dateIso ? Date.parse(`${e.dateIso}T00:00:00`) : NaN }))
    .filter((e) => Number.isFinite(e.t));
  dated.sort((a, b) => b.t - a.t);
  const sold = dated.find((e) => /sold/i.test(e.event) && e.price);
  if (!sold) return null;
  const earlier = dated.filter((e) => e.t < sold.t || (e.t === sold.t && e !== sold));
  const pending = earlier.find((e) => /pending/i.test(e.event));
  // List event with a price BEFORE the sold event (skip prior-cycle Sold→Listed bounces).
  let listed = null;
  for (const e of earlier) {
    if (/sold/i.test(e.event)) break;
    if (/listed/i.test(e.event) && e.price) { listed = e; break; }
  }
  return {
    soldDate: sold.dateIso,
    soldPrice: sold.price,
    pendingDate: pending?.dateIso || null,
    listDate: listed?.dateIso || null,
    listPriceAtPending: listed?.price ?? null,
    bidUpAmount: listed?.price ? sold.price - listed.price : null,
    bidUpPct: listed?.price ? (sold.price - listed.price) / listed.price : null,
    mlsNumber: sold.mlsNumber || listed?.mlsNumber || pending?.mlsNumber || null,
  };
}

async function fetchPropertyHtml(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": UA,
      Referer: REDFIN_ORIGIN,
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return res.text();
}

async function fetchAndParse(url) {
  const html = await fetchPropertyHtml(url);
  const events = parsePropertyHistory(html);
  const summary = summarizeMostRecentSale(events);
  return { url, events, summary };
}

async function main() {
  const url = process.argv[2];
  if (!url) {
    console.error("Usage: node scripts/scrape_redfin_property_history.js <redfin-property-url>");
    process.exit(1);
  }
  const result = await fetchAndParse(url);
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
}

module.exports = {
  parsePropertyHistory,
  summarizeMostRecentSale,
  fetchPropertyHtml,
  fetchAndParse,
};
