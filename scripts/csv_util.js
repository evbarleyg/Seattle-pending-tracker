"use strict";

// Minimal RFC-4180-ish CSV helpers shared by the ledger and sold-accumulation
// scripts. Quoted fields may contain commas, doubled quotes and no newlines
// (the pipeline never writes multi-line fields).

const fs = require("fs");

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") { cur += "\""; i += 1; }
      else inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

function safeCsv(value) {
  const s = value == null ? "" : String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, "\"\"")}"` : s;
}

function readCsvText(text) {
  const lines = String(text || "").split(/\r?\n/).filter((l) => l.length);
  if (!lines.length) return { headers: [], rows: [] };
  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((h, j) => { row[h] = cols[j] ?? ""; });
    rows.push(row);
  }
  return { headers, rows };
}

function readCsv(filePath) {
  return readCsvText(fs.readFileSync(filePath, "utf8"));
}

function toCsvText(headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) lines.push(headers.map((h) => safeCsv(row[h])).join(","));
  return `${lines.join("\n")}\n`;
}

function writeCsv(filePath, headers, rows) {
  fs.writeFileSync(filePath, toCsvText(headers, rows));
}

function num(value) {
  const n = Number(String(value ?? "").replace(/[$,]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

module.exports = { parseCsvLine, safeCsv, readCsvText, readCsv, toCsvText, writeCsv, num };
