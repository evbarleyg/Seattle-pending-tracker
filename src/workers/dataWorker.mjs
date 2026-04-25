import { parseCsv, normalizeRows } from "../domain/data.mjs";

function post(type, payload = {}) {
  self.postMessage({ type, ...payload });
}

async function loadDataset(url, datasetName) {
  const startedAt = performance.now();
  post("status", { message: "Fetching dataset..." });
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`Dataset fetch failed: HTTP ${response.status}`);
  const text = await response.text();
  post("status", { message: "Parsing CSV..." });
  const rows = parseCsv(text);
  post("status", { message: "Normalizing records..." });
  const normalizedRows = normalizeRows(rows);
  post("loaded", {
    datasetName,
    rowCount: rows.length,
    normalizedRows,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

async function loadUploadedText(text, datasetName) {
  const startedAt = performance.now();
  post("status", { message: "Parsing uploaded CSV..." });
  const rows = parseCsv(text);
  post("status", { message: "Normalizing uploaded records..." });
  const normalizedRows = normalizeRows(rows);
  post("loaded", {
    datasetName,
    rowCount: rows.length,
    normalizedRows,
    elapsedMs: Math.round(performance.now() - startedAt),
  });
}

self.addEventListener("message", (event) => {
  const message = event.data || {};
  if (message.type === "load-url") {
    loadDataset(message.url, message.datasetName || message.url)
      .catch((error) => post("error", { message: error?.message || String(error) }));
  }
  if (message.type === "load-text") {
    loadUploadedText(message.text || "", message.datasetName || "uploaded.csv")
      .catch((error) => post("error", { message: error?.message || String(error) }));
  }
});
