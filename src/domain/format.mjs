export const DAY_MS = 24 * 60 * 60 * 1000;

export function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

export function num(value) {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Number(String(value).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

export function safeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function toDate(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const iso = /^\d{4}-\d{2}-\d{2}$/;
  const parsed = iso.test(raw) ? new Date(`${raw}T00:00:00`) : new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function toIso(value) {
  const parsed = toDate(value);
  if (!parsed) return "";
  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}-${String(parsed.getDate()).padStart(2, "0")}`;
}

export function addDays(value, days) {
  const parsed = toDate(value);
  if (!parsed) return null;
  const next = new Date(parsed.getTime());
  next.setDate(next.getDate() + Number(days || 0));
  return next;
}

export function daysBetween(a, b) {
  const d1 = toDate(a);
  const d2 = toDate(b);
  if (!d1 || !d2) return null;
  return Math.round((d2 - d1) / DAY_MS);
}

export function daysAgo(isoDate) {
  const parsed = toDate(isoDate);
  if (!parsed) return null;
  return Math.max(0, Math.round((new Date() - parsed) / DAY_MS));
}

export function median(values) {
  const nums = (values || []).filter((value) => Number.isFinite(value)).slice().sort((a, b) => a - b);
  if (!nums.length) return 0;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

export function nullableMedian(values) {
  const nums = (values || []).filter((value) => Number.isFinite(value)).slice().sort((a, b) => a - b);
  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

export function quantile(values, q) {
  const nums = (values || []).filter((value) => Number.isFinite(value)).slice().sort((a, b) => a - b);
  if (!nums.length) return 0;
  const pos = (nums.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  return nums[base + 1] === undefined ? nums[base] : nums[base] + rest * (nums[base + 1] - nums[base]);
}

export function weightedQuantile(values, weights, q) {
  const pairs = values
    .map((value, index) => ({ value: Number(value || 0), weight: Number(weights[index] || 0) }))
    .filter((pair) => Number.isFinite(pair.value) && Number.isFinite(pair.weight) && pair.weight > 0)
    .sort((a, b) => a.value - b.value);
  if (!pairs.length) return quantile(values, q);
  const total = pairs.reduce((sum, pair) => sum + pair.weight, 0);
  const target = total * clamp(q, 0, 1);
  let acc = 0;
  for (const pair of pairs) {
    acc += pair.weight;
    if (acc >= target) return pair.value;
  }
  return pairs[pairs.length - 1].value;
}

export function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

export function roundToNearest(value, step = 1000) {
  if (!Number.isFinite(value)) return 0;
  const safeStep = Math.max(1, Number(step || 1));
  return Math.round(value / safeStep) * safeStep;
}

export function formatMoney(value) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

export function formatMoneyOrNa(value) {
  const n = Number(value || 0);
  return n > 0 ? formatMoney(n) : "n/a";
}

export function formatMoneyCompact(value, digits = 2) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "$0";
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(n);
  if (abs >= 1_000_000) {
    return `${sign}$${(abs / 1_000_000)
      .toFixed(digits)
      .replace(/\.0+$/, "")
      .replace(/(\.\d*[1-9])0+$/, "$1")}M`;
  }
  if (abs >= 1_000) return `${sign}$${Math.round(abs / 1_000)}K`;
  return `${sign}$${Math.round(abs)}`;
}

export function formatWholeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n).toLocaleString("en-US") : "n/a";
}

export function formatPct(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

export function formatRatio(value) {
  const n = Number(value || 0);
  return n > 0 ? `${n.toFixed(2)}x` : "n/a";
}

export function formatLot(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "n/a";
  return `${Math.round(n).toLocaleString("en-US")} sf`;
}

export function formatDateShort(value) {
  const parsed = toDate(value);
  if (!parsed) return "n/a";
  return `${parsed.getMonth() + 1}/${parsed.getDate()}/${String(parsed.getFullYear()).slice(-2)}`;
}

export function formatDateTime(value) {
  const parsed = toDate(value);
  if (!parsed) return "n/a";
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function monthKey(value) {
  const parsed = toDate(value);
  if (!parsed) return "Unknown";
  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}`;
}

export function monthLabel(month) {
  const text = String(month || "");
  if (!/^\d{4}-\d{2}$/.test(text)) return text || "Unknown";
  const [year, monthPart] = text.split("-");
  const parsed = new Date(`${year}-${monthPart}-01T00:00:00`);
  return Number.isNaN(parsed.getTime()) ? text : parsed.toLocaleString("en-US", { month: "short", year: "numeric" });
}

export function monthLabelCompact(month) {
  const text = String(month || "");
  if (!/^\d{4}-\d{2}$/.test(text)) return text || "Unknown";
  const [year, monthPart] = text.split("-");
  const parsed = new Date(`${year}-${monthPart}-01T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return text;
  return `${parsed.toLocaleString("en-US", { month: "short" })} '${String(year).slice(2)}`;
}
