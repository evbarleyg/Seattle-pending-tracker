// Shared trust layer: universe captions plus "what is this?" explain popovers.
//
// Render helpers return HTML strings (same template-string style as main.mjs);
// initExplainLayer wires one delegated listener set so popovers keep working
// after any innerHTML re-render. Popover content is looked up lazily through
// the getMetric callback, so this module stays independent of the glossary.
//
// Expected glossary entry shape (all fields optional, aliases accepted):
//   {
//     id: "medianClose",
//     label: "Median close price",
//     definition: "Plain-language meaning of the number.",   // or plain / description
//     formula: "How it is computed, in words.",              // or formulaInWords / how
//     source: "KC assessor recorded sales plus manual MLS exports.",
//     cadence: "KC assessor publishes about every two weeks.", // or freshness
//     caveats: ["Only some rows carry a real list price."],  // or notes
//   }

import { esc, formatWholeNumber } from "../domain/format.mjs";

const initializedRoots = new WeakMap();

export function explainPopId(metricId) {
  const slug = String(metricId ?? "").replace(/[^A-Za-z0-9_-]/g, "-");
  return `explain-pop-${slug}`;
}

// Small caption placed under a headline stat, e.g.
// "of 2,179 closed comps in your slice (last 12 mo)".
export function renderUniverseCaption({ count, universeLabel, windowLabel } = {}) {
  const numericCount = count === null || count === undefined || count === "" ? NaN : Number(count);
  const hasCount = Number.isFinite(numericCount);
  const label = String(universeLabel ?? "").trim();
  if (!hasCount && !label) return "";
  const lead = hasCount
    ? `of ${formatWholeNumber(numericCount)}${label ? ` ${label}` : ""}`
    : label;
  const window = String(windowLabel ?? "").trim();
  const text = window ? `${lead} (${window})` : lead;
  return `<span class="universe-caption">${esc(text)}</span>`;
}

// Quiet inline trigger. Drop next to a stat label; initExplainLayer opens the
// matching popover inside the returned wrapper span.
export function renderExplainButton(metricId) {
  if (!metricId) return "";
  const popId = explainPopId(metricId);
  return [
    `<span class="explain-wrap">`,
    `<button type="button" class="explain-trigger" data-explain-trigger="${esc(metricId)}"`,
    ` aria-expanded="false" aria-controls="${esc(popId)}" aria-haspopup="dialog">what is this?</button>`,
    `</span>`,
  ].join("");
}

function partHtml(heading, bodyHtml) {
  if (!bodyHtml) return "";
  return `<div class="explain-part"><span class="explain-part-label">${esc(heading)}</span>${bodyHtml}</div>`;
}

// Popover body built from a glossary entry plus optional runtime context:
//   context.note      -> live coverage line, e.g. "612 of 2,179 comps carry a real list price."
//   context.freshness -> live freshness line, e.g. "Newest recorded sale: Jun 28."
export function renderExplainPopover(metric, context = {}) {
  if (!metric || typeof metric !== "object") return "";
  const id = metric.id ?? metric.metricId ?? "";
  const label = metric.label ?? metric.title ?? metric.name ?? "This metric";
  const definition = metric.definition ?? metric.plain ?? metric.description ?? "";
  const formula = metric.formula ?? metric.formulaInWords ?? metric.how ?? "";
  const source = metric.source ?? "";
  const cadence = metric.cadence ?? metric.freshness ?? "";
  const caveats = (Array.isArray(metric.caveats) ? metric.caveats : Array.isArray(metric.notes) ? metric.notes : [])
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
  const liveNote = String(context?.note ?? "").trim();
  const liveFreshness = String(context?.freshness ?? "").trim();

  const sourceLines = [source, cadence, liveFreshness]
    .map((line) => String(line ?? "").trim())
    .filter(Boolean)
    .map((line) => `<p>${esc(line)}</p>`)
    .join("");
  const caveatItems = caveats.map((item) => `<li>${esc(item)}</li>`).join("");

  return [
    `<div class="explain-pop"${id ? ` id="${esc(explainPopId(id))}"` : ""} role="dialog" aria-label="About ${esc(label)}" tabindex="-1">`,
    `<div class="explain-pop-head">`,
    `<span class="explain-pop-title">${esc(label)}</span>`,
    `<button type="button" class="explain-close" data-explain-close aria-label="Close explanation">&times;</button>`,
    `</div>`,
    definition ? `<p class="explain-def">${esc(definition)}</p>` : "",
    partHtml("How it is computed", formula ? `<p>${esc(formula)}</p>` : ""),
    partHtml("Where the data comes from", sourceLines),
    liveNote ? `<p class="explain-live">${esc(liveNote)}</p>` : "",
    partHtml("Keep in mind", caveatItems ? `<ul class="explain-caveats">${caveatItems}</ul>` : ""),
    `</div>`,
  ].join("");
}

// Wires open/close behavior once per root element (delegated, so markup can be
// re-rendered freely). getMetric(metricId) returns a glossary entry, or
// { metric, context } when live numbers should be woven in. Returns
// { close, destroy }; re-initializing the same root returns the existing API.
export function initExplainLayer(rootEl, { getMetric } = {}) {
  if (!rootEl || typeof document === "undefined") return null;
  if (initializedRoots.has(rootEl)) return initializedRoots.get(rootEl);

  let openPop = null;
  let openTrigger = null;

  function close(restoreFocus = false) {
    if (openTrigger && openTrigger.isConnected) {
      openTrigger.setAttribute("aria-expanded", "false");
      if (restoreFocus) openTrigger.focus();
    }
    if (openPop && openPop.isConnected) openPop.remove();
    openPop = null;
    openTrigger = null;
  }

  function open(trigger) {
    const metricId = trigger.getAttribute("data-explain-trigger");
    const raw = typeof getMetric === "function" ? getMetric(metricId) : null;
    if (!raw || typeof raw !== "object") return;
    const hasEnvelope = raw.metric && typeof raw.metric === "object";
    const metric = hasEnvelope ? raw.metric : raw;
    const context = hasEnvelope ? raw.context || {} : {};
    const html = renderExplainPopover(metric, context);
    if (!html) return;
    trigger.insertAdjacentHTML("afterend", html);
    const pop = trigger.nextElementSibling;
    if (!pop || !pop.classList.contains("explain-pop")) return;
    // Keep aria-controls truthful even if the glossary id drifts from the
    // trigger's metric id.
    const controls = trigger.getAttribute("aria-controls");
    if (controls) pop.id = controls;
    if (typeof window !== "undefined") {
      const rect = trigger.getBoundingClientRect();
      if (rect.left > window.innerWidth / 2) pop.classList.add("explain-pop-right");
    }
    trigger.setAttribute("aria-expanded", "true");
    openPop = pop;
    openTrigger = trigger;
    pop.focus();
  }

  function onDocClick(event) {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    // A re-render can silently remove an open popover; reset stale refs.
    if (openPop && !openPop.isConnected) {
      openPop = null;
      openTrigger = null;
    }
    const closeBtn = target.closest("[data-explain-close]");
    if (closeBtn && openPop && openPop.contains(closeBtn)) {
      close(true);
      return;
    }
    const trigger = target.closest("[data-explain-trigger]");
    if (trigger && rootEl.contains(trigger)) {
      if (trigger === openTrigger) {
        close(false);
      } else {
        close(false);
        open(trigger);
      }
      return;
    }
    if (openPop && !openPop.contains(target)) close(false);
  }

  function onDocKeydown(event) {
    if (event.key === "Escape" && openPop) close(true);
  }

  document.addEventListener("click", onDocClick);
  document.addEventListener("keydown", onDocKeydown);

  const api = {
    close: () => close(false),
    destroy() {
      document.removeEventListener("click", onDocClick);
      document.removeEventListener("keydown", onDocKeydown);
      close(false);
      initializedRoots.delete(rootEl);
    },
  };
  initializedRoots.set(rootEl, api);
  return api;
}
