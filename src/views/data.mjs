// Data view, extracted from src/main.mjs.
//
// This tab is a pipeline health readout: which sources fed the last refresh,
// what happened when it ran, and a stage-by-stage row count so a shrinking or
// missing batch is visible instead of hidden inside one "output rows" number.
//
// This view must also render before a dataset loads (it shows load status),
// so it tolerates a missing state.derived.
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs }
//
// Report shape note (this was the known bug): data_refresh_report.json has
// two layers written by two different scripts. The top level (generatedAt,
// source, counts, apnResolution, outputs) comes from the build step
// (scripts/build_mls_enriched_dataset.js). A nested "validation" object
// (generatedAt, status, errors, warnings, files, counts) is added on top by
// the separate check that runs after (scripts/validate_data_refresh.js).
// "Pass/fail" and the checked-file row counts live under report.validation.*,
// not at the top level — the same fix already made for the Overview
// freshness card (see freshnessCardHtml in src/views/overview.mjs).
import {
  daysAgo,
  esc,
  formatDateShort,
  formatDateTime,
  formatWholeNumber,
} from "../domain/format.mjs";
import { getMetric, formatCadenceNote } from "../domain/glossary.mjs";
import { renderExplainButton, renderUniverseCaption } from "../ui/explain.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

export function renderDataView(deps) {
  ctx = deps;
  const { state, qs } = ctx;
  const wrap = qs("#view-data");
  if (!wrap) return;
  const info = readReport(state);
  const recheckNote = recheckNoteHtml(info);
  const hasStageCounts = info.hasReport && Object.keys(info.stageCounts).length > 0;
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head"><div><p class="eyebrow">Data</p><h2>Refresh and dataset health</h2></div></section>
      ${healthCardHtml(state, info)}
      <div class="data-grid">
        ${dataTile("Dataset file", state.dataSource.datasetName || "n/a", "dataDatasetName")}
        ${dataTile("Rows loaded here", formatWholeNumber(state.dataSource.rowCount), "dataDatasetRows", { explainId: "dataRowCounts" })}
        ${dataTile("Last check", info.statusValue, "dataValidationStatus", { explainId: "dataValidation" })}
        ${dataTile("Checked on", info.validationTime ? formatDateTime(info.validationTime) : "n/a", "dataValidationTime")}
        ${dataTile("Newest sale in the data", latestSaleValue(state), "dataLatestSaleDate", { explainId: "freshness" })}
        ${dataTile("Rows in the last build", formatWholeNumber(info.outputRows), "dataOutputRows", { explainId: "dataRowCounts" })}
        ${dataTile("MLS export files found", formatWholeNumber(info.realtorFileCount), "dataRealtorFileCount", { explainId: "dataRowCounts" })}
      </div>
      <section class="section-block">
        <h3>How the pipeline built this file ${renderExplainButton("dataRowCounts")}</h3>
        ${
          hasStageCounts
            ? `
        <p>Each refresh runs the same stages in order. A count dropping to zero, or shrinking a lot from the last time you checked, is the sign something upstream broke.</p>
        <div class="stage-funnel">${stageFunnelHtml(info)}</div>
        ${recheckNote ? `<p class="data-recheck-note">${esc(recheckNote)}</p>` : ""}
        `
            : `<p>No pipeline run has been recorded yet, so there is nothing to break down here.</p>`
        }
      </section>
      <section class="section-block">
        <h3>Where each source stands ${renderExplainButton("dataSourceCadence")}</h3>
        <p>${esc(getMetric("dataSourceCadence").formulaWords)}</p>
      </section>
      <section class="section-block">
        <h3>Public assets</h3>
        <div class="mono">dataMode,addressSource,major,minor,parcelNbr,lat,lon,neighborhood,typeCode,zip,listDate,pendingDate,saleDate,originalListPrice,pendingListPrice,listPriceAtPending,closePrice,beds,baths,sqft,yearBuilt,mlsStatus,mlsListingPrice,mlsOriginalPrice,mlsDOM,mlsCDOM,mlsStyleCode,mlsParkingType,mlsParkingCoveredTotal,mlsTaxesAnnual,mlsBuildingCondition,mlsView,mlsBankOwned,mlsThirdPartyApprovalRequired,mlsNewConstructionState,mlsSquareFootageSource,hotMarketTag,saleToListRatio,saleToOriginalListRatio,bidUpAmount,bidUpPct,bidStrategy,bidSuggested,bidLow,bidHigh,bidRatio,bidConfidence,bidConfidenceLabel,bidCompCount,bidCompTier,bidStatus,isLikelyPresoldNewBuild,presoldRuleReason</div>
      </section>
    </div>
  `;
}

// ---------------------------------------------------------------------------
// Report reading: single place that knows about the nested validation shape,
// with fallbacks for older/partial reports so a half-written file never
// throws mid-render.

function readReport(state) {
  const report = state.dataSource.report || {};
  const hasReport = Object.keys(report).length > 0;
  const validation = report.validation || {};
  const status = String(validation.status || report.status || report.validationStatus || "").toLowerCase();
  const loadError = state.dataSource.error || "";
  const failed = !!loadError || (hasReport && !!status && status !== "pass");
  const validationTime = validation.generatedAt || report.generatedAt || report.timestamp || "";
  const buildTime = report.generatedAt || "";
  const outputRows =
    validation.counts?.outputRows ?? report.counts?.outputRows ?? report.outputRows ?? report.output_row_count ?? state.dataSource.rowCount ?? 0;
  const realtorFileCount =
    validation.files?.realtorCsvCount ?? report.source?.realtorFiles?.length ?? report.realtorFileCount ?? report.realtor_file_count ?? 0;
  const errors = Array.isArray(validation.errors) ? validation.errors : [];
  const warnings = Array.isArray(validation.warnings) ? validation.warnings : [];
  const statusValue = loadError
    ? loadError
    : status === "pass"
      ? "Pass"
      : status === "fail"
        ? "Fail"
        : status
          ? status
          : hasReport
            ? "Not checked yet"
            : "Not loaded";
  return {
    report,
    validation,
    hasReport,
    status,
    failed,
    validationTime,
    buildTime,
    outputRows,
    realtorFileCount,
    errors,
    warnings,
    statusValue,
    stageCounts: report.counts || {},
  };
}

function latestSaleValue(state) {
  const latestSale = state.derived?.latestSaleDate || "";
  return latestSale ? `${formatDateShort(latestSale)} (${daysAgo(latestSale)}d ago)` : "n/a";
}

// ---------------------------------------------------------------------------
// Health card: the cause-language answer to "is the pipeline OK, and why".

function healthCardHtml(state, info) {
  const headline = !info.hasReport && !state.dataSource.error
    ? "No refresh report loaded yet"
    : info.failed
      ? "Last refresh failed its checks"
      : info.status === "pass"
        ? "Last refresh passed its checks"
        : "Refresh not yet checked";
  const dates = {
    latestSaleDate: state.derived?.latestSaleDate || "",
    generatedAt: info.validationTime || info.buildTime || "",
  };
  const cadenceLine = formatCadenceNote("freshness", dates);
  return `
    <article class="state-panel${info.failed ? " alert" : ""}">
      <div class="panel-kicker">Pipeline health ${renderExplainButton("dataValidation")}</div>
      <h2>${esc(headline)}</h2>
      <p>${esc(pipelineNarrative(state, info))}</p>
      <p>${esc(cadenceLine)} ${renderExplainButton("freshness")}</p>
      ${renderUniverseCaption({ count: state.dataSource.rowCount, universeLabel: "rows loaded before filters" })}
    </article>
  `;
}

function pipelineNarrative(state, info) {
  if (state.dataSource.error) {
    return `The dataset itself failed to load: ${state.dataSource.error} The dashboard cannot show current numbers until this is fixed.`;
  }
  if (!info.hasReport) {
    return "No refresh report has been generated yet, so pipeline health cannot be checked from here. The rows shown elsewhere may still be current; this just means the last-checked story is missing.";
  }
  const fileWord = info.realtorFileCount === 1 ? "file" : "files";
  const sourceLine = `The last run combined King County recorded sales with ${formatWholeNumber(info.realtorFileCount)} manual MLS export ${fileWord} and the Redfin listing feed.`;
  if (info.failed) {
    const why = info.errors.length
      ? ` It failed because: ${info.errors.join("; ")}.`
      : " It failed one or more automated checks.";
    return `${sourceLine}${why} The dashboard keeps showing the last data that did pass, so what is on screen is not necessarily wrong, but this refresh needs attention.`;
  }
  const warnLine = info.warnings.length ? ` Worth noting: ${info.warnings.join("; ")}.` : "";
  return `${sourceLine} It passed every check.${warnLine}`;
}

// ---------------------------------------------------------------------------
// Pipeline stage funnel: every count in the build step's report, grouped into
// plain-language stages instead of raw field names. Grounded in
// scripts/build_mls_enriched_dataset.js's summary.counts (see the block
// around "mlsRowsParsed:" for the source of each field).

function stageFunnelHtml(info) {
  const c = info.stageCounts || {};
  const rows = [];
  rows.push(stageRow(c.baseRows, "County sales the pipeline started from", "Recorded sales from the county assessor file, before any MLS detail is added."));
  rows.push(stageRow(c.mlsRowsParsed, "Raw MLS export rows read", "Every row across all manual MLS export files, before removing duplicates."));
  rows.push(stageRow(
    c.mlsRowsAfterDedupe,
    "MLS rows after removing duplicates",
    `${formatWholeNumber(c.mlsDuplicateListingsCollapsed)} duplicate listing(s) were merged into one row, dropping ${formatWholeNumber(c.mlsDuplicateRowsCollapsed)} repeat rows.`
  ));
  if (Number(c.mlsApnConflictListings) > 0) {
    rows.push(stageRow(
      c.mlsApnConflictListings,
      "MLS listings with a conflicting property match",
      `Duplicate copies of the same listing disagreed on which county property they matched (${formatWholeNumber(c.mlsApnConflictRows)} rows affected), so the pipeline flagged them instead of guessing.`
    ));
  }
  rows.push(stageRow(c.mlsRowsWithResolvedApn, "MLS rows matched to a county property", "These rows carry a parcel number the pipeline could match to a county record."));
  rows.push(stageRow(c.mlsRowsMissingApn, "MLS rows left unmatched", "No matching county parcel number was found, so these stand alone as MLS-only rows."));
  const statusTotal = (Number(c.mlsClosedRows) || 0) + (Number(c.mlsActiveRows) || 0) + (Number(c.mlsOpenStatusRows) || 0);
  rows.push(stageRow(
    statusTotal,
    "Matched MLS rows, by sale status",
    `${formatWholeNumber(c.mlsClosedRows)} closed, ${formatWholeNumber(c.mlsActiveRows)} active, ${formatWholeNumber(c.mlsOpenStatusRows)} other open status.`
  ));
  rows.push(stageRow(
    c.matchedCountyRows,
    "County rows that gained MLS detail",
    `County sales that picked up a real list price, days on market, and other MLS fields from a matched export row${Number(c.countyRowsEnrichedWithListingStub) > 0 ? ` (${formatWholeNumber(c.countyRowsEnrichedWithListingStub)} of those got only a bare list-price stub, not the full detail)` : ""}.`
  ));
  rows.push(stageRow(c.mlsOnlySoldRowsAdded, "New closed sales added from MLS alone", "Sold homes found only in the MLS exports, with no matching county record, added as brand-new rows."));
  rows.push(stageRow(c.mlsOnlyOpenRowsAdded, "New active or pending listings added from MLS alone", "Active or pending MLS listings with no matching county record, added as brand-new rows."));
  rows.push(stageRow(c.outputRows, "Rows in the final combined file", "Every row above combined into one dataset: enriched county sales plus the MLS-only rows added."));
  return rows.join("");
}

function stageRow(value, label, detail) {
  return `<div class="stage-row"><strong>${esc(formatWholeNumber(value))}</strong><span><span class="stage-row-label">${esc(label)}.</span> ${esc(detail)}</span></div>`;
}

// A second, independent recount: the validation step re-reads the finished
// file directly rather than trusting the build step's self-reported counts.
// The two normally agree; a gap is a signal worth a plain-language flag, not
// automatically an error (validate_data_refresh.js does not fail on it).
function recheckNoteHtml(info) {
  const vCounts = info.validation.counts || {};
  const recount = vCounts.outputRows;
  if (!Number.isFinite(Number(recount))) return "";
  const modes = vCounts.modes || {};
  const hasModes = modes.PUBLIC_PROXY !== undefined || modes.MLS_ENRICHED !== undefined;
  const modeLine = hasModes
    ? ` That is ${formatWholeNumber(modes.PUBLIC_PROXY)} county-only rows and ${formatWholeNumber(modes.MLS_ENRICHED)} MLS-enriched rows (${formatWholeNumber(vCounts.mlsSoldRows)} sold, ${formatWholeNumber(vCounts.mlsOpenRows)} open, ${formatWholeNumber(vCounts.mlsActiveRows)} active).`
    : "";
  const buildCount = info.stageCounts.outputRows;
  const mismatch =
    Number.isFinite(Number(buildCount)) && Number(buildCount) !== Number(recount)
      ? ` This differs from the build step's own count of ${formatWholeNumber(buildCount)} rows. A small gap is normal if newer data landed between the two steps; a large one can mean the app is showing an older file than the last full refresh.`
      : "";
  return `The validation step re-read the finished file directly and counted ${formatWholeNumber(recount)} rows.${modeLine}${mismatch}`;
}

// ---------------------------------------------------------------------------

function dataTile(label, value, id, { explainId = "" } = {}) {
  const explain = explainId ? ` ${renderExplainButton(explainId)}` : "";
  return `<article class="data-tile"><span>${esc(label)}${explain}</span><strong id="${esc(id)}">${esc(value)}</strong></article>`;
}
