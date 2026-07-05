// Data view, extracted from src/main.mjs.
//
// Renders refresh/report metadata tiles and the public-asset column list.
// This view must also render before a dataset loads (it shows load status),
// so it tolerates a missing state.derived.
//
// main.mjs stays the owner of app state and shared UI helpers; it passes
// them in through the deps object on every render:
//   { state, qs }
import {
  daysAgo,
  esc,
  formatDateShort,
  formatDateTime,
  formatWholeNumber,
} from "../domain/format.mjs";

// Deps injected by main.mjs at the start of every render.
let ctx = null;

export function renderDataView(deps) {
  ctx = deps;
  const { state, qs } = ctx;
  const wrap = qs("#view-data");
  if (!wrap) return;
  const report = state.dataSource.report || {};
  const latestSale = state.derived?.latestSaleDate || "";
  wrap.innerHTML = `
    <div class="view-band">
      <section class="section-head"><div><p class="eyebrow">Data</p><h2>Refresh and dataset health</h2></div></section>
      <div class="data-grid">
        ${dataTile("Dataset", state.dataSource.datasetName, "dataDatasetName")}
        ${dataTile("Rows", formatWholeNumber(state.dataSource.rowCount), "dataDatasetRows")}
        ${dataTile("Validation", report.status || report.validationStatus || state.dataSource.error || "Not loaded", "dataValidationStatus")}
        ${dataTile("Validation time", report.generatedAt ? formatDateTime(report.generatedAt) : (report.timestamp ? formatDateTime(report.timestamp) : "n/a"), "dataValidationTime")}
        ${dataTile("Latest sale date", latestSale ? `${formatDateShort(latestSale)} (${daysAgo(latestSale)}d ago)` : "n/a", "dataLatestSaleDate")}
        ${dataTile("Output rows", formatWholeNumber(report.outputRows || report.output_row_count || state.dataSource.rowCount || 0), "dataOutputRows")}
        ${dataTile("Realtor files", formatWholeNumber(report.realtorFileCount || report.realtor_file_count || 0), "dataRealtorFileCount")}
      </div>
      <section class="section-block">
        <h3>Public assets</h3>
        <div class="mono">dataMode,addressSource,major,minor,parcelNbr,lat,lon,neighborhood,typeCode,zip,listDate,pendingDate,saleDate,originalListPrice,pendingListPrice,listPriceAtPending,closePrice,beds,baths,sqft,yearBuilt,mlsStatus,mlsListingPrice,mlsOriginalPrice,mlsDOM,mlsCDOM,mlsStyleCode,mlsParkingType,mlsParkingCoveredTotal,mlsTaxesAnnual,mlsBuildingCondition,mlsView,mlsBankOwned,mlsThirdPartyApprovalRequired,mlsNewConstructionState,mlsSquareFootageSource,hotMarketTag,saleToListRatio,saleToOriginalListRatio,bidUpAmount,bidUpPct,bidStrategy,bidSuggested,bidLow,bidHigh,bidRatio,bidConfidence,bidConfidenceLabel,bidCompCount,bidCompTier,bidStatus,isLikelyPresoldNewBuild,presoldRuleReason</div>
      </section>
    </div>
  `;
}

function dataTile(label, value, id) {
  return `<article class="data-tile"><span>${esc(label)}</span><strong id="${esc(id)}">${esc(value)}</strong></article>`;
}
