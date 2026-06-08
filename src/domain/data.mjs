import {
  daysBetween,
  num,
  toIso,
} from "./format.mjs";
import { pulseWatchlistGroup } from "./pulseMetrics.mjs";

export const DEFAULT_DATASET = "public_sales_proxy_mls_enriched_last12mo.csv";
export const REFRESH_REPORT_FILE = "data_refresh_report.json";
export const BUYER_PROFILE_FILE = "buyer_profile_memory.json";
export const DEFAULT_MIN_CLOSE = 1_100_000;
export const DEFAULT_MAX_CLOSE = 1_400_000;
export const PRICE_SLIDER_MIN = 0;
export const PRICE_SLIDER_CAP = 3_000_000;
export const PRICE_SLIDER_STEP = 10_000;
export const PRESOLD_NEW_BUILD_YEAR_MIN = 2023;

export const TYPE_LABELS = {
  "11": "Single Family",
  "12": "Multi-Family (2-4 Units)",
  "13": "Multi-Family (5+ Units)",
  "14": "Condo",
  "15": "Mobile home park/court",
  "18": "Other Residential",
  "19": "Vacation/cabin",
  "50": "Commercial / Non-Residential Condo",
  "91": "Land",
};

export const ZIP_NEIGHBORHOOD = {
  "98101": "Downtown",
  "98102": "Capitol Hill / Eastlake",
  "98103": "Fremont / Green Lake / Wallingford",
  "98104": "Pioneer Square / International District",
  "98105": "University District / Laurelhurst",
  "98106": "Delridge / South Park",
  "98107": "Ballard",
  "98108": "Georgetown / South Park",
  "98109": "South Lake Union / Queen Anne",
  "98111": "Downtown",
  "98112": "Capitol Hill / Madison Park",
  "98115": "Ravenna / Wedgwood",
  "98116": "West Seattle",
  "98117": "Ballard / Crown Hill",
  "98118": "Columbia City / Rainier Valley",
  "98119": "Queen Anne / Magnolia",
  "98121": "Belltown",
  "98122": "Capitol Hill / Central District",
  "98124": "Downtown",
  "98125": "Lake City / North Seattle",
  "98126": "West Seattle / Delridge",
  "98133": "Northgate / Bitter Lake",
  "98134": "SoDo",
  "98136": "West Seattle / Fauntleroy",
  "98144": "Mount Baker / Central District",
  "98154": "Downtown",
  "98164": "Downtown",
  "98174": "Downtown",
  "98177": "North Beach / Crown Hill",
  "98194": "Downtown",
  "98199": "Magnolia",
};

export const ZIP_COORDS = {
  "98101": { lat: 47.6101, lon: -122.3344 },
  "98102": { lat: 47.634, lon: -122.322 },
  "98103": { lat: 47.671, lon: -122.343 },
  "98104": { lat: 47.602, lon: -122.328 },
  "98105": { lat: 47.661, lon: -122.3 },
  "98106": { lat: 47.536, lon: -122.356 },
  "98107": { lat: 47.669, lon: -122.383 },
  "98108": { lat: 47.547, lon: -122.322 },
  "98109": { lat: 47.632, lon: -122.346 },
  "98111": { lat: 47.609, lon: -122.334 },
  "98112": { lat: 47.629, lon: -122.298 },
  "98115": { lat: 47.684, lon: -122.296 },
  "98116": { lat: 47.576, lon: -122.4 },
  "98117": { lat: 47.687, lon: -122.381 },
  "98118": { lat: 47.54, lon: -122.275 },
  "98119": { lat: 47.638, lon: -122.369 },
  "98121": { lat: 47.615, lon: -122.349 },
  "98122": { lat: 47.611, lon: -122.305 },
  "98124": { lat: 47.605, lon: -122.334 },
  "98125": { lat: 47.717, lon: -122.304 },
  "98126": { lat: 47.553, lon: -122.374 },
  "98133": { lat: 47.732, lon: -122.344 },
  "98134": { lat: 47.579, lon: -122.326 },
  "98136": { lat: 47.54, lon: -122.39 },
  "98144": { lat: 47.586, lon: -122.302 },
  "98154": { lat: 47.606, lon: -122.334 },
  "98164": { lat: 47.604, lon: -122.329 },
  "98174": { lat: 47.604, lon: -122.33 },
  "98177": { lat: 47.742, lon: -122.375 },
  "98194": { lat: 47.607, lon: -122.332 },
  "98199": { lat: 47.648, lon: -122.397 },
};

export function zip5(zip) {
  return (String(zip || "").match(/[0-9]{5}/) || [])[0] || "";
}

export function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const ch = line[index];
    if (ch === '"') {
      if (inQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  out.push(current.trim());
  return out;
}

export function parseCsv(text) {
  const lines = String(text || "").trim().split(/\r?\n/);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  const idx = Object.fromEntries(headers.map((header, index) => [header, index]));
  const missing = ["id", "address", "type", "closePrice"].filter((key) => idx[key] === undefined);
  if (missing.length) throw new Error(`Missing required columns: ${missing.join(", ")}`);

  return lines.slice(1).filter((line) => line.trim()).map((line, rowIndex) => {
    const cols = parseCsvLine(line);
    const pick = (name) => (idx[name] !== undefined ? (cols[idx[name]] || "").trim() : "");
    const pickAny = (...names) => {
      for (const name of names) {
        const value = pick(name);
        if (value !== "") return value;
      }
      return "";
    };
    const latVal = num(pick("lat"));
    const lonVal = num(pick("lon"));
    return {
      dataMode: pick("dataMode"),
      id: pick("id"),
      address: pick("address"),
      addressSource: pick("addressSource"),
      major: pick("major"),
      minor: pick("minor"),
      parcelNbr: pick("parcelNbr"),
      neighborhood: pick("neighborhood"),
      type: pick("type"),
      typeCode: pick("typeCode"),
      listDate: pick("listDate"),
      pendingDate: pick("pendingDate"),
      saleDate: pick("saleDate"),
      originalListPrice: num(pick("originalListPrice")),
      pendingListPrice: num(pick("pendingListPrice")),
      listPriceAtPending: num(pick("listPriceAtPending")),
      closePrice: num(pick("closePrice")),
      assessedValue: num(pick("assessedValue")),
      beds: num(pick("beds")),
      baths: num(pick("baths")),
      sqft: num(pick("sqft")),
      sqFtLot: num(pick("sqFtLot")),
      yearBuilt: num(pick("yearBuilt")),
      zip: pick("zip"),
      lat: Math.abs(latVal) > 1 ? latVal : null,
      lon: Math.abs(lonVal) > 1 ? lonVal : null,
      sourceRowIndex: rowIndex + 1,
      mlsListDate: pickAny("mlsListDate", "mlsListingDate"),
      mlsPendingDate: pickAny("mlsPendingDate", "mlsContractualDate"),
      mlsListPriceAtPending: num(pickAny("mlsListPriceAtPending", "mlsListingPrice")),
      mlsListingPrice: num(pickAny("mlsListingPrice", "mlsListPriceAtPending")),
      mlsOriginalPrice: num(pick("mlsOriginalPrice")),
      mlsClosePrice: num(pickAny("mlsClosePrice", "mlsSellingPrice")),
      mlsRegion: pick("mlsRegion"),
      mlsStyleCode: pick("mlsStyleCode"),
      mlsDOMRaw: pick("mlsDOM"),
      mlsStatus: pick("mlsStatus"),
      mlsDOM: num(pick("mlsDOM")),
      mlsCDOMRaw: pick("mlsCDOM"),
      mlsCDOM: num(pick("mlsCDOM")),
      mlsParkingType: pick("mlsParkingType"),
      mlsParkingCoveredTotalRaw: pick("mlsParkingCoveredTotal"),
      mlsTaxesAnnualRaw: pick("mlsTaxesAnnual"),
      mlsBuildingCondition: pick("mlsBuildingCondition"),
      mlsView: pick("mlsView"),
      mlsBankOwnedRaw: pick("mlsBankOwned"),
      mlsThirdPartyApprovalRequiredRaw: pick("mlsThirdPartyApprovalRequired"),
      mlsNewConstructionState: pick("mlsNewConstructionState"),
      mlsSquareFootageSource: pick("mlsSquareFootageSource"),
      hotMarketTag: pick("hotMarketTag"),
      saleToListRatio: num(pick("saleToListRatio")),
      saleToOriginalListRatio: num(pick("saleToOriginalListRatio")),
      bidUpAmount: num(pick("bidUpAmount")),
      bidUpPct: num(pick("bidUpPct")),
      bidStrategy: pick("bidStrategy"),
      bidSuggested: num(pick("bidSuggested")),
      bidLow: num(pick("bidLow")),
      bidHigh: num(pick("bidHigh")),
      bidRatio: num(pick("bidRatio")),
      bidConfidence: num(pick("bidConfidence")),
      bidConfidenceLabel: pick("bidConfidenceLabel"),
      bidCompCount: num(pick("bidCompCount")),
      bidCompTier: pick("bidCompTier"),
      bidStatus: pick("bidStatus"),
    };
  });
}

export function titleCase(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

export function inferMode(row) {
  if (row.dataMode) return row.dataMode;
  const hasMls = row.mlsListDate || row.mlsPendingDate || row.mlsListPriceAtPending || row.mlsClosePrice;
  return hasMls ? "MLS_ENRICHED" : "PUBLIC_PROXY";
}

export function labelNeighborhood(rawNeighborhood, zip) {
  const raw = String(rawNeighborhood || "").trim();
  const zipValue = zip5(zip);
  const broadMlsRegion = /^(nw|ne)\s+seattle$/i.test(raw)
    || /^qa[\s/_-]*magnolia$/i.test(raw)
    || /^central[\s/_-]*south\s+seattle$/i.test(raw);
  if (/[a-z]/i.test(raw) && !broadMlsRegion) return raw;
  return ZIP_NEIGHBORHOOD[zipValue] || "Seattle (Other)";
}

export function labelType(rawType, typeCode) {
  const raw = String(rawType || "").trim();
  if (/[a-z]/i.test(raw)) {
    const pretty = /^[A-Z0-9\s,()\-]+$/.test(raw) ? titleCase(raw) : raw;
    const norm = pretty.toLowerCase();
    if (norm.includes("single family")) return "Single Family";
    if (norm.includes("condomini")) return "Condo";
    if (norm.includes("2-4 units")) return "Multi-Family (2-4 Units)";
    if (norm.includes("5+ units")) return "Multi-Family (5+ Units)";
    if (norm.includes("land")) return "Land";
    return pretty;
  }
  const code = String(num(typeCode || raw) || "");
  return TYPE_LABELS[code] || (code ? `Type ${code}` : "Unknown");
}

export function normalizeMlsStatus(rawStatus) {
  const raw = String(rawStatus || "").trim();
  if (!raw) return "";
  const upper = raw.toUpperCase();
  if (upper === "SOLD") return "Sold";
  if (upper === "ACTIVE") return "Active";
  if (upper === "PENDING") return "Pending";
  if (upper === "PENDING INSPECTION") return "Pending Inspection";
  if (upper === "PENDING BU REQUESTED") return "Pending BU Requested";
  if (upper === "CONTINGENT") return "Contingent";
  return /^[A-Z0-9\s/-]+$/.test(raw) ? titleCase(raw) : raw;
}

export function normalizeBooleanText(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) return "";
  if (/^(true|yes|y|1)$/i.test(raw)) return "true";
  if (/^(false|no|n|0)$/i.test(raw)) return "false";
  return raw;
}

export function normalizeThirdPartyApproval(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) return "";
  if (/^none$/i.test(raw)) return "None";
  if (/^short sale$/i.test(raw)) return "Short Sale";
  if (/^other\b/i.test(raw)) return "Other - See Remarks";
  return raw;
}

export function isBankOwnedFlag(value) {
  return String(value || "").trim().toLowerCase() === "true";
}

export function hasThirdPartyApprovalRequirement(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  return !/^none$/i.test(raw) && !/^false$/i.test(raw) && !/^no$/i.test(raw);
}

export function matchesSpecialSaleFilter(row, filterValue) {
  const mode = String(filterValue || "all").toLowerCase();
  if (mode === "only") return !!row.isSpecialSale;
  if (mode === "exclude") return !row.isSpecialSale;
  return true;
}

export function specialSaleFilterLabel(value) {
  if (value === "only") return "Special-Sale Only";
  if (value === "exclude") return "Exclude Special-Sale";
  return "All";
}

export function normalizeForMatch(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

export function mapPropertyKey(rowLike) {
  const id = String(rowLike.id || "").trim();
  const parcel = String(rowLike.parcelNbr || `${rowLike.major || ""}${rowLike.minor || ""}`).replace(/[^0-9]/g, "");
  const saleDate = String(rowLike.saleDate || rowLike.pendingDate || rowLike.listDate || "").trim();
  const address = String(rowLike.address || "").trim().toUpperCase();
  const idx = String(rowLike.sourceRowIndex || "").trim();
  return [id, parcel, saleDate, address, idx].join("|");
}

export function normalizeRow(source) {
  const dataMode = inferMode(source);
  const saleDate = source.saleDate || source.pendingDate || source.listDate || "";
  const listDate = source.mlsListDate || source.listDate || "";
  const pendingDate = source.mlsPendingDate || source.pendingDate || "";
  const closePrice = num(source.mlsClosePrice || source.closePrice);
  const assessedValue = num(source.assessedValue);
  const pendingListPrice = num(
    source.mlsListingPrice
      || source.mlsListPriceAtPending
      || source.pendingListPrice
      || source.listPriceAtPending
      || source.assessedValue
      || closePrice
  );
  const originalListPriceRaw = num(source.mlsOriginalPrice || source.originalListPrice);
  const originalListPrice = originalListPriceRaw > 0 ? originalListPriceRaw : 0;
  const listPriceAtPending = pendingListPrice;
  // A *genuine* market list price exists only when an MLS/Redfin listing price
  // is present. For PUBLIC_PROXY (county-only) rows, pendingListPrice falls back
  // to the tax-assessed value, which is NOT a list price — so sale/list ratio
  // and bid-up computed against it are meaningless and must not be shown.
  const hasMarketListPrice = num(source.mlsListingPrice) > 0
    || num(source.mlsListPriceAtPending) > 0
    || num(source.saleToListRatio) > 0
    || num(source.bidUpAmount) !== 0;
  const daysToPendingRaw = daysBetween(listDate, pendingDate);
  const daysToPending = dataMode === "MLS_ENRICHED" ? daysToPendingRaw : null;
  const mlsDOMRaw = String(source.mlsDOMRaw ?? source.mlsDOM ?? "").trim();
  const hasMlsDomValue = mlsDOMRaw !== "";
  const mlsDOM = num(mlsDOMRaw);
  const mlsCDOMRaw = String(source.mlsCDOMRaw ?? source.mlsCDOM ?? "").trim();
  const hasMlsCdomValue = mlsCDOMRaw !== "";
  const mlsCDOM = num(mlsCDOMRaw);
  const marketDomDays = hasMlsCdomValue ? mlsCDOM : (hasMlsDomValue ? mlsDOM : daysToPending);
  // Only derive sale/list + bid-up when we have a real market list price.
  // Otherwise leave them at 0 so every surface (map color, Records, Pulse,
  // tooltips) treats them as unavailable rather than charting close-vs-assessed.
  const saleToList = num(source.saleToListRatio)
    || (hasMarketListPrice && pendingListPrice > 0 ? closePrice / pendingListPrice : 0);
  const saleToOriginalList = num(source.saleToOriginalListRatio)
    || (originalListPrice > 0 ? closePrice / originalListPrice : 0);
  const delta = num(source.bidUpAmount)
    || (hasMarketListPrice ? (closePrice - listPriceAtPending) : 0);
  const deltaPct = num(source.bidUpPct)
    || (hasMarketListPrice && listPriceAtPending > 0 ? delta / listPriceAtPending : 0);
  const tagRaw = String(source.hotMarketTag || "").toUpperCase();
  const ultraByTag = /ULTRA[_\s]?HOT/.test(tagRaw);
  const hotByTag = /HOT_MARKET/.test(tagRaw) || ultraByTag;
  const ultraByDom = marketDomDays !== null && Number.isFinite(marketDomDays) && marketDomDays <= 5;
  const hotByDom = marketDomDays !== null && Number.isFinite(marketDomDays) && marketDomDays <= 10;
  const isUltraHot = ultraByTag || ultraByDom;
  const isHotMarket = isUltraHot || hotByTag || hotByDom;
  const sqft = num(source.sqft);
  const lotSize = num(source.sqFtLot);
  const pricePerSqft = sqft > 0 ? closePrice / sqft : 0;
  const mlsStatusLabel = normalizeMlsStatus(source.mlsStatus);
  const mlsStatusNorm = mlsStatusLabel.toUpperCase();
  const styleCode = String(source.mlsStyleCode || "").trim();
  const mlsParkingCoveredTotalRaw = String(source.mlsParkingCoveredTotalRaw ?? source.mlsParkingCoveredTotal ?? "").trim();
  const mlsTaxesAnnualRaw = String(source.mlsTaxesAnnualRaw ?? source.mlsTaxesAnnual ?? "").trim();
  const mlsBankOwned = normalizeBooleanText(source.mlsBankOwnedRaw ?? source.mlsBankOwned);
  const mlsThirdPartyApprovalRequired = normalizeThirdPartyApproval(
    source.mlsThirdPartyApprovalRequiredRaw ?? source.mlsThirdPartyApprovalRequired
  );
  const hasMlsParkingCoveredTotal = mlsParkingCoveredTotalRaw !== "";
  const hasMlsTaxesAnnual = mlsTaxesAnnualRaw !== "";
  const isBankOwned = isBankOwnedFlag(mlsBankOwned);
  const hasThirdPartyApproval = hasThirdPartyApprovalRequirement(mlsThirdPartyApprovalRequired);
  const isSpecialSale = isBankOwned || hasThirdPartyApproval;
  const hasNewBuildSignal = num(source.yearBuilt) >= PRESOLD_NEW_BUILD_YEAR_MIN
    || /townhouse|new construction|new build/i.test(styleCode);
  const hasSoldContext = dataMode === "MLS_ENRICHED" && closePrice > 0 && listPriceAtPending > 0;
  const domLeZero = hasMlsDomValue && mlsDOM <= 0;
  const saleEqualsList = Math.abs(closePrice - listPriceAtPending) < 1;
  const isLikelyPresoldNewBuild = hasSoldContext && domLeZero && saleEqualsList && hasNewBuildSignal;
  const presoldReasonTokens = [];
  if (isLikelyPresoldNewBuild) {
    presoldReasonTokens.push("dom_le_0", "sale_eq_list");
    if (num(source.yearBuilt) >= PRESOLD_NEW_BUILD_YEAR_MIN) presoldReasonTokens.push("year_built_gte_2023");
    if (/townhouse|new construction|new build/i.test(styleCode)) presoldReasonTokens.push("style_new_build_signal");
  }
  const zipCoord = ZIP_COORDS[zip5(source.zip)];
  const mapLat = Number.isFinite(source.lat) ? source.lat : (zipCoord ? zipCoord.lat : null);
  const mapLon = Number.isFinite(source.lon) ? source.lon : (zipCoord ? zipCoord.lon : null);
  const hasActualClose = closePrice > 0 && !!toIso(saleDate);
  const effectiveDate = toIso(saleDate || pendingDate || listDate);
  const neighborhoodLabel = labelNeighborhood(source.neighborhood, source.zip);
  const pulseGroup = pulseWatchlistGroup(neighborhoodLabel);

  return {
    ...source,
    mapPropertyKey: mapPropertyKey(source),
    dataMode,
    typeLabel: labelType(source.type, source.typeCode),
    neighborhoodLabel,
    pulseWatchlistGroup: pulseGroup,
    mlsStatusLabel,
    mlsStatusNorm,
    mlsRegion: source.mlsRegion || "",
    mlsStyleCode: styleCode,
    mlsParkingType: String(source.mlsParkingType || "").trim(),
    mlsParkingCoveredTotal: hasMlsParkingCoveredTotal ? num(mlsParkingCoveredTotalRaw) : 0,
    mlsParkingCoveredTotalRaw,
    hasMlsParkingCoveredTotal,
    mlsTaxesAnnual: hasMlsTaxesAnnual ? num(mlsTaxesAnnualRaw) : 0,
    mlsTaxesAnnualRaw,
    hasMlsTaxesAnnual,
    mlsBuildingCondition: String(source.mlsBuildingCondition || "").trim(),
    mlsView: String(source.mlsView || "").trim(),
    mlsBankOwned,
    isBankOwned,
    mlsThirdPartyApprovalRequired,
    hasThirdPartyApprovalRequirement: hasThirdPartyApproval,
    isSpecialSale,
    mlsNewConstructionState: String(source.mlsNewConstructionState || "").trim(),
    mlsSquareFootageSource: String(source.mlsSquareFootageSource || "").trim(),
    saleDate: toIso(saleDate),
    listDate: toIso(listDate),
    pendingDate: toIso(pendingDate),
    effectiveDate,
    hasActualClose,
    isProjectionRow: false,
    projectedClosePrice: 0,
    projectedCloseLow: 0,
    projectedCloseHigh: 0,
    projectionBasisCount: 0,
    closePrice,
    assessedValue,
    pendingListPrice,
    originalListPrice,
    listPriceAtPending,
    mlsDOM,
    mlsCDOM,
    hasMlsDomValue,
    hasMlsCdomValue,
    beds: num(source.beds),
    baths: num(source.baths),
    sqft,
    lotSize,
    yearBuilt: num(source.yearBuilt),
    daysToPending,
    hasMarketListPrice,
    saleToList,
    saleToOriginalList,
    delta,
    deltaPct,
    bidStrategy: source.bidStrategy || "",
    bidSuggested: num(source.bidSuggested),
    bidLow: num(source.bidLow),
    bidHigh: num(source.bidHigh),
    bidRatio: num(source.bidRatio),
    bidConfidence: num(source.bidConfidence),
    bidConfidenceLabel: source.bidConfidenceLabel || "N/A",
    bidCompCount: num(source.bidCompCount),
    bidCompTier: source.bidCompTier || "NONE",
    bidStatus: source.bidStatus || "",
    hotMarketTag: isUltraHot ? "ULTRA_HOT_<=5D" : (isHotMarket ? "HOT_MARKET_<=10D" : ""),
    isUltraHot,
    isHotMarket,
    isLikelyPresoldNewBuild,
    presoldRuleReason: presoldReasonTokens.join("|"),
    pricePerSqft,
    mapLat,
    mapLon,
  };
}

export function normalizeRows(rows) {
  return (Array.isArray(rows) ? rows : []).map(normalizeRow);
}

export function findDefaultSingleFamilyLabel(types) {
  const options = Array.isArray(types) ? types : [];
  const normalized = options.map((value) => ({ value, norm: normalizeForMatch(value) }));
  return normalized.find((item) => item.norm === "single family")?.value
    || normalized.find((item) => item.norm.includes("single family"))?.value
    || normalized.find((item) => item.norm.includes("household single family"))?.value
    || "All";
}

export function uniqueValues(rows, key) {
  return ["All", ...Array.from(new Set((rows || []).map((row) => row[key]).filter(Boolean))).sort()];
}

export function domMetric(row) {
  if (row?.hasMlsCdomValue && Number.isFinite(row.mlsCDOM) && row.mlsCDOM >= 0) return row.mlsCDOM;
  if (row?.hasMlsDomValue && Number.isFinite(row.mlsDOM) && row.mlsDOM >= 0) return row.mlsDOM;
  if (row?.dataMode === "MLS_ENRICHED" && row.daysToPending !== null && Number.isFinite(row.daysToPending) && row.daysToPending >= 0) {
    return row.daysToPending;
  }
  return null;
}

export function hotCategory(row) {
  if (row?.isUltraHot) return "Ultra Hot";
  if (row?.isHotMarket) return "Hot";
  return "Normal";
}

export function inRatioBucket(value, key) {
  if (key === "lt095") return value > 0 && value < 0.95;
  if (key === "095to100") return value >= 0.95 && value < 1.0;
  if (key === "100to105") return value >= 1.0 && value <= 1.05;
  if (key === "gt105") return value > 1.05;
  if (key === "gt100") return value > 1.0;
  return true;
}

export function rowInViewport(row, bounds) {
  if (!bounds) return true;
  if (!Number.isFinite(row.mapLat) || !Number.isFinite(row.mapLon)) return false;
  return row.mapLat >= bounds.south
    && row.mapLat <= bounds.north
    && row.mapLon >= bounds.west
    && row.mapLon <= bounds.east;
}

export function zillowUrl(row) {
  const raw = String(row.address || "").replace(/\s+/g, " ").trim();
  const core = raw.replace(/,\s*Seattle\b.*$/i, "").trim() || raw;
  const zip = zip5(row.zip) || (String(raw).match(/\b[0-9]{5}\b/) || [])[0] || "";
  const full = `${core} Seattle WA ${zip}`.trim();
  const slug = full.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  return `https://www.zillow.com/homes/${slug}_rb/`;
}

export function countyRecordUrl(row) {
  const parcel = String(row.parcelNbr || `${row.major || ""}${row.minor || ""}`).replace(/[^0-9]/g, "");
  return parcel ? `https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr=${parcel}` : "";
}
