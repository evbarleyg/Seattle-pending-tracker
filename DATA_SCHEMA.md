# Data Modes

## Public Proxy (`PUBLIC_PROXY`)
Used when only county public data is available.

Core columns:
- `id,address,neighborhood,type,listDate,pendingDate,listPriceAtPending,closePrice`

Typical optional enrichment:
- `assessedValue,beds,baths,sqft,yearBuilt,zip,districtName,area,subArea,sqFtLot,zoning`
- `major,minor,parcelNbr,lat,lon` (for parcel-accurate geo pins)

## MLS Enriched (`MLS_ENRICHED`)
Used when realtor-provided MLS fields are available.

Recommended extra MLS columns:
- `mlsListDate`
- `mlsPendingDate`
- `mlsListPriceAtPending`
- `mlsClosePrice`
- `mlsDOM`
- `mlsCDOM`
- `mlsStyleCode`
- `hotMarketTag` (`HOT_MARKET_<=10D` when DOM/list-to-pending is <=10 days; `ULTRA_HOT_<=5D` when <=5 days)
- `saleToListRatio`
- `bidUpAmount`
- `bidUpPct`
- `isLikelyPresoldNewBuild` (derived boolean for likely pre-sold new-build behavior)
- `presoldRuleReason` (machine-readable reason tokens, e.g. `dom_le_0|sale_eq_list|year_built_gte_2023`)
- `mlsJoinMethod` (`APN_PRICE_DATE_WINDOW` for county-matched rows, `MLS_SOLD_NOT_IN_COUNTY` for MLS sold rows not yet in county close exports)

Normalization behavior in app:
- `listDate = mlsListDate || listDate`
- `pendingDate = mlsPendingDate || pendingDate`
- `listPriceAtPending = mlsListPriceAtPending || listPriceAtPending`
- `closePrice = mlsClosePrice || closePrice`
- `saleToList = saleToListRatio || (closePrice / listPriceAtPending)`
- `map coordinates = lat/lon (if present) || inferred zip/neighborhood anchors`
- `isLikelyPresoldNewBuild = (MLS_ENRICHED sold row) && (explicit mlsDOM <= 0) && (closePrice ~= listPriceAtPending) && (yearBuilt >= 2023 || mlsStyleCode contains townhouse/new construction/new build)`

# Build and Run

1. Optional: build parcel coordinate join file from KC GIS export:

```bash
node scripts/build_parcel_coord_lookup.js /path/to/king_county_parcel_points.csv
```

This creates `/Users/evanbarley-greenfield/Documents/Evan Tester Project/parcel_coords_major_minor.csv` with:

`major,minor,parcelNbr,lat,lon`

2. Rebuild proxy CSV from county files:

```bash
node scripts/build_public_proxy_csv.js
```

3. Build MLS-enriched dataset from realtor exports:

```bash
node scripts/build_mls_enriched_dataset.js
```

4. Serve app locally so auto-load works:

```bash
node scripts/serve.js
```

5. Open:

`http://localhost:4173`

The app auto-loads `public_sales_proxy_mls_enriched_last12mo.csv` when served over HTTP.
