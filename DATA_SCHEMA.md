# Data Modes

## Public Proxy (`PUBLIC_PROXY`)
Used when only county public data is available.

Core columns:
- `id,address,neighborhood,type,listDate,pendingDate,listPriceAtPending,closePrice`

Typical optional enrichment:
- `assessedValue,beds,baths,sqft,yearBuilt,zip,districtName,area,subArea,sqFtLot,zoning`

## MLS Enriched (`MLS_ENRICHED`)
Used when realtor-provided MLS fields are available.

Recommended extra MLS columns:
- `mlsListDate`
- `mlsPendingDate`
- `mlsListPriceAtPending`
- `mlsClosePrice`

Normalization behavior in app:
- `listDate = mlsListDate || listDate`
- `pendingDate = mlsPendingDate || pendingDate`
- `listPriceAtPending = mlsListPriceAtPending || listPriceAtPending || assessedValue`
- `closePrice = mlsClosePrice || closePrice`

# Build and Run

1. Rebuild proxy CSV from county files:

```bash
node scripts/build_public_proxy_csv.js
```

2. Serve app locally so auto-load works:

```bash
node scripts/serve.js
```

3. Open:

`http://localhost:4173`

The app auto-loads `public_sales_proxy_all_prices_last6mo.csv` when served over HTTP.
