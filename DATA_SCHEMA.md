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
- `mlsListPriceAtPending` (legacy alias; `mlsListingPrice` is preferred)
- `mlsListingPrice`
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
- `REDFIN_SOLD` rows and county twins (2026-11-09): when a Redfin sold row is the same sale as a bare county row (`PUBLIC_PROXY`, no MLS#, no asking price), matched on street + ZIP + unit within 14 days or on house number + ZIP + exact price within 60, the Redfin row REPLACES the county row and inherits `major`, `minor`, `parcelNbr`, `assessedValue`. Such a row is carried over even after the sold feed stops supplying it. A county rebuild restores the county row and the next merge replaces it again.
- `mlsJoinMethod` (`APN_PRICE_DATE_WINDOW` for county-matched rows, `MLS_SOLD_NOT_IN_COUNTY` for MLS sold rows not yet in county close exports)
- `listPriceSource` (additive, 2026-09-19): `ACTIVE_SNAPSHOT` when `listPriceAtPending`, `pendingDate`, list date and DOM were recovered by `scripts/backfill_list_from_active_snapshots.js` from the daily active-listing snapshots (the last day the MLS# was seen for sale; that day is written as `pendingDate`, so the true pending date is a lower bound, up to one snapshot interval later). `REDFIN_HISTORY` when it was restored from the tracked sold-enrichment ledger (`redfin_sold_enrichment_ledger.csv`, built by `scripts/sold_enrichment_ledger.js` from past snapshots and the Redfin history scrape) or written by `backfill_redfin_history.js`; `MLS_EXPORT` when the ledger restored a realtor-export list price. Blank on rows enriched before the column existed. `addressSource` is not touched by any of these.
- `mlsDOM` / `mlsCDOM` / `mlsListDate` / `mlsOriginalPrice` on `REDFIN_ACTIVE` rows (semantics changed 2026-11-09): Redfin stopped returning days on market for ~99% of actives on 2026-09-21. A blank feed value is now "unknown", never 0. Order of trust: the feed's DOM, then the actives ledger's `listDate`, then blank. `mlsCDOM` is `mlsDOM` plus the ledger's `chainPriorDays` and `mlsOriginalPrice` is the ledger's `chainFirstAsk`, so a cancel-and-relist under a new MLS# resets neither. A "Coming Soon" teaser reads 0 days. A listing the feed reports as under contract is written with `mlsStatus = Active Under Contract`, not `Active`. `REDFIN_SOLD` rows filled from the ledger follow the same rules.
- `priorSaleDate`, `priorSalePrice` (additive, 2026-11-09): on `REDFIN_ACTIVE` rows only, the home's most recent closing already in the dataset (matched on street + ZIP + unit), written by `merge_redfin_actives.js`. Blank when there is none, or when the ask is under 0.5x or over 3.5x that price (a builder's multi-lot purchase, a partial-interest deed).
- `mlsStatus` canonicalized to: `Active`, `Pending`, `Pending Inspection`, `Pending BU Requested`, `Contingent`, `Sold`

Normalization behavior in app:
- `listDate = mlsListDate || listDate`
- `pendingDate = mlsPendingDate || pendingDate`
- `pendingListPrice = mlsListingPrice || mlsListPriceAtPending || pendingListPrice || listPriceAtPending`
- `originalListPrice = mlsOriginalPrice || originalListPrice`
- `listPriceAtPending` retained as compatibility alias to `pendingListPrice`
- `closePrice = mlsClosePrice || closePrice`
- `saleToList = saleToListRatio || (closePrice / pendingListPrice)`
- `saleToOriginalList = saleToOriginalListRatio || (closePrice / originalListPrice)`
- `map coordinates = lat/lon (if present) || inferred zip/neighborhood anchors`
- `isLikelyPresoldNewBuild = (MLS_ENRICHED sold row) && (explicit mlsDOM <= 0) && (closePrice ~= listPriceAtPending) && (yearBuilt >= 2023 || mlsStyleCode contains townhouse/new construction/new build)`

## Bid Recommendation Fields (Active MLS)

Bid guidance is produced for rows where:
- `dataMode = MLS_ENRICHED`
- `mlsStatus = Active`
- `closePrice` missing/zero
- `pendingListPrice > 0`

Default model behavior:
- comps (`src/domain/bidComps.mjs`): every same-type sold MLS row from the last 365 days, weighted by recency (60-day half-life), closeness of days to pending (log scale, bandwidth 0.5; 0-7 days is pooled and walled off from later sales), closeness of asking price (log scale, bandwidth 0.25), same neighborhood or ZIP (up to 20x: `1 + 19n / (n + 8)` for n effective nearby comps) and same build era (under / over 15 years old; 0.05x across)
- the lens's price band and affordability filter are not applied to comps; its other filters are
- bid anchor: `pendingListPrice`
- listing stage (`src/domain/listingStage.mjs`) is a label: cumulative days on market 0-7 `fresh`, 8-14 `past`, 15-45 `sitting`, 46+ `stale`
- minimum comps: an effective 6 (Kish: squared sum of weights over sum of squared weights), measured before the location boost
- under 6 near a late listing's days on market: every sale past week one (`bidDomMatch = side`)
- no days-on-market reading, or still under 6: comps regardless of days on market (`bidDomMatch = none`, `bidStageMatched = false`), 25 confidence points off
- confidence: 100, minus 5 per percentage point between the comps' 20th and 80th percentile ratios, minus 1.5 per effective comp short of 20, minus up to 25 when the newest comp that matters is over 45 days old
- ratio clamp 0.85x to 1.25x
- likely pre-sold new-build comps excluded by default

Output fields:
- `bidStrategy`
- `bidSuggested`
- `bidLow`
- `bidHigh`
- `bidRatio`
- `bidConfidence`
- `bidConfidenceLabel` (`High` / `Medium` / `Low` / `N/A`)
- `bidCompCount` (effective number of comps, rounded)
- `bidCompTier` (`T1_NEIGHBORHOOD_TYPE` = at least half the weight is in the same neighborhood or ZIP | `T3_CITY_TYPE` | `NONE`)
- `bidStatus` (`SCored` | `InsufficientComps`)
- `bidStage` (`fresh` | `past` | `sitting` | `stale` | blank), `bidStageLabel`, `bidStageMatched`
- in the app only: `bidOutcomeLow` / `bidOutcomeHigh` (10th and 90th percentile), `bidLadder` (bid at each win chance, 50% to 90%), `bidDomMatch` (`near` | `side` | `none`), `bidNewerBuild`

## Actives ledger (`redfin_active_ledger.csv`, published slim as `public/listing_ledger.csv`)

One row per MLS# ever seen in the Redfin actives feed. Columns derived on every write (2026-11-09):
- `listDateSource`: `FEED` (fetch date minus the feed's DOM) or `FIRST_SEEN` (estimated as the day before the first snapshot that held the listing, only when the feed was also sampled within the 3 days before). On the 1,749 listings first seen Jul 23 to Sep 19, which have both readings, the first sighting was within 1 day of the list date 94.5% of the time and within 2 days 98.3%.
- `chainId`, `chainPosition`, `chainListDate`, `chainFirstAsk`, `chainPriorDays`: relist chain. Consecutive MLS#s at one street + ZIP + unit are one marketing period when the next appears within 90 days of the last, asks no more than 10% above and no more than 30% below it, and (if the two overlap by more than a day) asks within 10% of it. `chainId` is the first MLS# in the chain; `chainPriorDays` is the days the earlier listings spent on the market, so cumulative days exclude time off the market. A "Coming Soon" teaser id never joins a chain.

- `askHistory`: every asking price with the first morning it was seen, oldest first, as `date:ask|date:ask` (`2026-07-15:1250000|2026-08-02:1199000`). Appended to by the daily upsert. The first run on a ledger without the column fills the past from the daily snapshots in git, writing no other column (about 30 seconds; on the 5,051-row ledger of 2026-11-08 the result matched `askChangeCount` on 5,045 rows, and `firstAsk`, `lastAsk` and `lastAskChangeDate` on all of them). Where git has no depth it falls back to the first and last asks and tries again on each run until under 2% of the listings that changed price are missing changes. The app ignores a history with fewer entries than `askChangeCount + 1`, and shows no cut or sale odds at all unless 95% of homes have a complete one. Blank in the published copy means "not recorded", never "no change".

The app groups the ledger by `chainId` (`src/domain/listingLedger.mjs`) for standing inventory, months of supply, price cuts, relists and withdrawals, and reads `askHistory` (`src/domain/listingOutlook.mjs`) for the odds of a cut or a sale in the next 14 days.

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

4. Validate refresh output and write report:

```bash
node scripts/validate_data_refresh.js
```

5. Optional one-command rebuild + validate (+ push):

```bash
node scripts/refresh_data_pipeline.js
node scripts/refresh_data_pipeline.js --push
```

6. Serve app locally so auto-load works:

```bash
node scripts/serve.js
```

7. Open:

`http://localhost:4173`

The app auto-loads `public_sales_proxy_mls_enriched_last12mo.csv` when served over HTTP.
