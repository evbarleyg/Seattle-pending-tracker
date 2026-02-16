# Seattle Housing Buyer Lens

Buyer-focused Seattle housing dashboard built from King County public data, with optional MLS-enriched fields when available.

Live site: [https://evbarleyg.github.io/Seattle-pending-tracker/](https://evbarleyg.github.io/Seattle-pending-tracker/)

## What This App Does

- Loads an MLS-enriched Seattle dataset by default (`public_sales_proxy_mls_enriched_last12mo.csv`).
- Starts with default filters:
  - `Property Type`: Single Family
  - `Close Price`: $1.1M to $1.4M
- Supports dynamic cross-filtering across:
  - KPI cards
  - Charts
  - Geo view
  - Row-level records
- Provides outbound links for each row:
  - Zillow search link
  - King County official parcel link (`KC Record`)

## Data Modes

- `PUBLIC_PROXY`: Built from public county records.
- `MLS_ENRICHED`: County rows matched to realtor exports with list/pending/DOM/bid-up fields, plus MLS sold rows not yet posted in county-close exports.

## Buyer-Focused Flags

- `Pending Price Projection (Experimental)`: projects close price ranges for pending MLS rows using filtered comp ratios.
- `Bids` tab: active-listing offer guidance using recent sold MLS comps (default 90 days), with suggested bid point/range and confidence.
- `Exclude likely pre-sold new builds` (off by default): removes rows matching:
  - `dataMode = MLS_ENRICHED`
  - `closePrice > 0` and `listPriceAtPending > 0`
  - explicit MLS DOM present and `DOM <= 0`
  - `closePrice == listPriceAtPending` (within <$1 tolerance)
  - plus new-build signal: `yearBuilt >= 2023` or style text containing `townhouse/new construction/new build`

## Local Run

Install local tooling (for checks/tests/CI parity):

```bash
npm ci
```

1. Optional but recommended for exact map pins: normalize a King County GIS parcel-point export to `major+minor+lat+lon`:

```bash
node scripts/build_parcel_coord_lookup.js /path/to/king_county_parcel_points.csv
```

This writes `/Users/evanbarley-greenfield/Documents/Evan Tester Project/parcel_coords_major_minor.csv`.

2. Rebuild the default proxy dataset:

```bash
node scripts/build_public_proxy_csv.js
```

3. Build the MLS-enriched default dataset (from `realtor_exports/*.csv`):

```bash
node scripts/build_mls_enriched_dataset.js
```

4. Validate refresh output:

```bash
node scripts/validate_data_refresh.js
```

5. Start the local server:

```bash
node scripts/serve.js
```

6. Open:

`http://localhost:4173`

## Quality Checks

Run the same gates used in CI/deploy:

```bash
npm run check
```

Individual commands:

```bash
npm run lint
npm run typecheck
npm run test
npm run build
```

## Automated Refresh

One command rebuild + validate:

```bash
node scripts/refresh_data_pipeline.js
```

Rebuild + validate + commit/push to `main` (triggers GitHub Pages deploy):

```bash
node scripts/refresh_data_pipeline.js --push
```

Optional flags:

- `--skip-public` skips county proxy rebuild.
- `--skip-mls` skips MLS enrichment rebuild.
- `--report-only` runs validation/report only.

Refresh summary is saved to:

- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/data_refresh_report.json`

## Bid Recommendations (Active MLS)

- Scope: `MLS_ENRICHED` rows where `mlsStatus = Active` and no close price.
- Bid anchor: `List@Pending` (`pendingListPrice`).
- Default strategy: `Balanced`.
- Output per listing:
  - Suggested bid
  - Bid range
  - Suggested Sale/List ratio
  - Confidence score/label
  - Comp count and comp tier
- Manual scenario support:
  - In the `Bids` tab, `Try A Listing (Manual Scenario)` lets you enter ask + neighborhood/type/ZIP + DOM/CDOM.
  - The app returns a suggested bid and shows the comp rows used.
- Comp model defaults:
  - sold MLS comps in the last 90 days
  - hierarchical tiers: neighborhood+type, zip+type, city+type
  - likely pre-sold new-build comps excluded by default

## Deployment

- GitHub Pages deploys automatically from `main` via `.github/workflows/deploy-pages.yml`.
- Deployment now runs a quality gate first (`npm run check`).
- Separate CI workflow runs on push/PR via `.github/workflows/ci.yml`.
- Push updates with:

```bash
git push -u origin main
```

## Favicon

- Favicon files live in the repo root:
  - `/Users/evanbarley-greenfield/Documents/Evan Tester Project/favicon.ico`
  - `/Users/evanbarley-greenfield/Documents/Evan Tester Project/favicon.svg`
- Head tags used in `/Users/evanbarley-greenfield/Documents/Evan Tester Project/index.html`:

```html
<link rel="icon" type="image/x-icon" href="./favicon.ico" />
<link rel="icon" type="image/svg+xml" sizes="any" href="./favicon.svg" />
<link rel="shortcut icon" href="./favicon.ico" />
```

## Important Data Caveats

- Public data does **not** provide true original MLS listing timeline fields.
- Some rows use mailing-style address fallbacks if a clean situs proxy is unavailable.
- Rooftop-accurate geo pins require `lat/lon` by parcel. Without `parcel_coords_major_minor.csv`, map pins are approximate.
- Use the `KC Record` link in the app for source-of-truth parcel/sale verification.

## Key Files

- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/index.html` - App UI + logic
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_public_proxy_csv.js` - Public dataset builder
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_parcel_coord_lookup.js` - Normalize GIS export to `major,minor,lat,lon` join file
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/public_sales_proxy_mls_enriched_last12mo.csv` - Default loaded dataset
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_mls_enriched_dataset.js` - MLS merge/enrichment builder
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/validate_data_refresh.js` - Refresh validator + report writer
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/refresh_data_pipeline.js` - End-to-end local refresh orchestrator
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/DATA_SCHEMA.md` - Field notes and normalization behavior
