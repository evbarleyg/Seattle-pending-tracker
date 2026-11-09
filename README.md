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
- `Bids` tab: active-listing offer guidance from the last 12 months of sold MLS comps, weighted by resemblance, with a suggested bid, win odds, a likely sale range, confidence, and the odds of a price cut or a sale in the next 14 days.
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

5. Sync deploy assets and start the Vite dev server:

```bash
npm run sync:public
npm run dev
```

6. Open:

`http://127.0.0.1:4173`

Preview the production build locally:

```bash
npm run build
npm run preview
```

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
npm run validate:data
npm run sync:public
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

## 2026 Pending Context Export

King County parcel links are for parcel/sale verification only; they do not provide true MLS pending dates or list-at-pending prices. For 2026 sold homes, extract the pending context from the local Realtor/MLS CSV exports:

```bash
npm run pending:2026
```

This writes:

- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/tmp/sold_2026_pending_context.csv`
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/tmp/sold_2026_pending_context_missing.csv`
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/tmp/sold_2026_pending_context_report.json`

Use `npm run scrape:pending2026` as an alias when the workflow is focused on collecting the export-derived pending fields.

## Bid Recommendations (Active MLS)

- Scope: `MLS_ENRICHED` rows where `mlsStatus = Active` and no close price.
- Bid anchor: `List@Pending` (`pendingListPrice`).
- Default strategy: `Balanced`.
- Output per listing:
  - Suggested bid and bid range (one step more cautious, one step more aggressive)
  - Likely sale: the range 8 in 10 similar homes sold inside
  - Confidence score/label: how close the number is likely to be
  - Effective comp count, and whether the weight is mostly nearby or mostly citywide
  - What tends to happen next: odds of a price cut and of a sale within 14 days
- Manual scenario support:
  - In the `Bids` tab the comp finder takes ask + neighborhood/type/ZIP + DOM/CDOM + year built, or loads an active listing.
  - It returns the suggested bid, the price at each win chance from 50% to 90%, and the 12 comps that count most with each one's share of the weight.
- Comp model (`src/domain/bidComps.mjs`): every same-type sale from the last 12 months counts, weighted by resemblance.
  - recency: weight halves every 60 days
  - days on market: week one (0-7 days) is pooled and walled off from everything later; after that a sale counts by how close its days to pending were, on a log scale
  - asking price: by how close the two asks are, on a log scale (the lens's price band does not limit comps)
  - location: same neighborhood or ZIP counts up to 20x; the boost is earned (one nearby sale 3x, eight 10x, fifty 17x), so a lone nearby sale cannot decide a bid
  - build era: homes under 15 years old and older homes inform each other at a twentieth
  - a suggestion needs an effective 6 comparable sales, counted before the location boost; likely pre-sold new-build comps are excluded
  - too few sales near a late listing's age: every sale past week one is used before any week-one sale is
- Confidence starts at 100 and loses 5 points per percentage point between the comps' 20th and 80th percentile ratios, plus penalties for thin, stale or unmatched comps.
- Backtest (1,113 single-family sales listed at $1.1M to $1.6M, Oct 2025 to Nov 2026, each scored with only the sales closed before it went pending):

  | | hard tiers (before) | weighted (now) |
  |---|---|---|
  | quantile loss, mean of 50th/60th/70th | 2.135 | 2.019 |
  | mean miss | 4.57% | 4.28% |
  | win rate at 50 / 60 / 70 (/ 80 / 90) | 52 / 65 / 74 | 51 / 62 / 74 / 83 / 92 |
  | mean miss by confidence, High / Medium / Low | 6.6% / 4.1% / 2.3% (backwards) | 2.4% / 3.5% / 5.9% |
  | range shown contains the sale | 24% (bid range) | 83% (likely sale) |
  | newer builds in week one: win rate, mean overpayment | 89%, $72K | 64%, $25K |

  Slices it was not tuned on improved too: quantile loss down 4.5% ($0.6M to $1.1M houses), 2.8% ($1.6M to $3.0M), 4.5% (townhouses), 2.3% (condos).
  A gradient-boosted model given every available field does no better, so this is close to what the data can say. Tried and dropped for having no effect: ask against a fitted fair value, price per square foot, list weekday, round or charm pricing, size of earlier cuts, trailing market heat.

## Supply And Leverage (Overview)

- `When does leverage flip?`: over / at / under ask by days to pending, next to how many homes for sale now sit in each stage.
- `How much choice is there?`: standing inventory, months of supply, time on market, price cuts, relists and withdrawals, read from `public/listing_ledger.csv`. Relisted homes count once.
- `Is there any hurry?` (`src/domain/listingOutlook.mjs`): of listings at the same point in their life and the same distance from their last cut, how many cut again and how many went under contract within 14 days. Built from the ledger's `askHistory`. Replayed on ten dates (Aug 20 to Oct 22 2026): predicted sales 10.8% against 10.5% actual; listings labelled "Time is on your side" sold 6% of the time and cut 31%, "Likely gone soon" sold 30%. Cuts ranked correctly but ran above prediction (28% against 22%) because autumn sellers cut more than summer ones. A home already on the market when tracking found it has no known cut state until it is seen to cut, and mornings that look ahead into a sampling gap of more than 3 days are left out of the cut odds.

## Data Health Warnings

`npm run validate:data` also checks what the numbers mean, not just the file's shape, and prints warnings that the Data tab repeats under "Worth noting": active listings that nearly all read 0 days on market or have no reading, recent sales losing their asking price or days on market, King County more than 60 days behind, and duplicate closed sales.

## Deployment

- GitHub Pages deploys automatically from `main` via `.github/workflows/deploy-pages.yml`.
- Deployment now runs a quality gate first (`npm run check`), rebuilds the Vite app, and uploads `dist/`.
- Netlify builds with `npm run build` and publishes `dist/`.
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

- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/index.html` - Vite shell
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/src/main.mjs` - App UI, events, targeted rendering, and lazy view mounting
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/src/workers/dataWorker.mjs` - CSV fetch/parse/normalization worker
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/src/domain/` - Data normalization, selectors, Pulse, and buyer-profile logic
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/public/` - Vite public deploy assets copied from generated root outputs
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_public_proxy_csv.js` - Public dataset builder
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_parcel_coord_lookup.js` - Normalize GIS export to `major,minor,lat,lon` join file
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/public_sales_proxy_mls_enriched_last12mo.csv` - Default loaded dataset
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_mls_enriched_dataset.js` - MLS merge/enrichment builder
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/validate_data_refresh.js` - Refresh validator + report writer
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/refresh_data_pipeline.js` - End-to-end local refresh orchestrator
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/DATA_SCHEMA.md` - Field notes and normalization behavior
