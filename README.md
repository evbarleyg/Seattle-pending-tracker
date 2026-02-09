# Seattle Housing Buyer Lens

Buyer-focused Seattle housing dashboard built from King County public data, with optional MLS-enriched fields when available.

Live site: [https://evbarleyg.github.io/Seattle-pending-tracker/](https://evbarleyg.github.io/Seattle-pending-tracker/)

## What This App Does

- Loads a Seattle sales proxy dataset by default (`public_sales_proxy_all_prices_last6mo.csv`).
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

- `PUBLIC_PROXY`: Built from public county records; useful for close-price/assessed-value analysis.
- `MLS_ENRICHED`: Optional mode when realtor MLS fields are supplied (true list/pending timeline fields).

## Local Run

1. Rebuild the default proxy dataset:

```bash
node scripts/build_public_proxy_csv.js
```

2. Start the local server:

```bash
node scripts/serve.js
```

3. Open:

`http://localhost:4173`

## Deployment

- GitHub Pages deploys automatically from `main` via `.github/workflows/deploy-pages.yml`.
- Push updates with:

```bash
git push -u origin main
```

## Important Data Caveats

- Public data does **not** provide true original MLS listing timeline fields.
- Some rows use mailing-style address fallbacks if a clean situs proxy is unavailable.
- Use the `KC Record` link in the app for source-of-truth parcel/sale verification.

## Key Files

- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/index.html` - App UI + logic
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/scripts/build_public_proxy_csv.js` - Public dataset builder
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/public_sales_proxy_all_prices_last6mo.csv` - Default loaded dataset
- `/Users/evanbarley-greenfield/Documents/Evan Tester Project/DATA_SCHEMA.md` - Field notes and normalization behavior
