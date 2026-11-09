---
name: verify
description: Build, launch and drive the Seattle Pending Tracker to verify a change in the running app.
---

# Verify this app

Static Vite app. Everything a user sees is computed in the browser from `public/*.csv` and `public/*.json`.

## Launch

```bash
npm ci                      # if a pinned version is missing on your mirror: npm install --no-save --no-package-lock
npm run build               # validates data, syncs public/, builds dist/
npx vite preview --host 127.0.0.1 --port 4173
```

`npm run build` rewrites the timestamp in `data_refresh_report.json` and re-syncs `public/`. Run
`git checkout -- public/ data_refresh_report.json` before committing from a feature branch.

## Drive

Playwright is a dev dependency. Use the system Chromium if the bundled one is not downloaded:
`chromium.launch({ executablePath: "/usr/bin/chromium", args: ["--no-sandbox"] })`.

- Wait for `#commandCenter .insight-tile` (dataset parsed in a worker, about 2 s). `#supplyLeverage` appears once `listing_ledger.csv` has also loaded, and `#listingOutlook` only when that ledger carries `askHistory` (the committed one may not: run `build_active_ledger.js --ledger <copy> --ref origin/main` after `git fetch --deepen=400 origin main`, and build with the copy in place).
- Tabs: `.tab[data-view="overview|pulse|bids|afford|geo|records|data"]`.
- Filters: click `#filtersToggle`, then `#fType`, `#fMinLot`, `#fMinClose` / `#fMaxClose` (range inputs, dispatch `input`), `#fNeighborhoodOptions input[value="..."]`, `#resetBtn`. The filter form re-renders after every change, so re-open `#fNeighborhood` (a `<details>`) before each checkbox.
- Explain popovers: `[data-explain-trigger="<glossary id>"]` opens `#explain-pop-<id>`.
- Bids: `#bidCards .bid-card`, `[data-bid-view-mode="table"]`, manual scenario `#manualBid*` inputs plus `#manualBidRun`, `#manualBidSource` (load a listing), `#manualBidLadder`. Changing a manual field redraws only `#manualBidStatus`, `#manualBidResult` and `#manualBidCompRows`.
- "What changed" feed: `.since-visit`; its baseline lives in localStorage under `buyer_lens_changes_baselines_v1`. Edit a stored price or delete a key, then reload, to fake a price cut or a new listing.
- Upload: `setInputFiles("#csvFile", ...)`; `#reloadDatasetBtn` goes back to the site dataset. A file named exactly like the site dataset is treated as the site dataset, so rename the copy.
- Theme: `#themeToggle`. Check 390 px width for horizontal overflow (`documentElement.scrollWidth`).

## Pipeline scripts

`fetch_*` and `build_*` need the network or gitignored inputs. These run on tracked files alone and all take
`--enriched / --ledger / --report` paths, so point them at scratch copies:
`build_active_ledger.js`, `merge_redfin_sold.js`, `sold_enrichment_ledger.js`,
`backfill_list_from_active_snapshots.js`, `sanitize_enriched_rows.js`. `merge_redfin_actives.js` needs a
`redfin_active_listings.csv`; an equivalent can be rebuilt from the ledger rows whose `lastSeen` is the newest date.
Run the chain twice and diff the outputs to check it is idempotent.
