#!/bin/bash
# Refresh the full pipeline: King County source -> public proxy -> MLS-enriched
# -> Redfin actives merge -> bid-up backfill (cached) -> /public sync.
#
# KC requires manual download (no stable programmatic URL). The expected flow:
#   1. Open https://info.kingcounty.gov/assessor/datadownload/default.aspx
#      Click through to "Assessment Mainframe File Extracts" and download
#      these 5 zips into ~/Downloads (or pass --src):
#         EXTR_RPSale.zip
#         EXTR_RPAcct_NoName.zip
#         EXTR_ResBldg.zip
#         EXTR_LookUp.zip
#         EXTR_Parcel.zip      (optional, only needed for parcel coordinates)
#   2. Run this script. It:
#         - moves any matching .zip from --src into the project root, unzips
#           and overwrites the EXTR_*.csv files in place
#         - runs scripts/refresh_data_pipeline.js which rebuilds:
#             public_sales_proxy_all_prices_last12mo.csv  (county base)
#             public_sales_proxy_mls_enriched_last12mo.csv (with realtor MLS)
#         - re-merges Redfin actives back in (npm run merge:actives)
#         - re-applies the bid-up backfill cache (npm run backfill:history)
#         - syncs the result into /public for the dashboard
#
# Why the Redfin re-merge: build_mls_enriched_dataset.js produces a fresh CSV
# from county + realtor sources only. Without re-merging, every refresh would
# wipe REDFIN_ACTIVE rows and REDFIN_HISTORY enrichments. The backfill cache
# means re-application is fast (no fresh property fetches needed).
#
# Usage: scripts/refresh_kc_data.sh [--src ~/Downloads] [--force]
#   --force: skip the no-fresh-zips check; rebuild from existing source CSVs.

set -euo pipefail

PROJECT_DIR="/Users/evanbarley-greenfield/Documents/Evan Tester Project"
SRC_DIR="${HOME}/Downloads"
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC_DIR="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

cd "$PROJECT_DIR"
mkdir -p tmp

KC_FILES=(EXTR_RPSale EXTR_RPAcct_NoName EXTR_ResBldg EXTR_LookUp EXTR_Parcel)

echo "===== 1/5  Checking for fresh King County zips in ${SRC_DIR} ====="
found_any=0
for name in "${KC_FILES[@]}"; do
  zip="${SRC_DIR}/${name}.zip"
  if [[ -f "$zip" ]]; then
    found_any=1
    echo "[$name] found ${zip}"
    mv "$zip" "${PROJECT_DIR}/${name}.zip"
    echo "[$name] unzipping..."
    unzip -o -q "${PROJECT_DIR}/${name}.zip" -d "${PROJECT_DIR}"
    rm "${PROJECT_DIR}/${name}.zip"
  else
    if [[ -f "${PROJECT_DIR}/${name}.csv" ]]; then
      mtime=$(stat -f "%Sm" "${PROJECT_DIR}/${name}.csv")
      echo "[$name] no fresh zip; keeping existing CSV ($mtime)"
    else
      echo "[$name] MISSING (no zip, no existing CSV)"
    fi
  fi
done

if [[ "$found_any" -eq 0 && "$FORCE" -eq 0 ]]; then
  echo ""
  echo "No fresh KC zips in ${SRC_DIR}. Skipping rebuild."
  echo "Download fresh zips from https://info.kingcounty.gov/assessor/datadownload/default.aspx"
  echo "Or pass --force to rebuild from existing source CSVs."
  exit 0
fi

echo ""
echo "===== 2/5  Rebuilding county + MLS-enriched pipeline ====="
node scripts/refresh_data_pipeline.js

# At this point public_sales_proxy_mls_enriched_last12mo.csv has been freshly
# built and contains zero REDFIN_ACTIVE / REDFIN_HISTORY rows. Restore them.

echo ""
echo "===== 3/5  Re-merging Redfin actives ====="
if [[ -f "redfin_active_listings.csv" ]]; then
  node scripts/merge_redfin_actives.js
else
  echo "redfin_active_listings.csv missing — run 'npm run fetch:actives' first."
fi

echo ""
echo "===== 4/5  Re-applying Redfin bid-up backfill (uses cache) ====="
if [[ -f "redfin_history_cache.json" && -f "redfin_url_index.json" ]]; then
  # Re-run on the standard buyer-relevant band; cache makes this fast since
  # property HTML doesn't get re-fetched for already-cached URLs.
  node scripts/backfill_redfin_history.js --min-price 700000 --max-price 2000000
else
  echo "Backfill skipped — cache or URL index missing."
  echo "Bootstrap with: npm run build:url-index && npm run backfill:history"
fi

echo ""
echo "===== 5/5  Syncing public assets ====="
node scripts/sync_public_assets.js

echo ""
echo "===== Done ====="
echo "Updated: public_sales_proxy_mls_enriched_last12mo.csv"
echo "Validation: data_refresh_report.json"
echo "Backfill report: redfin_backfill_report.json"
