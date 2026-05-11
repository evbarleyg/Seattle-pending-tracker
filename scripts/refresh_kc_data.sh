#!/bin/bash
# Refresh King County source data, then rebuild the public proxy + MLS-enriched
# datasets and sync to /public.
#
# KC requires manual download (no stable programmatic URL). The expected flow:
#   1. Open https://info.kingcounty.gov/assessor/datadownload/default.aspx in
#      your browser. Click through to "Assessment Mainframe File Extracts" and
#      download these 5 zips into ~/Downloads (or pass a different directory):
#         EXTR_RPSale.zip
#         EXTR_RPAcct_NoName.zip
#         EXTR_ResBldg.zip
#         EXTR_LookUp.zip
#         EXTR_Parcel.zip      (optional, large — only needed when adding
#                               parcel-level coordinates / boundaries)
#   2. Run this script. It:
#         - moves any matching .zip from --src into the project root
#         - unzips and overwrites the EXTR_*.csv files in place
#         - runs scripts/refresh_data_pipeline.js (build_public_proxy_csv,
#           build_mls_enriched_dataset, validate_data_refresh)
#         - syncs the result into /public for the dashboard
#
# Usage: scripts/refresh_kc_data.sh [--src ~/Downloads]

set -euo pipefail

PROJECT_DIR="/Users/evanbarley-greenfield/Documents/Evan Tester Project"
SRC_DIR="${HOME}/Downloads"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src) SRC_DIR="$2"; shift 2 ;;
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
      echo "[$name] no fresh zip in ${SRC_DIR}; keeping existing CSV ($mtime)"
    else
      echo "[$name] MISSING (not in ${SRC_DIR}, no existing CSV in project root)"
    fi
  fi
done

if [[ "$found_any" -eq 0 ]]; then
  echo ""
  echo "No fresh KC zip files found in ${SRC_DIR}."
  echo "Download them from https://info.kingcounty.gov/assessor/datadownload/default.aspx"
  echo "  -> click 'Assessment Mainframe File Extracts' link"
  echo "Then re-run this script."
  exit 0
fi

echo ""
echo "===== Rebuilding pipeline ====="
node scripts/refresh_data_pipeline.js

echo ""
echo "===== Syncing public assets ====="
node scripts/sync_public_assets.js

echo ""
echo "===== Done. ====="
echo "Updated CSV: public_sales_proxy_mls_enriched_last12mo.csv"
echo "Validation:  data_refresh_report.json"
echo ""
echo "Next: run 'npm run backfill:history' if you want to fill bid-up on"
echo "any newly-arrived closed sales that aren't in the realtor MLS export."
