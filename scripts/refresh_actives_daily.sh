#!/bin/bash
# Daily refresh run by launchd (com.evbarleyg.seattle-actives, 06:00 local).
# Logs to tmp/refresh_actives_daily.log alongside the project root.
#
# Pipeline: fetch Redfin actives -> merge into enriched -> sync /public ->
# detect active->pending/sold transitions -> build gate -> commit + push the
# refreshed data to the deploy branch. The push is best-effort: it rebases on
# the remote first and skips cleanly on conflict or offline, leaving the commit
# local for the next run. A failed build never deploys.

set -euo pipefail

# launchd runs with a minimal PATH; make node/npm/git resolvable.
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

PROJECT_DIR="/Users/evanbarley-greenfield/Documents/Evan Tester Project"
LOG_DIR="$PROJECT_DIR/tmp"
LOG_FILE="$LOG_DIR/refresh_actives_daily.log"
NPM=/usr/local/bin/npm
DEPLOY_BRANCH="codex/staging-buyer-profile-toggle"

mkdir -p "$LOG_DIR"
{
  echo ""
  echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') ====="
  cd "$PROJECT_DIR"
  echo "fetch:actives"
  "$NPM" run fetch:actives
  echo "merge:actives"
  "$NPM" run merge:actives
  echo "sync:public"
  "$NPM" run sync:public
  echo "detect:transitions"
  "$NPM" run detect:transitions

  echo "build (deploy gate)"
  if "$NPM" run build; then
    echo "stage data (surgical: derived data only, not dev/untracked files)"
    git add \
      public/*.csv public/*.json \
      public_sales_proxy_all_prices_last12mo.csv \
      public_sales_proxy_mls_enriched_last12mo.csv \
      data_refresh_report.json 2>/dev/null || true
    if git diff --cached --quiet; then
      echo "no data changes to commit"
    else
      git commit -m "Daily data refresh $(date +%F)" || true
      echo "rebase on remote + push to ${DEPLOY_BRANCH} (best effort)"
      if git pull --rebase origin "$DEPLOY_BRANCH" && git push origin "$DEPLOY_BRANCH"; then
        echo "pushed; preview Pages deploy triggered"
      else
        echo "push skipped (conflict/offline); commit kept local for next run"
        git rebase --abort 2>/dev/null || true
      fi
    fi
  else
    echo "build FAILED; skipping commit/push (refreshed data left local)"
  fi
  echo "DONE"
} >> "$LOG_FILE" 2>&1
