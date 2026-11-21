#!/bin/bash
# Daily refresh run by launchd (com.evbarleyg.seattle-actives, 06:00 local).
# Logs to tmp/refresh_actives_daily.log alongside the project root.
#
# Pipeline: fetch Redfin actives -> merge into enriched -> upsert the actives
# ledger -> sold leg (best effort: fetch 30d -> accumulate -> merge) -> list@pending
# from the ledger -> sync /public -> detect active->pending/sold transitions ->
# build gate -> commit + push the refreshed data to the deploy branch. The push is best-effort: it rebases on
# the remote first and skips cleanly on conflict or offline, leaving the commit
# local for the next run. A failed build never deploys.
#
# Actives gate: fetch:actives exits non-zero and writes nothing when every query
# fails, 0 rows come back, or the count is far below the last good fetch. The
# fetch is retried after a wait (the failures so far were network loss around
# wake from sleep). If it still fails, merge:actives, ledger:upsert and
# detect:transitions are skipped, so the previous REDFIN_ACTIVE rows stay in the
# published data; the sold leg and the rest of the run continue.
#
# REFRESH_PROJECT_DIR and REFRESH_NPM override the defaults for tests.

set -euo pipefail

# launchd runs with a minimal PATH; make node/npm/git resolvable.
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

PROJECT_DIR="${REFRESH_PROJECT_DIR:-/Users/evanbarley-greenfield/repos/seattle-tracker}"
LOG_DIR="$PROJECT_DIR/tmp"
LOG_FILE="$LOG_DIR/refresh_actives_daily.log"
NPM="${REFRESH_NPM:-/usr/local/bin/npm}"
DEPLOY_BRANCH="main"
ACTIVES_FETCH_ATTEMPTS="${ACTIVES_FETCH_ATTEMPTS:-3}"
ACTIVES_RETRY_WAIT_S="${ACTIVES_RETRY_WAIT_S:-300}"

fetch_actives_with_retry() {
  local attempt
  for ((attempt = 1; attempt <= ACTIVES_FETCH_ATTEMPTS; attempt++)); do
    if "$NPM" run fetch:actives; then return 0; fi
    if [ "$attempt" -lt "$ACTIVES_FETCH_ATTEMPTS" ]; then
      echo "fetch:actives failed (attempt ${attempt}/${ACTIVES_FETCH_ATTEMPTS}); retrying in ${ACTIVES_RETRY_WAIT_S}s"
      sleep "$ACTIVES_RETRY_WAIT_S"
    fi
  done
  return 1
}

mkdir -p "$LOG_DIR"
{
  echo ""
  echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') ====="
  cd "$PROJECT_DIR"
  echo "fetch:actives"
  ACTIVES_OK=1
  if fetch_actives_with_retry; then
    echo "merge:actives"
    "$NPM" run merge:actives
    echo "ledger:upsert (actives ledger: first/last seen, first/last ask per MLS#)"
    "$NPM" run ledger:upsert
  else
    ACTIVES_OK=0
    echo "fetch:actives FAILED after ${ACTIVES_FETCH_ATTEMPTS} attempts; keeping the previous REDFIN_ACTIVE rows (merge:actives, ledger:upsert, detect:transitions skipped)"
  fi
  echo "sold leg (best effort): fetch 30d -> accumulate -> merge"
  if "$NPM" run refresh:sold-daily; then
    echo "sold leg ok"
  else
    echo "sold leg FAILED (Redfin/network?); continuing with the existing REDFIN_SOLD rows"
  fi
  echo "enrichment:apply (list@pending from the tracked sold-enrichment ledger)"
  "$NPM" run enrichment:apply
  echo "backfill:snapshots (list@pending for REDFIN_SOLD rows from the actives ledger)"
  "$NPM" run backfill:snapshots
  echo "sanitize:rows (expire stale March open rows; blank fabricated list==pending timelines)"
  "$NPM" run sanitize:rows
  echo "report:sources (per-source fetch times into data_refresh_report.json)"
  "$NPM" run report:sources
  echo "sync:public"
  "$NPM" run sync:public
  if [ "$ACTIVES_OK" = 1 ]; then
    echo "detect:transitions"
    "$NPM" run detect:transitions
  else
    echo "detect:transitions skipped (no new actives snapshot)"
  fi

  echo "build (deploy gate)"
  if "$NPM" run build; then
    # Only commit/push when the checkout is actually on the deploy branch, so
    # a morning run never entangles data with someone's in-progress feature branch.
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [ "$CURRENT_BRANCH" != "$DEPLOY_BRANCH" ]; then
      echo "on ${CURRENT_BRANCH}, not ${DEPLOY_BRANCH}; refreshed data left local (no commit/push)"
      echo "DONE"
      exit 0
    fi
    echo "stage data (surgical: derived data only, not dev/untracked files)"
    git add \
      public/*.csv public/*.json \
      public_sales_proxy_all_prices_last12mo.csv \
      public_sales_proxy_mls_enriched_last12mo.csv \
      data_refresh_report.json \
      redfin_active_ledger.csv redfin_sold_cumulative.csv redfin_sold_enrichment_ledger.csv 2>/dev/null || true
    if git diff --cached --quiet; then
      echo "no data changes to commit"
    else
      COMMIT_MSG="Daily data refresh $(date +%F)"
      [ "$ACTIVES_OK" = 1 ] || COMMIT_MSG="${COMMIT_MSG} (actives fetch failed; previous actives kept)"
      git commit -m "$COMMIT_MSG" || true
      echo "rebase on remote + push to ${DEPLOY_BRANCH} (best effort)"
      if git pull --rebase origin "$DEPLOY_BRANCH" && git push origin "$DEPLOY_BRANCH"; then
        echo "pushed; Pages deploy triggered"
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
