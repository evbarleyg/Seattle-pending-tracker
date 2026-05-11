#!/bin/bash
# Daily refresh script run by launchd. Logs to tmp/refresh_actives_daily.log
# alongside the project root.

set -euo pipefail

PROJECT_DIR="/Users/evanbarley-greenfield/Documents/Evan Tester Project"
LOG_DIR="$PROJECT_DIR/tmp"
LOG_FILE="$LOG_DIR/refresh_actives_daily.log"
NPM=/usr/local/bin/npm

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
  echo "DONE"
} >> "$LOG_FILE" 2>&1
