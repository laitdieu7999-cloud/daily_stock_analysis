#!/bin/zsh
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "please run with sudo" >&2
  exit 1
fi

SOURCE_PLIST="/Users/laitdieu/Documents/github/daily_stock_analysis/scripts/com.laitdieu.daily-stock-analysis.metaphysical.daemon.plist.template"
TARGET_PLIST="/Library/LaunchDaemons/com.laitdieu.daily-stock-analysis.metaphysical.daemon.plist"
LABEL="com.laitdieu.daily-stock-analysis.metaphysical.daemon"

if [[ ! -f "$SOURCE_PLIST" ]]; then
  echo "missing template: $SOURCE_PLIST" >&2
  exit 1
fi

cp "$SOURCE_PLIST" "$TARGET_PLIST"
chown root:wheel "$TARGET_PLIST"
chmod 644 "$TARGET_PLIST"

launchctl bootout system "$TARGET_PLIST" 2>/dev/null || true
launchctl bootstrap system "$TARGET_PLIST"
launchctl enable "system/$LABEL"

echo "installed: $TARGET_PLIST"
launchctl print "system/$LABEL" | sed -n '1,80p'
