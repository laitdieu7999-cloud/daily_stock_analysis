#!/bin/zsh
set -euo pipefail

LABEL="com.laitdieu.daily-stock-analysis.stock-intraday"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true
rm -f "$PLIST_PATH"

echo "uninstalled: $PLIST_PATH"
