#!/bin/zsh
set -euo pipefail

PLIST_PATH="$HOME/Library/LaunchAgents/com.laitdieu.daily-stock-analysis.metaphysical.plist"
LABEL="com.laitdieu.daily-stock-analysis.metaphysical"

mkdir -p "$HOME/Library/LaunchAgents"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/$LABEL"

echo "installed: $PLIST_PATH"
launchctl print "gui/$(id -u)/$LABEL" | sed -n '1,80p'
