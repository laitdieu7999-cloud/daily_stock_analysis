#!/bin/zsh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv311/bin/python"
LABEL="com.laitdieu.daily-stock-analysis.stock-intraday"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/Library/Application Support/Daily Stock Analysis/logs"
INTERVAL_SECONDS="${1:-60}"

if [[ ! "$INTERVAL_SECONDS" =~ '^[0-9]+$' ]] || [[ "$INTERVAL_SECONDS" -lt 30 ]]; then
  echo "StartInterval must be an integer >= 30 seconds" >&2
  exit 2
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="/usr/bin/python3"
fi

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR" "$PROJECT_ROOT/reports"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>

  <key>ProgramArguments</key>
  <array>
    <string>$PYTHON_BIN</string>
    <string>$PROJECT_ROOT/scripts/run_stock_intraday_reminder.py</string>
  </array>

  <key>WorkingDirectory</key>
  <string>$PROJECT_ROOT</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>PYTHONUTF8</key>
    <string>1</string>
    <key>PYTHONIOENCODING</key>
    <string>utf-8</string>
    <key>DSA_PROJECT_ROOT</key>
    <string>$PROJECT_ROOT</string>
  </dict>

  <key>RunAtLoad</key>
  <true/>

  <key>StartInterval</key>
  <integer>$INTERVAL_SECONDS</integer>

  <key>ProcessType</key>
  <string>Background</string>

  <key>StandardOutPath</key>
  <string>$LOG_DIR/stock-intraday.stdout.log</string>

  <key>StandardErrorPath</key>
  <string>$LOG_DIR/stock-intraday.stderr.log</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/$LABEL"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "installed: $PLIST_PATH"
echo "interval_seconds: $INTERVAL_SECONDS"
launchctl print "gui/$(id -u)/$LABEL" | sed -n '1,80p'
