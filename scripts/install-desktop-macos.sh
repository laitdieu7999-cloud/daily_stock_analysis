#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

APP_NAME="股票分析桌面端.app"
TARGET="/Applications/${APP_NAME}"
SOURCE="${1:-}"

if [[ -z "${SOURCE}" ]]; then
  SOURCE="$(find "${ROOT_DIR}/apps/dsa-desktop/dist" -maxdepth 3 -type d -name 'daily-stock-analysis-desktop.app' | head -n 1)"
fi

if [[ -z "${SOURCE}" || ! -d "${SOURCE}" ]]; then
  echo "Desktop app bundle not found. Run scripts/build-all-macos.sh first, or pass the app bundle path." >&2
  exit 1
fi

echo "Installing desktop app:"
echo "  source: ${SOURCE}"
echo "  target: ${TARGET}"

osascript -e 'tell application "股票分析桌面端" to quit' >/dev/null 2>&1 || true
sleep 2

tmp_parent="$(mktemp -d "/tmp/dsa-desktop-install.XXXXXX")"
tmp_backup="${tmp_parent}/${APP_NAME}.previous"

cleanup_tmp() {
  rm -rf "${tmp_parent}"
}
trap cleanup_tmp EXIT

if [[ -d "${TARGET}" ]]; then
  mv "${TARGET}" "${tmp_backup}"
fi

if ! ditto "${SOURCE}" "${TARGET}"; then
  status=$?
  rm -rf "${TARGET}"
  if [[ -d "${tmp_backup}" ]]; then
    mv "${tmp_backup}" "${TARGET}"
  fi
  echo "Install failed; previous app restored." >&2
  exit "${status}"
fi

# Old manual deploy commands used visible /Applications/*.backup-* folders.
# They confuse Finder/Launchpad as duplicate apps, so keep /Applications clean.
find /Applications -maxdepth 1 -name "${APP_NAME}.backup-*" -exec rm -rf {} +
find /Applications -maxdepth 1 -name "${APP_NAME}.broken-*" -exec rm -rf {} +
find /Applications -maxdepth 1 -name "${APP_NAME}.pre-*" -exec rm -rf {} +

echo "Installed successfully. No visible backup app was left in /Applications."
