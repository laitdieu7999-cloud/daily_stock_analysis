#!/bin/zsh
set -euo pipefail

PROJECT_ROOT="/Users/laitdieu/Documents/github/daily_stock_analysis"
PYTHON_BIN="$PROJECT_ROOT/.venv311/bin/python"
REPORT_FILE="$PROJECT_ROOT/reports/gemini_daily.md"
GOOGLE_DRIVE_SYNC_FILE="${GOOGLE_DRIVE_GEMINI_SYNC_FILE:-}"
LATEST_SYNC_JSON="$PROJECT_ROOT/reports/metaphysical_latest_report_sync.json"

DAILY_DESKTOP_DIR="$HOME/Desktop/玄学治理日报"
DAILY_ARCHIVE_DIR="$PROJECT_ROOT/reports/metaphysical_daily_archive"
WEEKLY_ARCHIVE_DIR="$PROJECT_ROOT/reports/metaphysical_weekly_archive"
ACCURACY_ARCHIVE_DIR="$PROJECT_ROOT/reports/metaphysical_accuracy_archive"

LATEST_SIGNAL_JSON="$PROJECT_ROOT/reports/metaphysical_latest_signal.json"
LATEST_GOVERNANCE_JSON="$PROJECT_ROOT/reports/metaphysical_latest_governance.json"
LATEST_STAGE_HEALTH_JSON="$PROJECT_ROOT/reports/metaphysical_latest_stage_health.json"
LATEST_LOCAL_STORAGE_MIGRATION_JSON="$PROJECT_ROOT/reports/local_storage_latest_migration.json"

mkdir -p "$DAILY_DESKTOP_DIR" "$DAILY_ARCHIVE_DIR" "$WEEKLY_ARCHIVE_DIR" "$ACCURACY_ARCHIVE_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python runtime: $PYTHON_BIN" >&2
  exit 1
fi

TODAY="$(date +%F)"
WEEKDAY="$(date +%u)"

TMP_SYNC="$(mktemp)"
SYNC_ARGS=(
  --target-file "$REPORT_FILE"
  --archive-dir "$PROJECT_ROOT/reports/gemini_daily_archive"
  --expected-report-date "$TODAY"
  --json
)
if [[ -n "$GOOGLE_DRIVE_SYNC_FILE" ]]; then
  SYNC_ARGS=(--source-file "$GOOGLE_DRIVE_SYNC_FILE" "${SYNC_ARGS[@]}")
fi
"$PYTHON_BIN" "$PROJECT_ROOT/scripts/sync_gemini_drive_report.py" \
  "${SYNC_ARGS[@]}" > "$TMP_SYNC" || true
mv "$TMP_SYNC" "$LATEST_SYNC_JSON"

if [[ ! -f "$REPORT_FILE" ]]; then
  echo "missing report file: $REPORT_FILE" >&2
  exit 0
fi

TMP_SIGNAL="$(mktemp)"
"$PYTHON_BIN" "$PROJECT_ROOT/scripts/generate_next_production_signal.py" \
  --tactical-report-file "$REPORT_FILE" \
  --record-snapshot \
  --json > "$TMP_SIGNAL"
mv "$TMP_SIGNAL" "$LATEST_SIGNAL_JSON"

DAILY_REPORT_NAME="${TODAY}_玄学治理日报.md"
DAILY_REPORT_PATH="$DAILY_ARCHIVE_DIR/$DAILY_REPORT_NAME"
"$PYTHON_BIN" "$PROJECT_ROOT/scripts/generate_metaphysical_daily_report.py" \
  --tactical-report-file "$REPORT_FILE" > "$DAILY_REPORT_PATH"
cp "$DAILY_REPORT_PATH" "$DAILY_DESKTOP_DIR/$DAILY_REPORT_NAME"

"$PYTHON_BIN" "$PROJECT_ROOT/scripts/migrate_local_storage_archives.py" \
  --json > "$LATEST_LOCAL_STORAGE_MIGRATION_JSON"

python3 - <<'PY'
from pathlib import Path
desktop_dir = Path.home() / "Desktop" / "玄学治理日报"
reports = sorted(desktop_dir.glob("*_玄学治理日报.md"), key=lambda p: p.stat().st_mtime, reverse=True)
for stale in reports[3:]:
    stale.unlink(missing_ok=True)
PY

if [[ "$WEEKDAY" == "7" ]]; then
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/backfill_metaphysical_learning_outcomes.py"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/train_next_production_metaphysical_example.py" --record-run
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/backtest_next_production_metaphysical_model.py" \
    --record-stage-performance \
    --stage candidate
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/evaluate_metaphysical_promotion.py" \
    --record-governance \
    --record-lifecycle \
    --record-switch-proposal \
    --json > "$LATEST_GOVERNANCE_JSON"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/evaluate_metaphysical_stage_health.py" \
    --stage candidate \
    --json > "$LATEST_STAGE_HEALTH_JSON"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/generate_metaphysical_weekly_summary.py" \
    > "$WEEKLY_ARCHIVE_DIR/${TODAY}_玄学模型周治理摘要.md"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/generate_metaphysical_accuracy_dashboard.py" \
    > "$ACCURACY_ARCHIVE_DIR/${TODAY}_玄学模型命中率看板.md"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/generate_feishu_push_accuracy_dashboard.py" \
    > "$ACCURACY_ARCHIVE_DIR/${TODAY}_飞书推送建议验证看板.md"
fi
