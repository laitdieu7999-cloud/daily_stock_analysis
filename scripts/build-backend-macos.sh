#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() {
  echo "$1"
}

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv311/bin/python" ]]; then
    PYTHON_BIN="${ROOT_DIR}/.venv311/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    PYTHON_BIN="python"
  fi
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python not found. Please install Python 3.10+ and retry."
  exit 1
fi

log "Building React UI (static assets)..."
pushd "${ROOT_DIR}/apps/dsa-web" >/dev/null
if [[ ! -d node_modules ]]; then
  npm install
fi
npm run build
popd >/dev/null

log "Verifying static asset references (source)..."
"${PYTHON_BIN}" "${SCRIPT_DIR}/check_static_assets.py" "${ROOT_DIR}/static"

log "Building backend executable..."
if ! "${PYTHON_BIN}" -m PyInstaller --version >/dev/null 2>&1; then
  "${PYTHON_BIN}" -m pip install pyinstaller
fi

log "Installing backend dependencies..."
"${PYTHON_BIN}" -m pip install -r "${ROOT_DIR}/requirements.txt"

log "Checking python-multipart availability..."
"${PYTHON_BIN}" -c "import multipart, multipart.multipart"

if [[ -d "${ROOT_DIR}/dist/backend" ]]; then
  rm -rf "${ROOT_DIR}/dist/backend"
fi
mkdir -p "${ROOT_DIR}/dist/backend"

if [[ -d "${ROOT_DIR}/dist/stock_analysis" ]]; then
  rm -rf "${ROOT_DIR}/dist/stock_analysis"
fi

if [[ -d "${ROOT_DIR}/build/stock_analysis" ]]; then
  rm -rf "${ROOT_DIR}/build/stock_analysis"
fi

log "Creating stable static asset snapshot..."
STATIC_SNAPSHOT="${ROOT_DIR}/build/static_snapshot"
rm -rf "${STATIC_SNAPSHOT}"
mkdir -p "$(dirname "${STATIC_SNAPSHOT}")"
cp -R "${ROOT_DIR}/static" "${STATIC_SNAPSHOT}"

hidden_imports=(
  "multipart"
  "multipart.multipart"
  "json_repair"
  "tiktoken"
  "tiktoken_ext"
  "tiktoken_ext.openai_public"
  "api"
  "api.app"
  "api.deps"
  "api.v1"
  "api.v1.router"
  "api.v1.endpoints"
  "api.v1.endpoints.analysis"
  "api.v1.endpoints.history"
  "api.v1.endpoints.system_config"
  "api.v1.endpoints.data_center"
  "api.v1.endpoints.backtest"
  "api.v1.endpoints.stocks"
  "api.v1.endpoints.health"
  "api.v1.schemas"
  "api.v1.schemas.analysis"
  "api.v1.schemas.history"
  "api.v1.schemas.system_config"
  "api.v1.schemas.system_overview"
  "api.v1.schemas.data_center"
  "api.v1.schemas.backtest"
  "api.v1.schemas.stocks"
  "api.v1.schemas.common"
  "api.middlewares"
  "api.middlewares.error_handler"
  "src.logging_config"
  "src.services"
  "src.services.task_queue"
  "src.services.analysis_service"
  "src.services.history_service"
  "src.services.system_overview_service"
  "src.services.data_center_service"
  "src.services.shadow_dashboard_service"
  "src.services.market_data_warehouse_service"
  "src.services.portfolio_daily_review_service"
  "src.services.workstation_cleanup_service"
  "src.services.ai_workload_routing_service"
  "uvicorn.logging"
  "uvicorn.loops"
  "uvicorn.loops.auto"
  "uvicorn.protocols"
  "uvicorn.protocols.http"
  "uvicorn.protocols.http.auto"
  "uvicorn.protocols.websockets"
  "uvicorn.protocols.websockets.auto"
  "uvicorn.lifespan"
  "uvicorn.lifespan.on"
)

hidden_import_args=()
for module in "${hidden_imports[@]}"; do
  hidden_import_args+=("--hidden-import=${module}")
done

pushd "${ROOT_DIR}" >/dev/null
cmd=("${PYTHON_BIN}" -m PyInstaller --name stock_analysis --onedir --noconfirm --noconsole --add-data "${STATIC_SNAPSHOT}:static" --add-data "strategies:strategies" --collect-data litellm --collect-data tiktoken --collect-data akshare --collect-data py_mini_racer --collect-binaries py_mini_racer)
cmd+=("${hidden_import_args[@]}" "main.py")

echo "Running: ${cmd[*]}"
"${cmd[@]}"
popd >/dev/null

cp -R "${ROOT_DIR}/dist/stock_analysis" "${ROOT_DIR}/dist/backend/stock_analysis"

log "Verifying static asset references (packaged)..."
packaged_static="${ROOT_DIR}/dist/backend/stock_analysis/_internal/static"
if [[ ! -d "${packaged_static}" ]]; then
  packaged_static="${ROOT_DIR}/dist/backend/stock_analysis/static"
fi
if [[ -d "${packaged_static}" ]]; then
  "${PYTHON_BIN}" "${SCRIPT_DIR}/check_static_assets.py" "${packaged_static}"
else
  log "WARNING: could not locate packaged static directory under dist/backend/stock_analysis; skipping post-package check."
fi

log "Backend build completed."
