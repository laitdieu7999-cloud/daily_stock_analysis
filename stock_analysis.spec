# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.utils.hooks import collect_dynamic_libs

datas = [('/Users/laitdieu/Documents/github/daily_stock_analysis/build/static_snapshot', 'static'), ('strategies', 'strategies')]
binaries = []
datas += collect_data_files('litellm')
datas += collect_data_files('tiktoken')
datas += collect_data_files('akshare')
datas += collect_data_files('py_mini_racer')
binaries += collect_dynamic_libs('py_mini_racer')


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=['multipart', 'multipart.multipart', 'json_repair', 'tiktoken', 'tiktoken_ext', 'tiktoken_ext.openai_public', 'api', 'api.app', 'api.deps', 'api.v1', 'api.v1.router', 'api.v1.endpoints', 'api.v1.endpoints.analysis', 'api.v1.endpoints.history', 'api.v1.endpoints.system_config', 'api.v1.endpoints.data_center', 'api.v1.endpoints.backtest', 'api.v1.endpoints.stocks', 'api.v1.endpoints.health', 'api.v1.schemas', 'api.v1.schemas.analysis', 'api.v1.schemas.history', 'api.v1.schemas.system_config', 'api.v1.schemas.system_overview', 'api.v1.schemas.data_center', 'api.v1.schemas.backtest', 'api.v1.schemas.stocks', 'api.v1.schemas.common', 'api.middlewares', 'api.middlewares.error_handler', 'src.logging_config', 'src.services', 'src.services.task_queue', 'src.services.analysis_service', 'src.services.history_service', 'src.services.system_overview_service', 'src.services.data_center_service', 'src.services.shadow_dashboard_service', 'src.services.market_data_warehouse_service', 'src.services.portfolio_daily_review_service', 'src.services.workstation_cleanup_service', 'src.services.ai_workload_routing_service', 'uvicorn.logging', 'uvicorn.loops', 'uvicorn.loops.auto', 'uvicorn.protocols', 'uvicorn.protocols.http', 'uvicorn.protocols.http.auto', 'uvicorn.protocols.websockets', 'uvicorn.protocols.websockets.auto', 'uvicorn.lifespan', 'uvicorn.lifespan.on'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='stock_analysis',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='stock_analysis',
)
app = BUNDLE(
    coll,
    name='stock_analysis.app',
    icon=None,
    bundle_identifier=None,
)
