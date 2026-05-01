#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run one local market-data warehouse refresh cycle."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config  # noqa: E402
from src.logging_config import setup_logging  # noqa: E402
from src.services.market_data_warehouse_service import MarketDataWarehouseService  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="补齐持仓和自选股票的本地历史行情")
    parser.add_argument("--force", action="store_true", help="强制按完整回看窗口刷新")
    parser.add_argument("--lookback-days", type=int, default=None, help="首次补齐回看天数")
    parser.add_argument("--overlap-days", type=int, default=None, help="已有数据时回刷最近几天")
    parser.add_argument("--max-symbols", type=int, default=None, help="本次最多处理多少只标的")
    parser.add_argument("--symbols", default="", help="只刷新指定代码，逗号分隔")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = get_config()
    setup_logging(log_prefix="market_data_warehouse", debug=False, log_dir=config.log_dir)
    symbols = [code.strip() for code in args.symbols.split(",") if code.strip()] or None
    service = MarketDataWarehouseService()
    try:
        payload = service.run_refresh(
            config=config,
            symbols=symbols,
            lookback_days=args.lookback_days,
            refresh_overlap_days=args.overlap_days,
            max_symbols=args.max_symbols,
            force=args.force,
        )
    finally:
        service.close()

    print(
        json.dumps(
            {
                "status": payload.get("status"),
                "totals": payload.get("totals"),
                "ledger_path": payload.get("ledger_path"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if payload.get("status") in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
