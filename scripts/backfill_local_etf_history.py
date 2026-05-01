#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill local ETF daily history cache using AkShare."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from src.services.local_etf_history import (
    DEFAULT_LOCAL_ETF_HISTORY_DIR,
    normalize_cn_etf_symbol,
    save_cached_etf_daily_ohlcv,
)
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "backtests"
DEFAULT_SYMBOLS = [
    "510300",
    "510500",
    "512980",
    "159201",
    "159326",
    "159613",
    "159869",
    "159937",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill local ETF daily history cache.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated ETF codes.")
    parser.add_argument("--cache-dir", default=str(DEFAULT_LOCAL_ETF_HISTORY_DIR), help="Local ETF cache directory.")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="Directory for the markdown backfill report.")
    return parser


def _parse_symbols(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _fetch_etf_history(symbol: str) -> pd.DataFrame:
    import akshare as ak

    normalized = normalize_cn_etf_symbol(symbol)
    frame = ak.fund_etf_hist_sina(symbol=normalized)
    if frame is None or frame.empty:
        raise RuntimeError(f"未获取到 {normalized} 的 ETF 日线")
    frame["date"] = pd.to_datetime(frame["date"])
    return frame[["date", "open", "close", "high", "low", "volume"]].sort_values("date").reset_index(drop=True)


def _render_report(rows: Iterable[dict[str, str]]) -> str:
    lines = [
        f"# {datetime.now().strftime('%Y-%m-%d')} ETF本地历史回填报告",
        "",
        "| ETF代码 | 归一化代码 | 行数 | 起始日期 | 结束日期 | 本地文件 | 状态 |",
        "| --- | --- | ---: | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['input_code']} | {row['normalized_code']} | {row['rows']} | {row['start_date']} | {row['end_date']} | {row['path']} | {row['status']} |"
        )
    lines.extend(
        [
            "",
            "## 结论",
            "- 这些 ETF 日线会作为本地离线缓存仓，优先给回测脚本使用。",
            "- 这样后续 510500 / 159937 等 ETF 线路不必完全依赖在线源。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = _build_parser().parse_args()
    cache_dir = Path(args.cache_dir).expanduser().resolve()
    report_dir = Path(args.report_dir).expanduser().resolve()
    symbols = _parse_symbols(args.symbols)

    rows: list[dict[str, str]] = []
    for symbol in symbols:
        normalized = normalize_cn_etf_symbol(symbol)
        try:
            frame = _fetch_etf_history(normalized)
            path = save_cached_etf_daily_ohlcv(normalized, frame, cache_dir)
            rows.append(
                {
                    "input_code": symbol,
                    "normalized_code": normalized,
                    "rows": str(len(frame)),
                    "start_date": frame["date"].iloc[0].strftime("%Y-%m-%d"),
                    "end_date": frame["date"].iloc[-1].strftime("%Y-%m-%d"),
                    "path": str(path),
                    "status": "成功",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "input_code": symbol,
                    "normalized_code": normalized,
                    "rows": "0",
                    "start_date": "-",
                    "end_date": "-",
                    "path": "-",
                    "status": f"失败: {exc}",
                }
            )

    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{datetime.now().date().isoformat()}_ETF本地历史回填报告.md"
    report_path.write_text(_render_report(rows), encoding="utf-8")
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
