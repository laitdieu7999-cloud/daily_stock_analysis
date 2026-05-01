#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quick checker for Jin10 VIP watch endpoints.

Usage:
    python scripts/check_jin10_vip_watch.py

Reads JIN10_X_TOKEN from the current environment and performs a small
connectivity check against the VIP watch APIs.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.market_data_fetcher import MarketDataFetcher  # noqa: E402


def main() -> int:
    load_dotenv(ROOT / ".env")
    x_token = (os.getenv("JIN10_X_TOKEN") or "").strip()
    if not x_token:
        print("JIN10_X_TOKEN 未设置，无法测试会员盯盘接口。")
        return 1

    fetcher = MarketDataFetcher(
        jin10_api_key=os.getenv("JIN10_API_KEY", ""),
        jin10_x_token=x_token,
    )
    try:
        events = fetcher.list_vip_watch_events(limit=3)
        resonance = fetcher.get_vip_watch_indicator_resonance("000905")
        payload = {
            "events_count": len(events),
            "events_preview": events[:2],
            "resonance": resonance,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    finally:
        fetcher.close()


if __name__ == "__main__":
    raise SystemExit(main())
