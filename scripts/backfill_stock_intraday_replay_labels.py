#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill T+1/T+3/T+5 labels for the stock intraday replay ledger."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.stock_intraday_replay_labeler import (  # noqa: E402
    DEFAULT_REPLAY_LEDGER_PATH,
    StockIntradayReplayLabeler,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill intraday replay ledger forward labels")
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_REPLAY_LEDGER_PATH)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = StockIntradayReplayLabeler(ledger_path=args.ledger_path).run(dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
