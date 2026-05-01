# -*- coding: utf-8 -*-
"""Tests for lightweight local Qlib history reader."""

from __future__ import annotations

import struct
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from src.services.qlib_local_history import (
    find_latest_bootstrapped_qlib_root,
    load_qlib_daily_ohlcv,
    normalize_cn_symbol,
)


def _write_day_bin(path: Path, start_index: int, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(struct.pack("<f", float(start_index)))
        handle.write(np.asarray(values, dtype="<f4").tobytes())


class QlibLocalHistoryTestCase(unittest.TestCase):
    def test_normalize_cn_symbol_handles_index_and_etf(self) -> None:
        self.assertEqual(normalize_cn_symbol("600519"), "sh600519")
        self.assertEqual(normalize_cn_symbol("159937"), "sz159937")
        self.assertEqual(normalize_cn_symbol("sh000905"), "sh000905")

    def test_find_latest_bootstrapped_qlib_root_prefers_latest_tag(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for tag in ["2026-04-25", "2026-04-26"]:
                qlib_root = root / tag / "extracted" / "qlib_bin"
                (qlib_root / "features").mkdir(parents=True)
                (qlib_root / "instruments").mkdir(parents=True)
            found = find_latest_bootstrapped_qlib_root(root)
            self.assertEqual(found.resolve(), (root / "2026-04-26" / "extracted" / "qlib_bin").resolve())

    def test_load_qlib_daily_ohlcv_reads_simple_day_bins(self) -> None:
        with TemporaryDirectory() as tmp:
            qlib_root = Path(tmp) / "qlib_bin"
            (qlib_root / "calendars").mkdir(parents=True)
            (qlib_root / "calendars" / "day.txt").write_text(
                "2026-04-01\n2026-04-02\n2026-04-03\n",
                encoding="utf-8",
            )
            for field, values in {
                "open": [10.0, 11.0],
                "high": [12.0, 13.0],
                "low": [9.0, 10.0],
                "close": [11.0, 12.0],
                "volume": [1000.0, 1100.0],
            }.items():
                _write_day_bin(qlib_root / "features" / "sh600519" / f"{field}.day.bin", 1, values)

            frame = load_qlib_daily_ohlcv("600519", qlib_root)

            self.assertIsNotNone(frame)
            self.assertEqual(len(frame), 2)
            self.assertEqual(frame.loc[0, "date"].strftime("%Y-%m-%d"), "2026-04-02")
            self.assertAlmostEqual(frame.loc[1, "close"], 12.0)
