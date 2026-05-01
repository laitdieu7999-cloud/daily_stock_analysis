# -*- coding: utf-8 -*-
"""Tests for GitHub historical data bootstrap helpers."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.bootstrap_github_history_data import (
    _normalize_cn_code,
    _parse_instrument_code,
    build_focus_coverage_rows,
    locate_qlib_root,
)


class GitHubHistoryBootstrapTestCase(unittest.TestCase):
    def test_normalize_cn_code_handles_a_share_and_etf_prefixes(self) -> None:
        self.assertEqual(_normalize_cn_code("600519"), "sh600519")
        self.assertEqual(_normalize_cn_code("510500"), "sh510500")
        self.assertEqual(_normalize_cn_code("159937"), "sz159937")
        self.assertEqual(_normalize_cn_code("sh000905"), "sh000905")

    def test_parse_instrument_code_handles_tab_and_csv_lines(self) -> None:
        self.assertEqual(_parse_instrument_code("sh600519\t2010-01-01\t2026-04-26"), "sh600519")
        self.assertEqual(_parse_instrument_code("sz159937,2015-01-01,2026-04-26"), "sz159937")
        self.assertIsNone(_parse_instrument_code(""))

    def test_locate_qlib_root_finds_nested_dataset_directory(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            nested = root / "qlib_bin"
            (nested / "features").mkdir(parents=True)
            (nested / "instruments").mkdir(parents=True)
            found = locate_qlib_root(root)
            self.assertEqual(found, nested)

    def test_build_focus_coverage_rows_marks_present_symbols(self) -> None:
        rows = build_focus_coverage_rows(["510500", "600519", "159937"], {"sh510500", "sz159937"})
        self.assertEqual(rows[0]["covered"], "yes")
        self.assertEqual(rows[1]["covered"], "no")
        self.assertEqual(rows[2]["covered"], "yes")
