#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

import pandas as pd

from scripts.run_ic_option_term_combo_validation import _compute_combo_thresholds, _search_combo_rules


class ICOptionTermComboValidationTests(unittest.TestCase):
    def test_search_combo_rules_includes_front_end_plus_qvix_combo(self) -> None:
        frame = pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=8, freq="D"),
                "spot_close": [100, 99, 98, 97, 96, 95, 94, 93],
                "spot_ret_1d_fwd": [-0.01, -0.01, -0.01, -0.01, -0.01, -0.01, -0.01, None],
                "spot_ret_3d_fwd": [-0.03, -0.03, -0.03, -0.03, -0.03, None, None, None],
                "qvix_known": [20, 21, 22, 30, 31, 20, 19, 18],
                "qvix_z": [0.2, 0.5, 0.8, 1.5, 1.6, 0.3, 0.2, 0.1],
                "qvix_jump_pct_hist": [0.0, 2.0, 3.0, 9.0, 8.5, 1.0, 1.0, 1.0],
                "front_end_gap": [0.01, 0.02, 0.01, 0.06, 0.07, 0.01, 0.00, 0.00],
                "q1_q2_annualized": [-0.06, -0.06, -0.06, -0.12, -0.13, -0.06, -0.06, -0.06],
            }
        )
        thresholds = _compute_combo_thresholds(frame)
        rows = _search_combo_rules(frame, thresholds)
        labels = {row["rule"] for row in rows}
        self.assertIn("前端塌陷 + QVIX共振", labels)


if __name__ == "__main__":
    unittest.main()
