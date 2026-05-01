#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from scripts import backtest_next_production_metaphysical_model as backtest_script
from scripts import train_next_production_metaphysical_example as train_script
from src.models.metaphysical import NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES


class NextProductionOfflineCandidateMergeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-24", "2026-04-25"]),
                "open": [1.0, 1.1],
                "close": [1.0, 1.2],
                "high": [1.1, 1.25],
                "low": [0.95, 1.05],
                "volume": [100, 120],
                "quant_return_1d": [0.0, 0.2],
                "target_extreme_volatility": [0, 1],
            }
        )
        self.candidate_slice = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-24", "2026-04-25"]),
                "quant_return_1d": [9.0, 9.0],
                "bb_width_ratio": [0.1, 0.2],
                "weighted_volatility_plus_score": [0.3, 0.4],
            }
        )

    @patch.object(backtest_script, "build_next_production_backtest_features")
    def test_backtest_candidate_frame_deduplicates_and_fills_missing_features(self, mock_builder) -> None:
        mock_builder.return_value = self.candidate_slice
        merged = backtest_script._build_candidate_frame(self.base_frame, cache_dir=".cache/xgb_cache")

        self.assertEqual(merged.columns.tolist().count("quant_return_1d"), 1)
        self.assertListEqual(merged["quant_return_1d"].tolist(), [0.0, 0.2])
        self.assertListEqual(merged["bb_width_ratio"].tolist(), [0.1, 0.2])
        self.assertListEqual(merged["weighted_volatility_plus_score"].tolist(), [0.3, 0.4])
        for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
            self.assertIn(feature, merged.columns)

    @patch.object(train_script, "build_next_production_backtest_features")
    def test_training_frame_deduplicates_and_fills_missing_features(self, mock_builder) -> None:
        mock_builder.return_value = self.candidate_slice
        merged = train_script._build_training_frame(self.base_frame, cache_dir=".cache/xgb_cache")

        self.assertEqual(merged.columns.tolist().count("quant_return_1d"), 1)
        self.assertListEqual(merged["quant_return_1d"].tolist(), [0.0, 0.2])
        self.assertListEqual(merged["bb_width_ratio"].tolist(), [0.1, 0.2])
        self.assertListEqual(merged["weighted_volatility_plus_score"].tolist(), [0.3, 0.4])
        for feature in NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES:
            self.assertIn(feature, merged.columns)


if __name__ == "__main__":
    unittest.main()
