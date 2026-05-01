# -*- coding: utf-8 -*-
"""Tests for Shadow dashboard file reader."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.services.shadow_dashboard_service import ShadowDashboardService


class ShadowDashboardServiceTestCase(unittest.TestCase):
    def test_get_dashboard_reads_scorecard_and_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scorecard = {
                "generated_at": "2026-04-30T16:20:00",
                "primary_window": 5,
                "min_samples": 50,
                "daily_meta": {"code_count": 2},
                "graduation_scorecard": {
                    "rows": [
                        {
                            "module": "日线技术信号",
                            "direction_type": "offensive",
                            "rule": "VWAP成本线(看多)",
                            "sample_count": 100,
                            "final_decision": "可进Shadow",
                        },
                        {
                            "module": "日线技术信号",
                            "direction_type": "defensive",
                            "rule": "趋势破坏",
                            "sample_count": 80,
                            "final_decision": "只作风险提示",
                        },
                    ]
                },
                "symbol_attribution": {
                    "summary": [
                        {
                            "module": "日线技术信号",
                            "direction_type": "offensive",
                            "rule": "VWAP成本线(看多)",
                            "top3_contribution_pct": 21.0,
                            "concentration_status": "分散",
                        }
                    ]
                },
            }
            (root / "2026-04-30_theory_signal_scorecard.json").write_text(
                json.dumps(scorecard, ensure_ascii=False),
                encoding="utf-8",
            )
            (root / "stock_signal_shadow_ledger.jsonl").write_text(
                json.dumps(
                    {
                        "signal_date": "2026-04-29",
                        "code": "600519",
                        "rule": "VWAP成本线(看多)",
                        "status": "open",
                        "entry_price": 100.0,
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            reports_dir = root / "reports"
            reports_dir.mkdir()
            (reports_dir / "stock_intraday_replay_ledger.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "signal_id": "buy-1",
                                "trigger_timestamp": "2026-04-29T14:35:00+08:00",
                                "code": "600519",
                                "name": "贵州茅台",
                                "scope": "watchlist_buy",
                                "signal_type": "BUY_SETUP",
                                "current_price": 100.0,
                                "forward_labels": {
                                    "t_plus_1": {"return_pct": 1.0},
                                    "t_plus_3": {"return_pct": 2.0},
                                    "t_plus_5": {"return_pct": 3.0},
                                },
                                "outcome_reference_window": {
                                    "outcome_max_favorable_1d": 4.0,
                                    "outcome_max_adverse_1d": -1.0,
                                },
                            },
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            {
                                "signal_id": "risk-1",
                                "trigger_timestamp": "2026-04-28T10:00:00+08:00",
                                "code": "300251",
                                "name": "光线传媒",
                                "scope": "holding_risk",
                                "signal_type": "RISK_STOP",
                                "current_price": 20.0,
                                "forward_labels": {
                                    "t_plus_1": {"return_pct": -1.0},
                                    "t_plus_3": {"return_pct": -2.0},
                                    "t_plus_5": {"return_pct": -5.0},
                                },
                            },
                            ensure_ascii=False,
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            payload = ShadowDashboardService(root, project_root=root).get_dashboard(limit=10)

            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["scorecard"]["primary_window"], 5)
            self.assertEqual(len(payload["scorecard"]["candidates"]), 1)
            self.assertEqual(
                payload["scorecard"]["candidates"][0]["symbol_attribution"]["concentration_status"],
                "分散",
            )
            self.assertEqual(payload["ledger"]["total_count"], 1)
            self.assertEqual(payload["ledger"]["open_count"], 1)
            self.assertEqual(payload["ledger"]["rule_counts"], [{"rule": "VWAP成本线(看多)", "count": 1}])
            self.assertEqual(payload["intraday_replay"]["total_count"], 2)
            self.assertEqual(payload["intraday_replay"]["labeled_count"], 2)
            self.assertEqual(payload["intraday_replay"]["effective_rate_pct"], 100.0)
            self.assertEqual(payload["intraday_replay"]["avg_primary_return_pct"], -1.0)
            self.assertEqual(len(payload["intraday_replay"]["signal_type_counts"]), 2)

    def test_get_dashboard_handles_missing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            payload = ShadowDashboardService(Path(tmpdir)).get_dashboard()

            self.assertEqual(payload["status"], "missing")
            self.assertEqual(payload["scorecard"]["status"], "missing")
            self.assertEqual(payload["ledger"]["status"], "missing")

    def test_default_backtest_dir_prefers_candidate_with_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            packaged_dir = root / "packaged" / "reports" / "backtests"
            user_reports_dir = root / "Reports" / "projects" / "daily_stock_analysis" / "backtests"
            packaged_dir.mkdir(parents=True)
            user_reports_dir.mkdir(parents=True)
            (user_reports_dir / "stock_signal_shadow_ledger.jsonl").write_text(
                json.dumps(
                    {
                        "signal_date": "2026-04-30",
                        "code": "600519",
                        "rule": "VWAP成本线(看多)",
                        "status": "open",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            with patch(
                "src.services.shadow_dashboard_service._default_backtest_candidates",
                return_value=[packaged_dir, user_reports_dir],
            ):
                payload = ShadowDashboardService().get_dashboard()

            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["backtest_dir"], str(user_reports_dir.resolve()))
            self.assertEqual(payload["ledger"]["total_count"], 1)


if __name__ == "__main__":
    unittest.main()
