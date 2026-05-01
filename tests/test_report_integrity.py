# -*- coding: utf-8 -*-
"""
===================================
Report Engine - Content integrity tests
===================================

Tests for check_content_integrity, apply_placeholder_fill, and retry/placeholder behavior.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.analyzer import AnalysisResult, GeminiAnalyzer, check_content_integrity, apply_placeholder_fill


class TestCheckContentIntegrity(unittest.TestCase):
    """Content integrity check tests."""

    def test_pass_when_all_required_present(self) -> None:
        """Integrity passes when all mandatory fields are present."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="稳健",
            decision_type="hold",
            dashboard={
                "core_conclusion": {"one_sentence": "持有观望"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "112元",
                        "secondary_buy": "110元",
                        "stop_loss": "108元",
                        "take_profit": "120元",
                    }
                },
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertTrue(ok)
        self.assertEqual(missing, [])

    def test_fail_when_analysis_summary_empty(self) -> None:
        """Integrity fails when analysis_summary is empty."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="",
            decision_type="hold",
            dashboard={
                "core_conclusion": {"one_sentence": "持有"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "112",
                        "secondary_buy": "110",
                        "stop_loss": "108",
                        "take_profit": "120",
                    }
                },
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertFalse(ok)
        self.assertIn("analysis_summary", missing)

    def test_fail_when_one_sentence_missing(self) -> None:
        """Integrity fails when core_conclusion.one_sentence is missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="稳健",
            decision_type="hold",
            dashboard={
                "core_conclusion": {},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "112",
                        "secondary_buy": "110",
                        "stop_loss": "108",
                        "take_profit": "120",
                    }
                },
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertFalse(ok)
        self.assertIn("dashboard.core_conclusion.one_sentence", missing)

    def test_fail_when_stop_loss_missing_for_buy(self) -> None:
        """Integrity fails when sniper points are missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="买入",
            analysis_summary="稳健",
            decision_type="buy",
            dashboard={
                "core_conclusion": {"one_sentence": "可买入"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {"sniper_points": {}},
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertFalse(ok)
        self.assertIn("dashboard.battle_plan.sniper_points.ideal_buy", missing)
        self.assertIn("dashboard.battle_plan.sniper_points.secondary_buy", missing)
        self.assertIn("dashboard.battle_plan.sniper_points.stop_loss", missing)
        self.assertIn("dashboard.battle_plan.sniper_points.take_profit", missing)

    def test_fail_when_sniper_points_missing_for_sell(self) -> None:
        """Integrity still requires visible sniper/waiting levels for sell reports."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看空",
            sentiment_score=35,
            operation_advice="卖出",
            analysis_summary="弱势",
            decision_type="sell",
            dashboard={
                "core_conclusion": {"one_sentence": "建议卖出"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {"sniper_points": {}},
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertFalse(ok)
        self.assertIn("dashboard.battle_plan.sniper_points.stop_loss", missing)

    def test_fail_when_risk_alerts_missing(self) -> None:
        """Integrity fails when intelligence.risk_alerts field is missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="稳健",
            decision_type="hold",
            dashboard={
                "core_conclusion": {"one_sentence": "持有"},
                "intelligence": {},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "112",
                        "secondary_buy": "110",
                        "stop_loss": "108",
                        "take_profit": "120",
                    }
                },
            },
        )
        ok, missing = check_content_integrity(result)
        self.assertFalse(ok)
        self.assertIn("dashboard.intelligence.risk_alerts", missing)


class TestApplyPlaceholderFill(unittest.TestCase):
    """Placeholder fill tests."""

    def test_fills_missing_analysis_summary(self) -> None:
        """Placeholder fills analysis_summary when missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="",
            decision_type="hold",
            dashboard={},
        )
        apply_placeholder_fill(result, ["analysis_summary"])
        self.assertEqual(result.analysis_summary, "待补充")

    def test_fills_missing_stop_loss(self) -> None:
        """Placeholder fills stop_loss when missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="买入",
            analysis_summary="稳健",
            decision_type="buy",
            dashboard={"battle_plan": {"sniper_points": {}}},
        )
        apply_placeholder_fill(result, ["dashboard.battle_plan.sniper_points.stop_loss"])
        self.assertEqual(
            result.dashboard["battle_plan"]["sniper_points"]["stop_loss"],
            "待补充",
        )

    def test_fills_risk_alerts_empty_list(self) -> None:
        """Placeholder fills risk_alerts with empty list when missing."""
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="稳健",
            decision_type="hold",
            dashboard={"intelligence": {}},
        )
        apply_placeholder_fill(result, ["dashboard.intelligence.risk_alerts"])
        self.assertEqual(result.dashboard["intelligence"]["risk_alerts"], [])


class TestSniperPointFallback(unittest.TestCase):
    """Fallback sniper point construction tests."""

    def test_ensure_sniper_points_fills_all_missing_fields_from_context(self) -> None:
        with patch.object(GeminiAnalyzer, "_init_litellm", return_value=None):
            analyzer = GeminiAnalyzer()
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            trend_prediction="看多",
            sentiment_score=70,
            operation_advice="持有",
            analysis_summary="稳健",
            decision_type="hold",
            dashboard={"battle_plan": {"sniper_points": {}}},
        )
        context = {
            "today": {"close": 100.0, "ma5": 99.0, "ma10": 96.0, "ma20": 92.0},
            "trend_analysis": {"resistance_levels": [108.0]},
        }

        analyzer._ensure_sniper_points(result, context)
        sniper = result.dashboard["battle_plan"]["sniper_points"]

        self.assertIn("99.00元", sniper["ideal_buy"])
        self.assertIn("96.00元", sniper["secondary_buy"])
        self.assertIn("90.16元", sniper["stop_loss"])
        self.assertIn("108.00元", sniper["take_profit"])

    def test_ensure_sniper_points_replaces_non_actionable_none_text(self) -> None:
        with patch.object(GeminiAnalyzer, "_init_litellm", return_value=None):
            analyzer = GeminiAnalyzer()
        result = AnalysisResult(
            code="512980",
            name="传媒ETF广发",
            trend_prediction="看空",
            sentiment_score=48,
            operation_advice="观望",
            analysis_summary="空头趋势",
            decision_type="sell",
            dashboard={
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "无（空头趋势）",
                        "secondary_buy": "无（空头趋势）",
                        "stop_loss": "0.990元（跌破MA20）",
                        "take_profit": "无",
                    }
                }
            },
        )
        context = {
            "today": {"close": 0.982, "ma5": 1.0064, "ma10": 1.0186, "ma20": 1.00855},
            "trend_analysis": {"resistance_levels": [1.0619]},
        }

        analyzer._ensure_sniper_points(result, context)
        sniper = result.dashboard["battle_plan"]["sniper_points"]

        self.assertIn("1.01元", sniper["ideal_buy"])
        self.assertIn("1.02元", sniper["secondary_buy"])
        self.assertEqual(sniper["stop_loss"], "0.990元（跌破MA20）")
        self.assertEqual(sniper["take_profit"], "反抽出局线：1.01元附近（不是止盈目标）")


class TestIntegrityRetryPrompt(unittest.TestCase):
    """Retry prompt construction tests."""

    def test_retry_prompt_includes_previous_response(self) -> None:
        """Retry prompt should carry previous response so补全是增量的。"""
        with patch.object(GeminiAnalyzer, "_init_litellm", return_value=None):
            analyzer = GeminiAnalyzer()
        prompt = analyzer._build_integrity_retry_prompt(
            "原始提示",
            '{"analysis_summary": "已有内容"}',
            ["dashboard.core_conclusion.one_sentence"],
        )
        self.assertIn("原始提示", prompt)
        self.assertIn('{"analysis_summary": "已有内容"}', prompt)
        self.assertIn("dashboard.core_conclusion.one_sentence", prompt)


class TestPortfolioPositionPrompt(unittest.TestCase):
    """Current holding context prompt tests."""

    def test_format_prompt_includes_portfolio_position_requirements(self) -> None:
        with patch.object(GeminiAnalyzer, "_init_litellm", return_value=None):
            analyzer = GeminiAnalyzer()

        prompt = analyzer._format_prompt(
            {
                "code": "159326",
                "stock_name": "电网设备",
                "date": "2026-04-28",
                "today": {"close": 1.5},
                "portfolio_position": {
                    "has_position": True,
                    "as_of": "2026-04-28T10:00:00",
                    "currency": "CNY",
                    "quantity": 100,
                    "avg_cost": 1.2,
                    "last_price": 1.5,
                    "cost_basis": 120,
                    "market_value": 150,
                    "unrealized_pnl": 30,
                    "unrealized_pnl_pct": 25,
                    "weight_pct": 7.5,
                    "accounts": [
                        {
                            "account_name": "主账户",
                            "quantity": 100,
                            "avg_cost": 1.2,
                            "last_price": 1.5,
                            "market_value": 150,
                            "unrealized_pnl": 30,
                            "unrealized_pnl_pct": 25,
                        }
                    ],
                },
            },
            "电网设备",
            report_language="zh",
        )

        self.assertIn("当前持仓信息", prompt)
        self.assertIn("持仓均价", prompt)
        self.assertIn("浮动盈亏率", prompt)
        self.assertIn("dashboard.core_conclusion.position_advice.has_position", prompt)
