# -*- coding: utf-8 -*-
"""
===================================
Report Engine - Report renderer tests
===================================

Tests for Jinja2 report rendering and fallback behavior.
"""

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.analyzer import AnalysisResult
from src.services.report_renderer import render


def _make_result(
    code: str = "600519",
    name: str = "贵州茅台",
    trend_prediction: str = "看多",
    sentiment_score: int = 72,
    operation_advice: str = "持有",
    analysis_summary: str = "稳健",
    decision_type: str = "hold",
    dashboard: dict = None,
    report_language: str = "zh",
    current_price: float | None = None,
) -> AnalysisResult:
    if dashboard is None:
        dashboard = {
            "core_conclusion": {"one_sentence": "持有观望"},
            "intelligence": {"risk_alerts": []},
            "battle_plan": {"sniper_points": {"stop_loss": "110"}},
        }
    return AnalysisResult(
        code=code,
        name=name,
        trend_prediction=trend_prediction,
        sentiment_score=sentiment_score,
        operation_advice=operation_advice,
        analysis_summary=analysis_summary,
        decision_type=decision_type,
        dashboard=dashboard,
        report_language=report_language,
        current_price=current_price,
    )


class TestReportRenderer(unittest.TestCase):
    """Report renderer tests."""

    def test_render_markdown_summary_only(self) -> None:
        """Markdown platform renders with summary_only."""
        r = _make_result()
        out = render("markdown", [r], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("决策仪表盘", out)
        self.assertIn("贵州茅台", out)
        self.assertIn("持有", out)

    def test_render_markdown_full(self) -> None:
        """Markdown platform renders full report."""
        r = _make_result()
        with TemporaryDirectory() as tmpdir, patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = render("markdown", [r], summary_only=False)
        self.assertIsNotNone(out)
        self.assertIn("核心结论", out)
        self.assertIn("作战计划", out)

    def test_render_wechat(self) -> None:
        """Wechat platform renders."""
        r = _make_result()
        out = render("wechat", [r])
        self.assertIsNotNone(out)
        self.assertIn("贵州茅台", out)

    def test_render_brief(self) -> None:
        """Brief platform renders 3-5 sentence summary."""
        r = _make_result()
        out = render("brief", [r])
        self.assertIsNotNone(out)
        self.assertIn("决策简报", out)
        self.assertIn("贵州茅台", out)

    def test_render_markdown_in_english(self) -> None:
        """Markdown renderer switches headings and summary labels for English reports."""
        r = _make_result(
            name="Kweichow Moutai",
            operation_advice="Buy",
            analysis_summary="Momentum remains constructive.",
            report_language="en",
        )
        out = render("markdown", [r], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("Decision Dashboard", out)
        self.assertIn("Summary", out)
        self.assertIn("Buy", out)

    def test_render_markdown_market_snapshot_uses_template_context(self) -> None:
        """Market snapshot macro should render localized labels with template context."""
        r = _make_result(
            code="AAPL",
            name="Apple",
            operation_advice="Buy",
            report_language="en",
        )
        r.market_snapshot = {
            "close": "180.10",
            "prev_close": "178.25",
            "open": "179.00",
            "high": "181.20",
            "low": "177.80",
            "pct_chg": "+1.04%",
            "change_amount": "1.85",
            "amplitude": "1.91%",
            "volume": "1200000",
            "amount": "215000000",
            "price": "180.35",
            "volume_ratio": "1.2",
            "turnover_rate": "0.8%",
            "source": "polygon",
        }

        with TemporaryDirectory() as tmpdir, patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertIn("Market Snapshot", out)
        self.assertIn("Volume Ratio", out)

    def test_render_markdown_cleans_messy_sniper_payloads(self) -> None:
        """Markdown reports should not leak raw dict/list sniper payloads."""
        r = _make_result(
            dashboard={
                "core_conclusion": {"one_sentence": "等待回踩"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": {"price": 112, "reason": "MA5附近"},
                        "secondary_buy": '{"low": 110, "high": 111, "condition": "缩量回踩"}',
                        "stop_loss": ["108", {"debug": "ignore"}],
                        "take_profit": {"value": 120},
                    }
                },
            }
        )

        with TemporaryDirectory() as tmpdir, patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertIn("| 🎯 买入区 | 🔵 加仓区 | 🛑 止损线 | 🎊 目标区 |", out)
        self.assertIn("112.00元（MA5附近）", out)
        self.assertIn("110.00-111.00元（缩量回踩）", out)
        self.assertIn("108.00元", out)
        self.assertNotIn("{'price'", out)
        self.assertNotIn('"low"', out)

    def test_render_markdown_uses_bearish_action_labels(self) -> None:
        """Bearish reports should render action labels instead of profit-target language."""
        r = _make_result(
            trend_prediction="强烈看空",
            operation_advice="卖出",
            decision_type="sell",
            dashboard={
                "core_conclusion": {"one_sentence": "趋势转弱，先防守。"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "30.34",
                        "secondary_buy": "31.14",
                        "stop_loss": "29.32",
                        "take_profit": "30.34",
                    }
                },
            },
        )

        with TemporaryDirectory() as tmpdir, patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertIn("重新评估线", out)
        self.assertIn("确认转强线", out)
        self.assertIn("立即减仓区", out)
        self.assertIn("反抽出局线", out)
        self.assertNotIn("目标区", out)

        wechat_out = render("wechat", [r])
        self.assertIsNotNone(wechat_out)
        self.assertIn("确认转强线", wechat_out)
        self.assertIn("反抽出局线", wechat_out)
        self.assertNotIn("目标区", wechat_out)

    def test_render_markdown_downgrades_far_sniper_levels(self) -> None:
        """Templates should not display stale/hallucinated prices far from current price."""
        r = _make_result(
            current_price=27.48,
            operation_advice="买入",
            decision_type="buy",
            dashboard={
                "core_conclusion": {"one_sentence": "点位需重新校验。"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "12.00",
                        "secondary_buy": "13.00",
                        "stop_loss": "10.00",
                        "take_profit": "36.00",
                    }
                },
            },
        )

        with TemporaryDirectory() as tmpdir, patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertIn("暂不设买入区", out)
        self.assertIn("暂不设加仓区", out)
        self.assertIn("暂不设止损线", out)
        self.assertIn("暂不设目标区", out)
        self.assertNotIn("36.00元", out)

    def test_render_unknown_platform_returns_none(self) -> None:
        """Unknown platform returns None (caller fallback)."""
        r = _make_result()
        out = render("unknown_platform", [r])
        self.assertIsNone(out)

    def test_render_empty_results_returns_content(self) -> None:
        """Empty results still produces header."""
        out = render("markdown", [], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("0", out)
