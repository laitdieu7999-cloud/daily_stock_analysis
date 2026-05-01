# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 通知服务单元测试
===================================

职责：
1. 验证通知服务的配置检测逻辑
2. 验证通知服务的渠道检测逻辑
3. 验证通知服务的消息发送逻辑

TODO: 
1. 添加发送渠道以外的测试，如：
    - 生成日报
2. 添加 send_to_context 的测试
"""
import os
import sys
import unittest
import json
from unittest import mock
from typing import Optional
from tempfile import TemporaryDirectory
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Keep this test runnable when optional LLM/runtime deps are not installed.
for optional_module in ("litellm", "json_repair"):
    try:
        __import__(optional_module)
    except ModuleNotFoundError:
        sys.modules[optional_module] = mock.MagicMock()

from src.config import Config
from src.notification import NotificationService, NotificationChannel
from src.analyzer import AnalysisResult
import requests


def _make_config(**overrides) -> Config:
    """Create a Config instance overriding only notification-related fields."""
    return Config(stock_list=[], **overrides)


def _make_response(status_code: int, json: Optional[dict] = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    if json:
        response.json = lambda: json
    return response


class TestNotificationServiceSendToMethods(unittest.TestCase):
    """测试通知发送服务

    测试设计：

    测试按照渠道的字母顺序排列，在合适位置添加新的测试方法。
    如果采用长消息分批发送，必须单独测试分批发送的逻辑，
        e.g. test_send_to_discord_via_notification_service_with_bot_requires_chunking

    1. 添加模拟配置：
    使用 mock.patch 装饰器来模拟 get_config 函数，
    使用 _make_config 函数添加配置，并返回 Config 实例。

    2. 检查配置是否正确：
    使用 assertIn 检查 NotificationChannel.xxxx 是否在
    `NotificationService.get_available_channels()` 返回值中。

    3. 模拟请求响应：
    使用 mock.patch 装饰器来模拟 requests.post 函数，
    使用 _make_response 函数模拟请求响应，并返回 Response 实例。
    若使用其他函数模拟请求响应，则使用 mock.patch 装饰器来模拟该函数。

    4. 使用 assertTrue 检查 send 的返回值。

    5. 使用 assert_called_once 检查请求函数是否被调用一次。
    测试分批发送时，使用 assertAlmostEqual(mock_post.call_count, ...) 检查请求函数被调用次数

    """

    @mock.patch("src.notification.get_config")
    def test_no_channels_service_unavailable_and_send_returns_false(self, mock_get_config):
        mock_get_config.return_value = _make_config()

        service = NotificationService()

        self.assertFalse(service.is_available())
        result = service.send("test content")
        self.assertFalse(result)

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_astrbot_via_notification_service(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(astrbot_url="https://astrbot.example")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200)

        service = NotificationService()
        self.assertIn(NotificationChannel.ASTRBOT, service.get_available_channels())

        ok = service.send("astrbot content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_custom_webhook_via_notification_service(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(custom_webhook_urls=["https://example.com/webhook"])
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200)

        service = NotificationService()
        self.assertIn(NotificationChannel.CUSTOM, service.get_available_channels())

        ok = service.send("custom content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_discord_via_notification_service_with_webhook(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(discord_webhook_url="https://discord.example/webhook")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(204)

        service = NotificationService()
        self.assertIn(NotificationChannel.DISCORD, service.get_available_channels())

        ok = service.send("discord content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_discord_via_notification_service_with_bot(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(discord_bot_token="TOKEN", discord_main_channel_id="123")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200)

        service = NotificationService()
        self.assertIn(NotificationChannel.DISCORD, service.get_available_channels())

        ok = service.send("discord content")

        self.assertTrue(ok)
        mock_post.assert_called_once()
        
    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_discord_via_notification_service_with_bot_requires_chunking(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(
            discord_bot_token="TOKEN",
            discord_main_channel_id="123",
            discord_max_words=2000,
        )
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200)

        service = NotificationService()
        self.assertIn(NotificationChannel.DISCORD, service.get_available_channels())

        ok = service.send("A" * 6000)

        self.assertTrue(ok)
        self.assertAlmostEqual(mock_post.call_count, 4, delta=1)


class TestNotificationServiceReportGeneration(unittest.TestCase):
    """报告生成与选路相关测试。"""

    @mock.patch("src.notification.get_config")
    def test_generate_aggregate_report_routes_by_report_type(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config()
        service = NotificationService()
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="持有",
            analysis_summary="稳健",
        )

        with mock.patch.object(service, "generate_dashboard_report", return_value="dashboard") as mock_dashboard, mock.patch.object(
            service, "generate_brief_report", return_value="brief"
        ) as mock_brief:
            self.assertEqual(service.generate_aggregate_report([result], "simple"), "dashboard")
            self.assertEqual(service.generate_aggregate_report([result], "full"), "dashboard")
            self.assertEqual(service.generate_aggregate_report([result], "detailed"), "dashboard")
            self.assertEqual(service.generate_aggregate_report([result], "brief"), "brief")

        self.assertEqual(mock_dashboard.call_count, 3)
        mock_brief.assert_called_once()

    @mock.patch("src.notification.get_config")
    def test_generate_single_stock_report_keeps_legacy_simple_format(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=True)
        service = NotificationService()
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="持有",
            analysis_summary="稳健",
        )

        with mock.patch("src.services.report_renderer.render") as mock_render:
            out = service.generate_single_stock_report(result)

        mock_render.assert_not_called()
        self.assertIn("贵州茅台", out)
        self.assertIn("600519", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_localizes_english_fallback(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="en")
        service = NotificationService()
        result = AnalysisResult(
            code="AAPL",
            name="Apple",
            sentiment_score=78,
            trend_prediction="Bullish",
            operation_advice="Buy",
            analysis_summary="Momentum remains constructive.",
            decision_type="buy",
            report_language="en",
            dashboard={
                "core_conclusion": {
                    "one_sentence": "Favor buying on pullbacks.",
                    "position_advice": {
                        "no_position": "Open a starter position.",
                        "has_position": "Hold and trail the stop.",
                    },
                },
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "180-182",
                        "stop_loss": "172",
                        "take_profit": "195",
                    }
                },
            },
        )

        out = service.generate_dashboard_report([result], report_date="2026-03-18")

        self.assertIn("Decision Dashboard", out)
        self.assertIn("Summary", out)
        self.assertIn("Execution Plan", out)
        self.assertIn("Veto Risk", out)
        self.assertIn("Buy", out)
        self.assertNotIn("Data View", out)
        self.assertNotIn("Battle Plan", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_appends_ma_prices_in_one_sentence(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="zh")
        service = NotificationService()
        result = AnalysisResult(
            code="510500",
            name="南方中证500ETF",
            sentiment_score=82,
            trend_prediction="看多",
            operation_advice="买入",
            analysis_summary="测试",
            decision_type="buy",
            report_language="zh",
            market_snapshot={
                "price": "8.323",
                "pct_chg": "-0.12%",
                "volume_ratio": "0.63",
                "turnover_rate": "0.38%",
                "source": "tencent",
            },
            dashboard={
                "core_conclusion": {
                    "one_sentence": "多头排列+缩量回踩MA5，可轻仓介入，止损设于MA20。",
                    "position_advice": {
                        "no_position": "现价回踩MA5附近可轻仓介入。",
                        "has_position": "继续持有，跌破MA20减仓。",
                    },
                },
                "data_perspective": {
                    "price_position": {
                        "ma5": "8.32",
                        "ma20": "8.18",
                    }
                },
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "8.30-8.33",
                        "stop_loss": "8.18",
                        "take_profit": "8.55",
                    }
                },
            },
        )

        out = service.generate_dashboard_report([result], report_date="2026-03-18")

        self.assertIn("**盘面**: 现价 8.32元 | 涨跌 -0.12% | 量比 0.63 | 换手 0.38% | 来源 腾讯财经", out)
        self.assertIn("MA5(8.32)", out)
        self.assertIn("MA20(8.18)", out)
        self.assertIn("现价回踩MA5(8.32)附近可轻仓介入。", out)
        self.assertIn("继续持有，跌破MA20(8.18)减仓。", out)
        self.assertIn("8.30-8.33元", out)
        self.assertIn("8.18元", out)
        self.assertIn("8.55元", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_uses_bearish_execution_labels(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="zh")
        service = NotificationService()
        result = AnalysisResult(
            code="002580",
            name="圣阳股份",
            sentiment_score=32,
            trend_prediction="强烈看空",
            operation_advice="卖出",
            analysis_summary="趋势转弱，先防守。",
            decision_type="sell",
            report_language="zh",
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

        out = service.generate_dashboard_report([result], report_date="2026-04-30")

        self.assertIn("重新评估线", out)
        self.assertIn("确认转强线", out)
        self.assertIn("立即减仓区", out)
        self.assertIn("反抽出局线", out)
        self.assertNotIn("**目标区**", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_downgrades_far_sniper_levels(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="zh")
        service = NotificationService()
        result = AnalysisResult(
            code="002580",
            name="圣阳股份",
            sentiment_score=68,
            trend_prediction="看多",
            operation_advice="买入",
            analysis_summary="点位需重新校验。",
            decision_type="buy",
            report_language="zh",
            current_price=27.48,
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

        with TemporaryDirectory() as tmpdir, mock.patch(
            "src.services.sniper_points.SNIPER_POINT_DOWNGRADE_AUDIT_PATH",
            Path(tmpdir) / "audit.jsonl",
        ):
            out = service.generate_dashboard_report([result], report_date="2026-04-30")

        self.assertIn("暂不设买入区", out)
        self.assertIn("暂不设加仓区", out)
        self.assertIn("暂不设止损线", out)
        self.assertIn("暂不设目标区", out)
        self.assertNotIn("36.00元", out)

    def test_clean_sniper_value_formats_price_points(self):
        self.assertEqual(NotificationService._clean_sniper_value("8.3-8.33"), "8.30-8.33元")
        self.assertEqual(NotificationService._clean_sniper_value("8.18"), "8.18元")
        self.assertEqual(NotificationService._clean_sniper_value(195), "195.00元")
        self.assertEqual(
            NotificationService._annotate_ma_levels(
                "跌破MA20（8.18元）减仓。",
                {"data_perspective": {"price_position": {"ma20": "8.18"}}},
            ),
            "跌破MA20（8.18元）减仓。",
        )

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_skips_data_gap_checklist_for_veto(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="zh")
        service = NotificationService()
        result = AnalysisResult(
            code="510500",
            name="南方中证500ETF",
            sentiment_score=76,
            trend_prediction="看多",
            operation_advice="买入",
            analysis_summary="测试",
            decision_type="buy",
            report_language="zh",
            risk_warning="若跌破MA20则本轮计划立即作废。",
            dashboard={
                "core_conclusion": {
                    "one_sentence": "回踩均线后可试仓。",
                    "position_advice": {
                        "no_position": "现价轻仓介入。",
                        "has_position": "继续持有。",
                    },
                },
                "data_perspective": {
                    "price_position": {
                        "ma20": "8.18",
                    }
                },
                "battle_plan": {
                    "action_checklist": [
                        "⚠️ 检查项5：筹码结构未知（数据缺失）",
                        "✅ 检查项1：多头排列",
                    ],
                    "sniper_points": {
                        "ideal_buy": "8.30-8.33",
                        "stop_loss": "8.18",
                        "take_profit": "8.55",
                    }
                },
            },
        )

        out = service.generate_dashboard_report([result], report_date="2026-03-18")

        self.assertIn("若跌破MA20(8.18)则本轮计划立即作废。", out)
        self.assertNotIn("筹码结构未知（数据缺失）", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_localizes_english_no_dashboard_fallback(
        self, mock_get_config: mock.MagicMock
    ):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="en")
        service = NotificationService()
        result = AnalysisResult(
            code="AAPL",
            name="Apple",
            sentiment_score=61,
            trend_prediction="看多",
            operation_advice="持有",
            analysis_summary="Wait for confirmation.",
            report_language="en",
            buy_reason="Momentum remains constructive.",
            risk_warning="Watch for a failed breakout.",
            ma_analysis="Price remains above MA20.",
            volume_analysis="Volume is steady.",
            news_summary="Product cycle remains supportive.",
        )

        out = service.generate_dashboard_report([result], report_date="2026-03-19")

        self.assertIn("Rationale", out)
        self.assertIn("Risk Warning", out)
        self.assertIn("Technicals", out)
        self.assertIn("Moving Averages", out)
        self.assertIn("Volume", out)
        self.assertIn("News Flow", out)
        self.assertNotIn("操作理由", out)
        self.assertNotIn("风险提示", out)
        self.assertNotIn("技术面", out)
        self.assertNotIn("消息面", out)

    @mock.patch("src.notification.get_config")
    def test_generate_dashboard_report_surfaces_failed_results_explicitly(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="zh")
        service = NotificationService()
        result = AnalysisResult(
            code="159869",
            name="游戏ETF",
            sentiment_score=0,
            trend_prediction="震荡",
            operation_advice="观望",
            analysis_summary="",
            decision_type="hold",
            report_language="zh",
            success=False,
            error_message="All LLM models failed: Your account balance is insufficient.",
        )

        out = service.generate_dashboard_report([result], report_date="2026-03-19")

        self.assertIn("❌失败:1", out)
        self.assertIn("❌ **游戏ETF(159869)**: 分析失败", out)
        self.assertIn("## ❌ 游戏ETF (159869)", out)
        self.assertIn("本轮未生成有效决策，未纳入买卖建议汇总。", out)
        self.assertIn("Your account balance is insufficient", out)

    @mock.patch("src.notification.get_config")
    def test_generate_single_stock_report_localizes_english_fallback(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_renderer_enabled=False, report_language="en")
        service = NotificationService()
        result = AnalysisResult(
            code="AAPL",
            name="Apple",
            sentiment_score=65,
            trend_prediction="Sideways",
            operation_advice="Hold",
            analysis_summary="Wait for a cleaner breakout.",
            report_language="en",
            dashboard={
                "core_conclusion": {"one_sentence": "Wait for confirmation."},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "190",
                        "stop_loss": "182",
                        "take_profit": "205",
                    }
                },
            },
        )

        out = service.generate_single_stock_report(result)

        self.assertIn("Core Conclusion", out)
        self.assertIn("Execution Plan", out)
        self.assertIn("Veto Risk", out)
        self.assertIn("Hold", out)
        self.assertNotIn("Market Snapshot", out)

    @mock.patch("src.notification.get_config")
    def test_history_compare_context_uses_cache(self, mock_get_config: mock.MagicMock):
        mock_get_config.return_value = _make_config(report_history_compare_n=3)
        service = NotificationService()
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="持有",
            analysis_summary="稳健",
            query_id="q-1",
        )

        with mock.patch(
            "src.services.history_comparison_service.get_signal_changes_batch",
            return_value={"600519": []},
        ) as mock_batch:
            first = service._get_history_compare_context([result])
            second = service._get_history_compare_context([result])

        self.assertEqual(first, {"history_by_code": {"600519": []}})
        self.assertEqual(second, {"history_by_code": {"600519": []}})
        mock_batch.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("smtplib.SMTP_SSL")
    def test_send_to_email_via_notification_service(
        self, mock_smtp_ssl: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(
            email_sender="user@qq.com",
            email_password="PASS",
            email_receivers=["default@example.com"],
        )
        mock_get_config.return_value = cfg

        service = NotificationService()
        self.assertIn(NotificationChannel.EMAIL, service.get_available_channels())

        ok = service.send("email content")

        self.assertTrue(ok)
        mock_smtp_ssl.assert_called_once()
        mock_smtp_ssl.return_value.send_message.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("smtplib.SMTP_SSL")
    def test_send_to_email_with_stock_group_routing(
        self, mock_smtp_ssl: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(
            email_sender="user@qq.com",
            email_password="PASS",
            email_receivers=["default@example.com"],
            stock_email_groups=[(["000001", "600519"], ["group@example.com"])],
        )
        mock_get_config.return_value = cfg

        service = NotificationService()
        self.assertIn(NotificationChannel.EMAIL, service.get_available_channels())

        server = mock_smtp_ssl.return_value

        ok = service.send("content", email_stock_codes=["000001"])

        self.assertTrue(ok)
        mock_smtp_ssl.assert_called_once()
        server.send_message.assert_called_once()
        msg = server.send_message.call_args[0][0]
        self.assertIn("group@example.com", msg["To"])

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_feishu_via_notification_service(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(feishu_webhook_url="https://feishu.example")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 0})

        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "feishu_push_audit.jsonl"
            archive_dir = Path(tmpdir) / "archive"
            with mock.patch("src.notification.FEISHU_PUSH_AUDIT_PATH", audit_path), \
                 mock.patch("src.notification.FEISHU_PUSH_AUDIT_ARCHIVE_DIR", archive_dir):
                service = NotificationService()
                self.assertIn(NotificationChannel.FEISHU, service.get_available_channels())

                ok = service.send("hello feishu")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_feishu_writes_audit_record(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(feishu_webhook_url="https://feishu.example")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 0})

        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "feishu_push_audit.jsonl"
            archive_dir = Path(tmpdir) / "archive"
            with mock.patch("src.notification.FEISHU_PUSH_AUDIT_PATH", audit_path), \
                 mock.patch("src.notification.FEISHU_PUSH_AUDIT_ARCHIVE_DIR", archive_dir):
                service = NotificationService()
                ok = service.send("## 玄学治理日报\n\n- 结论: 偏防守")
                rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                archive_exists = Path(rows[0]["archive_path"]).exists()

        self.assertTrue(ok)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["success"])
        self.assertEqual(rows[0]["channel"], "feishu")
        self.assertEqual(rows[0]["push_kind"], "metaphysical_daily")
        self.assertTrue(archive_exists)
        
    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_feishu_via_notification_service_requires_chunking(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(feishu_webhook_url="https://feishu.example", feishu_max_bytes=2000)
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 0})

        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "feishu_push_audit.jsonl"
            archive_dir = Path(tmpdir) / "archive"
            with mock.patch("src.notification.FEISHU_PUSH_AUDIT_PATH", audit_path), \
                 mock.patch("src.notification.FEISHU_PUSH_AUDIT_ARCHIVE_DIR", archive_dir):
                service = NotificationService()
                self.assertIn(NotificationChannel.FEISHU, service.get_available_channels())

                ok = service.send("A" * 6000)

        self.assertTrue(ok)
        self.assertAlmostEqual(mock_post.call_count, 4, delta=1)

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_feishu_failure_still_writes_audit_record(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(feishu_webhook_url="https://feishu.example")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(500)

        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "feishu_push_audit.jsonl"
            archive_dir = Path(tmpdir) / "archive"
            with mock.patch("src.notification.FEISHU_PUSH_AUDIT_PATH", audit_path), \
                 mock.patch("src.notification.FEISHU_PUSH_AUDIT_ARCHIVE_DIR", archive_dir):
                service = NotificationService()
                ok = service.send("hello feishu failed")
                rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                archive_exists = Path(rows[0]["archive_path"]).exists()

        self.assertFalse(ok)
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["success"])
        self.assertEqual(rows[0]["error"], "send_returned_false")
        self.assertTrue(archive_exists)

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_pushover_via_notification_service(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(
            pushover_user_key="USER",
            pushover_api_token="TOKEN",
        )
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"status": 1})

        service = NotificationService()
        self.assertIn(NotificationChannel.PUSHOVER, service.get_available_channels())

        ok = service.send("pushover content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_pushplus_via_notification_service(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(pushplus_token="TOKEN")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 200})

        service = NotificationService()
        self.assertIn(NotificationChannel.PUSHPLUS, service.get_available_channels())

        ok = service.send("pushplus content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification_sender.pushplus_sender.time.sleep")
    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_pushplus_via_notification_service_requires_chunking(
        self,
        mock_post: mock.MagicMock,
        mock_get_config: mock.MagicMock,
        _mock_sleep: mock.MagicMock,
    ):
        cfg = _make_config(pushplus_token="TOKEN")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 200})

        service = NotificationService()
        self.assertIn(NotificationChannel.PUSHPLUS, service.get_available_channels())

        ok = service.send("A" * 25000)

        self.assertTrue(ok)
        self.assertGreaterEqual(mock_post.call_count, 2)

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_slack_via_notification_service_with_webhook(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(slack_webhook_url="https://hooks.slack.com/services/T/B/xxx")
        mock_get_config.return_value = cfg
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.text = "ok"
        mock_post.return_value = resp

        service = NotificationService()
        self.assertIn(NotificationChannel.SLACK, service.get_available_channels())

        ok = service.send("slack content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_slack_via_notification_service_with_bot(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(slack_bot_token="xoxb-test", slack_channel_id="C123")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"ok": True})

        service = NotificationService()
        self.assertIn(NotificationChannel.SLACK, service.get_available_channels())

        ok = service.send("slack bot content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_serverchan3_via_notification_service(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(serverchan3_sendkey="SCTKEY")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"code": 0})

        service = NotificationService()
        self.assertIn(NotificationChannel.SERVERCHAN3, service.get_available_channels())

        ok = service.send("serverchan content")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_telegram_via_notification_service(
        self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock
    ):
        cfg = _make_config(telegram_bot_token="TOKEN", telegram_chat_id="123")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"ok": True})

        service = NotificationService()
        self.assertIn(NotificationChannel.TELEGRAM, service.get_available_channels())

        ok = service.send("hello telegram")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_wechat_via_notification_service(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(wechat_webhook_url="https://wechat.example")
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"errcode": 0})

        service = NotificationService()
        self.assertIn(NotificationChannel.WECHAT, service.get_available_channels())

        ok = service.send("hello wechat")

        self.assertTrue(ok)
        mock_post.assert_called_once()

    @mock.patch("src.notification.get_config")
    @mock.patch("requests.post")
    def test_send_to_wechat_via_notification_service_requires_chunking(self, mock_post: mock.MagicMock, mock_get_config: mock.MagicMock):
        cfg = _make_config(wechat_webhook_url="https://wechat.example", wechat_max_bytes=2000)
        mock_get_config.return_value = cfg
        mock_post.return_value = _make_response(200, {"errcode": 0})

        service = NotificationService()
        self.assertIn(NotificationChannel.WECHAT, service.get_available_channels())

        ok = service.send("A" * 6000)

        self.assertTrue(ok)
        self.assertAlmostEqual(mock_post.call_count, 4, delta=1)


if __name__ == "__main__":
    unittest.main()
