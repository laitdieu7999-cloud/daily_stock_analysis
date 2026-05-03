# -*- coding: utf-8 -*-
"""Regression tests for scheduled mode stock selection behavior."""

import json
import logging
import os
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

import main
from src.config import Config


class _DummyConfig(SimpleNamespace):
    def validate(self):
        return []


class MainScheduleModeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.env_path.write_text("STOCK_LIST=600519\n", encoding="utf-8")
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.env_patch = patch.dict(os.environ, {"ENV_FILE": str(self.env_path)}, clear=False)
        self.env_patch.start()
        Config.reset_instance()
        root_logger = logging.getLogger()
        self._original_root_handlers = list(root_logger.handlers)
        self._original_root_level = root_logger.level

    def tearDown(self) -> None:
        root_logger = logging.getLogger()
        current_handlers = list(root_logger.handlers)
        for handler in current_handlers:
            if handler not in self._original_root_handlers:
                root_logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        root_logger.setLevel(self._original_root_level)
        os.chdir(self.original_cwd)
        Config.reset_instance()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def test_ensure_litellm_log_defaults_preserves_existing_env(self) -> None:
        with patch.dict(
            os.environ,
            {"LITELLM_LOG": "WARNING", "LITELLM_LOCAL_MODEL_COST_MAP": "false"},
            clear=False,
        ):
            main._ensure_litellm_log_defaults()
            self.assertEqual(os.environ["LITELLM_LOG"], "WARNING")
            self.assertEqual(os.environ["LITELLM_LOCAL_MODEL_COST_MAP"], "false")

        with patch.dict(os.environ, {}, clear=True):
            main._ensure_litellm_log_defaults()
            self.assertEqual(os.environ["LITELLM_LOG"], "INFO")
            self.assertEqual(os.environ["LITELLM_LOCAL_MODEL_COST_MAP"], "true")

    def _make_args(self, **overrides):
        defaults = {
            "debug": False,
            "stocks": None,
            "webui": False,
            "webui_only": False,
            "serve": False,
            "serve_only": False,
            "host": "0.0.0.0",
            "port": 8000,
            "backtest": False,
            "market_review": False,
            "schedule": False,
            "no_run_immediately": False,
            "no_notify": False,
            "no_market_review": False,
            "dry_run": False,
            "workers": 1,
            "force_run": False,
            "single_notify": False,
            "no_context_snapshot": False,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _make_config(self, **overrides):
        defaults = {
            "log_dir": self.temp_dir.name,
            "webui_enabled": False,
            "dingtalk_stream_enabled": False,
            "feishu_stream_enabled": False,
            "schedule_enabled": False,
            "schedule_time": "18:00",
            "schedule_run_immediately": True,
            "run_immediately": True,
        }
        defaults.update(overrides)
        return _DummyConfig(**defaults)

    def test_schedule_mode_ignores_cli_stock_snapshot(self) -> None:
        args = self._make_args(schedule=True, stocks="600519,000001")
        config = self._make_config(schedule_enabled=False)
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )
            task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main.logger.warning") as warning_log, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            scheduled_call,
            {
                "schedule_time": "18:00",
                "run_immediately": True,
                "background_tasks": [],
                "resolved_schedule_time": "18:00",
            },
        )
        run_full_analysis.assert_called_once()
        called_config, called_args, called_stocks = run_full_analysis.call_args.args
        self.assertIs(called_config, config)
        self.assertEqual(called_stocks, None)
        self.assertIsNot(called_args, args)
        self.assertTrue(getattr(called_args, "_scheduled_invocation", False))
        warning_log.assert_any_call(
            "定时模式下检测到 --stocks 参数；计划执行将忽略启动时股票快照，并在每次运行前重新读取最新的 STOCK_LIST。"
        )

    def test_schedule_mode_registers_close_reminder_and_event_monitor_tasks(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            close_reminder_enabled=True,
            close_reminder_time="15:10",
            agent_event_monitor_enabled=True,
            agent_event_monitor_interval_minutes=7,
        )
        scheduled_call = {}
        monitor = object()

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.agent.events.build_event_monitor_from_config", return_value=monitor), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(scheduled_call["schedule_time"], "18:00")
        self.assertEqual(scheduled_call["resolved_schedule_time"], "18:00")
        self.assertEqual(len(scheduled_call["extra_daily_tasks"]), 1)
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["name"], "close_reminder")
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["schedule_time"], "15:10")
        self.assertEqual(len(scheduled_call["background_tasks"]), 1)
        self.assertEqual(scheduled_call["background_tasks"][0]["name"], "agent_event_monitor")
        self.assertEqual(scheduled_call["background_tasks"][0]["interval_seconds"], 420)
        self.assertTrue(scheduled_call["background_tasks"][0]["run_immediately"])

    def test_event_monitor_task_skips_outside_intraday_session(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            agent_event_monitor_enabled=True,
        )
        scheduled_call = {}
        monitor = object()
        run_event_monitor_once = MagicMock(return_value=[])

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["background_tasks"] = background_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("main._is_cn_intraday_monitoring_session", return_value=False), \
             patch("src.agent.events.build_event_monitor_from_config", return_value=monitor), \
             patch("src.agent.events.run_event_monitor_once", run_event_monitor_once), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()
            self.assertEqual(exit_code, 0)
            task_entry = next(
                item for item in scheduled_call["background_tasks"]
                if item["name"] == "agent_event_monitor"
            )
            task_entry["task"]()

        run_event_monitor_once.assert_not_called()

    def test_schedule_mode_registers_nightly_market_outlook_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            nightly_market_outlook_enabled=True,
            nightly_market_outlook_time="22:30",
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(scheduled_call["extra_daily_tasks"]), 1)
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["name"], "nightly_market_outlook")
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["schedule_time"], "22:30")

    def test_schedule_mode_registers_premarket_health_check_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            premarket_health_check_enabled=True,
            premarket_health_check_time="08:50",
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(scheduled_call["extra_daily_tasks"]), 1)
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["name"], "premarket_health_check")
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["schedule_time"], "08:50")

    def test_schedule_mode_registers_market_data_warehouse_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            market_data_warehouse_enabled=True,
            market_data_warehouse_time="15:45",
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(scheduled_call["extra_daily_tasks"]), 1)
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["name"], "market_data_warehouse")
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["schedule_time"], "15:45")

    def test_schedule_mode_registers_post_close_shadow_refresh_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            post_close_shadow_refresh_enabled=True,
            post_close_shadow_refresh_time="16:20",
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(scheduled_call["extra_daily_tasks"]), 1)
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["name"], "post_close_shadow_refresh")
        self.assertEqual(scheduled_call["extra_daily_tasks"][0]["schedule_time"], "16:20")

    def test_post_close_shadow_refresh_task_runs_paper_refresh(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            post_close_shadow_refresh_enabled=True,
            post_close_shadow_refresh_time="16:20",
            post_close_shadow_refresh_timeout_seconds=120,
            post_close_shadow_refresh_rebuild_ledger=True,
        )
        captured = {}
        refresh_runner = MagicMock(return_value={"summary_path": "/tmp/summary.md", "json_path": "/tmp/summary.json"})

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            captured["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("scripts.run_post_close_shadow_refresh.run_post_close_shadow_refresh", refresh_runner), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()
            self.assertEqual(exit_code, 0)
            captured["extra_daily_tasks"][0]["task"]()

        refresh_runner.assert_called_once()
        call_kwargs = refresh_runner.call_args.kwargs
        self.assertEqual(call_kwargs["timeout_seconds"], 120)
        self.assertTrue(call_kwargs["rebuild_ledger"])

    def test_schedule_mode_registers_portfolio_review_and_cleanup_tasks(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            portfolio_daily_review_enabled=True,
            portfolio_daily_review_time="16:05",
            workstation_cleanup_enabled=True,
            workstation_cleanup_time="02:20",
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["extra_daily_tasks"] = extra_daily_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        tasks = {item["name"]: item["schedule_time"] for item in scheduled_call["extra_daily_tasks"]}
        self.assertEqual(tasks["portfolio_daily_review"], "16:05")
        self.assertEqual(tasks["workstation_cleanup"], "02:20")

    def test_schedule_mode_registers_intraday_snapshot_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            intraday_snapshot_enabled=True,
            intraday_snapshot_interval_minutes=5,
        )
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["background_tasks"] = background_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        task_names = [item["name"] for item in scheduled_call["background_tasks"]]
        self.assertIn("intraday_snapshot_collector", task_names)
        task_entry = next(item for item in scheduled_call["background_tasks"] if item["name"] == "intraday_snapshot_collector")
        self.assertEqual(task_entry["interval_seconds"], 300)
        self.assertTrue(task_entry["run_immediately"])

    def test_cn_intraday_monitoring_session_windows(self) -> None:
        with patch("src.core.trading_calendar.is_market_open", return_value=True):
            self.assertTrue(
                main._is_cn_intraday_monitoring_session(datetime(2026, 4, 28, 10, 0))
            )
            self.assertTrue(
                main._is_cn_intraday_monitoring_session(datetime(2026, 4, 28, 14, 30))
            )
            self.assertFalse(
                main._is_cn_intraday_monitoring_session(datetime(2026, 4, 28, 12, 0))
            )
            self.assertFalse(
                main._is_cn_intraday_monitoring_session(datetime(2026, 4, 28, 15, 37))
            )

        with patch("src.core.trading_calendar.is_market_open", return_value=False):
            self.assertFalse(
                main._is_cn_intraday_monitoring_session(datetime(2026, 4, 28, 10, 0))
            )

    def test_close_reminder_task_sends_notification(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=True,
            close_reminder_enabled=True,
            close_reminder_time="15:10",
            agent_event_monitor_enabled=False,
        )
        captured = {}
        notification_service = MagicMock()
        notification_builder = MagicMock()
        notification_builder.build_simple_alert.return_value = "alert-text"

        notification_module = ModuleType("src.notification")
        notification_module.NotificationBuilder = notification_builder
        notification_module.NotificationService = MagicMock(return_value=notification_service)

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            captured["extra_daily_tasks"] = extra_daily_tasks or []

        with patch.dict(sys.modules, {"src.notification": notification_module}):
            with patch("main.parse_arguments", return_value=args), \
                 patch("main.get_config", return_value=config), \
                 patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
                 patch("main._acquire_schedule_singleton_guard", return_value=True), \
                 patch("main.setup_logging"), \
                 patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
                exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured["extra_daily_tasks"]), 1)
        captured["extra_daily_tasks"][0]["task"]()
        notification_builder.build_simple_alert.assert_called_once()
        notification_service.send.assert_called_once_with("alert-text")

    def test_schedule_mode_reload_uses_latest_runtime_config(self) -> None:
        args = self._make_args(schedule=True)
        startup_config = self._make_config(schedule_enabled=True, schedule_time="18:00")
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )
            task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=startup_config), \
             patch("main._reload_runtime_config", return_value=runtime_config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "09:30"), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            scheduled_call,
            {"schedule_time": "18:00", "resolved_schedule_time": "09:30"},
        )
        run_full_analysis.assert_called_once()
        called_config, called_args, called_stocks = run_full_analysis.call_args.args
        self.assertIs(called_config, runtime_config)
        self.assertEqual(called_stocks, None)
        self.assertTrue(getattr(called_args, "_scheduled_invocation", False))

    def test_should_send_runtime_notifications_only_for_scheduled_or_explicit(self) -> None:
        config = self._make_config()
        manual_args = self._make_args()
        scheduled_args = self._make_args()
        setattr(scheduled_args, "_scheduled_invocation", True)
        explicit_args = self._make_args(single_notify=True)

        self.assertFalse(main._should_send_runtime_notifications(config, manual_args))
        self.assertTrue(main._should_send_runtime_notifications(config, scheduled_args))
        self.assertTrue(main._should_send_runtime_notifications(config, explicit_args))

    def test_next_day_market_direction_turns_defensive_on_risk_off_bias(self) -> None:
        decision = main._calculate_next_day_market_direction(
            [
                {"asset_name": "中证500指数", "label": "利空", "strength": "强", "score": -5, "reasons": ["风险偏好回落"]},
                {"asset_name": "纳斯达克中国金龙指数", "label": "利空", "strength": "中", "score": -3, "reasons": ["金龙下跌"]},
                {"asset_name": "黄金期货", "label": "利多", "strength": "中", "score": 3, "reasons": ["避险升温"]},
            ]
        )

        self.assertEqual(decision["direction"], "高风险回避")
        self.assertIn("自选股买入提醒全局锁死", decision["watchlist_action"])
        self.assertIn("认沽保护", decision["put_action"])

    def test_build_next_day_market_outlook_content_contains_execution_plan(self) -> None:
        content = main._build_next_day_market_outlook_content(
            report_date="2026-04-28",
            target_date=date(2026, 4, 29),
            market_payload={
                "content": "中证500指数\n现价: 5600.00\nMA5: 5580.00\nMA20: 5520.00",
                "macro_bias_items": [
                    {"asset_name": "中证500指数", "label": "利多", "strength": "中", "score": 3, "reasons": ["政策支持"]},
                ],
            },
            generated_at=datetime(2026, 4, 28, 21, 45),
        )

        self.assertIn("2026-04-29 明日大盘预判", content)
        self.assertIn("## 明日执行", content)
        self.assertIn("自选股", content)
        self.assertIn("IC贴水", content)
        self.assertIn("MA5(5580.00)", content)
        self.assertIn("## 一票否决", content)

    def test_run_nightly_market_outlook_persists_and_pushes_once(self) -> None:
        config = self._make_config(
            jin10_api_key="",
            jin10_x_token="",
            nightly_market_outlook_ai_enabled=False,
        )
        notifier = MagicMock()
        notifier.is_available.return_value = True
        notifier.send.return_value = True
        market_payload = {
            "content": "中证500指数\n现价: 5600.00",
            "macro_bias_items": [
                {"asset_name": "中证500指数", "label": "中性", "strength": "弱", "score": 0, "reasons": []},
            ],
        }

        with patch("src.notification.NotificationService", return_value=notifier), \
             patch("main._build_nightly_market_payload_with_timeout", return_value=market_payload) as market_payload_builder, \
             patch("main._resolve_next_cn_trading_date", return_value=date(2026, 4, 29)), \
             patch("main._load_fresh_gemini_tactical_reports", return_value=None), \
             patch(
                 "main._persist_archive_markdown_report",
                 return_value={"archive_path": "/tmp/archive.md"},
             ) as persist_report:
            paths = main.run_nightly_market_outlook(
                config,
                now=datetime(2026, 4, 28, 21, 45),
            )

        self.assertEqual(paths["archive_path"], "/tmp/archive.md")
        self.assertNotIn("desktop_path", paths)
        self.assertIs(market_payload_builder.call_args.kwargs["config"], config)
        self.assertEqual(persist_report.call_args.kwargs["filename_suffix"], "明日大盘预判")
        notifier.send.assert_called_once()

    def test_gemini_market_outlook_archive_builds_comparison(self) -> None:
        saved = []

        def fake_persist_archive_markdown_report(**kwargs):
            saved.append(kwargs)
            return {"archive_path": f"/tmp/{kwargs['filename_suffix']}.md"}

        archived_events = []

        def fake_append_signal_event_archive(event, **kwargs):
            archived_events.append(event)
            return {"archive_path": "/tmp/gemini_external_views.jsonl"}

        with patch("main._persist_archive_markdown_report", side_effect=fake_persist_archive_markdown_report), \
             patch("src.services.signal_router.append_signal_event_archive", side_effect=fake_append_signal_event_archive):
            paths = main._archive_gemini_market_outlook_comparison(
                report_date="2026-04-28",
                target_date=date(2026, 4, 29),
                local_content="# 2026-04-29 明日大盘预判\n\n- **方向判断**: 偏弱",
                gemini_content="# 2026-04-29 Gemini明日大盘预判\n\n- **方向判断**: 偏强",
            )

        self.assertEqual(paths["comparison_path"], "/tmp/明日大盘预判对比.md")
        self.assertEqual(paths["comparison_signal_path"], "/tmp/gemini_external_views.jsonl")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["filename_suffix"], "明日大盘预判对比")
        self.assertIn("明显分歧", saved[0]["content"])
        self.assertEqual(len(archived_events), 1)
        self.assertEqual(archived_events[0].priority, "P4")
        self.assertFalse(archived_events[0].should_notify)

    def test_load_fresh_gemini_tactical_reports_skips_stale_docs(self) -> None:
        reports_dir = Path(self.temp_dir.name) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        stale = reports_dir / "gemini_daily.md"
        stale.write_text("2026-04-24 旧报告", encoding="utf-8")
        old_ts = datetime(2026, 4, 24, 18, 0).timestamp()
        os.utime(stale, (old_ts, old_ts))
        config = self._make_config(external_tactical_report_path="reports/gemini_daily.md")

        loaded = main._load_fresh_gemini_tactical_reports(
            config,
            report_date="2026-04-28",
            target_date=date(2026, 4, 29),
        )

        self.assertIsNone(loaded)

    def test_load_fresh_gemini_tactical_reports_loads_today_synced_docs(self) -> None:
        reports_dir = Path(self.temp_dir.name) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        fresh = reports_dir / "gemini_daily.md"
        fresh.write_text("2026-04-29 Gemini明日大盘预判\n方向判断: 偏弱", encoding="utf-8")
        config = self._make_config(external_tactical_report_path="reports/gemini_daily.md")

        loaded = main._load_fresh_gemini_tactical_reports(
            config,
            report_date=datetime.now().strftime("%Y-%m-%d"),
            target_date=date(2026, 4, 29),
        )

        self.assertIn("Gemini明日大盘预判", loaded or "")

    def test_build_strategy_impact_section_uses_macro_bias(self) -> None:
        section = main._build_strategy_impact_section(
            [
                {"asset_name": "黄金期货", "label": "利多", "trend_note": "连续走强"},
                {"asset_name": "白银期货", "label": "利空", "trend_note": "最新转弱"},
                {"asset_name": "中证500指数", "label": "利空", "trend_note": "连续走弱"},
            ]
        )

        self.assertIsNotNone(section)
        self.assertIn("## 策略影响", section)
        self.assertIn("黄金ETF（做多）", section)
        self.assertIn("白银期货", section)
        self.assertIn("**白银期货**: 利空", section)
        self.assertNotIn("白银空头", section)
        self.assertIn("IC贴水策略", section)
        self.assertIn("认沽期权保护", section)
        self.assertIn("高增长行业ETF", section)
        self.assertIn("利空", section)
        self.assertIn("候选执行层", section)
        self.assertIn("空仓2日", section)

    def test_build_black_swan_playbook_section_only_on_high_risk_day(self) -> None:
        high_risk = main._build_black_swan_playbook_section(
            [
                {"asset_name": "黄金期货", "label": "利多", "trend_note": "连续走强"},
                {"asset_name": "白银期货", "label": "利空", "trend_note": "最新转弱"},
                {"asset_name": "中证500指数", "label": "利空", "trend_note": "连续走弱"},
            ]
        )
        low_risk = main._build_black_swan_playbook_section(
            [
                {"asset_name": "黄金期货", "label": "中性", "trend_note": ""},
                {"asset_name": "白银期货", "label": "中性", "trend_note": ""},
                {"asset_name": "中证500指数", "label": "中性", "trend_note": ""},
            ]
        )

        self.assertIsNotNone(high_risk)
        self.assertIn("## 高风险日应对沙盘", high_risk)
        self.assertIn("偏防守方案", high_risk)
        self.assertIn("偏进攻方案", high_risk)
        self.assertIn("候选执行层", high_risk)
        self.assertIn("空仓2日", high_risk)
        self.assertIsNone(low_risk)

    def test_load_external_tactical_report_from_relative_path(self) -> None:
        report_path = Path(self.temp_dir.name) / "reports" / "gemini_daily.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("外部报告内容", encoding="utf-8")
        config = self._make_config(external_tactical_report_path="reports/gemini_daily.md")

        loaded = main._load_external_tactical_report(config)

        self.assertEqual(loaded, "外部报告内容")

    def test_load_external_tactical_report_supports_multiple_paths(self) -> None:
        reports_dir = Path(self.temp_dir.name) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "gemini_daily.md").write_text("报告A", encoding="utf-8")
        (reports_dir / "gemini_black_swan.md").write_text("报告B", encoding="utf-8")
        config = self._make_config(
            external_tactical_report_path="reports/gemini_daily.md,reports/gemini_black_swan.md"
        )

        loaded = main._load_external_tactical_report(config)

        self.assertEqual(loaded, "报告A\n\n---\n\n报告B")

    def test_classify_external_reference_line(self) -> None:
        self.assertEqual(
            main._classify_external_reference_line("IC主力合约贴水扩大至 213.66 点（年化 -11.6%）"),
            "可直接参考",
        )
        self.assertEqual(
            main._classify_external_reference_line("今日干支甲辰月戊子日，火星动态刑克进入峰值区间"),
            "仅作辅助",
        )
        self.assertEqual(
            main._classify_external_reference_line("核心态势：流动性风险共振，黑天鹅预警生效"),
            "需二次验证",
        )

    def test_build_daily_push_summary_includes_date_title_and_sections(self) -> None:
        result = SimpleNamespace(
            code="510500",
            name="南方中证500ETF",
            sentiment_score=75,
            operation_advice="买入",
            get_core_conclusion=lambda: "关注贴水修复与指数企稳机会",
        )

        summary = main._build_daily_push_summary(
            report_date="2026-04-25",
            results=[result],
            macro_bias_items=[
                {"asset_name": "黄金期货", "label": "利多", "reasons": ["强利多"], "trend_note": "连续走强"},
                {"asset_name": "白银期货", "label": "利空", "reasons": ["弱利空"], "trend_note": "最新转弱"},
                {"asset_name": "中证500指数", "label": "利空", "reasons": ["利空股市"], "trend_note": "连续走弱"},
            ],
            external_tactical_report="核心态势：流动性风险共振，黑天鹅预警生效",
        )

        self.assertIn("# 📌 2026-04-25 盘前总报告", summary)
        self.assertIn("## 今日结论", summary)
        self.assertIn("## 策略影响", summary)
        self.assertIn("## 重点标的", summary)
        self.assertIn("## 高风险日应对沙盘", summary)

    def test_build_daily_conclusion_section_includes_metaphysical_signal_when_report_present(self) -> None:
        cache_dir = Path(self.temp_dir.name) / ".cache" / "metaphysical_probabilities"
        cache_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "tail_risk_probability": [0.20, 0.17],
            }
        ).to_pickle(cache_dir / "510500.SS_2016-01-01_2026-04-20_min756_retrain42.pkl")

        section = main._build_daily_conclusion_section(
            report_date="2026-04-24",
            macro_bias_items=[],
            external_tactical_report=(
                "报告日期：2026年4月24日\n"
                "核心态势：流动性风险共振，黑天鹅预警生效\n"
                "中东局势：霍尔木兹海峡物理封锁持续。\n"
                "全球流动性：市场处于去杠杆阶段，避险资金向美元现金归笼。\n"
                "机构大单呈净流出状态。\n"
            ),
        )

        self.assertIn("玄学模型:", section)
        self.assertIn("full_risk -> risk_off", section)
        self.assertIn("外部参考[需二次验证]: 核心态势：流动性风险共振，黑天鹅预警生效", section)

    def test_summarize_external_tactical_report_skips_heading_only_lines(self) -> None:
        lines = main._summarize_external_tactical_report(
            "今日黑天鹅触发状态：已触发\n"
            "五、 核心结论\n"
            "今日必须维持最高级别的交易警觉。\n"
        )

        self.assertIn("今日黑天鹅触发状态：已触发", lines)
        self.assertNotIn("五、 核心结论", lines)

    def test_external_direction_conflict_normalizes_silver_short_wording(self) -> None:
        section = main._build_external_direction_conflict_section(
            macro_bias_items=[
                {"asset_name": "白银期货", "label": "利空"},
            ],
            external_tactical_report="策略影响：白银空头 (利多)。",
        )

        self.assertIsNone(section)

    def test_external_direction_conflict_detects_direct_asset_disagreement(self) -> None:
        section = main._build_external_direction_conflict_section(
            macro_bias_items=[
                {"asset_name": "白银期货", "label": "利空"},
            ],
            external_tactical_report="白银期货: 利多，短线转强。",
        )

        self.assertIsNotNone(section)
        self.assertIn("## 本地 vs Gemini 方向冲突", section)
        self.assertIn("白银期货", section)
        self.assertIn("本地=利空，Gemini=利多", section)

    def test_black_swan_signal_ignores_old_triggered_history(self) -> None:
        event = main._build_black_swan_signal_event_from_report(
            report_date="2026-04-28",
            report_content=(
                "黑天鹅监控分析报告汇总\n"
                "【2026-04-28】 盘前监控报告\n"
                "今日黑天鹅触发状态：未触发\n"
                "结论：暂不需要特别警惕。\n"
                "【2026-04-24】 盘前监控报告\n"
                "今日黑天鹅触发状态：已触发\n"
                "结论：必须极度警惕。\n"
            ),
        )

        self.assertIsNone(event)

    def test_black_swan_signal_builds_p0_for_today_trigger(self) -> None:
        event = main._build_black_swan_signal_event_from_report(
            report_date="2026-04-28",
            report_content=(
                "【2026-04-28】 盘前监控报告\n"
                "今日黑天鹅触发状态：已触发\n"
                "触发依据：原油突破关键位，IC贴水异常扩大。\n"
            ),
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.priority, "P0")
        self.assertEqual(event.category, "black_swan")
        self.assertTrue(event.should_notify)

    def test_black_swan_dispatch_dedupes_same_day_alert(self) -> None:
        class _FakeNotifier:
            def __init__(self) -> None:
                self.messages = []

            def send(self, content: str) -> bool:
                self.messages.append(content)
                return True

        with tempfile.TemporaryDirectory() as tmpdir:
            notifier = _FakeNotifier()
            state_path = Path(tmpdir) / "black_swan_state.json"
            archive_path = Path(tmpdir) / "black_swan_events.jsonl"
            report = (
                "【2026-04-28】 盘前监控报告\n"
                "今日黑天鹅触发状态：已触发\n"
                "触发依据：原油突破关键位，IC贴水异常扩大。\n"
            )

            first = main._dispatch_black_swan_signal_if_needed(
                report_date="2026-04-28",
                report_content=report,
                notifier=notifier,
                state_path=state_path,
                archive_path=archive_path,
            )
            second = main._dispatch_black_swan_signal_if_needed(
                report_date="2026-04-28",
                report_content=report,
                notifier=notifier,
                state_path=state_path,
                archive_path=archive_path,
            )

            self.assertEqual(first["black_swan_signal"], "sent")
            self.assertEqual(second["black_swan_signal"], "suppressed_duplicate")
            self.assertEqual(len(notifier.messages), 1)
            self.assertTrue(archive_path.exists())

    def test_build_focus_targets_section_is_concise(self) -> None:
        result = SimpleNamespace(
            code="159201",
            name="华夏国证自由现金流ETF",
            sentiment_score=45,
            operation_advice="观望",
            get_core_conclusion=lambda: "继续作为防守底仓观察，等待更清晰的增量信号出现",
        )

        section = main._build_focus_targets_section([result])

        self.assertIn("## 重点标的", section)
        self.assertIn("华夏国证自由现金流ETF(159201)", section)
        self.assertIn("观望 | 评分 45", section)

    def test_build_brief_monitor_alert_is_short(self) -> None:
        alert = main._build_brief_monitor_alert(
            trigger_event="中证500贴水继续走阔",
            strategy="IC贴水 / 认沽保护",
            bias="先防基差继续恶化",
            immediate_attention=True,
        )

        self.assertIn("触发事件: 中证500贴水继续走阔", alert)
        self.assertIn("影响策略: IC贴水 / 认沽保护", alert)
        self.assertIn("当前倾向: 先防基差继续恶化", alert)
        self.assertIn("建议动作: 先看IC贴水与保护仓位", alert)
        self.assertIn("是否立即关注: 是", alert)

    def test_persist_daily_push_report_keeps_latest_three_on_desktop(self) -> None:
        desktop_dir = Path(self.temp_dir.name) / "desktop_reports"
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        desktop_root = desktop_dir.parent

        for report_date in ["2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24"]:
            main._persist_daily_push_report(
                report_date=report_date,
                content=f"报告 {report_date}",
                desktop_dir=desktop_dir,
                archive_dir=archive_dir,
                desktop_keep_days=3,
            )

        desktop_files = sorted(p.name for p in desktop_root.glob("*_盘前总报告.md"))
        history_files = sorted(p.name for p in desktop_dir.glob("*_盘前总报告.md"))
        archive_files = sorted(p.name for p in archive_dir.glob("*_盘前总报告.md"))

        self.assertEqual(
            desktop_files,
            [
                "2026-04-24_盘前总报告.md",
            ],
        )
        self.assertEqual(
            history_files,
            [
                "2026-04-22_盘前总报告.md",
                "2026-04-23_盘前总报告.md",
            ],
        )
        self.assertEqual(len(archive_files), 4)
        self.assertIn("2026-04-21_盘前总报告.md", archive_files)

    def test_persist_daily_push_report_does_not_overwrite_richer_report_with_fallback(self) -> None:
        desktop_dir = Path(self.temp_dir.name) / "desktop_reports"
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        desktop_root = desktop_dir.parent
        report_date = "2026-04-30"
        rich_content = "\n".join(
            [
                "# 📌 2026-04-30 盘前总报告",
                "",
                "## 今日结论",
                "",
                "- 黄金期货: 利多 | 最新转强，连续走强",
                "- 白银期货: 利空 | 最新转弱，连续走弱",
                "",
                "## 重点标的",
                "",
                "- 长江电力(600900): 买入 | 评分 80",
            ]
        )
        fallback_content = "\n".join(
            [
                "# 📌 2026-04-30 盘前总报告",
                "",
                "## 今日结论",
                "",
                "- 2026-04-30 暂无明确单边结论，按既有计划观察。",
                "",
                "## 策略影响",
                "",
                "- **黄金ETF（做多）**: 中性 | 黄金方向暂未形成明显单边优势",
                "- **白银期货**: 中性 | 白银期货暂未给出明显单边信号",
                "- **IC贴水策略**: 中性 | IC主线暂偏震荡，先看基差变化再决策",
                "- **认沽期权保护**: 中性 | 暂不需要明显强化保护",
            ]
        )

        main._persist_daily_push_report(
            report_date=report_date,
            content=rich_content,
            desktop_dir=desktop_dir,
            archive_dir=archive_dir,
        )
        main._persist_daily_push_report(
            report_date=report_date,
            content=fallback_content,
            desktop_dir=desktop_dir,
            archive_dir=archive_dir,
        )

        archive_path = archive_dir / f"{report_date}_盘前总报告.md"
        desktop_path = desktop_root / f"{report_date}_盘前总报告.md"
        self.assertEqual(archive_path.read_text(encoding="utf-8"), rich_content)
        self.assertEqual(desktop_path.read_text(encoding="utf-8"), rich_content)

    def test_persist_daily_push_report_uses_user_reports_archive_from_env(self) -> None:
        desktop_dir = Path(self.temp_dir.name) / "desktop_reports"
        reports_root = Path(self.temp_dir.name) / "user_reports"

        with patch.dict(os.environ, {"DSA_REPORTS_DIR": str(reports_root)}, clear=False):
            paths = main._persist_daily_push_report(
                report_date="2026-04-30",
                content="有效报告",
                desktop_dir=desktop_dir,
                archive_dir=None,
            )

        expected_archive = reports_root / "daily_push_archive" / "2026-04-30_盘前总报告.md"
        self.assertEqual(paths["archive_path"], str(expected_archive))
        self.assertEqual(expected_archive.read_text(encoding="utf-8"), "有效报告")

    def test_persist_daily_push_report_recovers_from_desktop_when_archive_missing(self) -> None:
        desktop_dir = Path(self.temp_dir.name) / "desktop_reports"
        desktop_root = desktop_dir.parent
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        report_date = "2026-04-30"
        desktop_root.mkdir(parents=True, exist_ok=True)
        rich_content = "# 📌 2026-04-30 盘前总报告\n\n## 重点标的\n\n- 长江电力(600900): 买入"
        fallback_content = "\n".join(
            [
                "# 📌 2026-04-30 盘前总报告",
                "",
                "## 今日结论",
                "",
                "- 2026-04-30 暂无明确单边结论，按既有计划观察。",
                "",
                "## 策略影响",
                "",
                "- **黄金ETF（做多）**: 中性 | 黄金方向暂未形成明显单边优势",
                "- **白银期货**: 中性 | 白银期货暂未给出明显单边信号",
            ]
        )
        (desktop_root / f"{report_date}_盘前总报告.md").write_text(rich_content, encoding="utf-8")

        main._persist_daily_push_report(
            report_date=report_date,
            content=fallback_content,
            desktop_dir=desktop_dir,
            archive_dir=archive_dir,
        )

        self.assertEqual(
            (desktop_root / f"{report_date}_盘前总报告.md").read_text(encoding="utf-8"),
            rich_content,
        )
        self.assertEqual(
            (archive_dir / f"{report_date}_盘前总报告.md").read_text(encoding="utf-8"),
            rich_content,
        )

    def test_mirror_existing_markdown_to_desktop_keeps_latest_three(self) -> None:
        desktop_dir = Path(self.temp_dir.name) / "desktop_reports"
        source_dir = Path(self.temp_dir.name) / "reports"
        source_dir.mkdir(parents=True, exist_ok=True)
        desktop_root = desktop_dir.parent

        for report_date in ["2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24"]:
            source_path = source_dir / f"report_{report_date.replace('-', '')}.md"
            source_path.write_text(f"详细报告 {report_date}", encoding="utf-8")
            main._mirror_existing_markdown_to_desktop(
                report_date=report_date,
                source_path=source_path,
                desktop_dir=desktop_dir,
                filename_suffix="详细版决策仪表盘",
                desktop_keep_days=3,
            )

        desktop_files = sorted(p.name for p in desktop_root.glob("*_详细版决策仪表盘.md"))
        history_files = sorted(p.name for p in desktop_dir.glob("*_详细版决策仪表盘.md"))
        self.assertEqual(
            desktop_files,
            [
                "2026-04-24_详细版决策仪表盘.md",
            ],
        )
        self.assertEqual(
            history_files,
            [
                "2026-04-22_详细版决策仪表盘.md",
                "2026-04-23_详细版决策仪表盘.md",
            ],
        )

    def test_persist_runtime_placeholder_reports_overwrites_stale_daily_files(self) -> None:
        reports_dir = Path(self.temp_dir.name) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "report_20260425.md").write_text("旧的单股日报", encoding="utf-8")
        (reports_dir / "market_review_20260425.md").write_text("旧的大盘复盘", encoding="utf-8")

        saved = main._persist_runtime_placeholder_reports(
            run_started_at=datetime(2026, 4, 25, 13, 40, 49),
            stock_codes=["600519", "510500"],
            merge_notification=True,
            reports_dir=reports_dir,
        )

        dashboard_text = Path(saved["dashboard_path"]).read_text(encoding="utf-8")
        market_review_text = Path(saved["market_review_path"]).read_text(encoding="utf-8")
        self.assertIn("当前这轮完整分析仍在执行中", dashboard_text)
        self.assertIn("计划分析标的数: 2", dashboard_text)
        self.assertIn("合并推送模式: 是", dashboard_text)
        self.assertIn("本文件会在本轮分析完成后被正式结果覆盖", dashboard_text)
        self.assertIn("当前这轮大盘复盘仍在生成中", market_review_text)
        self.assertNotIn("旧的单股日报", dashboard_text)
        self.assertNotIn("旧的大盘复盘", market_review_text)

    def test_resolve_manual_report_filenames_for_single_stock_cli_run(self) -> None:
        args = self._make_args(stocks="510500", no_notify=True, force_run=True)

        with patch("main.datetime") as mock_datetime:
            mock_now = datetime(2026, 4, 25, 15, 58, 0)
            mock_datetime.now.return_value = mock_now
            mock_datetime.strftime = datetime.strftime
            filenames = main._resolve_manual_report_filenames(args, ["510500"])

        self.assertEqual(
            filenames,
            {
                "dashboard_filename": "report_20260425_510500.md",
                "market_review_filename": "market_review_20260425_510500.md",
                "daily_push_filename": "2026-04-25_510500_盘前总报告.md",
            },
        )

    def test_persist_runtime_placeholder_reports_supports_custom_manual_filenames(self) -> None:
        reports_dir = Path(self.temp_dir.name) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        saved = main._persist_runtime_placeholder_reports(
            run_started_at=datetime(2026, 4, 25, 15, 40, 49),
            stock_codes=["510500"],
            merge_notification=False,
            reports_dir=reports_dir,
            dashboard_filename="report_20260425_510500.md",
            market_review_filename="market_review_20260425_510500.md",
        )

        self.assertTrue(saved["dashboard_path"].endswith("report_20260425_510500.md"))
        self.assertTrue(saved["market_review_path"].endswith("market_review_20260425_510500.md"))
        self.assertTrue(Path(saved["dashboard_path"]).exists())
        self.assertTrue(Path(saved["market_review_path"]).exists())

    def test_run_full_analysis_manual_targeted_run_does_not_overwrite_primary_daily_push(self) -> None:
        args = self._make_args(stocks="510500", no_notify=True, force_run=True, no_market_review=False)
        config = self._make_config(
            stock_list=["510500"],
            market_daily_push_enabled=False,
            market_review_enabled=True,
        )
        fake_results = [
            SimpleNamespace(
                code="510500",
                name="南方中证500ETF",
                sentiment_score=82,
                operation_advice="买入",
                trend_prediction="强烈看多",
            )
        ]
        fake_pipeline = MagicMock()
        fake_pipeline.run.return_value = fake_results
        fake_pipeline.notifier = MagicMock()
        fake_pipeline.analyzer = MagicMock()
        fake_pipeline.search_service = MagicMock()
        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = MagicMock(return_value=fake_pipeline)
        fake_market_review_module = ModuleType("src.core.market_review")
        fake_market_review_module.run_market_review = MagicMock(return_value="mock review")

        with patch("main._persist_runtime_placeholder_reports") as placeholder_mock, \
             patch.dict(
                 sys.modules,
                 {
                     "src.core.pipeline": fake_pipeline_module,
                     "src.core.market_review": fake_market_review_module,
                 },
             ), \
             patch("main._build_daily_push_summary", return_value="manual summary"), \
             patch("main._load_external_tactical_report", return_value=""), \
             patch("main._persist_daily_push_report") as daily_push_mock:
            main.run_full_analysis(config, args, stock_codes=["510500"])

        expected_date = datetime.now().strftime("%Y%m%d")
        placeholder_kwargs = placeholder_mock.call_args.kwargs
        self.assertEqual(
            placeholder_kwargs["dashboard_filename"],
            f"report_{expected_date}_510500.md",
        )
        self.assertEqual(
            placeholder_kwargs["market_review_filename"],
            f"market_review_{expected_date}_510500.md",
        )
        self.assertEqual(
            fake_pipeline_module.StockAnalysisPipeline.call_args.kwargs["local_report_filename"],
            f"report_{expected_date}_510500.md",
        )
        self.assertEqual(
            fake_market_review_module.run_market_review.call_args.kwargs["report_filename"],
            f"market_review_{expected_date}_510500.md",
        )
        daily_push_mock.assert_not_called()

    def test_build_daily_push_index_record_contains_core_fields(self) -> None:
        result = SimpleNamespace(
            code="510500",
            name="南方中证500ETF",
            sentiment_score=75,
            operation_advice="买入",
            trend_prediction="看多",
        )
        record = main._build_daily_push_index_record(
            report_date="2026-04-24",
            content="# 📌 2026-04-24 盘前总报告\n\n## 高风险日应对沙盘",
            archive_path="/tmp/2026-04-24_盘前总报告.md",
            macro_bias_items=[
                {"asset_name": "黄金期货", "label": "利多", "trend_note": "连续走强", "reasons": ["强利多"]},
                {"asset_name": "纳斯达克中国金龙指数", "label": "利多", "trend_note": "", "reasons": ["金龙上涨"]},
            ],
            results=[result],
        )

        self.assertEqual(record["report_date"], "2026-04-24")
        self.assertEqual(record["archive_path"], "/tmp/2026-04-24_盘前总报告.md")
        self.assertTrue(record["has_high_risk_playbook"])
        self.assertEqual(record["macro_bias"]["黄金期货"]["label"], "利多")
        self.assertEqual(record["macro_bias"]["纳斯达克中国金龙指数"]["label"], "利多")
        self.assertEqual(record["top_targets"][0]["code"], "510500")

    def test_append_daily_push_index_record_replaces_same_date(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)

        first_path = main._append_daily_push_index_record(
            report_date="2026-04-24",
            content="first",
            archive_dir=archive_dir,
            archive_path="/tmp/first.md",
            macro_bias_items=[],
            results=[],
        )
        second_path = main._append_daily_push_index_record(
            report_date="2026-04-24",
            content="second",
            archive_dir=archive_dir,
            archive_path="/tmp/second.md",
            macro_bias_items=[],
            results=[],
        )

        self.assertEqual(first_path, second_path)
        lines = [line for line in Path(second_path).read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["archive_path"], "/tmp/second.md")

    def test_compute_forward_return_metrics(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-04-24", "close": 100},
                {"date": "2026-04-25", "close": 102},
                {"date": "2026-04-28", "close": 101},
                {"date": "2026-04-29", "close": 105},
                {"date": "2026-04-30", "close": 110},
                {"date": "2026-05-06", "close": 108},
            ]
        )

        metrics = main._compute_forward_return_metrics(
            stock_code="510500",
            report_date="2026-04-24",
            as_of_date=datetime(2026, 5, 6).date(),
            fetch_history=lambda *args, **kwargs: df,
        )

        self.assertEqual(metrics["entry_date"], "2026-04-24")
        self.assertEqual(metrics["t_plus_1"]["return_pct"], 2.0)
        self.assertEqual(metrics["t_plus_3"]["return_pct"], 5.0)
        self.assertEqual(metrics["t_plus_5"]["return_pct"], 8.0)

    def test_refresh_daily_push_index_outcomes_appends_forward_eval(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            json.dumps(
                {
                    "report_date": "2026-04-24",
                    "archive_path": "/tmp/report.md",
                    "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

        fake_df = pd.DataFrame(
            [
                {"date": "2026-04-24", "close": 100},
                {"date": "2026-04-25", "close": 103},
                {"date": "2026-04-28", "close": 106},
                {"date": "2026-04-29", "close": 108},
                {"date": "2026-04-30", "close": 109},
                {"date": "2026-05-06", "close": 111},
            ]
        )

        class _Manager:
            def get_daily_data(self, stock_code, start_date=None, end_date=None, days=30):
                return fake_df, "stub"

            def close(self):
                return None

        with patch("data_provider.base.DataFetcherManager", return_value=_Manager()):
            updated = main._refresh_daily_push_index_outcomes(
                index_path,
                as_of_date=datetime(2026, 5, 6).date(),
            )

        self.assertEqual(updated, 1)
        payload = json.loads(index_path.read_text(encoding="utf-8").splitlines()[0])
        self.assertIn("forward_eval", payload)
        self.assertEqual(payload["forward_eval"]["510500"]["t_plus_1"]["return_pct"], 3.0)

    def test_build_weekly_review_from_index_summarizes_forward_eval(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        records = [
            {
                "report_date": "2026-04-21",
                "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                "forward_eval": {
                    "510500": {
                        "t_plus_1": {"return_pct": 2.0},
                        "t_plus_3": {"return_pct": 5.0},
                        "t_plus_5": {"return_pct": 8.0},
                    }
                },
            },
            {
                "report_date": "2026-04-23",
                "top_targets": [{"code": "518880", "name": "黄金ETF"}],
                "forward_eval": {
                    "518880": {
                        "t_plus_1": {"return_pct": -1.0},
                        "t_plus_3": {"return_pct": -2.0},
                        "t_plus_5": None,
                    }
                },
            },
        ]
        index_path.write_text(
            "\n".join(json.dumps(item, ensure_ascii=False) for item in records) + "\n",
            encoding="utf-8",
        )

        summary = main._build_weekly_review_from_index(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(summary)
        self.assertIn("# 2026-04-24 周度效果复盘", summary)
        self.assertIn("T+1: 样本 2 | 胜率 50.0% | 平均收益 0.50%", summary)
        self.assertIn("T+3: 样本 2 | 胜率 50.0% | 平均收益 1.50%", summary)
        self.assertIn("最优(T+3): 南方中证500ETF(510500)", summary)
        self.assertIn("最弱(T+3): 黄金ETF(518880)", summary)

    def test_persist_weekly_review_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_weekly_review(
            report_date="2026-04-24",
            content="# 2026-04-24 周度效果复盘",
            archive_dir=archive_dir,
        )

        self.assertIsNotNone(saved)
        self.assertTrue(Path(saved["weekly_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())
        self.assertEqual(
            Path(saved["latest_path"]).read_text(encoding="utf-8"),
            "# 2026-04-24 周度效果复盘",
        )

    def test_build_weekly_dashboard_from_index_is_compact(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        records = [
            {
                "report_date": "2026-04-21",
                "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                "has_high_risk_playbook": True,
                "macro_bias": {
                    "黄金期货": {"label": "利多"},
                    "中证500指数": {"label": "利空"},
                },
                "forward_eval": {
                    "510500": {"t_plus_3": {"return_pct": 5.0}},
                },
            },
            {
                "report_date": "2026-04-23",
                "top_targets": [{"code": "518880", "name": "黄金ETF"}],
                "has_high_risk_playbook": False,
                "macro_bias": {
                    "黄金期货": {"label": "中性"},
                    "中证500指数": {"label": "利空"},
                },
                "forward_eval": {
                    "518880": {"t_plus_3": {"return_pct": -2.0}},
                },
            },
        ]
        index_path.write_text(
            "\n".join(json.dumps(item, ensure_ascii=False) for item in records) + "\n",
            encoding="utf-8",
        )

        dashboard = main._build_weekly_dashboard_from_index(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(dashboard)
        self.assertIn("# 2026-04-24 周度命中率看板", dashboard)
        self.assertIn("- 日报数: 2", dashboard)
        self.assertIn("- 标的样本数: 2", dashboard)
        self.assertIn("- T+3 胜率: 50.0%", dashboard)
        self.assertIn("- T+3 平均收益: 1.50%", dashboard)

    def test_persist_weekly_dashboard_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_weekly_dashboard(
            report_date="2026-04-24",
            content="# 2026-04-24 周度命中率看板",
            archive_dir=archive_dir,
        )

        self.assertIsNotNone(saved)
        self.assertTrue(Path(saved["dashboard_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())
        self.assertEqual(
            Path(saved["latest_path"]).read_text(encoding="utf-8"),
            "# 2026-04-24 周度命中率看板",
        )

    def test_build_monthly_review_and_dashboard_from_index(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "report_date": "2026-04-01",
                            "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                            "has_high_risk_playbook": True,
                            "macro_bias": {
                                "黄金期货": {"label": "利多"},
                                "中证500指数": {"label": "利空"},
                            },
                            "forward_eval": {"510500": {"t_plus_3": {"return_pct": 4.0}}},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "report_date": "2026-04-20",
                            "top_targets": [{"code": "518880", "name": "黄金ETF"}],
                            "has_high_risk_playbook": False,
                            "macro_bias": {
                                "黄金期货": {"label": "中性"},
                                "中证500指数": {"label": "利空"},
                            },
                            "forward_eval": {"518880": {"t_plus_3": {"return_pct": -1.0}}},
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        review = main._build_monthly_review_from_index(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )
        dashboard = main._build_monthly_dashboard_from_index(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIn("# 2026-04-24 月度稳定性复盘", review)
        self.assertIn("当前近30天样本", review)
        self.assertIn("# 2026-04-24 月度稳定性看板", dashboard)
        self.assertIn("- T+3 胜率: 50.0%", dashboard)

    def test_persist_monthly_review_and_dashboard_write_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        review_saved = main._persist_monthly_review(
            report_date="2026-04-24",
            content="# 2026-04-24 月度稳定性复盘",
            archive_dir=archive_dir,
        )
        dashboard_saved = main._persist_monthly_dashboard(
            report_date="2026-04-24",
            content="# 2026-04-24 月度稳定性看板",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(review_saved["review_path"]).exists())
        self.assertTrue(Path(review_saved["latest_path"]).exists())
        self.assertTrue(Path(dashboard_saved["dashboard_path"]).exists())
        self.assertTrue(Path(dashboard_saved["latest_path"]).exists())

    def test_build_strategy_group_performance_table(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "report_date": "2026-04-01",
                            "top_targets": [{"code": "518880", "name": "黄金ETF"}],
                            "forward_eval": {"518880": {"t_plus_3": {"return_pct": 4.0}}},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "report_date": "2026-04-20",
                            "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                            "forward_eval": {"510500": {"t_plus_3": {"return_pct": -1.0}}},
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        table = main._build_strategy_group_performance_table(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(table)
        self.assertIn("# 2026-04-24 策略分组表现表", table)
        self.assertIn("| 黄金相关 | 1 | 100.0% | 4.00% |", table)
        self.assertIn("| 中证500相关 | 1 | 0.0% | -1.00% |", table)

    def test_persist_strategy_group_performance_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_strategy_group_performance(
            report_date="2026-04-24",
            content="# 2026-04-24 策略分组表现表",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["table_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_build_golden_dragon_effectiveness_table(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "report_date": "2026-04-20",
                            "macro_bias": {"纳斯达克中国金龙指数": {"label": "利多"}},
                            "top_targets": [{"code": "510500", "name": "南方中证500ETF"}],
                            "forward_eval": {
                                "510500": {
                                    "t_plus_1": {"return_pct": 1.5},
                                    "t_plus_3": {"return_pct": 2.0},
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "report_date": "2026-04-21",
                            "macro_bias": {"纳斯达克中国金龙指数": {"label": "利空"}},
                            "top_targets": [{"code": "159922", "name": "中证500ETF"}],
                            "forward_eval": {
                                "159922": {
                                    "t_plus_1": {"return_pct": -1.2},
                                    "t_plus_3": {"return_pct": -0.8},
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        table = main._build_golden_dragon_effectiveness_table(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(table)
        self.assertIn("# 2026-04-24 金龙指数参考效果表", table)
        self.assertIn("| 利多 | 1 | 100.0% | 1.50% | 100.0% | 2.00% |", table)
        self.assertIn("| 利空 | 1 | 0.0% | -1.20% | 0.0% | -0.80% |", table)

    def test_persist_golden_dragon_effectiveness_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_golden_dragon_effectiveness(
            report_date="2026-04-24",
            content="# 2026-04-24 金龙指数参考效果表",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["table_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_build_overnight_signal_effectiveness_table(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        intraday_dir = Path(self.temp_dir.name) / "intraday_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        intraday_dir.mkdir(parents=True, exist_ok=True)

        (intraday_dir / "2026-04-24_market_snapshots.jsonl").write_text(
            json.dumps(
                {
                    "captured_at": "2026-04-24T08:30:00",
                    "golden_dragon": {"change_pct": 1.35},
                },
                ensure_ascii=False,
            ) + "\n",
            encoding="utf-8",
        )
        (intraday_dir / "2026-04-24_jin10_events.jsonl").write_text(
            json.dumps(
                {
                    "headline": "中东局势升级，原油拉升",
                    "time": "2026-04-24T02:10:00",
                },
                ensure_ascii=False,
            ) + "\n",
            encoding="utf-8",
        )
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            json.dumps(
                {
                    "report_date": "2026-04-24",
                    "top_targets": [{"code": "510500", "name": "中证500ETF"}],
                    "forward_eval": {
                        "510500": {
                            "t_plus_1": {"return_pct": 1.8},
                            "t_plus_3": {"return_pct": 2.6},
                        }
                    },
                },
                ensure_ascii=False,
            ) + "\n",
            encoding="utf-8",
        )

        table = main._build_overnight_signal_effectiveness_table(
            index_path,
            intraday_archive_dir=intraday_dir,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(table)
        self.assertIn("# 2026-04-24 隔夜信号效果表", table)
        self.assertIn("金龙走强+夜间事件", table)
        self.assertIn("1.80%", table)
        self.assertIn("2.60%", table)

    def test_persist_overnight_signal_effectiveness_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_overnight_signal_effectiveness(
            report_date="2026-04-24",
            content="# 2026-04-24 隔夜信号效果表",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["table_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_build_recommendation_scenario_performance_table(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        index_path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "report_date": "2026-04-01",
                            "has_high_risk_playbook": True,
                            "top_targets": [
                                {"code": "518880", "name": "黄金ETF", "operation_advice": "买入"}
                            ],
                            "forward_eval": {"518880": {"t_plus_3": {"return_pct": 4.0}}},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "report_date": "2026-04-20",
                            "has_high_risk_playbook": False,
                            "top_targets": [
                                {"code": "510500", "name": "南方中证500ETF", "operation_advice": "观望"}
                            ],
                            "forward_eval": {"510500": {"t_plus_3": {"return_pct": -1.0}}},
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        table = main._build_recommendation_scenario_performance_table(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(table)
        self.assertIn("# 2026-04-24 推荐场景表现表", table)
        self.assertIn("| 买入类 | 1 | 100.0% | 4.00% |", table)
        self.assertIn("| 观望类 | 1 | 0.0% | -1.00% |", table)
        self.assertIn("| 高风险日 | 1 | 100.0% | 4.00% |", table)
        self.assertIn("| 普通日 | 1 | 0.0% | -1.00% |", table)

    def test_persist_recommendation_scenario_performance_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_recommendation_scenario_performance(
            report_date="2026-04-24",
            content="# 2026-04-24 推荐场景表现表",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["table_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_build_recommendation_adjustment_notes(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        rows = []
        for idx in range(3):
            rows.append(
                json.dumps(
                    {
                        "report_date": f"2026-04-0{idx + 1}",
                        "has_high_risk_playbook": True,
                        "top_targets": [
                            {"code": f"51888{idx}", "name": "黄金ETF", "operation_advice": "买入"}
                        ],
                        "forward_eval": {f"51888{idx}": {"t_plus_3": {"return_pct": 3.0}}},
                    },
                    ensure_ascii=False,
                )
            )
        for idx in range(3):
            rows.append(
                json.dumps(
                    {
                        "report_date": f"2026-04-1{idx}",
                        "has_high_risk_playbook": False,
                        "top_targets": [
                            {"code": f"51050{idx}", "name": "南方中证500ETF", "operation_advice": "观望"}
                        ],
                        "forward_eval": {f"51050{idx}": {"t_plus_3": {"return_pct": -1.0}}},
                    },
                    ensure_ascii=False,
                )
            )
        index_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

        notes = main._build_recommendation_adjustment_notes(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(notes)
        self.assertIn("# 2026-04-24 推荐调整建议", notes)
        self.assertIn("买入类: 样本 3 | 胜率 100.0% | 平均收益 3.00%", notes)
        self.assertIn("观望类: 样本 3 | 胜率 0.0% | 平均收益 -1.00%", notes)

    def test_persist_recommendation_adjustment_notes_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_recommendation_adjustment_notes(
            report_date="2026-04-24",
            content="# 2026-04-24 推荐调整建议",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["notes_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_build_first_review_readiness(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"
        archive_dir.mkdir(parents=True, exist_ok=True)
        index_path = archive_dir / "daily_push_index.jsonl"
        rows = []
        for idx in range(6):
            rows.append(
                json.dumps(
                    {
                        "report_date": f"2026-04-{idx + 1:02d}",
                        "has_high_risk_playbook": idx % 2 == 0,
                        "top_targets": [
                            {"code": f"5188{idx}", "name": "黄金ETF", "operation_advice": "买入"}
                        ],
                        "forward_eval": {f"5188{idx}": {"t_plus_3": {"return_pct": 2.0}}},
                    },
                    ensure_ascii=False,
                )
            )
        for idx in range(6):
            rows.append(
                json.dumps(
                    {
                        "report_date": f"2026-04-{idx + 10:02d}",
                        "has_high_risk_playbook": idx % 2 == 1,
                        "top_targets": [
                            {"code": f"5105{idx}", "name": "南方中证500ETF", "operation_advice": "观望"}
                        ],
                        "forward_eval": {f"5105{idx}": {"t_plus_3": {"return_pct": -1.0}}},
                    },
                    ensure_ascii=False,
                )
            )
        index_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

        readiness = main._build_first_review_readiness(
            index_path,
            as_of_date=datetime(2026, 4, 24).date(),
        )

        self.assertIsNotNone(readiness)
        self.assertIn("# 2026-04-24 真复盘就绪判断", readiness)
        self.assertIn("当前场景样本总数: 24", readiness)
        self.assertIn("已达到第一次真复盘条件", readiness)

    def test_persist_first_review_readiness_writes_latest_copy(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "archive_reports"

        saved = main._persist_first_review_readiness(
            report_date="2026-04-24",
            content="# 2026-04-24 真复盘就绪判断",
            archive_dir=archive_dir,
        )

        self.assertTrue(Path(saved["readiness_path"]).exists())
        self.assertTrue(Path(saved["latest_path"]).exists())

    def test_acquire_schedule_singleton_guard_rejects_alive_pid(self) -> None:
        lock_path = Path(self.temp_dir.name) / ".dsa_schedule.lock"
        lock_path.write_text("12345", encoding="utf-8")

        with patch("main._is_process_alive", return_value=True):
            allowed = main._acquire_schedule_singleton_guard(lock_path)

        self.assertFalse(allowed)
        self.assertEqual(lock_path.read_text(encoding="utf-8"), "12345")

    def test_should_run_in_schedule_mode_respects_explicit_one_shot_flags(self) -> None:
        config = self._make_config(schedule_enabled=True)
        args = self._make_args(schedule=False, stocks="600519", no_notify=True, force_run=True)

        self.assertFalse(main._should_run_in_schedule_mode(args, config))

    def test_should_run_in_schedule_mode_does_not_block_desktop_server(self) -> None:
        config = self._make_config(schedule_enabled=True)

        self.assertFalse(
            main._should_run_in_schedule_mode(
                self._make_args(schedule=False, serve_only=True),
                config,
            )
        )
        self.assertFalse(
            main._should_run_in_schedule_mode(
                self._make_args(schedule=False, webui_only=True),
                config,
            )
        )

    def test_should_run_in_schedule_mode_uses_config_default_when_no_one_shot_flags(self) -> None:
        config = self._make_config(schedule_enabled=True)
        args = self._make_args(schedule=False)

        self.assertTrue(main._should_run_in_schedule_mode(args, config))

    def test_has_explicit_one_shot_request_detects_manual_analysis_flags(self) -> None:
        args = self._make_args(stocks="600519", no_notify=True, force_run=True)

        self.assertTrue(main._has_explicit_one_shot_request(args))

    def test_run_full_analysis_skips_market_push_when_disabled(self) -> None:
        args = self._make_args(no_notify=True, no_market_review=True)
        config = self._make_config(
            market_daily_push_enabled=False,
            market_review_enabled=False,
            stock_list=["600519"],
            single_stock_notify=False,
            analysis_delay=0,
            merge_email_notification=False,
        )
        pipeline_instance = MagicMock()
        pipeline_instance.run.return_value = []
        pipeline_instance.notifier = MagicMock()
        pipeline_instance.analyzer = MagicMock()
        pipeline_instance.search_service = MagicMock()
        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = MagicMock(return_value=pipeline_instance)
        fake_market_review_module = ModuleType("src.core.market_review")
        fake_market_review_module.run_market_review = MagicMock(return_value="")

        with patch.dict(
            sys.modules,
            {
                "src.core.pipeline": fake_pipeline_module,
                "src.core.market_review": fake_market_review_module,
            },
        ):
            with patch("main.uuid.uuid4", return_value=SimpleNamespace(hex="query-id")), \
                 patch("main._compute_trading_day_filter", return_value=(["600519"], "", False)), \
                 patch("src.daily_push_pipeline.DailyPushPipeline") as market_push_cls:
                main.run_full_analysis(config, args, ["600519"])

        market_push_cls.assert_not_called()
        pipeline_instance.run.assert_called_once()

    def test_run_full_analysis_skips_market_push_when_notifications_are_disabled(self) -> None:
        args = self._make_args(no_notify=True, no_market_review=True)
        config = self._make_config(
            market_daily_push_enabled=True,
            market_review_enabled=False,
            stock_list=["600519"],
            single_stock_notify=False,
            analysis_delay=0,
            merge_email_notification=False,
            report_type="simple",
        )
        pipeline_instance = MagicMock()
        pipeline_instance.run.return_value = []
        pipeline_instance.notifier = MagicMock()
        pipeline_instance.analyzer = MagicMock()
        pipeline_instance.search_service = MagicMock()
        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = MagicMock(return_value=pipeline_instance)
        fake_market_review_module = ModuleType("src.core.market_review")
        fake_market_review_module.run_market_review = MagicMock(return_value="")
        market_push_instance = MagicMock()
        fake_daily_push_module = ModuleType("src.daily_push_pipeline")
        fake_daily_push_module.DailyPushPipeline = MagicMock(return_value=market_push_instance)

        with patch.dict(
            sys.modules,
            {
                "src.core.pipeline": fake_pipeline_module,
                "src.core.market_review": fake_market_review_module,
                "src.daily_push_pipeline": fake_daily_push_module,
            },
        ):
            with patch("main.uuid.uuid4", return_value=SimpleNamespace(hex="query-id")), \
                 patch("main._compute_trading_day_filter", return_value=(["600519"], "", False)):
                main.run_full_analysis(config, args, ["600519"])

        fake_daily_push_module.DailyPushPipeline.assert_called_once()
        market_push_instance.build_market_summary_payload.assert_called_once()
        market_push_instance.push_market_summary.assert_not_called()
        pipeline_instance.run.assert_called_once_with(
            stock_codes=["600519"],
            dry_run=False,
            send_notification=False,
            merge_notification=False,
        )

    def test_run_full_analysis_merges_market_stock_and_preposition_sections(self) -> None:
        args = self._make_args(no_notify=False, no_market_review=False)
        setattr(args, "_scheduled_invocation", True)
        config = self._make_config(
            market_daily_push_enabled=True,
            market_daily_push_ai_enabled=True,
            market_review_enabled=True,
            stock_list=["600519"],
            single_stock_notify=False,
            analysis_delay=0,
            merge_email_notification=False,
            jin10_api_key="jin10-key",
            report_type="simple",
        )
        result = SimpleNamespace(
            code="510300",
            name="沪深300ETF",
            sentiment_score=78,
            operation_advice="买入",
            decision_type="buy",
            trend_prediction="看多",
            dashboard={
                "intelligence": {
                    "positive_catalysts": ["2026-04-24 政策预期改善", "2026-04-24 风险偏好回升"],
                    "risk_alerts": ["2026-04-24 短线波动可能放大"],
                }
            },
            get_sniper_points=lambda: {"首仓区": "3.80-3.85", "加仓区": "3.70附近"},
            get_position_advice=lambda has_position=False: "分批建仓",
            get_core_conclusion=lambda: "政策与风险偏好共振，适合提前观察建仓。",
            get_emoji=lambda: "🟢",
        )
        stock_result = SimpleNamespace(
            code="600519",
            name="贵州茅台",
            sentiment_score=74,
            operation_advice="买入",
            decision_type="buy",
            trend_prediction="看多",
            dashboard={
                "intelligence": {
                    "positive_catalysts": ["2026-04-24 消费修复预期升温"],
                    "risk_alerts": ["2026-04-24 短线追高风险"],
                }
            },
            get_sniper_points=lambda: {"首仓区": "1600-1620"},
            get_position_advice=lambda has_position=False: "回踩分批吸纳",
            get_core_conclusion=lambda: "消费龙头趋势未破，适合回踩跟踪。",
            get_emoji=lambda: "🟢",
        )

        pipeline_instance = MagicMock()
        pipeline_instance.run.return_value = [result, stock_result]
        pipeline_instance.notifier = MagicMock()
        pipeline_instance.notifier.generate_aggregate_report.return_value = "个股仪表盘正文"
        pipeline_instance.notifier.is_available.return_value = True
        pipeline_instance.notifier.send.return_value = True
        pipeline_instance.analyzer = MagicMock()
        pipeline_instance.search_service = MagicMock()

        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = MagicMock(return_value=pipeline_instance)
        fake_market_review_module = ModuleType("src.core.market_review")
        fake_market_review_module.run_market_review = MagicMock(return_value="大盘复盘补充")

        market_push_instance = MagicMock()
        market_push_instance.build_market_summary_payload.return_value = {
            "content": "市场品种正文",
            "macro_bias_items": [
                {"asset_name": "中证500指数", "label": "利多", "strength": "中", "score": 2, "reasons": ["政策支持"]},
            ],
        }
        fake_daily_push_module = ModuleType("src.daily_push_pipeline")
        fake_daily_push_module.DailyPushPipeline = MagicMock(return_value=market_push_instance)

        fake_feishu_doc_module = ModuleType("src.feishu_doc")
        fake_feishu_doc_module.FeishuDocManager = MagicMock(
            return_value=SimpleNamespace(is_configured=lambda: False)
        )

        with patch.dict(
            sys.modules,
            {
                "src.core.pipeline": fake_pipeline_module,
                "src.core.market_review": fake_market_review_module,
                "src.daily_push_pipeline": fake_daily_push_module,
                "src.feishu_doc": fake_feishu_doc_module,
            },
        ):
            with patch("main.uuid.uuid4", return_value=SimpleNamespace(hex="query-id")), \
                 patch("main._compute_trading_day_filter", return_value=(["600519"], "cn", False)):
                main.run_full_analysis(config, args, ["600519"])

        market_push_instance.build_market_summary_payload.assert_called_once()
        pipeline_instance.run.assert_called_once_with(
            stock_codes=["600519"],
            dry_run=False,
            send_notification=True,
            merge_notification=True,
        )
        pipeline_instance.notifier.send.assert_called_once()
        combined_content = pipeline_instance.notifier.send.call_args[0][0]
        self.assertIn(f"# 📌 {datetime.now().strftime('%Y-%m-%d')} 盘前总报告", combined_content)
        self.assertIn("## 今日结论", combined_content)
        self.assertIn("## 策略影响", combined_content)
        self.assertIn("## 重点标的", combined_content)
        self.assertIn("沪深300ETF(510300)", combined_content)
        self.assertIn("贵州茅台(600519)", combined_content)
        self.assertIn("中证500指数: 利多", combined_content)

    def test_run_full_analysis_persists_merged_reports_even_when_silent(self) -> None:
        args = self._make_args(no_notify=True, no_market_review=True)
        config = self._make_config(
            market_daily_push_enabled=True,
            market_daily_push_ai_enabled=False,
            market_review_enabled=False,
            stock_list=["600519"],
            single_stock_notify=False,
            analysis_delay=0,
            merge_email_notification=False,
            report_type="simple",
            external_tactical_report_path="",
        )
        result = SimpleNamespace(
            code="600519",
            name="贵州茅台",
            sentiment_score=74,
            operation_advice="买入",
            decision_type="buy",
            trend_prediction="看多",
            dashboard={"intelligence": {"positive_catalysts": [], "risk_alerts": []}},
            get_sniper_points=lambda: {},
            get_position_advice=lambda has_position=False: "观察",
            get_core_conclusion=lambda: "趋势未破，等待确认。",
            get_emoji=lambda: "🟢",
        )

        pipeline_instance = MagicMock()
        pipeline_instance.run.return_value = [result]
        pipeline_instance.notifier = MagicMock()
        pipeline_instance.notifier.is_available.return_value = True
        pipeline_instance.notifier.send.return_value = True
        pipeline_instance.analyzer = MagicMock()
        pipeline_instance.search_service = MagicMock()
        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = MagicMock(return_value=pipeline_instance)
        fake_market_review_module = ModuleType("src.core.market_review")
        fake_market_review_module.run_market_review = MagicMock(return_value="")
        market_push_instance = MagicMock()
        market_push_instance.build_market_summary_payload.return_value = {
            "content": "市场品种正文",
            "macro_bias_items": [
                {"asset_name": "中证500指数", "label": "利多", "strength": "中", "score": 2, "reasons": ["政策支持"]},
            ],
        }
        fake_daily_push_module = ModuleType("src.daily_push_pipeline")
        fake_daily_push_module.DailyPushPipeline = MagicMock(return_value=market_push_instance)
        fake_feishu_doc_module = ModuleType("src.feishu_doc")
        fake_feishu_doc_module.FeishuDocManager = MagicMock(
            return_value=SimpleNamespace(is_configured=lambda: False)
        )

        with patch.dict(
            sys.modules,
            {
                "src.core.pipeline": fake_pipeline_module,
                "src.core.market_review": fake_market_review_module,
                "src.daily_push_pipeline": fake_daily_push_module,
                "src.feishu_doc": fake_feishu_doc_module,
            },
        ):
            with patch("main.uuid.uuid4", return_value=SimpleNamespace(hex="query-id")), \
                 patch("main._compute_trading_day_filter", return_value=(["600519"], "", False)), \
                 patch("main._persist_daily_push_report", return_value={"desktop_path": "/tmp/Desktop/2026-04-25_盘前总报告.md", "archive_path": "/tmp/archive/2026-04-25_盘前总报告.md"}) as persist_report, \
                 patch("main._mirror_existing_markdown_to_desktop", side_effect=[{"desktop_path": "/tmp/dashboard.md"}, {"desktop_path": "/tmp/review.md"}]) as mirror_report, \
                 patch("main._append_daily_push_index_record", return_value="/tmp/index.jsonl"), \
                 patch("main._refresh_daily_push_index_outcomes", return_value=0), \
                 patch("main._build_weekly_review_from_index", return_value=None), \
                 patch("main._build_weekly_dashboard_from_index", return_value=None), \
                 patch("main._build_monthly_review_from_index", return_value=None), \
                 patch("main._build_monthly_dashboard_from_index", return_value=None), \
                 patch("main._build_strategy_group_performance_table", return_value=None), \
                 patch("main._build_golden_dragon_effectiveness_table", return_value=None), \
                 patch("main._build_overnight_signal_effectiveness_table", return_value=None), \
                 patch("main._build_recommendation_scenario_performance_table", return_value=None), \
                 patch("main._build_recommendation_adjustment_notes", return_value=None), \
                 patch("main._build_first_review_readiness", return_value=None), \
                 patch("main._build_metaphysical_daily_report_content", return_value=None):
                main.run_full_analysis(config, args, ["600519"])

        market_push_instance.build_market_summary_payload.assert_called_once()
        market_push_instance.push_market_summary.assert_not_called()
        persist_report.assert_called_once()
        self.assertEqual(mirror_report.call_count, 2)
        for call in mirror_report.call_args_list:
            self.assertEqual(call.kwargs["desktop_dir"], Path("/tmp/Desktop/每日分析报告"))
        pipeline_instance.notifier.send.assert_not_called()

    def test_build_prepositioning_section_links_commodity_etf_to_gold_silver_bias(self) -> None:
        commodity_etf = SimpleNamespace(
            code="518880",
            name="黄金ETF",
            sentiment_score=72,
            operation_advice="买入",
            decision_type="buy",
            dashboard={
                "intelligence": {
                    "positive_catalysts": ["避险情绪回升"],
                    "risk_alerts": [],
                }
            },
            get_sniper_points=lambda: {"首仓区": "5.10-5.15"},
            get_position_advice=lambda has_position=False: "回踩分批吸纳",
            get_core_conclusion=lambda: "商品ETF跟踪黄金主线。",
        )

        section = main._build_prepositioning_section(
            [commodity_etf],
            macro_bias_items=[
                {"asset_name": "黄金期货", "label": "利多", "trend_note": "最新转强，连续走强"},
                {"asset_name": "白银期货", "label": "利多", "trend_note": "最新转强"},
            ],
            max_items=3,
        )

        self.assertIsNotNone(section)
        self.assertIn("## 商品ETF优先观察", section)
        self.assertIn("黄金期货利多", section)
        self.assertIn("黄金期货最新转强，连续走强", section)
        self.assertIn("参考建仓: 首仓区:5.10-5.15元", section)

    def test_build_prepositioning_section_cleans_messy_sniper_points(self) -> None:
        stock = SimpleNamespace(
            code="600519",
            name="贵州茅台",
            sentiment_score=74,
            operation_advice="买入",
            decision_type="buy",
            dashboard={
                "intelligence": {
                    "positive_catalysts": ["消费修复"],
                    "risk_alerts": [],
                }
            },
            get_sniper_points=lambda: {
                "首仓区": '{"low": 1600, "high": 1620, "condition": "回踩确认"}',
                "加仓区": {"price": 1580, "reason": "支撑附近"},
                "debug": {"raw": ["无"]},
            },
            get_position_advice=lambda has_position=False: "回踩分批吸纳",
            get_core_conclusion=lambda: "消费龙头趋势未破。",
        )

        section = main._build_prepositioning_section([stock], max_items=1)

        self.assertIsNotNone(section)
        self.assertIn("参考建仓: 首仓区:1600.00-1620.00元（回踩确认）；加仓区:1580.00元（支撑附近）", section)
        self.assertNotIn('"low"', section)
        self.assertNotIn("debug", section)

    def test_build_prepositioning_section_does_not_bind_generic_commodity_etf_to_gold_bias(self) -> None:
        commodity_etf = SimpleNamespace(
            code="159981",
            name="有色ETF",
            sentiment_score=72,
            operation_advice="买入",
            decision_type="buy",
            dashboard={
                "intelligence": {
                    "positive_catalysts": ["商品板块活跃"],
                    "risk_alerts": [],
                }
            },
            get_sniper_points=lambda: {"首仓区": "0.98-1.00"},
            get_position_advice=lambda has_position=False: "分批观察",
            get_core_conclusion=lambda: "有色板块偏强。",
        )

        section = main._build_prepositioning_section(
            [commodity_etf],
            macro_bias_items=[
                {"asset_name": "黄金期货", "label": "利多", "trend_note": "最新转强，连续走强"},
                {"asset_name": "白银期货", "label": "利多", "trend_note": "最新转强"},
            ],
            max_items=3,
        )

        self.assertIsNotNone(section)
        self.assertIn("## 商品ETF优先观察", section)
        self.assertNotIn("> 宏观映射: 黄金期货利多", section)
        self.assertNotIn("> 宏观映射: 白银期货利多", section)

    def test_reload_runtime_config_preserves_process_env_overrides(self) -> None:
        self.env_path.write_text(
            "OPENAI_API_KEY=stale-file\nSCHEDULE_TIME=09:30\n",
            encoding="utf-8",
        )
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")

        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "18:00",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"OPENAI_API_KEY": "runtime-secret"},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"SCHEDULE_TIME"},
        ), patch(
            "main.get_config",
            return_value=runtime_config,
        ) as get_config_mock:
            reloaded_config = main._reload_runtime_config()
            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")

        self.assertIs(reloaded_config, runtime_config)
        get_config_mock.assert_called_once_with()

    def test_reload_env_file_values_preserves_managed_env_vars_when_read_fails(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "09:30",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"OPENAI_API_KEY", "SCHEDULE_TIME"},
        ), patch(
            "main.dotenv_values",
            side_effect=OSError("boom"),
        ):
            main._reload_env_file_values_preserving_overrides()

            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")
            self.assertEqual(
                main._RUNTIME_ENV_FILE_KEYS,
                {"OPENAI_API_KEY", "SCHEDULE_TIME"},
            )

    def test_reload_runtime_config_refreshes_env_before_resetting_singleton(self) -> None:
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        call_order = []

        def fake_reload_env() -> None:
            call_order.append("reload_env")

        def fake_reset_instance() -> None:
            call_order.append("reset_instance")

        def fake_get_config():
            call_order.append("get_config")
            return runtime_config

        with patch(
            "main._reload_env_file_values_preserving_overrides",
            side_effect=fake_reload_env,
        ), patch(
            "main.Config.reset_instance",
            side_effect=fake_reset_instance,
        ), patch(
            "main.get_config",
            side_effect=fake_get_config,
        ):
            reloaded_config = main._reload_runtime_config()

        self.assertIs(reloaded_config, runtime_config)
        self.assertEqual(call_order, ["reload_env", "reset_instance", "get_config"])

    def test_schedule_time_provider_propagates_config_read_failures(self) -> None:
        with patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=RuntimeError("boom"),
        ), patch.object(main, "_INITIAL_PROCESS_ENV", {}):
            provider = main._build_schedule_time_provider("18:00")

            with self.assertRaisesRegex(RuntimeError, "boom"):
                provider()

    def test_schedule_time_provider_respects_process_env_precedence(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "18:00"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"SCHEDULE_TIME": "18:00"},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=AssertionError("should not read .env when process env override exists"),
        ):
            provider = main._build_schedule_time_provider("09:30")

            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_clear(self) -> None:
        """When SCHEDULE_TIME is cleared/removed from config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_empty(self) -> None:
        """When SCHEDULE_TIME is empty string in config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={"SCHEDULE_TIME": "  "},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_should_catch_up_missed_daily_report_after_schedule_when_archive_missing(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "reports" / "daily_push_archive"

        self.assertTrue(
            main._should_catch_up_missed_daily_report(
                "09:40",
                now=datetime(2026, 4, 28, 10, 18),
                archive_dir=archive_dir,
            )
        )

    def test_should_not_catch_up_missed_daily_report_before_schedule_or_existing_archive(self) -> None:
        archive_dir = Path(self.temp_dir.name) / "reports" / "daily_push_archive"
        archive_dir.mkdir(parents=True)
        (archive_dir / "2026-04-28_盘前总报告.md").write_text("ok", encoding="utf-8")

        self.assertFalse(
            main._should_catch_up_missed_daily_report(
                "09:40",
                now=datetime(2026, 4, 28, 9, 30),
                archive_dir=archive_dir,
            )
        )
        self.assertFalse(
            main._should_catch_up_missed_daily_report(
                "09:40",
                now=datetime(2026, 4, 28, 10, 18),
                archive_dir=archive_dir,
            )
        )
        self.assertFalse(
            main._should_catch_up_missed_daily_report(
                "25:99",
                now=datetime(2026, 4, 28, 10, 18),
                archive_dir=archive_dir,
            )
        )

    def test_get_analysis_stock_codes_merges_stock_list_and_watchlist(self) -> None:
        config = self._make_config(
            stock_list=["600519", "159937"],
            watchlist_stock_list=["159937", "300750"],
        )

        self.assertEqual(
            main._get_analysis_stock_codes(config),
            ["600519", "159937", "300750"],
        )

    def test_schedule_mode_catches_up_missed_daily_report_when_startup_immediate_disabled(self) -> None:
        args = self._make_args(schedule=True, no_run_immediately=True)
        config = self._make_config(schedule_enabled=True, schedule_time="09:40")
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            extra_daily_tasks=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "09:40"), \
             patch("main._should_catch_up_missed_daily_report", return_value=True), \
             patch("main._acquire_schedule_singleton_guard", return_value=True), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            scheduled_call,
            {
                "schedule_time": "09:40",
                "run_immediately": False,
                "background_tasks": [],
            },
        )
        run_full_analysis.assert_called_once()

    def test_single_run_keeps_cli_stock_override(self) -> None:
        args = self._make_args(stocks="600519,000001")
        config = self._make_config(run_immediately=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_full_analysis.assert_called_once_with(config, args, ["600519", "000001"])

    def test_bootstrap_logging_persists_when_config_load_fails(self) -> None:
        """Config load failure must be logged to stderr and return exit code 1.

        Bootstrap logging is stderr-only so healthy runs never write to a
        hard-coded directory.  The error is still captured by process runners
        (e.g. GitHub Actions) that collect stderr output.
        """
        import io

        args = self._make_args()

        capture_stream = io.StringIO()
        capture_handler = logging.StreamHandler(capture_stream)
        capture_handler.setLevel(logging.DEBUG)
        capture_handler.setFormatter(logging.Formatter("%(message)s"))

        root_logger = logging.getLogger()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", side_effect=RuntimeError("config boom")):
            root_logger.addHandler(capture_handler)
            try:
                exit_code = main.main()
            finally:
                root_logger.removeHandler(capture_handler)
                capture_handler.close()

        self.assertEqual(exit_code, 1)
        output = capture_stream.getvalue()
        self.assertIn("加载配置失败", output)
        self.assertIn("config boom", output)

    def test_bootstrap_logging_failure_does_not_block_startup(self) -> None:
        """Bootstrap log dir unwritable must not prevent startup (P1 regression)."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._setup_bootstrap_logging", side_effect=OSError("read-only fs")), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_mock:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_mock.assert_called_once()

    def test_run_full_analysis_import_failure_propagates(self) -> None:
        """P1: import failures in run_full_analysis must propagate, not be swallowed."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch.dict("sys.modules", {"src.core.pipeline": None}):
            exit_code = main.main()

        self.assertEqual(exit_code, 1)

    def test_lazy_pipeline_triggers_env_bootstrap(self) -> None:
        """P2: lazy StockAnalysisPipeline access must call _bootstrap_environment."""
        # Reset the lazy descriptor cache so __get__ fires again
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False

        fake_pipeline_module = ModuleType("src.core.pipeline")
        fake_pipeline_module.StockAnalysisPipeline = type("FakePipeline", (), {})

        with patch("main._bootstrap_environment", wraps=main._bootstrap_environment) as mock_boot, \
             patch.dict(sys.modules, {"src.core.pipeline": fake_pipeline_module}):
            try:
                _ = main.StockAnalysisPipeline
            except Exception:
                pass
            mock_boot.assert_called()

        # Cleanup: reset state
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False


if __name__ == "__main__":
    unittest.main()
