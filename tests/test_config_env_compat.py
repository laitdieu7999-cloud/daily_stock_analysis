# -*- coding: utf-8 -*-
"""Tests for backward-compatible config env aliases and TickFlow loading."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.config import Config, setup_env


class ConfigEnvCompatibilityTestCase(unittest.TestCase):
    def tearDown(self):
        Config.reset_instance()

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_load_from_env_reads_tickflow_api_key(
        self, _mock_parse_litellm_yaml, _mock_setup_env
    ):
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "TICKFLOW_API_KEY": "tf-secret",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertEqual(config.tickflow_api_key, "tf-secret")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_load_from_env_reads_separate_watchlist_stock_list(
        self, _mock_parse_litellm_yaml, _mock_setup_env
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "STOCK_LIST=600519,159937\n"
                "WATCHLIST_STOCK_LIST=300750,159937,01810,ndx100\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"ENV_FILE": str(env_path)}, clear=True):
                config = Config._load_from_env()

        self.assertEqual(config.stock_list, ["600519", "159937"])
        self.assertEqual(config.watchlist_stock_list, ["300750", "159937", "01810", "NDX100"])
        self.assertEqual(
            config.get_analysis_stock_list(),
            ["600519", "159937", "300750", "01810", "NDX100"],
        )
        self.assertEqual(config.stock_intraday_watchlist_buy_start_time, "14:30")
        self.assertEqual(config.stock_intraday_watchlist_buy_end_time, "14:55")
        self.assertEqual(config.stock_intraday_watchlist_max_stop_loss_distance_pct, 3.5)
        self.assertEqual(config.stock_intraday_holding_cooldown_minutes, 30)
        self.assertTrue(config.stock_intraday_self_check_enabled)
        self.assertEqual(config.stock_intraday_self_check_time, "09:25")
        self.assertTrue(config.stock_intraday_replay_ledger_enabled)
        self.assertEqual(config.stock_intraday_watchlist_daily_limit, 1)
        self.assertEqual(config.stock_intraday_systemic_batch_threshold, 3)
        self.assertEqual(config.stock_intraday_bad_tick_max_abs_change_pct, 25.0)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_load_from_env_reads_intraday_routing_guardrails(
        self, _mock_parse_litellm_yaml, _mock_setup_env
    ):
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "STOCK_INTRADAY_HOLDING_COOLDOWN_MINUTES": "45",
                "STOCK_INTRADAY_WATCHLIST_DAILY_LIMIT": "2",
                "STOCK_INTRADAY_SYSTEMIC_BATCH_THRESHOLD": "4",
                "STOCK_INTRADAY_BAD_TICK_MAX_ABS_CHANGE_PCT": "18.5",
                "STOCK_INTRADAY_SELF_CHECK_ENABLED": "false",
                "STOCK_INTRADAY_SELF_CHECK_TIME": "09:24",
                "STOCK_INTRADAY_REPLAY_LEDGER_ENABLED": "false",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertEqual(config.stock_intraday_holding_cooldown_minutes, 45)
        self.assertFalse(config.stock_intraday_self_check_enabled)
        self.assertEqual(config.stock_intraday_self_check_time, "09:24")
        self.assertFalse(config.stock_intraday_replay_ledger_enabled)
        self.assertEqual(config.stock_intraday_watchlist_daily_limit, 2)
        self.assertEqual(config.stock_intraday_systemic_batch_threshold, 4)
        self.assertEqual(config.stock_intraday_bad_tick_max_abs_change_pct, 18.5)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_load_from_env_keeps_default_behavior_without_tickflow_api_key(
        self, _mock_parse_litellm_yaml, _mock_setup_env
    ):
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertIsNone(config.tickflow_api_key)
        self.assertEqual(
            config.realtime_source_priority,
            "tencent,akshare_sina,efinance,akshare_em",
        )

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_schedule_run_immediately_falls_back_to_legacy_run_immediately(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "RUN_IMMEDIATELY": "false",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertFalse(config.schedule_run_immediately)
        self.assertFalse(config.run_immediately)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_schedule_run_immediately_prefers_schedule_specific_setting(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "RUN_IMMEDIATELY": "false",
            "SCHEDULE_RUN_IMMEDIATELY": "true",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertTrue(config.schedule_run_immediately)
        self.assertFalse(config.run_immediately)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_empty_legacy_run_immediately_stays_false_when_schedule_alias_is_unset(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "RUN_IMMEDIATELY": "",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertFalse(config.schedule_run_immediately)
        self.assertFalse(config.run_immediately)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_empty_schedule_run_immediately_stays_false_without_falling_back(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "RUN_IMMEDIATELY": "true",
            "SCHEDULE_RUN_IMMEDIATELY": "",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertFalse(config.schedule_run_immediately)
        self.assertTrue(config.run_immediately)

    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_blank_schedule_time_falls_back_to_default(
        self,
        _mock_parse_yaml,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "STOCK_LIST=600519",
                        "SCHEDULE_TIME=",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                },
                clear=True,
            ):
                config = Config._load_from_env()

        self.assertEqual(config.schedule_time, "18:00")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_close_reminder_env_is_loaded(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "CLOSE_REMINDER_ENABLED": "true",
                "CLOSE_REMINDER_TIME": "15:20",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertTrue(config.close_reminder_enabled)
        self.assertEqual(config.close_reminder_time, "15:20")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_premarket_health_check_env_is_loaded(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "PREMARKET_HEALTH_CHECK_ENABLED": "true",
                "PREMARKET_HEALTH_CHECK_TIME": "08:55",
                "PREMARKET_HEALTH_CHECK_PUSH_OK": "false",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertTrue(config.premarket_health_check_enabled)
        self.assertEqual(config.premarket_health_check_time, "08:55")
        self.assertFalse(config.premarket_health_check_push_ok)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_custom_extension_toggles_are_loaded(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        with patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "MARKET_DAILY_PUSH_ENABLED": "false",
                "MARKET_DAILY_PUSH_AI_ENABLED": "false",
                "ENABLE_METAPHYSICAL_FEATURES": "true",
                "METAPHYSICAL_CACHE_DIR": "./tmp/meta-cache",
                "NIGHTLY_MARKET_OUTLOOK_ENABLED": "true",
                "NIGHTLY_MARKET_OUTLOOK_TIME": "21:55",
                "NIGHTLY_MARKET_OUTLOOK_AI_ENABLED": "false",
                "NIGHTLY_MARKET_OUTLOOK_TIMEOUT_SECONDS": "30",
                "JIN10_API_KEY": "jin10-secret",
                "JIN10_X_TOKEN": "jin10-x-token",
            },
            clear=True,
        ):
            config = Config._load_from_env()

        self.assertFalse(config.market_daily_push_enabled)
        self.assertFalse(config.market_daily_push_ai_enabled)
        self.assertTrue(config.enable_metaphysical_features)
        self.assertEqual(config.metaphysical_cache_dir, "./tmp/meta-cache")
        self.assertTrue(config.nightly_market_outlook_enabled)
        self.assertEqual(config.nightly_market_outlook_time, "21:55")
        self.assertFalse(config.nightly_market_outlook_ai_enabled)
        self.assertEqual(config.nightly_market_outlook_timeout_seconds, 30)
        self.assertEqual(config.jin10_api_key, "jin10-secret")
        self.assertEqual(config.jin10_x_token, "jin10-x-token")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_report_language_prefers_preexisting_process_env_over_env_file(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("REPORT_LANGUAGE=zh\n", encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                    "REPORT_LANGUAGE": "en",
                },
                clear=True,
            ):
                config = Config._load_from_env()

        self.assertEqual(config.report_language, "en")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_report_language_uses_env_file_when_process_env_is_absent(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("REPORT_LANGUAGE=en\n", encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                },
                clear=True,
            ):
                config = Config._load_from_env()

        self.assertEqual(config.report_language, "en")

    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_runtime_mutable_keys_reload_from_updated_env_file_after_runtime_refresh(
        self,
        _mock_parse_yaml,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "STOCK_LIST=600519",
                        "SCHEDULE_ENABLED=false",
                        "SCHEDULE_TIME=18:00",
                        "RUN_IMMEDIATELY=true",
                        "SCHEDULE_RUN_IMMEDIATELY=false",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                    "STOCK_LIST": "600519",
                    "SCHEDULE_ENABLED": "false",
                    "SCHEDULE_TIME": "18:00",
                    "RUN_IMMEDIATELY": "true",
                    "SCHEDULE_RUN_IMMEDIATELY": "false",
                },
                clear=True,
            ):
                Config._load_from_env()
                env_path.write_text(
                    "\n".join(
                        [
                            "STOCK_LIST=300750,TSLA",
                            "SCHEDULE_ENABLED=true",
                            "SCHEDULE_TIME=09:30",
                            "RUN_IMMEDIATELY=false",
                            "SCHEDULE_RUN_IMMEDIATELY=true",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                Config.reset_instance()
                setup_env(override=True)
                config = Config._load_from_env()

        self.assertEqual(config.stock_list, ["300750", "TSLA"])
        self.assertTrue(config.schedule_enabled)
        self.assertEqual(config.schedule_time, "09:30")
        self.assertFalse(config.run_immediately)
        self.assertTrue(config.schedule_run_immediately)

    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_runtime_mutable_keys_prefer_process_env_when_values_differ(
        self,
        _mock_parse_yaml,
    ) -> None:
        """When process env explicitly sets a WEBUI-mutable key to a value
        that differs from .env (e.g. via docker-compose ``environment:``),
        the process env must win because ``_capture_bootstrap_runtime_env_overrides``
        runs before dotenv loads and the mismatch proves an intentional override.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "STOCK_LIST=300750,TSLA",
                        "SCHEDULE_ENABLED=true",
                        "SCHEDULE_TIME=09:30",
                        "RUN_IMMEDIATELY=false",
                        "SCHEDULE_RUN_IMMEDIATELY=true",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                    "STOCK_LIST": "600519,000001",
                    "SCHEDULE_ENABLED": "false",
                    "SCHEDULE_TIME": "18:00",
                    "RUN_IMMEDIATELY": "true",
                    "SCHEDULE_RUN_IMMEDIATELY": "false",
                },
                clear=True,
            ):
                config = Config._load_from_env()

        # Explicit process env overrides win when values differ from .env
        self.assertEqual(config.stock_list, ["600519", "000001"])
        self.assertFalse(config.schedule_enabled)
        self.assertEqual(config.schedule_time, "18:00")
        self.assertTrue(config.run_immediately)
        self.assertFalse(config.schedule_run_immediately)

    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_runtime_mutable_keys_use_process_env_when_absent_from_file(
        self,
        _mock_parse_yaml,
    ) -> None:
        """When a WEBUI-mutable key exists only in process env (not in .env),
        it IS a genuine explicit override and must be honoured.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            # .env has no STOCK_LIST or SCHEDULE_* keys at all
            env_path.write_text("LOG_LEVEL=INFO\n", encoding="utf-8")

            with patch.dict(
                os.environ,
                {
                    "ENV_FILE": str(env_path),
                    "STOCK_LIST": "600519,000001",
                },
                clear=True,
            ):
                config = Config._load_from_env()

        self.assertEqual(config.stock_list, ["600519", "000001"])

    def test_parse_report_language_accepts_known_alias_without_warning(self) -> None:
        with self.assertNoLogs("src.config", level="WARNING"):
            parsed = Config._parse_report_language("zh-cn")

        self.assertEqual(parsed, "zh")

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_invalid_numeric_env_values_fall_back_to_defaults(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "AGENT_ORCHESTRATOR_TIMEOUT_S": "oops",
            "NEWS_MAX_AGE_DAYS": "bad",
            "MAX_WORKERS": "",
            "WEBUI_PORT": "invalid",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertEqual(config.agent_orchestrator_timeout_s, 600)
        self.assertEqual(config.news_max_age_days, 3)
        self.assertEqual(config.max_workers, 3)
        self.assertEqual(config.webui_port, 8000)

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_stock_email_groups_support_case_insensitive_env_names(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        env = {
            "STOCK_LIST": "600519,300750",
            "Stock_Group_1": "600519",
            "Email_Group_1": "user1@example.com",
            "stock_group_2": "300750",
            "email_group_2": "user2@example.com",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        self.assertEqual(
            config.stock_email_groups,
            [
                (["600519"], ["user1@example.com"]),
                (["300750"], ["user2@example.com"]),
            ],
        )

    @patch("src.config.setup_env")
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def test_stock_email_groups_normalize_codes_at_parse_time(
        self,
        _mock_parse_yaml,
        _mock_setup_env,
    ) -> None:
        """STOCK_GROUP codes are canonicalized at parse time so that
        runtime email routing matches the same equivalence used in
        validate_structured()."""
        env = {
            "STOCK_LIST": "600519,HK00700",
            "STOCK_GROUP_1": "SH600519,1810.HK",
            "EMAIL_GROUP_1": "user@example.com",
        }

        with patch.dict(os.environ, env, clear=True):
            config = Config._load_from_env()

        stocks, emails = config.stock_email_groups[0]
        self.assertEqual(stocks, ["600519", "HK01810"])
        self.assertEqual(emails, ["user@example.com"])


if __name__ == "__main__":
    unittest.main()
