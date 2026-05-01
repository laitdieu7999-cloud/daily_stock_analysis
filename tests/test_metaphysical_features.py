# -*- coding: utf-8 -*-
"""Tests for metaphysical research feature package structure."""

import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from src.models import metaphysical_features as legacy
from src.models.metaphysical import service
from src.models.metaphysical.adapter import (
    attach_metaphysical_features,
    attach_next_production_metaphysical_features,
    finalize_next_production_candidate_frame,
)
from src.models.metaphysical.explainer import summarize_trigger_frame, summarize_trigger_row
from src.models.metaphysical.astro import get_pluto_lon
from src.models.metaphysical.gann import extract_key_gann_levels, gann_square_of_9, gann_time_square
from src.models.metaphysical.learning import (
    backfill_learning_snapshot_outcomes,
    build_daily_learning_snapshot,
    build_metaphysical_accuracy_dashboard,
    build_feishu_push_accuracy_dashboard,
    build_governance_run_record,
    build_lifecycle_run_record,
    build_stage_performance_record,
    build_daily_governance_summary,
    build_weekly_governance_summary,
    build_version_switch_change_request,
    build_version_switch_confirmation_draft,
    build_version_switch_execution_plan,
    build_version_switch_proposal,
    compute_learning_outcomes_from_prices,
    build_training_run_record,
    evaluate_governance_action,
    evaluate_candidate_promotion_readiness,
    evaluate_governance_stage_flow,
    evaluate_release_lifecycle,
    evaluate_stage_promotion_readiness,
    evaluate_stage_guardrail,
    latest_governance_run,
    latest_lifecycle_run,
    latest_stage_performance_run,
    latest_version_switch_proposal,
    render_daily_governance_summary,
    render_feishu_push_accuracy_dashboard,
    render_metaphysical_accuracy_dashboard,
    render_weekly_governance_summary,
    record_daily_learning_snapshot,
    record_governance_run,
    record_lifecycle_run,
    record_stage_performance_run,
    record_version_switch_proposal,
    record_training_run,
    select_matured_learning_samples,
    summarize_stage_performance_window,
    summarize_training_run_window,
)
from src.models.metaphysical.warehouse import (
    archive_candidate_dataset,
    archive_price_history,
    ensure_metaphysical_warehouse,
)
from src.models.metaphysical.resonance import (
    build_next_production_backtest_features,
    build_resonance_backtest_features,
)
from src.models.metaphysical.signals import compute_triggers, is_hard_aspect, is_soft_aspect
from src.models.metaphysical.strategy import generate_resonance_strategy_summary
from src.models.metaphysical.feature_sets import (
    AUTHOR_TIMING_FEATURES,
    CURRENT_PRODUCTION_CANDIDATE_FEATURES,
    EXPERIMENTAL_AUTHOR_CANDIDATE_FEATURES,
    LONG_CYCLE_FEATURES,
    MACRO_EVENT_METAPHYSICAL_FEATURES,
    NEXT_PRODUCTION_CANDIDATE_FEATURES,
    NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES,
    REGIME_WEIGHTED_AUTHOR_FEATURES,
    SLIM_AUTHOR_CANDIDATE_FEATURES,
)
from src.models.metaphysical.model_defaults import (
    NEXT_PRODUCTION_MODEL_DEFAULTS,
    get_next_production_model_defaults,
)
from src.models.metaphysical.model_backtest import (
    apply_tactical_report_signal_overlay,
    apply_next_production_position_sizing,
    build_next_production_signal_frame,
    latest_cached_next_production_signal,
    latest_next_production_signal,
    latest_next_production_signal_with_report_overlay,
    next_production_probability_cache_path,
    resolve_next_production_model_params,
    summarize_next_production_position_sizing,
)
from src.models.metaphysical.report_ingest import (
    assess_tactical_report_freshness,
    build_tactical_report_optimization_notes,
    compare_tactical_report_to_model_signal,
    extract_tactical_report_date_iso,
    parse_tactical_report_text,
    sync_tactical_report_cache,
    sync_tactical_report_text,
)
from src.models.metaphysical.regime import (
    AUTHOR_FEATURE_REGIME_WEIGHTS,
    apply_author_regime_weights,
    author_feature_weight,
    batch_author_regimes,
    classify_author_regime,
    infer_dynamic_author_regimes,
)
from src.models.metaphysical.theory_compare import (
    get_author_theory_catalog,
    summarize_author_theory_coverage,
)
from src.models.metaphysical.time_law import mars_events_in_range, solar_term_date, solar_terms_in_range, ten_gods_for, year_ganzhi
from src.models.metaphysical.time_law import (
    anniversary_cycle_distance,
    batch_long_cycle_features,
    nearest_lunar_phase_distance,
    saturn_jupiter_cycle_distance,
    solar_term_distance,
)
from src.models.metaphysical.time_law import uranus_cycle_distance, uranus_retrograde_state
from src.models.metaphysical.trend_law import (
    bb_breakout_strength,
    batch_month_turning_points,
    bollinger_bands,
    bollinger_state,
    is_bb_breakout,
    is_bb_squeeze,
    is_triple_resonance,
    is_volatility_plus_signal,
    month_turning_point_score,
    triple_resonance_score,
    volatility_plus_score,
)


class TestMetaphysicalAspectHelpers(unittest.TestCase):
    def test_hard_aspect_detects_square(self):
        self.assertEqual(is_hard_aspect(10, 100, orb=2), 1)

    def test_soft_aspect_detects_trine(self):
        self.assertEqual(is_soft_aspect(0, 120, orb=2), 1)


class TestMetaphysicalSignals(unittest.TestCase):
    def test_compute_triggers_sets_expected_columns(self):
        dates = pd.date_range("2026-04-20", periods=2, freq="D")
        df = pd.DataFrame(
            [
                {"tiangan": "壬", "dizhi": "子", "tg_dz": "壬子"},
                {"tiangan": "庚", "dizhi": "辰", "tg_dz": "庚申"},
            ],
            index=dates,
        )
        planet_lons = {
            "mars": [0, 0],
            "uranus": [90, 90],
            "saturn": [0, 0],
            "pluto": [180, 10],
            "neptune": [10, 180],
            "jupiter": [180, 90],
            "venus": [88, 10],
            "sun": [10, 92],
        }

        result = compute_triggers(df, planet_lons)

        self.assertEqual(result.loc[dates[0], "csi500_liquidity_crisis"], 1)
        self.assertEqual(result.loc[dates[1], "gold_panic_rush"], 1)
        self.assertIn("gold_currency_crisis", result.columns)


class TestMetaphysicalService(unittest.TestCase):
    def test_build_if_disabled_returns_none(self):
        result = service.build_metaphysical_features_if_enabled(
            pd.date_range("2026-04-20", periods=1, freq="D"),
            enabled=False,
        )
        self.assertIsNone(result)

    @patch("src.models.metaphysical.service.dependencies_available", return_value=False)
    def test_build_if_enabled_skips_when_dependencies_missing(self, _mock_available):
        result = service.build_metaphysical_features_if_enabled(
            pd.date_range("2026-04-20", periods=1, freq="D"),
            enabled=True,
            allow_missing_dependencies=True,
        )
        self.assertIsNone(result)

    @patch("src.models.metaphysical.service.dependencies_available", return_value=True)
    @patch("src.models.metaphysical.service.build_metaphysical_features")
    def test_build_if_enabled_delegates_when_ready(self, mock_build, _mock_available):
        mock_build.return_value = pd.DataFrame({"foo": [1]})
        dates = pd.date_range("2026-04-20", periods=1, freq="D")

        result = service.build_metaphysical_features_if_enabled(
            dates,
            enabled=True,
            cache_dir="./tmp/meta",
        )

        mock_build.assert_called_once_with(dates, cache_dir="./tmp/meta")
        self.assertEqual(list(result.columns), ["foo"])


class TestMetaphysicalExplainer(unittest.TestCase):
    def test_summarize_trigger_row_returns_active_labels(self):
        labels = summarize_trigger_row(
            {
                "csi500_liquidity_crisis": 1,
                "gold_macro_shock": 1,
                "gold_currency_crisis": 0,
            }
        )
        self.assertEqual(labels, ["中证500流动性危机触发", "黄金宏观冲击触发"])

    def test_summarize_trigger_frame_returns_human_summary(self):
        dates = pd.date_range("2026-04-20", periods=2, freq="D")
        df = pd.DataFrame(
            [
                {"csi500_liquidity_crisis": 1, "gold_macro_shock": 0},
                {"csi500_liquidity_crisis": 0, "gold_macro_shock": 0},
            ],
            index=dates,
        )
        records = summarize_trigger_frame(df)
        self.assertEqual(records[0]["summary"], "中证500流动性危机触发")
        self.assertEqual(records[1]["summary"], "无触发")


class TestMetaphysicalCompatibility(unittest.TestCase):
    def test_legacy_module_reexports_service_entrypoint(self):
        self.assertIs(legacy.build_metaphysical_features_if_enabled, service.build_metaphysical_features_if_enabled)

    def test_pluto_lon_interpolates_inside_supported_range(self):
        value = get_pluto_lon(pd.Timestamp("2025-05-01"))
        self.assertGreater(value, 304.0)
        self.assertLess(value, 304.5)

    def test_legacy_module_reexports_time_law_helper(self):
        self.assertIsNotNone(legacy.year_ganzhi)

    def test_feature_sets_keep_macro_pool_separate_from_current_candidates(self):
        overlap = set(CURRENT_PRODUCTION_CANDIDATE_FEATURES) & set(MACRO_EVENT_METAPHYSICAL_FEATURES)
        self.assertEqual(overlap, set())

    def test_long_cycle_features_are_reserved_for_macro_pool(self):
        self.assertTrue(set(LONG_CYCLE_FEATURES).issubset(set(MACRO_EVENT_METAPHYSICAL_FEATURES)))
        self.assertTrue(set(LONG_CYCLE_FEATURES).isdisjoint(set(CURRENT_PRODUCTION_CANDIDATE_FEATURES)))

    def test_author_timing_features_remain_outside_current_production_pool(self):
        self.assertTrue(set(AUTHOR_TIMING_FEATURES).isdisjoint(set(CURRENT_PRODUCTION_CANDIDATE_FEATURES)))

    def test_experimental_author_candidate_pool_extends_current_production_pool(self):
        self.assertTrue(set(CURRENT_PRODUCTION_CANDIDATE_FEATURES).issubset(set(EXPERIMENTAL_AUTHOR_CANDIDATE_FEATURES)))
        self.assertTrue(set(AUTHOR_TIMING_FEATURES).issubset(set(EXPERIMENTAL_AUTHOR_CANDIDATE_FEATURES)))
        self.assertTrue(set(LONG_CYCLE_FEATURES).issubset(set(EXPERIMENTAL_AUTHOR_CANDIDATE_FEATURES)))

    def test_slim_author_candidate_pool_keeps_only_selected_author_factors(self):
        self.assertTrue(set(CURRENT_PRODUCTION_CANDIDATE_FEATURES).issubset(set(SLIM_AUTHOR_CANDIDATE_FEATURES)))
        self.assertIn("uranus_retrograde_boundary_distance", SLIM_AUTHOR_CANDIDATE_FEATURES)
        self.assertIn("uranus_cycle_84_distance", SLIM_AUTHOR_CANDIDATE_FEATURES)
        self.assertIn("saturn_jupiter_cycle_distance", SLIM_AUTHOR_CANDIDATE_FEATURES)
        self.assertIn("volatility_plus_score", SLIM_AUTHOR_CANDIDATE_FEATURES)
        self.assertNotIn("month_turning_point_score", SLIM_AUTHOR_CANDIDATE_FEATURES)

    def test_next_production_candidate_pool_matches_slim_pool(self):
        self.assertEqual(NEXT_PRODUCTION_CANDIDATE_FEATURES, SLIM_AUTHOR_CANDIDATE_FEATURES)

    def test_next_production_weighted_pool_uses_weighted_author_features(self):
        self.assertTrue(set(CURRENT_PRODUCTION_CANDIDATE_FEATURES).issubset(set(NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES)))
        self.assertTrue(set(REGIME_WEIGHTED_AUTHOR_FEATURES).issubset(set(NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES)))

    def test_next_production_model_defaults_match_current_best_candidate(self):
        self.assertEqual(NEXT_PRODUCTION_MODEL_DEFAULTS["caution_threshold"], 0.40)
        self.assertEqual(NEXT_PRODUCTION_MODEL_DEFAULTS["risk_off_threshold"], 0.60)
        self.assertEqual(NEXT_PRODUCTION_MODEL_DEFAULTS["min_train_days"], 756)
        self.assertEqual(NEXT_PRODUCTION_MODEL_DEFAULTS["retrain_every"], 42)

    def test_next_production_model_defaults_helper_returns_copy(self):
        defaults = get_next_production_model_defaults()
        defaults["caution_threshold"] = 0.99
        self.assertEqual(NEXT_PRODUCTION_MODEL_DEFAULTS["caution_threshold"], 0.40)
        self.assertEqual(legacy.NEXT_PRODUCTION_MODEL_DEFAULTS["risk_off_threshold"], 0.60)

    def test_resolve_next_production_model_params_merges_overrides(self):
        params = resolve_next_production_model_params(retrain_every=21)
        self.assertEqual(params["min_train_days"], 756)
        self.assertEqual(params["retrain_every"], 21)

    def test_apply_next_production_position_sizing_uses_shared_three_bucket_mapping(self):
        frame = pd.DataFrame({"tail_risk_probability": [0.20, 0.45, 0.70]})
        result = apply_next_production_position_sizing(frame)
        self.assertEqual(result["position"].tolist(), [1.0, 0.5, 0.0])
        self.assertEqual(result["position_regime"].tolist(), ["full_risk", "caution", "risk_off"])

    def test_summarize_next_production_position_sizing_counts_buckets(self):
        frame = pd.DataFrame({"position": [1.0, 0.5, 0.0, 1.0]})
        summary = summarize_next_production_position_sizing(frame)
        self.assertEqual(summary["full_risk_days"], 2)
        self.assertEqual(summary["caution_days"], 1)
        self.assertEqual(summary["risk_off_days"], 1)

    def test_build_next_production_signal_frame_returns_daily_signal_columns(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21", "2026-04-22"]),
                "tail_risk_probability": [0.20, 0.45, 0.70],
            }
        )
        result = build_next_production_signal_frame(frame)
        self.assertEqual(
            result[["position_regime", "action"]].values.tolist(),
            [["full_risk", "hold_or_add"], ["caution", "reduce"], ["risk_off", "risk_off"]],
        )

    def test_latest_next_production_signal_returns_latest_plain_dict(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "tail_risk_probability": [0.20, 0.70],
            }
        )
        result = latest_next_production_signal(frame)
        self.assertEqual(result["signal_date"], "2026-04-21")
        self.assertEqual(result["position_regime"], "risk_off")
        self.assertEqual(result["action"], "risk_off")

    def test_parse_tactical_report_text_extracts_daily_report_fields(self):
        text = (
            "报告日期：2026年4月24日 (周五)\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "现货点位：8052.18 (-1.85%)，已有有效跌破保力加中轨。\n"
            "VWAP 成本线：分时价格始终受压于 8120 附近，机构大单呈净流出状态。\n"
            "江恩引力点：离核心锚点 7571 尚有约 500 点空间。\n"
            "全球流动性：市场处于“去杠杆”阶段，避险资金向美元现金归笼。\n"
            "已投入 30 万预备金布局 500ETF 沽 5月 7800/8000 合约。\n"
            "黄金 ETF：维持 10% 底仓。\n"
            "高成长赛道 ETF：暂停加仓。\n"
            "宁可踏空，绝不套牢。\n"
        )
        result = parse_tactical_report_text(text)
        self.assertEqual(result["report_date_text"], "2026年4月24日")
        self.assertEqual(result["core_stance"], "流动性风险共振，黑天鹅预警生效")
        self.assertEqual(result["csi500_spot"], 8052.18)
        self.assertEqual(result["vwap_pressure_level"], 8120.0)
        self.assertEqual(result["gann_anchor_level"], 7571.0)
        self.assertEqual(result["liquidity_risk_resonance"], 1)
        self.assertEqual(result["black_swan_warning"], 1)
        self.assertEqual(result["usd_cash_flight"], 1)

    def test_extract_tactical_report_date_iso_handles_iso_and_cn_date(self):
        text = (
            "2026-04-27 战区中枢：战略重心转移与资产架构重塑报告\n"
            "日期：2026年04月27日 (收盘)\n"
        )
        self.assertEqual(extract_tactical_report_date_iso(text), "2026-04-27")

    def test_assess_tactical_report_freshness_marks_stale_report(self):
        text = (
            "2026-04-27 战区中枢：战略重心转移与资产架构重塑报告\n"
            "日期：2026年04月27日 (收盘)\n"
        )
        result = assess_tactical_report_freshness(text, expected_date_iso="2026-04-28")
        self.assertFalse(result["is_fresh"])
        self.assertTrue(result["is_stale"])
        self.assertEqual(result["report_date_iso"], "2026-04-27")

    def test_compare_tactical_report_to_model_signal_detects_conflict(self):
        report = {
            "liquidity_risk_resonance": 1,
            "black_swan_warning": 1,
            "physical_blockade": 1,
            "usd_cash_flight": 1,
            "deleveraging_phase": 1,
            "institutional_outflow": 1,
        }
        signal = {"position_regime": "full_risk"}
        result = compare_tactical_report_to_model_signal(report, signal)
        self.assertEqual(result["alignment"], "conflict")
        self.assertEqual(result["report_risk_score"], 6)

    def test_apply_tactical_report_signal_overlay_downgrades_to_caution_on_conflict(self):
        signal = {
            "signal_date": "2026-04-24",
            "tail_risk_probability": 0.17,
            "position": 1.0,
            "position_regime": "full_risk",
            "action": "hold_or_add",
        }
        report = {
            "core_stance": "流动性风险共振，黑天鹅预警生效",
            "liquidity_risk_resonance": 1,
            "black_swan_warning": 1,
            "deleveraging_phase": 1,
        }
        result = apply_tactical_report_signal_overlay(signal, report_summary=report)
        self.assertTrue(result["overlay_active"])
        self.assertEqual(result["raw_position_regime"], "full_risk")
        self.assertEqual(result["position_regime"], "caution")
        self.assertEqual(result["action"], "reduce")

    def test_apply_tactical_report_signal_overlay_forces_risk_off_on_severe_pair(self):
        signal = {
            "signal_date": "2026-04-24",
            "tail_risk_probability": 0.17,
            "position": 1.0,
            "position_regime": "full_risk",
            "action": "hold_or_add",
        }
        report = {
            "core_stance": "黑天鹅预警生效，霍尔木兹海峡物理封锁持续",
            "liquidity_risk_resonance": 1,
            "black_swan_warning": 1,
            "physical_blockade": 1,
            "deleveraging_phase": 1,
            "institutional_outflow": 1,
            "usd_cash_flight": 1,
        }
        result = apply_tactical_report_signal_overlay(signal, report_summary=report)
        self.assertTrue(result["overlay_active"])
        self.assertEqual(result["position_regime"], "risk_off")
        self.assertEqual(result["action"], "risk_off")
        self.assertEqual(result["report_risk_score"], 6)

    def test_latest_next_production_signal_with_report_overlay_returns_adjusted_latest_signal(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "tail_risk_probability": [0.20, 0.17],
            }
        )
        text = (
            "报告日期：2026年4月24日\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
            "全球流动性：市场处于去杠杆阶段，避险资金向美元现金归笼。\n"
            "机构大单呈净流出状态。\n"
        )
        result = latest_next_production_signal_with_report_overlay(
            frame,
            report_text=text,
            expected_report_date="2026-04-24",
        )
        self.assertEqual(result["raw_position_regime"], "full_risk")
        self.assertEqual(result["position_regime"], "risk_off")
        self.assertTrue(result["overlay_active"])

    def test_latest_next_production_signal_with_report_overlay_skips_stale_report(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "tail_risk_probability": [0.20, 0.17],
            }
        )
        text = (
            "2026-04-27 战区中枢：战略重心转移与资产架构重塑报告\n"
            "日期：2026年04月27日 (收盘)\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
        )
        result = latest_next_production_signal_with_report_overlay(
            frame,
            report_text=text,
            expected_report_date="2026-04-28",
        )
        self.assertEqual(result["raw_position_regime"], "full_risk")
        self.assertEqual(result["position_regime"], "full_risk")
        self.assertFalse(result["overlay_active"])
        self.assertTrue(result["report_overlay_skipped"])
        self.assertFalse(result["report_is_fresh"])
        self.assertEqual(result["report_alignment"], "stale_report")

    def test_latest_cached_next_production_signal_loads_cache_and_applies_overlay(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "tail_risk_probability": [0.20, 0.17],
            }
        )
        report_text = (
            "报告日期：2026年4月24日\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
            "全球流动性：市场处于去杠杆阶段，避险资金向美元现金归笼。\n"
            "机构大单呈净流出状态。\n"
        )
        with TemporaryDirectory() as tmpdir:
            cache_path = next_production_probability_cache_path(
                tmpdir,
                symbol="510500.SS",
                start="2016-01-01",
                end="2026-04-20",
                min_train_days=756,
                retrain_every=42,
            )
            Path(tmpdir).mkdir(parents=True, exist_ok=True)
            frame.to_pickle(cache_path)
            result = latest_cached_next_production_signal(
                cache_dir=tmpdir,
                symbol="510500.SS",
                start="2016-01-01",
                end="2026-04-20",
                report_text=report_text,
                expected_report_date="2026-04-24",
            )
        self.assertTrue(result["overlay_active"])
        self.assertEqual(result["position_regime"], "risk_off")
        self.assertTrue(result["cache_path"].endswith("min756_retrain42.pkl"))

    def test_sync_tactical_report_cache_writes_target_and_archive(self):
        text = (
            "2026-04-27 战区中枢：战略重心转移与资产架构重塑报告\n"
            "日期：2026年04月27日 (收盘)\n"
            "核心态势：从危机狙击转向阵地消耗。\n"
        )
        with TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "drive_report.md"
            target = Path(tmpdir) / "gemini_daily.md"
            archive_dir = Path(tmpdir) / "archive"
            source.write_text(text, encoding="utf-8")
            payload = sync_tactical_report_cache(
                source_path=source,
                target_path=target,
                archive_dir=archive_dir,
                expected_date_iso="2026-04-27",
            )
            self.assertEqual(payload["status"], "synced")
            self.assertTrue(payload["freshness"]["is_fresh"])
            self.assertTrue(target.exists())
            self.assertTrue((archive_dir / "2026-04-27_gemini_daily.md").exists())

    def test_sync_tactical_report_text_writes_target_and_archive(self):
        text = (
            "2026-04-28 战区中枢：流动性消耗战报告\n"
            "日期：2026年04月28日 (收盘)\n"
            "核心态势：继续防守。\n"
        )
        with TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "gemini_daily.md"
            archive_dir = Path(tmpdir) / "archive"
            payload = sync_tactical_report_text(
                text=text,
                source_label="gdoc://fake-doc-id",
                target_path=target,
                archive_dir=archive_dir,
                expected_date_iso="2026-04-28",
            )
            self.assertEqual(payload["status"], "synced")
            self.assertEqual(payload["source_path"], "gdoc://fake-doc-id")
            self.assertTrue(payload["freshness"]["is_fresh"])
            self.assertTrue(target.exists())
            self.assertTrue((archive_dir / "2026-04-28_gemini_daily.md").exists())

    def test_render_daily_governance_summary_prefers_sync_stale_reason(self):
        rendered = render_daily_governance_summary(
            {
                "latest_signal": {
                    "position_regime": "full_risk",
                    "action": "hold_or_add",
                    "report_overlay_skipped": True,
                    "report_overlay_skip_reason": "旧的通用原因",
                },
                "latest_governance": {"reason": "训练质量达标，但成熟样本不足，先保留在 candidate。"},
                "latest_report_sync": {
                    "freshness": {
                        "is_fresh": False,
                        "freshness_reason": "今天没有新的 Gemini 归档，当前最新一份是 2026-04-28 的《战区中枢报告》。",
                    }
                },
            }
        )
        self.assertIn("今天没有新的 Gemini 归档", rendered)
        self.assertNotIn("旧的通用原因", rendered)

    def test_build_feishu_push_accuracy_dashboard_matches_push_with_learning_sample(self):
        with TemporaryDirectory() as tmpdir:
            audit_path = Path(tmpdir) / "feishu_push_audit.jsonl"
            snapshot_path = Path(tmpdir) / "metaphysical_learning_samples.jsonl"
            audit_row = {
                "sent_at": "2026-04-24T10:00:00",
                "channel": "feishu",
                "success": True,
                "push_kind": "metaphysical_daily",
                "content_preview": "结论: 偏防守",
                "archive_path": str(Path(tmpdir) / "push.md"),
            }
            snapshot_row = {
                "report_date": "2026年4月24日",
                "raw_position_regime": "full_risk",
                "final_position_regime": "risk_off",
                "report_alignment": "conflict",
                "tail_risk_probability": 0.17,
                "future_outcomes": {
                    "next_1d_return": -0.01,
                    "next_3d_return": -0.02,
                    "next_5d_return": -0.03,
                    "next_10d_return": -0.04,
                    "max_drawdown_10d": -0.05,
                },
            }
            audit_path.write_text(json.dumps(audit_row, ensure_ascii=False) + "\n", encoding="utf-8")
            snapshot_path.write_text(json.dumps(snapshot_row, ensure_ascii=False) + "\n", encoding="utf-8")

            summary = build_feishu_push_accuracy_dashboard(audit_path, snapshot_path, max_recent_rows=3)
            rendered = render_feishu_push_accuracy_dashboard(summary)

        self.assertEqual(summary["successful_feishu_push_count"], 1)
        self.assertEqual(summary["metaphysical_feishu_push_day_count"], 1)
        self.assertEqual(summary["matched_learning_sample_count"], 1)
        self.assertEqual(summary["matched_push_rows"][0]["final_position_regime"], "risk_off")
        self.assertIn("飞书推送建议验证看板", rendered)
        self.assertIn("2026-04-24", rendered)

    def test_build_tactical_report_optimization_notes_includes_feature_flags_and_signal_comparison(self):
        text = (
            "报告日期：2026年4月24日\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "全球流动性：市场处于去杠杆阶段，避险资金向美元现金归笼。\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
        )
        result = build_tactical_report_optimization_notes(
            text,
            model_signal={"position_regime": "full_risk"},
        )
        self.assertEqual(result["optimization_priority"], "high")
        self.assertIn("black_swan_warning", result["candidate_feature_flags"])
        self.assertEqual(result["signal_comparison"]["alignment"], "conflict")
        self.assertFalse(result["duplicate_check"]["has_duplicates"])

    def test_build_tactical_report_optimization_notes_flags_duplicate_lines(self):
        text = (
            "报告日期：2026年4月24日\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
        )
        result = build_tactical_report_optimization_notes(text)
        self.assertTrue(result["duplicate_check"]["has_duplicates"])
        self.assertEqual(result["duplicate_check"]["duplicate_line_count"], 1)

    def test_build_tactical_report_optimization_notes_detects_same_day_archive_and_similarity(self):
        text = (
            "报告日期：2026年4月24日\n"
            "核心态势：流动性风险共振，黑天鹅预警生效\n"
            "中东局势：霍尔木兹海峡物理封锁持续。\n"
        )
        with TemporaryDirectory() as tmpdir:
            archive_dir = Path(tmpdir)
            (archive_dir / "2026-04-24_gemini_daily.md").write_text(text, encoding="utf-8")
            result = build_tactical_report_optimization_notes(text, archive_dir=archive_dir)
        duplicate_check = result["duplicate_check"]
        self.assertEqual(duplicate_check["same_day_archive_count"], 1)
        self.assertTrue(duplicate_check["high_similarity_detected"])
        self.assertEqual(duplicate_check["highest_similarity_file"], "2026-04-24_gemini_daily.md")

    def test_build_metaphysical_accuracy_dashboard_summarizes_matured_rows(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "samples.jsonl"
            rows = [
                {
                    "report_date": "2026年4月24日",
                    "symbol": "510500.SS",
                    "candidate_feature_flags": ["black_swan_warning", "liquidity_risk_resonance"],
                    "raw_position_regime": "full_risk",
                    "final_position_regime": "risk_off",
                    "overlay_active": True,
                    "report_alignment": "conflict",
                    "future_outcomes": {
                        "next_1d_return": -0.01,
                        "next_3d_return": -0.02,
                        "next_5d_return": -0.03,
                        "next_10d_return": -0.05,
                        "max_drawdown_10d": -0.06,
                    },
                    "maturity_status": "matured_10d",
                },
                {
                    "report_date": "2026年4月25日",
                    "symbol": "510500.SS",
                    "candidate_feature_flags": ["gold_defense_core"],
                    "raw_position_regime": "full_risk",
                    "final_position_regime": "full_risk",
                    "overlay_active": False,
                    "report_alignment": "aligned_risk_on",
                    "future_outcomes": {
                        "next_1d_return": 0.01,
                        "next_3d_return": 0.015,
                        "next_5d_return": 0.02,
                        "next_10d_return": 0.03,
                        "max_drawdown_10d": -0.01,
                    },
                    "maturity_status": "matured_10d",
                },
            ]
            path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")
            summary = build_metaphysical_accuracy_dashboard(path)
        self.assertEqual(summary["sample_count"], 2)
        self.assertEqual(summary["maturity_counts"]["matured_10d"], 2)
        self.assertEqual(summary["risk_off_summary"]["sample_count"], 1)
        self.assertEqual(summary["full_risk_summary"]["sample_count"], 1)
        rendered = render_metaphysical_accuracy_dashboard(summary)
        self.assertIn("玄学模型命中率看板", rendered)
        self.assertIn("防守建议", rendered)

    def test_build_daily_learning_snapshot_contains_signal_and_future_outcomes(self):
        snapshot = build_daily_learning_snapshot(
            report_text=(
                "报告日期：2026年4月24日\n"
                "核心态势：流动性风险共振，黑天鹅预警生效\n"
            ),
            final_signal={
                "position_regime": "risk_off",
                "raw_position_regime": "full_risk",
                "tail_risk_probability": 0.17,
                "overlay_active": True,
                "report_risk_score": 4,
                "report_alignment": "conflict",
            },
        )
        self.assertEqual(snapshot["report_date"], "2026年4月24日")
        self.assertEqual(snapshot["final_position_regime"], "risk_off")
        self.assertIn("next_5d_return", snapshot["future_outcomes"])
        self.assertEqual(snapshot["maturity_status"], "observing")

    def test_record_daily_learning_snapshot_upserts_same_day_symbol(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "samples.jsonl"
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            report_text = "报告日期：2026年4月24日\n核心态势：流动性风险共振，黑天鹅预警生效\n"
            try:
                record_daily_learning_snapshot(
                    path,
                    report_text=report_text,
                    final_signal={"position_regime": "caution", "tail_risk_probability": 0.4},
                    symbol="510500.SS",
                )
                record_daily_learning_snapshot(
                    path,
                    report_text=report_text,
                    final_signal={"position_regime": "risk_off", "tail_risk_probability": 0.7},
                    symbol="510500.SS",
                )
                rows = path.read_text(encoding="utf-8").strip().splitlines()
                mirror_rows = (wh.LEDGER_DIR / "samples.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertEqual(len(rows), 1)
        self.assertIn("\"final_position_regime\": \"risk_off\"", rows[0])
        self.assertEqual(len(mirror_rows), 1)

    def test_build_training_run_record_returns_versionable_metrics(self):
        record = build_training_run_record(
            symbol="510500.SS",
            start="2016-01-01",
            end="2026-04-20",
            sample_count=2000,
            feature_count=22,
            auc=0.61,
            ap=0.18,
        )
        self.assertEqual(record["symbol"], "510500.SS")
        self.assertEqual(record["feature_count"], 22)
        self.assertEqual(record["auc"], 0.61)

    def test_record_training_run_appends_jsonl_row(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "training.jsonl"
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                record_training_run(
                    path,
                    symbol="510500.SS",
                    start="2016-01-01",
                    end="2026-04-20",
                    sample_count=2000,
                    feature_count=22,
                    auc=0.61,
                    ap=0.18,
                )
                rows = path.read_text(encoding="utf-8").strip().splitlines()
                mirror_rows = (wh.LEDGER_DIR / "training.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertEqual(len(rows), 1)
        self.assertIn("\"feature_pool\": \"NEXT_PRODUCTION_WEIGHTED_CANDIDATE_FEATURES\"", rows[0])
        self.assertEqual(len(mirror_rows), 1)

    def test_select_matured_learning_samples_filters_by_status(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "samples.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月24日","symbol":"510500.SS","maturity_status":"observing"}',
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            rows = select_matured_learning_samples(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["report_date"], "2026年4月10日")

    def test_summarize_training_run_window_computes_recent_means(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "training.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"auc":0.61,"ap":0.18}',
                        '{"auc":0.63,"ap":0.19}',
                        '{"auc":0.65,"ap":0.21}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            summary = summarize_training_run_window(path, recent_n=2)
        self.assertEqual(summary["run_count"], 3)
        self.assertAlmostEqual(summary["mean_auc"], 0.64)
        self.assertAlmostEqual(summary["mean_ap"], 0.20)

    def test_evaluate_candidate_promotion_readiness_requires_thresholds(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "training.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"auc":0.61,"ap":0.19}',
                        '{"auc":0.62,"ap":0.20}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            ready = evaluate_candidate_promotion_readiness(path, recent_n=2, min_runs=2, auc_floor=0.60, ap_floor=0.18)
            not_ready = evaluate_candidate_promotion_readiness(path, recent_n=2, min_runs=3, auc_floor=0.63, ap_floor=0.21)
        self.assertTrue(ready["promotion_ready"])
        self.assertIn("近期训练窗口达到候选升版门槛", ready["reasons"])
        self.assertFalse(not_ready["promotion_ready"])
        self.assertIn("训练记录数量不足", not_ready["reasons"])

    def test_evaluate_stage_promotion_readiness_attaches_stage_name(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "training.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"auc":0.63,"ap":0.20}',
                        '{"auc":0.64,"ap":0.21}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            ready = evaluate_stage_promotion_readiness(
                path,
                stage="shadow",
                recent_n=2,
                min_runs=2,
                auc_floor=0.62,
                ap_floor=0.20,
            )
        self.assertEqual(ready["stage"], "shadow")
        self.assertTrue(ready["promotion_ready"])

    def test_evaluate_governance_action_keeps_candidate_when_matured_samples_insufficient(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            samples.write_text(
                '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}\n',
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.61,"ap":0.19}',
                        '{"auc":0.62,"ap":0.20}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = evaluate_governance_action(
                samples,
                training,
                min_matured_samples_for_shadow=5,
            )
        self.assertEqual(result["action"], "keep_candidate")
        self.assertEqual(result["target_stage"], "candidate")

    def test_evaluate_governance_action_promotes_to_shadow_when_ready_and_matured(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            samples.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月11日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月12日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月13日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月14日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.61,"ap":0.19}',
                        '{"auc":0.62,"ap":0.20}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = evaluate_governance_action(
                samples,
                training,
                min_matured_samples_for_shadow=5,
            )
        self.assertEqual(result["action"], "promote_to_shadow")
        self.assertEqual(result["target_stage"], "shadow")

    def test_evaluate_governance_stage_flow_promotes_research_to_candidate(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            samples.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月11日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.61,"ap":0.19}',
                        '{"auc":0.62,"ap":0.20}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = evaluate_governance_stage_flow(
                samples,
                training,
                current_stage="research",
                min_matured_samples_for_candidate=2,
            )
        self.assertEqual(result["action"], "promote_to_candidate")
        self.assertEqual(result["target_stage"], "candidate")

    def test_evaluate_governance_stage_flow_promotes_shadow_to_production(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            samples.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月11日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月12日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月13日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月14日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月15日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月16日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月17日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月18日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月19日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月20日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月21日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.65,"ap":0.23}',
                        '{"auc":0.66,"ap":0.24}',
                        '{"auc":0.67,"ap":0.25}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = evaluate_governance_stage_flow(
                samples,
                training,
                current_stage="shadow",
                min_matured_samples_for_production=12,
                production_auc_floor=0.64,
                production_ap_floor=0.22,
                production_min_runs=3,
            )
        self.assertEqual(result["action"], "promote_to_production")
        self.assertEqual(result["target_stage"], "production")

    def test_record_governance_run_appends_and_loads_latest(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "governance.jsonl"
            governance = build_governance_run_record(
                governance={
                    "current_stage": "candidate",
                    "target_stage": "shadow",
                    "action": "promote_to_shadow",
                    "reason": "测试升版。",
                    "matured_sample_count": 5,
                    "required_status": "matured_10d",
                    "promotion_readiness": {
                        "latest_auc": 0.63,
                        "latest_ap": 0.21,
                        "promotion_ready": True,
                    },
                    "stage_thresholds": {"shadow": {"min_matured_samples": 5}},
                }
            )
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                record_governance_run(path, governance=governance)
                latest = latest_governance_run(path)
                mirror_rows = (wh.LEDGER_DIR / "governance.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertEqual(latest["action"], "promote_to_shadow")
        self.assertEqual(latest["target_stage"], "shadow")
        self.assertEqual(len(mirror_rows), 1)

    def test_record_lifecycle_run_appends_and_loads_latest(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "lifecycle.jsonl"
            lifecycle = build_lifecycle_run_record(
                lifecycle={
                    "current_stage": "candidate",
                    "lifecycle_action": "keep_candidate_under_review",
                    "lifecycle_target_stage": "candidate",
                    "reason": "测试生命周期。",
                    "governance": {"action": "promote_to_shadow", "target_stage": "shadow"},
                    "guardrail": {"action": "keep_candidate_under_review", "healthy": False, "stage": "candidate"},
                }
            )
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                record_lifecycle_run(path, lifecycle=lifecycle)
                latest = latest_lifecycle_run(path)
                mirror_rows = (wh.LEDGER_DIR / "lifecycle.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertEqual(latest["lifecycle_action"], "keep_candidate_under_review")
        self.assertEqual(latest["lifecycle_target_stage"], "candidate")
        self.assertEqual(len(mirror_rows), 1)

    def test_record_version_switch_proposal_appends_and_loads_latest(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "proposal.jsonl"
            proposal = build_version_switch_proposal(
                lifecycle={
                    "current_stage": "shadow",
                    "lifecycle_action": "promote_to_production",
                    "lifecycle_target_stage": "production",
                    "reason": "测试切换草案。",
                },
                current_profile="next_production_shadow",
            )
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                record_version_switch_proposal(
                    path,
                    lifecycle=proposal,
                    current_profile="next_production_shadow",
                )
                latest = latest_version_switch_proposal(path)
                mirror_rows = (wh.LEDGER_DIR / "proposal.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertEqual(proposal["proposal_status"], "pending_review")
        self.assertEqual(latest["proposal_action"], "promote_to_production")
        self.assertEqual(latest["proposed_profile"], "next_production_production")
        self.assertEqual(len(mirror_rows), 1)

    def test_build_version_switch_execution_plan_contains_review_checks(self):
        plan = build_version_switch_execution_plan(
            proposal={
                "proposal_status": "pending_review",
                "proposal_action": "promote_to_shadow",
                "current_profile": "next_production_candidate",
                "proposed_profile": "next_production_shadow",
                "should_switch": True,
                "recommended_defaults": {"caution_threshold": 0.4},
            }
        )
        self.assertEqual(plan["proposal_action"], "promote_to_shadow")
        self.assertTrue(plan["should_switch"])
        self.assertTrue(any("切换" in item for item in plan["review_checks"]))

    def test_build_version_switch_confirmation_draft_tracks_confirmation_state(self):
        draft = build_version_switch_confirmation_draft(
            proposal={
                "proposal_status": "pending_review",
                "proposal_action": "promote_to_shadow",
                "current_profile": "next_production_candidate",
                "proposed_profile": "next_production_shadow",
                "should_switch": True,
                "reason": "测试确认稿。",
            }
        )
        self.assertEqual(draft["confirmation_state"], "awaiting_confirmation")
        self.assertIn("建议切换到", draft["summary"])

    def test_build_version_switch_change_request_contains_rollback_points(self):
        request = build_version_switch_change_request(
            proposal={
                "proposal_status": "pending_review",
                "proposal_action": "promote_to_shadow",
                "current_profile": "next_production_candidate",
                "proposed_profile": "next_production_shadow",
                "should_switch": True,
            }
        )
        self.assertEqual(request["request_state"], "draft_ready")
        self.assertIn("版本切换变更单", request["title"])
        self.assertGreaterEqual(len(request["rollback_points"]), 3)

    def test_build_weekly_governance_summary_renders_core_sections(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            governance = Path(tmpdir) / "governance.jsonl"
            lifecycle = Path(tmpdir) / "lifecycle.jsonl"
            stage = Path(tmpdir) / "stage.jsonl"
            proposal = Path(tmpdir) / "proposal.jsonl"

            samples.write_text(
                '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}\n',
                encoding="utf-8",
            )
            training.write_text(
                '{"auc":0.63,"ap":0.20}\n',
                encoding="utf-8",
            )
            governance.write_text(
                '{"current_stage":"candidate","target_stage":"candidate","action":"keep_candidate","reason":"继续保留。"}\n',
                encoding="utf-8",
            )
            lifecycle.write_text(
                '{"current_stage":"candidate","lifecycle_target_stage":"candidate","lifecycle_action":"keep_candidate","reason":"继续保留。"}\n',
                encoding="utf-8",
            )
            stage.write_text(
                '{"stage":"candidate","strategy_sharpe":0.5,"excess_return":0.1,"drawdown_gap":0.04,"auc":0.63,"ap":0.20}\n',
                encoding="utf-8",
            )
            proposal.write_text(
                '{"current_profile":"next_production_candidate","proposed_profile":"next_production_candidate","proposal_status":"no_change","proposal_action":"keep_candidate","should_switch":false}\n',
                encoding="utf-8",
            )
            summary = build_weekly_governance_summary(
                snapshot_path=samples,
                training_path=training,
                governance_path=governance,
                lifecycle_path=lifecycle,
                stage_performance_path=stage,
                switch_proposal_path=proposal,
                current_stage="candidate",
                recent_n=1,
            )
            rendered = render_weekly_governance_summary(summary)
        self.assertEqual(summary["matured_sample_count"], 1)
        self.assertIn("玄学模型周治理摘要", rendered)
        self.assertIn("治理动作", rendered)
        self.assertIn("生命周期", rendered)

    def test_render_daily_governance_summary_renders_core_sections(self):
        rendered = render_daily_governance_summary(
            {
                "latest_signal": {
                    "raw_position_regime": "full_risk",
                    "position_regime": "caution",
                    "tail_risk_probability": 0.42,
                },
                "latest_governance": {
                    "current_stage": "candidate",
                    "target_stage": "candidate",
                    "action": "keep_candidate",
                    "reason": "继续保留。",
                },
                "latest_lifecycle": {
                    "current_stage": "candidate",
                    "lifecycle_target_stage": "candidate",
                    "lifecycle_action": "keep_candidate",
                    "reason": "继续保留。",
                },
                "stage_health": {
                    "stage": "candidate",
                    "action": "keep_candidate_under_review",
                    "reasons": ["近期回撤优势未达门槛"],
                },
                "latest_switch_proposal": {
                    "current_profile": "next_production_candidate",
                    "proposed_profile": "next_production_candidate",
                    "proposal_status": "no_change",
                    "proposal_action": "keep_candidate",
                },
                "switch_confirmation_draft": {
                    "confirmation_state": "not_required",
                    "summary": "测试确认稿",
                },
                "switch_change_request": {
                    "request_state": "standby",
                    "title": "测试变更单",
                },
            }
        )
        self.assertIn("玄学治理日报", rendered)
        self.assertIn("结论", rendered)
        self.assertIn("是否需要你介入", rendered)

    def test_record_stage_performance_run_and_guardrail_summary(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "stage.jsonl"
            metrics_a = {
                "sample_count": 1000,
                "auc": 0.65,
                "ap": 0.23,
                "strategy_total_return": 0.80,
                "buy_hold_total_return": 0.60,
                "strategy_max_drawdown": -0.26,
                "buy_hold_max_drawdown": -0.30,
                "strategy_sharpe": 0.55,
                "buy_hold_sharpe": 0.40,
                "avg_position": 0.72,
                "risk_off_threshold": 0.60,
                "caution_threshold": 0.40,
                "risk_off_days": 10,
                "caution_days": 20,
                "full_risk_days": 970,
            }
            metrics_b = {
                "sample_count": 900,
                "auc": 0.64,
                "ap": 0.22,
                "strategy_total_return": 0.75,
                "buy_hold_total_return": 0.61,
                "strategy_max_drawdown": -0.25,
                "buy_hold_max_drawdown": -0.29,
                "strategy_sharpe": 0.50,
                "buy_hold_sharpe": 0.39,
                "avg_position": 0.70,
                "risk_off_threshold": 0.60,
                "caution_threshold": 0.40,
                "risk_off_days": 12,
                "caution_days": 18,
                "full_risk_days": 870,
            }
            preview = build_stage_performance_record(
                stage="production",
                symbol="510500.SS",
                start="2016-01-01",
                end="2026-04-20",
                metrics=metrics_a,
            )
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                record_stage_performance_run(
                    path,
                    stage="production",
                    symbol="510500.SS",
                    start="2016-01-01",
                    end="2026-04-20",
                    metrics=metrics_a,
                )
                record_stage_performance_run(
                    path,
                    stage="production",
                    symbol="510500.SS",
                    start="2017-01-01",
                    end="2026-04-20",
                    metrics=metrics_b,
                )
                latest = latest_stage_performance_run(path, stage="production")
                summary = summarize_stage_performance_window(path, stage="production", recent_n=2)
                guardrail = evaluate_stage_guardrail(path, stage="production", recent_n=2, min_runs=2)
                mirror_rows = (wh.LEDGER_DIR / "stage.jsonl").read_text(encoding="utf-8").strip().splitlines()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertAlmostEqual(preview["excess_return"], 0.20)
        self.assertEqual(latest["stage"], "production")
        self.assertAlmostEqual(summary["mean_strategy_sharpe"], 0.525)
        self.assertTrue(guardrail["healthy"])
        self.assertEqual(guardrail["action"], "keep_production")
        self.assertEqual(len(mirror_rows), 2)

    def test_evaluate_release_lifecycle_blocks_candidate_promotion_when_guardrail_weak(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            stage_path = Path(tmpdir) / "stage.jsonl"
            samples.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月11日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月12日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月13日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月14日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.62,"ap":0.20}',
                        '{"auc":0.63,"ap":0.21}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            stage_path.write_text(
                '{"stage":"candidate","strategy_sharpe":0.50,"excess_return":0.10,"drawdown_gap":0.08,"auc":0.62,"ap":0.20}\n',
                encoding="utf-8",
            )
            result = evaluate_release_lifecycle(
                samples,
                training,
                stage_path,
                current_stage="candidate",
                min_matured_samples_for_shadow=5,
                recent_n=2,
                min_runs=2,
            )
        self.assertEqual(result["governance"]["action"], "promote_to_shadow")
        self.assertEqual(result["lifecycle_action"], "keep_candidate_under_review")
        self.assertEqual(result["lifecycle_target_stage"], "candidate")

    def test_evaluate_release_lifecycle_degrades_production_when_guardrail_fails(self):
        with TemporaryDirectory() as tmpdir:
            samples = Path(tmpdir) / "samples.jsonl"
            training = Path(tmpdir) / "training.jsonl"
            stage_path = Path(tmpdir) / "stage.jsonl"
            samples.write_text(
                "\n".join(
                    [
                        '{"report_date":"2026年4月10日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月11日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月12日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月13日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月14日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月15日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月16日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月17日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月18日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月19日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月20日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                        '{"report_date":"2026年4月21日","symbol":"510500.SS","maturity_status":"matured_10d"}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            training.write_text(
                "\n".join(
                    [
                        '{"auc":0.66,"ap":0.24}',
                        '{"auc":0.67,"ap":0.25}',
                        '{"auc":0.68,"ap":0.26}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            stage_path.write_text(
                "\n".join(
                    [
                        '{"stage":"production","strategy_sharpe":0.10,"excess_return":-0.10,"drawdown_gap":0.10,"auc":0.60,"ap":0.18}',
                        '{"stage":"production","strategy_sharpe":0.12,"excess_return":-0.08,"drawdown_gap":0.09,"auc":0.61,"ap":0.19}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = evaluate_release_lifecycle(
                samples,
                training,
                stage_path,
                current_stage="production",
                recent_n=2,
                min_runs=2,
            )
        self.assertEqual(result["guardrail"]["action"], "degrade_to_shadow")
        self.assertEqual(result["lifecycle_action"], "degrade_to_shadow")
        self.assertEqual(result["lifecycle_target_stage"], "shadow")

    def test_compute_learning_outcomes_from_prices_returns_forward_metrics(self):
        prices = pd.DataFrame(
            {
                "date": pd.date_range("2026-04-24", periods=11, freq="D"),
                "close": [100, 101, 102, 103, 99, 104, 105, 106, 107, 108, 109],
            }
        )
        outcomes = compute_learning_outcomes_from_prices("2026年4月24日", prices)
        self.assertAlmostEqual(outcomes["next_1d_return"], 0.01)
        self.assertAlmostEqual(outcomes["next_3d_return"], 0.03)
        self.assertAlmostEqual(outcomes["next_5d_return"], 0.04)
        self.assertAlmostEqual(outcomes["next_10d_return"], 0.09)
        self.assertAlmostEqual(outcomes["max_drawdown_10d"], -0.03883495145631066)

    def test_backfill_learning_snapshot_outcomes_updates_jsonl_rows(self):
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import learning as ln
            from src.models.metaphysical import warehouse as wh

            path = Path(tmpdir) / "samples.jsonl"
            path.write_text(
                '{"report_date":"2026年4月24日","symbol":"510500.SS","future_outcomes":{"next_1d_return":null}}\n',
                encoding="utf-8",
            )
            prices = pd.DataFrame(
                {
                    "date": pd.date_range("2026-04-24", periods=11, freq="D"),
                    "close": [100, 101, 102, 103, 99, 104, 105, 106, 107, 108, 109],
                }
            )
            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            original_learning_ledger_dir = ln.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            ln.LEDGER_DIR = wh.LEDGER_DIR
            try:
                backfill_learning_snapshot_outcomes(path, prices)
                row = path.read_text(encoding="utf-8").strip()
                mirror_row = (wh.LEDGER_DIR / "samples.jsonl").read_text(encoding="utf-8").strip()
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                ln.LEDGER_DIR = original_learning_ledger_dir
        self.assertIn('"next_10d_return": 0.09000000000000008', row)
        self.assertIn('"maturity_status": "matured_10d"', row)
        self.assertIn('"maturity_status": "matured_10d"', mirror_row)

    def test_warehouse_helpers_persist_price_and_dataset_csv(self):
        prices = pd.DataFrame({"date": pd.date_range("2026-04-24", periods=2, freq="D"), "close": [100, 101]})
        dataset = pd.DataFrame({"date": pd.date_range("2026-04-24", periods=2, freq="D"), "f1": [1, 2]})
        with TemporaryDirectory() as tmpdir:
            from src.models.metaphysical import warehouse as wh

            original_root = wh.WAREHOUSE_ROOT
            original_price_dir = wh.PRICE_HISTORY_DIR
            original_dataset_dir = wh.DATASET_SNAPSHOTS_DIR
            original_ledger_dir = wh.LEDGER_DIR
            wh.WAREHOUSE_ROOT = Path(tmpdir) / "meta"
            wh.PRICE_HISTORY_DIR = wh.WAREHOUSE_ROOT / "price_history"
            wh.DATASET_SNAPSHOTS_DIR = wh.WAREHOUSE_ROOT / "dataset_snapshots"
            wh.LEDGER_DIR = wh.WAREHOUSE_ROOT / "ledgers"
            try:
                ensure_metaphysical_warehouse()
                price_path = archive_price_history(prices, symbol="510500.SS")
                dataset_path = archive_candidate_dataset(
                    dataset,
                    symbol="510500.SS",
                    start="2016-01-01",
                    end="2026-04-20",
                )
            finally:
                wh.WAREHOUSE_ROOT = original_root
                wh.PRICE_HISTORY_DIR = original_price_dir
                wh.DATASET_SNAPSHOTS_DIR = original_dataset_dir
                wh.LEDGER_DIR = original_ledger_dir
                self.assertTrue(price_path.exists())
                self.assertTrue(dataset_path.exists())


class TestMetaphysicalRegimeHelpers(unittest.TestCase):
    def test_classify_author_regime_switches_after_cutoff(self):
        self.assertEqual(classify_author_regime(pd.Timestamp("2020-01-01")), "early")
        self.assertEqual(classify_author_regime(pd.Timestamp("2024-01-01")), "recent")

    def test_author_feature_weight_uses_configured_weights(self):
        self.assertEqual(
            author_feature_weight("uranus_retrograde_boundary_distance", pd.Timestamp("2020-01-01")),
            AUTHOR_FEATURE_REGIME_WEIGHTS["uranus_retrograde_boundary_distance"]["early"],
        )

    def test_batch_author_regimes_returns_alignment(self):
        rows = batch_author_regimes(pd.date_range("2020-01-01", periods=2, freq="1000D"))
        self.assertEqual(rows[0]["author_regime"], "early")
        self.assertEqual(rows[1]["author_regime"], "recent")

    def test_apply_author_regime_weights_adds_weighted_columns(self):
        frame = pd.DataFrame(
            {
                "close": [100.0, 100.0],
                "atr_20": [2.0, 5.0],
                "bollinger_width": [1.5, 4.0],
                "quant_return_5d": [0.01, 0.05],
                "quant_volume_ratio_5d": [0.95, 1.3],
                "bb_width_roc_3d": [0.02, 0.2],
                "uranus_retrograde_boundary_distance": [1.0, 1.0],
                "uranus_cycle_84_distance": [2.0, 2.0],
                "saturn_jupiter_cycle_distance": [3.0, 3.0],
                "volatility_plus_score": [4.0, 4.0],
            },
            index=pd.to_datetime(["2020-01-01", "2024-01-01"]),
        )
        result = apply_author_regime_weights(frame)
        self.assertIn("weighted_uranus_retrograde_boundary_distance", result.columns)
        self.assertIn("weighted_volatility_plus_score", result.columns)
        self.assertEqual(result.loc[pd.Timestamp("2020-01-01"), "author_regime"], "early")
        self.assertEqual(result.loc[pd.Timestamp("2024-01-01"), "author_regime"], "recent")
        self.assertLess(
            result.loc[pd.Timestamp("2020-01-01"), "weighted_uranus_retrograde_boundary_distance"],
            result.loc[pd.Timestamp("2024-01-01"), "weighted_uranus_retrograde_boundary_distance"],
        )

    def test_infer_dynamic_author_regimes_detects_recent_market_state(self):
        frame = pd.DataFrame(
            {
                "close": [100.0] * 30,
                "atr_20": [1.0] * 29 + [5.0],
                "bollinger_width": [1.0] * 29 + [4.0],
                "quant_return_5d": [0.01] * 29 + [0.08],
                "quant_volume_ratio_5d": [1.0] * 29 + [1.4],
                "bb_width_roc_3d": [0.01] * 29 + [0.2],
                "volatility_plus_score": [0.1] * 29 + [0.8],
            },
            index=pd.date_range("2020-01-01", periods=30, freq="D"),
        )
        rows = infer_dynamic_author_regimes(frame)
        self.assertEqual(rows[-1]["author_regime"], "recent")
        self.assertEqual(rows[-1]["is_author_recent_regime"], 1)


class TestMetaphysicalTheoryCompare(unittest.TestCase):
    def test_author_theory_catalog_contains_expected_gap(self):
        catalog = get_author_theory_catalog()
        ids = {item["theory_id"] for item in catalog}
        self.assertIn("saturn-jupiter-20y-cycle", ids)
        target = next(item for item in catalog if item["theory_id"] == "saturn-jupiter-20y-cycle")
        self.assertEqual(target["support_level"], "missing")

    def test_author_theory_coverage_summary_counts_items(self):
        summary = summarize_author_theory_coverage()
        self.assertEqual(summary["total_theories"], 8)
        self.assertIn("volatility-plus-turning-points", summary["priority_gap_ids"])
        self.assertIn("saturn-jupiter-20y-cycle", summary["best_next_targets"])


class TestMetaphysicalTimeLaw(unittest.TestCase):
    def test_year_ganzhi_returns_expected_pillar(self):
        result = year_ganzhi(2026)
        self.assertEqual(result["pillar"], "丙午")
        self.assertEqual(result["animal"], "马")

    def test_ten_gods_for_returns_known_mapping(self):
        result = ten_gods_for("壬")
        self.assertEqual(result["name"], "食神")

    def test_solar_terms_in_range_returns_critical_term(self):
        terms = solar_terms_in_range(pd.Timestamp("2026-02-01"), pd.Timestamp("2026-04-30"))
        names = [item["name"] for item in terms]
        self.assertIn("立春", names)
        self.assertTrue(any(item["critical"] for item in terms))

    def test_mars_events_in_range_filters_window(self):
        events = mars_events_in_range(pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-31"))
        self.assertEqual(len(events), 1)
        self.assertIn("木星", events[0]["title"])

    def test_nearest_lunar_phase_distance_returns_expected_keys(self):
        result = nearest_lunar_phase_distance(pd.Timestamp("2026-04-23"))
        self.assertIn("nearest_lunar_phase_distance", result)
        self.assertIn(result["nearest_lunar_phase"], {"new_moon", "full_moon"})

    def test_solar_term_distance_returns_term_bucket(self):
        result = solar_term_distance(pd.Timestamp("2026-03-20"))
        self.assertIn("nearest_solar_term", result)
        self.assertGreaterEqual(result["nearest_solar_term_distance"], 0)

    def test_anniversary_cycle_distance_returns_window_flag(self):
        result = anniversary_cycle_distance(
            pd.Timestamp("2026-09-11"),
            [pd.Timestamp("2011-09-12"), pd.Timestamp("2015-06-15")],
        )
        self.assertLessEqual(result["anniversary_cycle_distance"], 1)
        self.assertEqual(result["is_anniversary_window"], 1)

    def test_saturn_jupiter_cycle_distance_detects_2020_window(self):
        result = saturn_jupiter_cycle_distance(pd.Timestamp("2020-12-21"))
        self.assertEqual(result["saturn_jupiter_cycle_distance"], 0)
        self.assertEqual(result["is_saturn_jupiter_cycle_window"], 1)

    def test_uranus_cycle_distance_detects_2026_window(self):
        result = uranus_cycle_distance(pd.Timestamp("2026-07-28"))
        self.assertEqual(result["uranus_cycle_84_distance"], 0)
        self.assertEqual(result["is_uranus_cycle_window"], 1)

    def test_uranus_retrograde_state_detects_active_window(self):
        result = uranus_retrograde_state(pd.Timestamp("2026-10-01"))
        self.assertEqual(result["uranus_retrograde_active"], 1)
        self.assertGreaterEqual(result["uranus_retrograde_boundary_distance"], 0)

    def test_batch_long_cycle_features_returns_expected_columns(self):
        rows = batch_long_cycle_features(pd.date_range("2026-07-28", periods=2, freq="D"))
        self.assertEqual(len(rows), 2)
        self.assertIn("saturn_jupiter_cycle_distance", rows[0])
        self.assertIn("uranus_retrograde_active", rows[0])


class TestMetaphysicalGann(unittest.TestCase):
    def test_gann_square_of_9_returns_sorted_levels(self):
        levels = gann_square_of_9(100)
        self.assertEqual(levels[0]["kind"], "support")
        self.assertTrue(levels[0]["price"] < levels[-1]["price"])

    def test_extract_key_gann_levels_returns_primary_levels(self):
        levels = gann_square_of_9(100)
        extracted = extract_key_gann_levels(levels, 100)
        self.assertIsNotNone(extracted["primary_support"])
        self.assertIsNotNone(extracted["primary_resistance"])

    def test_gann_time_square_contains_core_cycle(self):
        result = gann_time_square(pd.Timestamp("2026-01-01"))
        self.assertTrue(any(item["days_from_start"] == 144 for item in result))


class TestMetaphysicalTrendLaw(unittest.TestCase):
    def test_bollinger_bands_returns_series(self):
        kline_data = [
            {"date": f"2026-01-{idx:02d}", "close": float(100 + idx)}
            for idx in range(1, 26)
        ]
        result = bollinger_bands(kline_data, period=20, stddev=2)
        self.assertEqual(len(result), 25)
        self.assertIsNone(result[0]["upper"])
        self.assertIsNotNone(result[-1]["upper"])
        self.assertIn("bb_width_ratio", result[-1])
        self.assertIn("bb_position_pct", result[-1])
        self.assertIn("bb_width_roc_3d", result[-1])

    def test_bollinger_state_detects_squeeze_like_state(self):
        bb_series = [
            {"date": "2026-01-01", "upper": 110.0, "lower": 90.0, "price": 100.0, "bandwidth": 20.0, "bb_width_ratio": 0.2, "bb_width_roc_3d": 0.1},
            {"date": "2026-01-02", "upper": 111.0, "lower": 89.0, "price": 100.0, "bandwidth": 22.0, "bb_width_ratio": 0.22, "bb_width_roc_3d": 0.1},
        ] * 10
        bb_series[-1] = {"date": "2026-01-20", "upper": 101.0, "lower": 99.0, "price": 100.0, "bandwidth": 2.0, "bb_width_ratio": 0.02, "bb_width_roc_3d": -0.8}
        result = bollinger_state(bb_series)
        self.assertEqual(result["state"], "squeeze")

    def test_is_bb_squeeze_detects_low_quantile_width(self):
        bb_series = [
            {"bb_width_ratio": value, "upper": 110.0, "lower": 90.0, "price": 100.0}
            for value in [0.25] * 19 + [0.02]
        ]
        self.assertTrue(is_bb_squeeze(bb_series))

    def test_is_bb_breakout_requires_breakout_and_positive_roc(self):
        bb_series = [
            {"upper": 110.0, "lower": 90.0, "price": 111.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.2}
        ]
        self.assertTrue(is_bb_breakout(bb_series))

    def test_bb_breakout_strength_returns_positive_score_for_breakout(self):
        bb_series = [
            {"upper": 110.0, "lower": 90.0, "price": 114.0, "bb_width_roc_3d": 0.3, "bb_width_ratio": 0.2}
        ]
        self.assertGreater(bb_breakout_strength(bb_series), 0.0)

    def test_is_triple_resonance_respects_volume_threshold(self):
        bb_series = [{"upper": 110.0, "lower": 90.0, "price": 111.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.2}] * 19
        bb_series.append({"upper": 101.0, "lower": 99.0, "price": 102.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.01})
        self.assertTrue(is_triple_resonance(bb_series, volume_ratio=1.5))
        self.assertFalse(is_triple_resonance(bb_series, volume_ratio=1.0))

    def test_triple_resonance_score_increases_with_breakout_and_volume(self):
        bb_series = [{"upper": 110.0, "lower": 90.0, "price": 111.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.2}] * 19
        bb_series.append({"upper": 101.0, "lower": 99.0, "price": 102.0, "bb_width_roc_3d": 0.5, "bb_width_ratio": 0.01})
        high_score = triple_resonance_score(bb_series, volume_ratio=1.8)
        low_score = triple_resonance_score(bb_series, volume_ratio=1.0)
        self.assertGreater(high_score, low_score)

    def test_month_turning_point_score_detects_window(self):
        result = month_turning_point_score(pd.Timestamp("2026-04-08"))
        self.assertEqual(result["is_month_turning_point_window"], 1)
        self.assertEqual(result["month_turning_point_label"], "early_pivot")

    def test_batch_month_turning_points_returns_aligned_records(self):
        rows = batch_month_turning_points(pd.date_range("2026-04-08", periods=2, freq="D"))
        self.assertEqual(len(rows), 2)
        self.assertIn("month_turning_point_score", rows[0])

    def test_volatility_plus_score_increases_with_turning_point_and_breakout(self):
        bb_series = [{"upper": 110.0, "lower": 90.0, "price": 111.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.2}] * 19
        bb_series.append({"date": "2026-04-08", "upper": 101.0, "lower": 99.0, "price": 102.0, "bb_width_roc_3d": 0.5, "bb_width_ratio": 0.01})
        high_score = volatility_plus_score(bb_series, dt=pd.Timestamp("2026-04-08"), volume_ratio=1.8)
        low_score = volatility_plus_score(bb_series, dt=pd.Timestamp("2026-04-12"), volume_ratio=1.0)
        self.assertGreater(high_score, low_score)

    def test_is_volatility_plus_signal_triggers_when_score_high_enough(self):
        bb_series = [{"upper": 110.0, "lower": 90.0, "price": 111.0, "bb_width_roc_3d": 0.2, "bb_width_ratio": 0.2}] * 19
        bb_series.append({"date": "2026-04-08", "upper": 101.0, "lower": 99.0, "price": 102.0, "bb_width_roc_3d": 0.5, "bb_width_ratio": 0.01})
        self.assertTrue(is_volatility_plus_signal(bb_series, dt=pd.Timestamp("2026-04-08"), volume_ratio=1.8))


class TestMetaphysicalStrategySummary(unittest.TestCase):
    def test_generate_resonance_strategy_summary_returns_time_and_space_laws(self):
        summary = generate_resonance_strategy_summary(
            100.0,
            pd.Timestamp("2026-01-01"),
            now=datetime(2026, 4, 1),
        )
        self.assertIn("time_law", summary)
        self.assertIn("space_law", summary)
        self.assertIn("next_pivot", summary)

    def test_generate_resonance_strategy_summary_includes_trend_law_when_kline_present(self):
        kline_data = [
            {"date": f"2026-01-{idx:02d}", "close": float(100 + idx)}
            for idx in range(1, 31)
        ]
        summary = generate_resonance_strategy_summary(
            130.0,
            pd.Timestamp("2026-01-01"),
            kline_data=kline_data,
            now=datetime(2026, 4, 1),
        )
        self.assertIn("trend_law", summary)
        self.assertIsNotNone(summary["trend_law"])
        self.assertIn("stance", summary)
        self.assertIn("is_bb_squeeze", summary["trend_law"])
        self.assertIn("volatility_plus_score", summary["trend_law"])
        self.assertIn("month_turning_point", summary["trend_law"])


class TestMetaphysicalAdapter(unittest.TestCase):
    def test_attach_returns_original_when_disabled(self):
        df = pd.DataFrame({"date": ["2026-04-20"], "close": [100.0]})
        result = attach_metaphysical_features(df, enabled=False)
        self.assertEqual(list(result.columns), ["date", "close"])

    @patch("src.models.metaphysical.adapter.build_metaphysical_features_if_enabled")
    def test_attach_merges_features_by_normalized_date(self, mock_build):
        df = pd.DataFrame(
            {
                "date": ["2026-04-20 09:30:00", "2026-04-21 15:00:00"],
                "close": [100.0, 101.0],
            }
        )
        mock_build.return_value = pd.DataFrame(
            {
                "csi500_flash_crash": [1, 0],
                "gold_macro_shock": [0, 1],
            },
            index=pd.to_datetime(["2026-04-20", "2026-04-21"]),
        )

        result = attach_metaphysical_features(df, enabled=True)

        self.assertEqual(result.loc[0, "csi500_flash_crash"], 1)
        self.assertEqual(result.loc[1, "gold_macro_shock"], 1)
        self.assertNotIn("__meta_date__", result.columns)

    @patch("src.models.metaphysical.adapter.build_metaphysical_features_if_enabled", return_value=None)
    def test_attach_skips_when_feature_builder_returns_none(self, _mock_build):
        df = pd.DataFrame({"date": ["2026-04-20"], "close": [100.0]})
        result = attach_metaphysical_features(df, enabled=True)
        self.assertEqual(list(result.columns), ["date", "close"])

    @patch("src.models.metaphysical.adapter.apply_author_regime_weights")
    @patch("src.models.metaphysical.adapter.attach_metaphysical_features")
    def test_attach_next_production_features_returns_weighted_frame(self, mock_attach, mock_apply):
        base = pd.DataFrame({"date": ["2026-04-20"], "close": [100.0]})
        weighted = pd.DataFrame(
            {
                "date": ["2026-04-20"],
                "weighted_uranus_retrograde_boundary_distance": [0.7],
                "weighted_uranus_cycle_84_distance": [0.6],
                "weighted_saturn_jupiter_cycle_distance": [0.4],
                "weighted_volatility_plus_score": [0.5],
            }
        )
        mock_attach.return_value = base
        mock_apply.return_value = weighted

        result = attach_next_production_metaphysical_features(base, enabled=True)

        mock_apply.assert_called_once()
        self.assertIn("weighted_uranus_retrograde_boundary_distance", result.columns)

    @patch("src.models.metaphysical.adapter.apply_author_regime_weights")
    @patch("src.models.metaphysical.adapter.attach_metaphysical_features")
    def test_attach_next_production_features_can_return_candidate_only(self, mock_attach, mock_apply):
        base = pd.DataFrame({"date": ["2026-04-20"], "close": [100.0]})
        weighted = pd.DataFrame(
            {
                "date": ["2026-04-20"],
                "close": [100.0],
                "quant_return_1d": [0.01],
                "weighted_uranus_retrograde_boundary_distance": [0.7],
                "weighted_uranus_cycle_84_distance": [0.6],
                "weighted_saturn_jupiter_cycle_distance": [0.4],
                "weighted_volatility_plus_score": [0.5],
            }
        )
        mock_attach.return_value = base
        mock_apply.return_value = weighted

        result = attach_next_production_metaphysical_features(base, enabled=True, candidate_only=True)

        self.assertIn("date", result.columns)
        self.assertIn("weighted_uranus_retrograde_boundary_distance", result.columns)
        self.assertNotIn("close", result.columns)

    @patch("src.models.metaphysical.adapter.apply_author_regime_weights")
    def test_finalize_next_production_candidate_frame_can_reduce_to_candidate_pool(self, mock_apply):
        weighted = pd.DataFrame(
            {
                "quant_return_1d": [0.01],
                "weighted_uranus_retrograde_boundary_distance": [0.7],
                "weighted_uranus_cycle_84_distance": [0.6],
                "weighted_saturn_jupiter_cycle_distance": [0.4],
                "weighted_volatility_plus_score": [0.5],
                "close": [100.0],
            }
        )
        mock_apply.return_value = weighted

        result = finalize_next_production_candidate_frame(weighted, candidate_only=True)

        self.assertIn("quant_return_1d", result.columns)
        self.assertIn("weighted_uranus_retrograde_boundary_distance", result.columns)
        self.assertNotIn("close", result.columns)


class TestMetaphysicalResonance(unittest.TestCase):
    @patch("src.models.metaphysical.resonance.build_metaphysical_features_if_enabled")
    def test_build_resonance_features_maps_shared_triggers(self, mock_build):
        dates = pd.date_range("2026-04-20", periods=2, freq="D")
        mock_build.return_value = pd.DataFrame(
            {
                "csi500_liquidity_crisis": [1, 0],
                "csi500_flash_crash": [1, 0],
                "csi500_capital_drain": [0, 1],
                "gold_panic_rush": [0, 0],
                "gold_macro_shock": [0, 0],
                "gold_currency_crisis": [0, 0],
            },
            index=dates,
        )

        result = build_resonance_backtest_features(dates)

        self.assertEqual(result.loc[dates[0], "bazi_risk"], 1)
        self.assertEqual(result.loc[dates[0], "astro_risk"], 1)
        self.assertEqual(result.loc[dates[0], "resonance"], 1)
        self.assertIn("流动性危机", result.loc[dates[0], "bazi_event"])
        self.assertIn("闪崩相位", result.loc[dates[0], "astro_event"])
        self.assertIn("资金抽离", result.loc[dates[1], "astro_event"])

    @patch("src.models.metaphysical.resonance.build_metaphysical_features_if_enabled", return_value=None)
    def test_build_resonance_features_returns_zero_frame_when_missing(self, _mock_build):
        dates = pd.date_range("2026-04-20", periods=1, freq="D")
        result = build_resonance_backtest_features(dates)
        self.assertEqual(int(result.iloc[0]["resonance"]), 0)

    @patch("src.models.metaphysical.resonance.attach_next_production_metaphysical_features")
    def test_build_next_production_backtest_features_uses_attach_path_when_date_exists(self, mock_attach):
        base = pd.DataFrame({"date": ["2026-04-20"], "close": [100.0]})
        mock_attach.return_value = pd.DataFrame({"date": ["2026-04-20"], "quant_return_1d": [0.01]})

        result = build_next_production_backtest_features(base, enabled=True)

        mock_attach.assert_called_once()
        self.assertIn("quant_return_1d", result.columns)

    @patch("src.models.metaphysical.resonance.finalize_next_production_candidate_frame")
    def test_build_next_production_backtest_features_falls_back_to_finalize_without_date(self, mock_finalize):
        base = pd.DataFrame({"close": [100.0], "quant_return_1d": [0.01]})
        mock_finalize.return_value = pd.DataFrame({"quant_return_1d": [0.01]})

        result = build_next_production_backtest_features(base, enabled=False)

        mock_finalize.assert_called_once()
        self.assertIn("quant_return_1d", result.columns)


if __name__ == "__main__":
    unittest.main()
