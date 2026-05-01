# -*- coding: utf-8 -*-
"""Integration tests for backtest service and repository.

These tests run against a temporary SQLite DB (same approach as other tests)
and validate idempotency/force semantics, result field correctness,
summary creation, and query methods.
"""

import os
import tempfile
import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from src.config import Config
from src.core.backtest_engine import OVERALL_SENTINEL_CODE
from src.services.backtest_service import BacktestService
from src.storage import AnalysisHistory, BacktestResult, BacktestSummary, DatabaseManager, StockDaily


class BacktestServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._temp_dir.name, "test_backtest_service.db")
        os.environ["DATABASE_PATH"] = self._db_path
        os.environ["BACKTEST_EVAL_WINDOW_DAYS"] = "3"

        Config._instance = None
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

        # Ensure analysis is old enough for default min_age_days=14
        old_created_at = datetime(2024, 1, 1, 0, 0, 0)

        with self.db.get_session() as session:
            session.add(
                AnalysisHistory(
                    query_id="q1",
                    code="600519",
                    name="贵州茅台",
                    report_type="simple",
                    sentiment_score=80,
                    operation_advice="买入",
                    trend_prediction="看多",
                    analysis_summary="test",
                    stop_loss=95.0,
                    take_profit=110.0,
                    created_at=old_created_at,
                    context_snapshot='{"enhanced_context": {"date": "2024-01-01"}}',
                )
            )

            # Analysis day close
            session.add(
                StockDaily(
                    code="600519",
                    date=date(2024, 1, 1),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                )
            )

            # Forward bars (3 days) that hit take-profit on day1
            session.add_all(
                [
                    StockDaily(code="600519", date=date(2024, 1, 2), high=111.0, low=100.0, close=105.0),
                    StockDaily(code="600519", date=date(2024, 1, 3), high=108.0, low=103.0, close=106.0),
                    StockDaily(code="600519", date=date(2024, 1, 4), high=109.0, low=104.0, close=107.0),
                ]
            )
            session.commit()

    def _seed_analysis(
        self,
        *,
        query_id: str,
        analysis_date: date,
        created_at: datetime,
        operation_advice: str,
        trend_prediction: str,
        start_close: float,
        forward_bars: list[StockDaily],
    ) -> None:
        with self.db.get_session() as session:
            session.add(
                AnalysisHistory(
                    query_id=query_id,
                    code="600519",
                    name="贵州茅台",
                    report_type="simple",
                    sentiment_score=60,
                    operation_advice=operation_advice,
                    trend_prediction=trend_prediction,
                    analysis_summary="extra-test",
                    stop_loss=None,
                    take_profit=None,
                    created_at=created_at,
                    context_snapshot=f'{{"enhanced_context": {{"date": "{analysis_date.isoformat()}"}}}}',
                )
            )
            session.add(
                StockDaily(
                    code="600519",
                    date=analysis_date,
                    open=start_close,
                    high=start_close,
                    low=start_close,
                    close=start_close,
                )
            )
            session.add_all(forward_bars)
            session.commit()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        self._temp_dir.cleanup()

    def _count_results(self) -> int:
        with self.db.get_session() as session:
            return session.query(BacktestResult).count()

    def test_force_semantics(self) -> None:
        service = BacktestService(self.db)

        stats1 = service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(stats1["candidate_count"], 1)
        self.assertEqual(stats1["saved"], 1)
        self.assertEqual(self._count_results(), 1)

        # Non-force should be idempotent
        stats2 = service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(stats2["saved"], 0)
        self.assertEqual(self._count_results(), 1)

        # Force should replace existing result without unique constraint errors
        stats3 = service.run_backtest(code="600519", force=True, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(stats3["saved"], 1)
        self.assertEqual(self._count_results(), 1)

    def _run_and_get_result(self) -> BacktestResult:
        """Helper: run backtest and return the single BacktestResult row."""
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)
        with self.db.get_session() as session:
            return session.query(BacktestResult).one()

    def test_result_fields_correct(self) -> None:
        """Verify BacktestResult row contains correct evaluation values."""
        result = self._run_and_get_result()

        self.assertEqual(result.eval_status, "completed")
        self.assertEqual(result.code, "600519")
        self.assertEqual(result.analysis_date, date(2024, 1, 1))
        self.assertEqual(result.operation_advice, "买入")
        self.assertAlmostEqual(result.ranking_score, 80.0)
        self.assertEqual(result.score_source, "sentiment_score")
        self.assertEqual(result.position_recommendation, "long")
        self.assertEqual(result.direction_expected, "up")

        # Prices
        self.assertAlmostEqual(result.start_price, 100.0)
        self.assertAlmostEqual(result.end_close, 107.0)
        self.assertAlmostEqual(result.stock_return_pct, 7.0)

        # Direction & outcome
        self.assertEqual(result.outcome, "win")
        self.assertTrue(result.direction_correct)

        # Target hits -- day2 high=111 >= take_profit=110
        self.assertTrue(result.hit_take_profit)
        self.assertFalse(result.hit_stop_loss)
        self.assertEqual(result.first_hit, "take_profit")
        self.assertEqual(result.first_hit_trading_days, 1)
        self.assertEqual(result.first_hit_date, date(2024, 1, 2))

        # Simulated execution
        self.assertAlmostEqual(result.simulated_entry_price, 100.0)
        self.assertAlmostEqual(result.simulated_exit_price, 110.0)
        self.assertEqual(result.simulated_exit_reason, "take_profit")
        self.assertAlmostEqual(result.simulated_return_pct, 10.0)

    def test_summaries_created_after_run(self) -> None:
        """Verify both overall and per-stock BacktestSummary rows are created."""
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        with self.db.get_session() as session:
            # Overall summary uses sentinel code
            overall = session.query(BacktestSummary).filter(
                BacktestSummary.scope == "overall",
                BacktestSummary.code == OVERALL_SENTINEL_CODE,
            ).first()
            self.assertIsNotNone(overall)
            self.assertEqual(overall.total_evaluations, 1)
            self.assertEqual(overall.completed_count, 1)
            self.assertEqual(overall.win_count, 1)
            self.assertEqual(overall.loss_count, 0)
            self.assertAlmostEqual(overall.win_rate_pct, 100.0)

            # Stock-level summary
            stock = session.query(BacktestSummary).filter(
                BacktestSummary.scope == "stock",
                BacktestSummary.code == "600519",
            ).first()
            self.assertIsNotNone(stock)
            self.assertEqual(stock.total_evaluations, 1)
            self.assertEqual(stock.completed_count, 1)
            self.assertEqual(stock.win_count, 1)

    def test_get_summary_overall_returns_sentinel_as_none(self) -> None:
        """Verify get_summary translates __overall__ sentinel back to None."""
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        summary = service.get_summary(scope="overall", code=None)
        self.assertIsNotNone(summary)
        self.assertIsNone(summary["code"])
        self.assertEqual(summary["scope"], "overall")
        self.assertEqual(summary["win_count"], 1)

    def test_agent_learning_summary_helpers_keep_skill_rollups_neutral_until_supported(self) -> None:
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        global_summary = service.get_global_summary(eval_window_days=3)
        stock_summary = service.get_stock_summary("600519", eval_window_days=3)
        skill_summary = service.get_skill_summary("bull_trend", eval_window_days=3)
        strategy_summary = service.get_strategy_summary("bull_trend", eval_window_days=3)

        self.assertIsNotNone(global_summary)
        self.assertEqual(global_summary["total_evaluations"], 1)
        self.assertAlmostEqual(global_summary["win_rate"], 1.0)
        self.assertAlmostEqual(global_summary["direction_accuracy"], 1.0)
        self.assertAlmostEqual(global_summary["avg_return"], 0.10)

        self.assertIsNotNone(stock_summary)
        self.assertEqual(stock_summary["code"], "600519")
        self.assertAlmostEqual(stock_summary["win_rate"], 1.0)

        self.assertIsNone(skill_summary)
        self.assertIsNone(strategy_summary)

    def test_get_recent_evaluations(self) -> None:
        """Verify get_recent_evaluations returns correct paginated results."""
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        data = service.get_recent_evaluations(code="600519", limit=10, page=1)
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["page"], 1)
        self.assertEqual(data["limit"], 10)
        self.assertEqual(len(data["items"]), 1)

        item = data["items"][0]
        self.assertEqual(item["code"], "600519")
        self.assertEqual(item["outcome"], "win")
        self.assertEqual(item["direction_expected"], "up")
        self.assertTrue(item["direction_correct"])
        self.assertAlmostEqual(item["ranking_score"], 80.0)
        self.assertEqual(item["score_source"], "sentiment_score")

    def test_get_recent_evaluations_supports_tracking_fields_and_analysis_date_filters(self) -> None:
        self._seed_analysis(
            query_id="q2",
            analysis_date=date(2024, 1, 10),
            created_at=datetime(2024, 1, 10, 0, 0, 0),
            operation_advice="买入",
            trend_prediction="看多",
            start_close=100.0,
            forward_bars=[
                StockDaily(code="600519", date=date(2024, 1, 11), high=101.0, low=95.0, close=96.0),
            ],
        )

        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=1, min_age_days=0, limit=20)

        data = service.get_recent_evaluations(
            code="600519",
            eval_window_days=1,
            limit=10,
            page=1,
            analysis_date_from=date(2024, 1, 10),
            analysis_date_to=date(2024, 1, 10),
        )
        self.assertEqual(data["total"], 1)
        item = data["items"][0]
        self.assertEqual(item["stock_name"], "贵州茅台")
        self.assertEqual(item["trend_prediction"], "看多")
        self.assertEqual(item["actual_movement"], "down")
        self.assertAlmostEqual(item["actual_return_pct"], -4.0)
        self.assertFalse(item["direction_correct"])

    def test_get_summary_supports_analysis_date_range(self) -> None:
        self._seed_analysis(
            query_id="q2",
            analysis_date=date(2024, 1, 10),
            created_at=datetime(2024, 1, 10, 0, 0, 0),
            operation_advice="买入",
            trend_prediction="看多",
            start_close=100.0,
            forward_bars=[
                StockDaily(code="600519", date=date(2024, 1, 11), high=101.0, low=95.0, close=96.0),
            ],
        )

        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=1, min_age_days=0, limit=20)

        summary = service.get_summary(
            scope="stock",
            code="600519",
            eval_window_days=1,
            analysis_date_from=date(2024, 1, 10),
            analysis_date_to=date(2024, 1, 10),
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary["total_evaluations"], 1)
        self.assertEqual(summary["completed_count"], 1)
        self.assertEqual(summary["win_count"], 0)
        self.assertEqual(summary["loss_count"], 1)
        self.assertAlmostEqual(summary["direction_accuracy_pct"], 0.0)

    def test_get_summary_date_range_filters_to_single_window_and_engine(self) -> None:
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        with self.db.get_session() as session:
            base_result = session.query(BacktestResult).filter(
                BacktestResult.code == "600519",
                BacktestResult.eval_window_days == 3,
                BacktestResult.engine_version == "v1",
            ).one()
            session.add_all([
                BacktestResult(
                    analysis_history_id=base_result.analysis_history_id,
                    code=base_result.code,
                    analysis_date=base_result.analysis_date,
                    eval_window_days=1,
                    engine_version="v1",
                    eval_status="completed",
                    evaluated_at=datetime(2024, 1, 5, 0, 0, 0),
                    operation_advice="买入",
                    position_recommendation="long",
                    start_price=100.0,
                    end_close=96.0,
                    stock_return_pct=-4.0,
                    direction_expected="up",
                    direction_correct=False,
                    outcome="loss",
                    simulated_return_pct=-4.0,
                ),
                BacktestResult(
                    analysis_history_id=base_result.analysis_history_id,
                    code=base_result.code,
                    analysis_date=base_result.analysis_date,
                    eval_window_days=3,
                    engine_version="v2",
                    eval_status="completed",
                    evaluated_at=datetime(2024, 1, 6, 0, 0, 0),
                    operation_advice="买入",
                    position_recommendation="long",
                    start_price=100.0,
                    end_close=96.0,
                    stock_return_pct=-4.0,
                    direction_expected="up",
                    direction_correct=False,
                    outcome="loss",
                    simulated_return_pct=-4.0,
                ),
            ])
            session.commit()

        rows = service.repo.list_results(
            code="600519",
            eval_window_days=3,
            engine_version="v1",
            analysis_date_from=date(2024, 1, 1),
            analysis_date_to=date(2024, 1, 1),
        )
        self.assertEqual(len(rows), 1)

        evaluations = service.get_recent_evaluations(
            code="600519",
            eval_window_days=3,
            limit=10,
            page=1,
            analysis_date_from=date(2024, 1, 1),
            analysis_date_to=date(2024, 1, 1),
        )
        self.assertEqual(evaluations["total"], 1)
        self.assertEqual(len(evaluations["items"]), 1)
        self.assertEqual(evaluations["items"][0]["engine_version"], "v1")

        # Without explicit eval_window_days, summary infers the smallest
        # window from matched rows (window=1 in this dataset) instead of
        # falling back to the config default.
        summary_inferred = service.get_summary(
            scope="stock",
            code="600519",
            analysis_date_from=date(2024, 1, 1),
            analysis_date_to=date(2024, 1, 1),
        )
        self.assertIsNotNone(summary_inferred)
        assert summary_inferred is not None
        self.assertEqual(summary_inferred["eval_window_days"], 1)
        self.assertEqual(summary_inferred["engine_version"], "v1")
        self.assertEqual(summary_inferred["total_evaluations"], 1)
        self.assertEqual(summary_inferred["completed_count"], 1)
        self.assertEqual(summary_inferred["win_count"], 0)
        self.assertEqual(summary_inferred["loss_count"], 1)
        self.assertAlmostEqual(summary_inferred["direction_accuracy_pct"], 0.0)

        # With explicit eval_window_days=3, summary filters to that window only.
        summary_explicit = service.get_summary(
            scope="stock",
            code="600519",
            eval_window_days=3,
            analysis_date_from=date(2024, 1, 1),
            analysis_date_to=date(2024, 1, 1),
        )
        self.assertIsNotNone(summary_explicit)
        assert summary_explicit is not None
        self.assertEqual(summary_explicit["eval_window_days"], 3)
        self.assertEqual(summary_explicit["engine_version"], "v1")
        self.assertEqual(summary_explicit["total_evaluations"], 1)
        self.assertEqual(summary_explicit["completed_count"], 1)
        self.assertEqual(summary_explicit["win_count"], 1)
        self.assertEqual(summary_explicit["loss_count"], 0)
        self.assertAlmostEqual(summary_explicit["direction_accuracy_pct"], 100.0)

    def test_get_summary_date_range_rejects_excessive_row_counts(self) -> None:
        service = BacktestService(self.db)
        service.run_backtest(code="600519", force=False, eval_window_days=3, min_age_days=0, limit=10)

        with patch.object(BacktestService, "MAX_DYNAMIC_SUMMARY_ROWS", 0):
            with self.assertRaisesRegex(ValueError, "Date-filtered summary matches too many rows"):
                service.get_summary(
                    scope="stock",
                    code="600519",
                    analysis_date_from=date(2024, 1, 1),
                    analysis_date_to=date(2024, 1, 1),
                )

    def test_multi_stock_summaries(self) -> None:
        """Verify separate summaries for multiple stocks + correct overall aggregate."""
        old_created_at = datetime(2024, 1, 1, 0, 0, 0)

        with self.db.get_session() as session:
            # Second stock with sell advice -- price drops (win for cash/down)
            session.add(
                AnalysisHistory(
                    query_id="q2",
                    code="000001",
                    name="平安银行",
                    report_type="simple",
                    sentiment_score=30,
                    operation_advice="卖出",
                    trend_prediction="看空",
                    analysis_summary="test2",
                    stop_loss=None,
                    take_profit=None,
                    created_at=old_created_at,
                    context_snapshot='{"enhanced_context": {"date": "2024-01-01"}}',
                )
            )
            session.add(
                StockDaily(code="000001", date=date(2024, 1, 1), open=10.0, high=10.2, low=9.8, close=10.0)
            )
            session.add_all([
                StockDaily(code="000001", date=date(2024, 1, 2), high=10.0, low=9.5, close=9.6),
                StockDaily(code="000001", date=date(2024, 1, 3), high=9.7, low=9.3, close=9.4),
                StockDaily(code="000001", date=date(2024, 1, 4), high=9.5, low=9.0, close=9.1),
            ])
            session.commit()

        service = BacktestService(self.db)
        stats = service.run_backtest(code=None, force=False, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(stats["saved"], 2)
        self.assertEqual(stats["completed"], 2)

        with self.db.get_session() as session:
            # Each stock has its own summary
            s1 = session.query(BacktestSummary).filter(
                BacktestSummary.scope == "stock", BacktestSummary.code == "600519"
            ).first()
            s2 = session.query(BacktestSummary).filter(
                BacktestSummary.scope == "stock", BacktestSummary.code == "000001"
            ).first()
            self.assertIsNotNone(s1)
            self.assertIsNotNone(s2)
            self.assertEqual(s1.win_count, 1)
            self.assertEqual(s2.win_count, 1)

            # Overall aggregates both
            overall = session.query(BacktestSummary).filter(
                BacktestSummary.scope == "overall",
                BacktestSummary.code == OVERALL_SENTINEL_CODE,
            ).first()
            self.assertIsNotNone(overall)
            self.assertEqual(overall.total_evaluations, 2)
            self.assertEqual(overall.completed_count, 2)
            self.assertEqual(overall.win_count, 2)

    def test_extract_ranking_score_prefers_signal_score_then_fallbacks(self) -> None:
        service = BacktestService(self.db)

        analysis_with_signal = SimpleNamespace(
            context_snapshot='{"enhanced_context":{"trend_analysis":{"signal_score":69}}}',
            raw_result='{"sentiment_score":55,"dashboard":{"data_perspective":{"trend_status":{"trend_score":88}}}}',
            sentiment_score=44,
        )
        score, source = service._extract_ranking_score(analysis_with_signal)
        self.assertEqual(score, 69.0)
        self.assertEqual(source, "signal_score")

        analysis_with_sentiment = SimpleNamespace(
            context_snapshot='{}',
            raw_result='{"sentiment_score":61}',
            sentiment_score=44,
        )
        score, source = service._extract_ranking_score(analysis_with_sentiment)
        self.assertEqual(score, 61.0)
        self.assertEqual(source, "sentiment_score")

        analysis_with_trend_only = SimpleNamespace(
            context_snapshot='{}',
            raw_result='{"dashboard":{"data_perspective":{"trend_status":{"trend_score":75}}}}',
            sentiment_score=None,
        )
        score, source = service._extract_ranking_score(analysis_with_trend_only)
        self.assertEqual(score, 75.0)
        self.assertEqual(source, "trend_score")

    def test_build_ranked_candidates_orders_by_score_desc(self) -> None:
        service = BacktestService(self.db)
        analyses = [
            SimpleNamespace(
                id=1,
                code="AAA",
                created_at=datetime(2024, 1, 1, 0, 0, 0),
                context_snapshot='{"enhanced_context":{"date":"2024-01-01","trend_analysis":{"signal_score":52}}}',
                raw_result='{}',
                sentiment_score=52,
            ),
            SimpleNamespace(
                id=2,
                code="BBB",
                created_at=datetime(2024, 1, 2, 0, 0, 0),
                context_snapshot='{"enhanced_context":{"date":"2024-01-02","trend_analysis":{"signal_score":88}}}',
                raw_result='{}',
                sentiment_score=88,
            ),
        ]

        ranked = service._build_ranked_candidates(analyses)
        self.assertEqual([item["analysis"].code for item in ranked], ["BBB", "AAA"])
        self.assertEqual([item["ranking_score"] for item in ranked], [88.0, 52.0])

    def test_run_backtest_supports_score_threshold(self) -> None:
        self._seed_analysis(
            query_id="q2",
            analysis_date=date(2024, 1, 10),
            created_at=datetime(2024, 1, 10, 0, 0, 0),
            operation_advice="买入",
            trend_prediction="看多",
            start_close=100.0,
            forward_bars=[
                StockDaily(code="600519", date=date(2024, 1, 11), high=101.0, low=99.0, close=100.0),
                StockDaily(code="600519", date=date(2024, 1, 12), high=102.0, low=99.0, close=101.0),
                StockDaily(code="600519", date=date(2024, 1, 13), high=103.0, low=99.0, close=102.0),
            ],
        )

        with self.db.get_session() as session:
            older = session.query(AnalysisHistory).filter(AnalysisHistory.query_id == "q1").one()
            newer = session.query(AnalysisHistory).filter(AnalysisHistory.query_id == "q2").one()
            older.context_snapshot = '{"enhanced_context":{"date":"2024-01-01","trend_analysis":{"signal_score":80}}}'
            newer.context_snapshot = '{"enhanced_context":{"date":"2024-01-10","trend_analysis":{"signal_score":55}}}'
            session.commit()

        service = BacktestService(self.db)
        stats = service.run_backtest(
            code="600519",
            force=False,
            eval_window_days=3,
            min_age_days=0,
            limit=20,
            score_threshold=60,
        )
        self.assertEqual(stats["candidate_count"], 1)
        self.assertEqual(stats["saved"], 1)

    def test_run_backtest_supports_top_n(self) -> None:
        old_created_at = datetime(2024, 1, 1, 0, 0, 0)
        with self.db.get_session() as session:
            session.add(
                AnalysisHistory(
                    query_id="q2",
                    code="000001",
                    name="平安银行",
                    report_type="simple",
                    sentiment_score=30,
                    operation_advice="卖出",
                    trend_prediction="看空",
                    analysis_summary="test2",
                    stop_loss=None,
                    take_profit=None,
                    created_at=old_created_at,
                    context_snapshot='{"enhanced_context":{"date":"2024-01-01","trend_analysis":{"signal_score":30}}}',
                )
            )
            session.add(StockDaily(code="000001", date=date(2024, 1, 1), open=10.0, high=10.2, low=9.8, close=10.0))
            session.add_all([
                StockDaily(code="000001", date=date(2024, 1, 2), high=10.0, low=9.5, close=9.6),
                StockDaily(code="000001", date=date(2024, 1, 3), high=9.7, low=9.3, close=9.4),
                StockDaily(code="000001", date=date(2024, 1, 4), high=9.5, low=9.0, close=9.1),
            ])
            session.commit()

        service = BacktestService(self.db)
        stats = service.run_backtest(
            code=None,
            force=False,
            eval_window_days=3,
            min_age_days=0,
            limit=20,
            top_n=1,
        )
        self.assertEqual(stats["candidate_count"], 1)
        self.assertEqual(stats["saved"], 1)

    def test_scan_parameter_grid_returns_in_memory_combo_summaries(self) -> None:
        self._seed_analysis(
            query_id="q2",
            analysis_date=date(2024, 1, 10),
            created_at=datetime(2024, 1, 10, 0, 0, 0),
            operation_advice="买入",
            trend_prediction="看多",
            start_close=100.0,
            forward_bars=[
                StockDaily(code="600519", date=date(2024, 1, 11), high=101.0, low=99.0, close=100.0),
                StockDaily(code="600519", date=date(2024, 1, 12), high=102.0, low=99.0, close=101.0),
                StockDaily(code="600519", date=date(2024, 1, 13), high=103.0, low=99.0, close=102.0),
            ],
        )
        with self.db.get_session() as session:
            older = session.query(AnalysisHistory).filter(AnalysisHistory.query_id == "q1").one()
            newer = session.query(AnalysisHistory).filter(AnalysisHistory.query_id == "q2").one()
            older.context_snapshot = '{"enhanced_context":{"date":"2024-01-01","trend_analysis":{"signal_score":80}}}'
            newer.context_snapshot = '{"enhanced_context":{"date":"2024-01-10","trend_analysis":{"signal_score":55}}}'
            session.commit()

        service = BacktestService(self.db)
        scan = service.scan_parameter_grid(
            code="600519",
            min_age_days=0,
            limit=20,
            eval_window_days_options=[3],
            score_threshold_options=[None, 60.0],
            top_n_options=[None, 1],
        )

        self.assertEqual(scan["raw_candidate_count"], 2)
        self.assertEqual(scan["ranked_candidate_count"], 2)
        self.assertEqual(len(scan["scans"]), 4)
        candidate_counts = {
            (item["score_threshold"], item["top_n"]): item["candidate_count"]
            for item in scan["scans"]
        }
        self.assertEqual(candidate_counts[(None, None)], 2)
        self.assertEqual(candidate_counts[(None, 1)], 1)
        self.assertEqual(candidate_counts[(60.0, None)], 1)
        self.assertEqual(candidate_counts[(60.0, 1)], 1)
        self.assertIsNotNone(scan["best_by_return"])
        self.assertIsNotNone(scan["best_by_win_rate"])
        self.assertEqual(scan["conclusion"]["status"], "ok")
        self.assertIn("优先考虑持有", scan["conclusion"]["summary_text"])
        self.assertEqual(scan["conclusion"]["recommended_scan"], scan["best_by_return"])
        self.assertEqual(scan["best_by_return"], scan["scans"][0])
        self.assertIn(scan["best_by_win_rate"]["candidate_count"], {1, 2})
        self.assertGreaterEqual(
            scan["scans"][0]["avg_simulated_return_pct"],
            scan["scans"][-1]["avg_simulated_return_pct"],
        )
        self.assertEqual(self._count_results(), 0)

    def test_scan_parameter_grid_local_data_only_skips_fill_attempts(self) -> None:
        with self.db.get_session() as session:
            row = session.query(StockDaily).filter(StockDaily.code == "600519", StockDaily.date == date(2024, 1, 4)).one()
            session.delete(row)
            session.commit()

        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data") as mocked_fill:
            scan = service.scan_parameter_grid(
                code="600519",
                min_age_days=0,
                limit=20,
                local_data_only=True,
                eval_window_days_options=[3],
                score_threshold_options=[None],
                top_n_options=[None],
            )

        mocked_fill.assert_not_called()
        self.assertTrue(scan["local_data_only"])
        self.assertEqual(len(scan["scans"]), 1)
        self.assertEqual(scan["scans"][0]["insufficient_count"], 1)
        self.assertEqual(scan["conclusion"]["status"], "insufficient_data")
        self.assertEqual(self._count_results(), 0)

    def test_recent_analysis_skips_future_data_fill(self) -> None:
        today = date.today()
        with self.db.get_session() as session:
            session.add(
                AnalysisHistory(
                    query_id="recent-q1",
                    code="000333",
                    name="美的集团",
                    report_type="simple",
                    sentiment_score=72,
                    operation_advice="买入",
                    trend_prediction="看多",
                    analysis_summary="recent-test",
                    stop_loss=None,
                    take_profit=None,
                    created_at=datetime(today.year, today.month, today.day, 9, 30, 0),
                    context_snapshot=f'{{"enhanced_context": {{"date": "{today.isoformat()}", "trend_analysis": {{"signal_score": 72}}}}}}',
                )
            )
            session.add(
                StockDaily(
                    code="000333",
                    date=today,
                    open=50.0,
                    high=50.5,
                    low=49.5,
                    close=50.0,
                )
            )
            session.commit()

        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data") as mocked_fill:
            stats = service.run_backtest(
                code="000333",
                force=True,
                eval_window_days=10,
                min_age_days=0,
                limit=10,
            )

        mocked_fill.assert_not_called()
        self.assertEqual(stats["candidate_count"], 1)
        self.assertEqual(stats["saved"], 1)
        self.assertEqual(stats["completed"], 0)
        self.assertEqual(stats["insufficient"], 1)

        with self.db.get_session() as session:
            result = session.query(BacktestResult).filter(BacktestResult.code == "000333").one()
            self.assertEqual(result.eval_status, "insufficient_data")
            self.assertEqual(result.analysis_date, today)


if __name__ == "__main__":
    unittest.main()
