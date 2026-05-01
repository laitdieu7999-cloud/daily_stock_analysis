# -*- coding: utf-8 -*-
"""Backtest orchestration service."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import and_, select

from src.config import get_config
from src.core.backtest_engine import OVERALL_SENTINEL_CODE, BacktestEngine, EvaluationConfig
from src.repositories.backtest_repo import BacktestRepository
from src.repositories.stock_repo import StockRepository
from src.storage import BacktestResult, BacktestSummary, DatabaseManager

logger = logging.getLogger(__name__)


class BacktestService:
    """Service layer to run and query backtests."""

    MAX_DYNAMIC_SUMMARY_ROWS = 2000

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()
        self.repo = BacktestRepository(self.db)
        self.stock_repo = StockRepository(self.db)

    def run_backtest(
        self,
        *,
        code: Optional[str] = None,
        force: bool = False,
        eval_window_days: Optional[int] = None,
        min_age_days: Optional[int] = None,
        limit: int = 200,
        score_threshold: Optional[float] = None,
        top_n: Optional[int] = None,
    ) -> Dict[str, Any]:
        config = get_config()

        if eval_window_days is None:
            eval_window_days = getattr(config, "backtest_eval_window_days", 10)
        if min_age_days is None:
            min_age_days = getattr(config, "backtest_min_age_days", 14)

        engine_version = getattr(config, "backtest_engine_version", "v1")
        neutral_band_pct = float(getattr(config, "backtest_neutral_band_pct", 2.0))

        eval_config = EvaluationConfig(
            eval_window_days=int(eval_window_days),
            neutral_band_pct=neutral_band_pct,
            engine_version=str(engine_version),
        )

        raw_candidates = self.repo.get_candidates(
            code=code,
            min_age_days=int(min_age_days),
            limit=int(limit),
            eval_window_days=int(eval_window_days),
            engine_version=str(engine_version),
            force=force,
        )
        candidates = self._build_ranked_candidates(raw_candidates)
        if score_threshold is not None:
            candidates = [
                candidate
                for candidate in candidates
                if candidate["ranking_score"] >= float(score_threshold)
            ]
        if top_n is not None:
            candidates = candidates[: max(int(top_n), 0)]

        processed = 0
        completed = 0
        insufficient = 0
        errors = 0
        touched_codes: set[str] = set()

        results_to_save: List[BacktestResult] = []

        for candidate in candidates:
            processed += 1
            result = self._evaluate_candidate(candidate, eval_config)
            touched_codes.add(result.code)
            if result.eval_status == "insufficient_data":
                insufficient += 1
            elif result.eval_status == "completed":
                completed += 1
            else:
                errors += 1
            results_to_save.append(result)

        saved = 0
        if results_to_save:
            saved = self.repo.save_results_batch(results_to_save, replace_existing=force)

        if saved:
            self._recompute_summaries(
                touched_codes=sorted(touched_codes),
                eval_window_days=int(eval_window_days),
                engine_version=str(engine_version),
            )

        return {
            "candidate_count": len(candidates),
            "processed": processed,
            "saved": saved,
            "completed": completed,
            "insufficient": insufficient,
            "errors": errors,
        }

    def scan_parameter_grid(
        self,
        *,
        code: Optional[str] = None,
        min_age_days: Optional[int] = None,
        limit: int = 200,
        eval_window_days_options: Optional[Sequence[int]] = None,
        score_threshold_options: Optional[Sequence[Optional[float]]] = None,
        top_n_options: Optional[Sequence[Optional[int]]] = None,
        local_data_only: bool = False,
    ) -> Dict[str, Any]:
        config = get_config()

        if min_age_days is None:
            min_age_days = getattr(config, "backtest_min_age_days", 14)
        if eval_window_days_options is None:
            eval_window_days_options = [int(getattr(config, "backtest_eval_window_days", 10))]
        if score_threshold_options is None:
            score_threshold_options = [None, 60.0, 70.0]
        if top_n_options is None:
            top_n_options = [None, 3, 5]

        engine_version = getattr(config, "backtest_engine_version", "v1")
        neutral_band_pct = float(getattr(config, "backtest_neutral_band_pct", 2.0))

        raw_candidates = self.repo.get_candidates(
            code=code,
            min_age_days=int(min_age_days),
            limit=int(limit),
            eval_window_days=int(next(iter(eval_window_days_options), getattr(config, "backtest_eval_window_days", 10))),
            engine_version=str(engine_version),
            force=True,
        )
        ranked_candidates = self._build_ranked_candidates(raw_candidates)

        scans: List[Dict[str, Any]] = []
        for eval_window_days in eval_window_days_options:
            eval_days = int(eval_window_days)
            eval_config = EvaluationConfig(
                eval_window_days=eval_days,
                neutral_band_pct=neutral_band_pct,
                engine_version=str(engine_version),
            )
            for score_threshold in score_threshold_options:
                filtered_candidates = list(ranked_candidates)
                if score_threshold is not None:
                    filtered_candidates = [
                        item for item in filtered_candidates if item["ranking_score"] >= float(score_threshold)
                    ]
                for top_n in top_n_options:
                    selected_candidates = list(filtered_candidates)
                    if top_n is not None:
                        selected_candidates = selected_candidates[: max(int(top_n), 0)]

                    results = [
                        self._evaluate_candidate(
                            candidate,
                            eval_config,
                            allow_data_fill=not local_data_only,
                        )
                        for candidate in selected_candidates
                    ]
                    summary = BacktestEngine.compute_summary(
                        results=results,
                        scope="overall",
                        code=OVERALL_SENTINEL_CODE,
                        eval_window_days=eval_days,
                        engine_version=str(engine_version),
                    )
                    scans.append(
                        {
                            "eval_window_days": eval_days,
                            "score_threshold": score_threshold,
                            "top_n": top_n,
                            "candidate_count": len(selected_candidates),
                            "completed_count": summary.get("completed_count"),
                            "insufficient_count": summary.get("insufficient_count"),
                            "win_count": summary.get("win_count"),
                            "loss_count": summary.get("loss_count"),
                            "neutral_count": summary.get("neutral_count"),
                            "win_rate_pct": summary.get("win_rate_pct"),
                            "direction_accuracy_pct": summary.get("direction_accuracy_pct"),
                            "avg_stock_return_pct": summary.get("avg_stock_return_pct"),
                            "avg_simulated_return_pct": summary.get("avg_simulated_return_pct"),
                            "stop_loss_trigger_rate": summary.get("stop_loss_trigger_rate"),
                            "take_profit_trigger_rate": summary.get("take_profit_trigger_rate"),
                            "advice_breakdown": summary.get("advice_breakdown"),
                            "diagnostics": summary.get("diagnostics"),
                        }
                    )

        scans.sort(key=self._scan_sort_key, reverse=True)
        best_by_return = self._select_best_scan(scans, metric="avg_simulated_return_pct")
        best_by_win_rate = self._select_best_scan(scans, metric="win_rate_pct")

        return {
            "raw_candidate_count": len(raw_candidates),
            "ranked_candidate_count": len(ranked_candidates),
            "local_data_only": local_data_only,
            "scans": scans,
            "best_by_return": best_by_return,
            "best_by_win_rate": best_by_win_rate,
            "conclusion": self._build_scan_conclusion(
                scans=scans,
                best_by_return=best_by_return,
                best_by_win_rate=best_by_win_rate,
                local_data_only=local_data_only,
            ),
        }

    def get_recent_evaluations(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        limit: int = 50,
        page: int = 1,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
    ) -> Dict[str, Any]:
        config = get_config()
        engine_version = str(getattr(config, "backtest_engine_version", "v1"))

        # When date filters are active and no explicit window is requested,
        # infer the smallest available window to stay aligned with summary metrics.
        if eval_window_days is None and (analysis_date_from is not None or analysis_date_to is not None):
            windows = self.repo.get_distinct_eval_windows(
                code=code,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
            )
            if windows:
                eval_window_days = windows[0]

        offset = max(page - 1, 0) * limit
        rows, total = self.repo.get_results_paginated(
            code=code,
            eval_window_days=eval_window_days,
            engine_version=engine_version,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
            days=None,
            offset=offset,
            limit=limit,
        )
        items = [self._result_to_dict(result, stock_name, trend_prediction) for result, stock_name, trend_prediction, _ in rows]
        return {"total": total, "page": page, "limit": limit, "items": items}

    def get_summary(
        self,
        *,
        scope: str,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
    ) -> Optional[Dict[str, Any]]:
        config = get_config()
        engine_version = str(getattr(config, "backtest_engine_version", "v1"))
        lookup_code = OVERALL_SENTINEL_CODE if scope == "overall" else code

        if analysis_date_from is not None or analysis_date_to is not None:
            ew = int(eval_window_days) if eval_window_days is not None else None
            count = self.repo.count_results(
                code=code,
                eval_window_days=ew,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
            )
            if count > self.MAX_DYNAMIC_SUMMARY_ROWS:
                raise ValueError(
                    "Date-filtered summary matches too many rows; narrow the analysis date range or stock code."
                )
            rows = self.repo.list_results(
                code=code,
                eval_window_days=ew,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
            )
            return self._build_dynamic_summary(
                rows=rows,
                scope=scope,
                code=lookup_code,
                eval_window_days=int(eval_window_days) if eval_window_days is not None else None,
                engine_version=engine_version,
                max_rows=self.MAX_DYNAMIC_SUMMARY_ROWS,
            )

        summary = self.repo.get_summary(
            scope=scope,
            code=lookup_code,
            eval_window_days=eval_window_days,
            engine_version=engine_version,
        )
        if summary is None:
            return None
        return self._summary_to_dict(summary)

    def get_global_summary(self, *, eval_window_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Return overall backtest metrics normalized for Agent memory consumers."""
        return self._normalize_learning_summary(
            self.get_summary(scope="overall", code=None, eval_window_days=eval_window_days)
        )

    def get_stock_summary(self, code: str, *, eval_window_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Return per-stock backtest metrics normalized for Agent memory consumers."""
        return self._normalize_learning_summary(
            self.get_summary(scope="stock", code=code, eval_window_days=eval_window_days)
        )

    def get_skill_summary(self, skill_id: str, *, eval_window_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Return skill-like summary metrics for Agent memory consumers.

        The current backtest storage layer only persists overall / per-stock rollups.
        Re-using the overall rollup here would fabricate skill-specific performance
        and mislead auto-weighting. Until real skill-tagged summaries exist, return
        ``None`` so downstream callers fall back to neutral weighting.
        """
        return None

    def get_strategy_summary(self, strategy_id: str, *, eval_window_days: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Compatibility wrapper for legacy strategy-based callers."""
        summary = self.get_skill_summary(strategy_id, eval_window_days=eval_window_days)
        if summary is None:
            return None
        normalized = dict(summary)
        normalized["strategy_id"] = strategy_id
        return normalized

    def _resolve_analysis_date(self, analysis) -> Optional[date]:
        parsed = self.repo.parse_analysis_date_from_snapshot(analysis.context_snapshot)
        if parsed:
            return parsed
        if getattr(analysis, "created_at", None):
            return analysis.created_at.date()
        logger.warning(f"无法确定分析日期，跳过记录: {analysis.code}#{getattr(analysis, 'id', '?')}")
        return None

    @staticmethod
    def _is_eval_window_mature(analysis_date: date, eval_window_days: int, *, today: Optional[date] = None) -> bool:
        current_date = today or date.today()
        return (current_date - analysis_date).days >= max(int(eval_window_days), 0)

    @staticmethod
    def _safe_load_json(raw: Optional[str]) -> Dict[str, Any]:
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _get_nested(payload: Dict[str, Any], path: Sequence[str]) -> Any:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    def _extract_ranking_score(self, analysis) -> Tuple[Optional[float], str]:
        snapshot = self._safe_load_json(getattr(analysis, "context_snapshot", None))
        raw_result = self._safe_load_json(getattr(analysis, "raw_result", None))

        score_candidates = (
            (
                self._get_nested(snapshot, ("enhanced_context", "trend_analysis", "signal_score")),
                "signal_score",
            ),
            (
                self._get_nested(raw_result, ("sentiment_score",)),
                "sentiment_score",
            ),
            (
                getattr(analysis, "sentiment_score", None),
                "sentiment_score",
            ),
            (
                self._get_nested(
                    raw_result,
                    ("dashboard", "data_perspective", "trend_status", "trend_score"),
                ),
                "trend_score",
            ),
        )

        for raw_score, source in score_candidates:
            if raw_score is None:
                continue
            try:
                return float(raw_score), source
            except (TypeError, ValueError):
                continue

        return None, "missing"

    def _build_ranked_candidates(self, analyses: Sequence[Any]) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        for analysis in analyses:
            analysis_date = self._resolve_analysis_date(analysis)
            if analysis_date is None:
                continue

            ranking_score, score_source = self._extract_ranking_score(analysis)
            if ranking_score is None:
                continue

            candidates.append(
                {
                    "analysis": analysis,
                    "analysis_date": analysis_date,
                    "ranking_score": ranking_score,
                    "score_source": score_source,
                }
            )

        candidates.sort(
            key=lambda item: (
                item["ranking_score"],
                getattr(item["analysis"], "created_at", datetime.min),
            ),
            reverse=True,
        )
        return candidates

    def _evaluate_candidate(
        self,
        candidate: Dict[str, Any],
        eval_config: EvaluationConfig,
        *,
        allow_data_fill: bool = True,
    ) -> BacktestResult:
        analysis = candidate["analysis"]
        ranking_score = candidate["ranking_score"]
        score_source = candidate["score_source"]
        eval_window_days = int(eval_config.eval_window_days)
        engine_version = str(eval_config.engine_version)

        try:
            analysis_date = candidate.get("analysis_date") or self._resolve_analysis_date(analysis)
            if analysis_date is None:
                return BacktestResult(
                    analysis_history_id=analysis.id,
                    code=analysis.code,
                    eval_window_days=eval_window_days,
                    engine_version=engine_version,
                    eval_status="error",
                    evaluated_at=datetime.now(),
                    operation_advice=analysis.operation_advice,
                    ranking_score=ranking_score,
                    score_source=score_source,
                )

            should_fill_data = allow_data_fill and self._is_eval_window_mature(analysis_date, eval_window_days)
            start_daily = self.stock_repo.get_start_daily(code=analysis.code, analysis_date=analysis_date)
            if (start_daily is None or start_daily.close is None) and should_fill_data:
                self._try_fill_daily_data(
                    code=analysis.code,
                    analysis_date=analysis_date,
                    eval_window_days=eval_window_days,
                )
                start_daily = self.stock_repo.get_start_daily(code=analysis.code, analysis_date=analysis_date)

            if start_daily is None or start_daily.close is None:
                return BacktestResult(
                    analysis_history_id=analysis.id,
                    code=analysis.code,
                    analysis_date=analysis_date,
                    eval_window_days=eval_window_days,
                    engine_version=engine_version,
                    eval_status="insufficient_data",
                    evaluated_at=datetime.now(),
                    operation_advice=analysis.operation_advice,
                    ranking_score=ranking_score,
                    score_source=score_source,
                )

            forward_bars = self.stock_repo.get_forward_bars(
                code=analysis.code,
                analysis_date=start_daily.date,
                eval_window_days=eval_window_days,
            )
            should_fill_forward_data = allow_data_fill and self._is_eval_window_mature(
                start_daily.date,
                eval_window_days,
            )
            if len(forward_bars) < eval_window_days and should_fill_forward_data:
                self._try_fill_daily_data(
                    code=analysis.code,
                    analysis_date=start_daily.date,
                    eval_window_days=eval_window_days,
                )
                forward_bars = self.stock_repo.get_forward_bars(
                    code=analysis.code,
                    analysis_date=start_daily.date,
                    eval_window_days=eval_window_days,
                )

            evaluation = BacktestEngine.evaluate_single(
                operation_advice=analysis.operation_advice,
                analysis_date=start_daily.date,
                start_price=float(start_daily.close),
                forward_bars=forward_bars,
                stop_loss=analysis.stop_loss,
                take_profit=analysis.take_profit,
                config=eval_config,
            )

            return BacktestResult(
                analysis_history_id=analysis.id,
                code=analysis.code,
                analysis_date=evaluation.get("analysis_date"),
                eval_window_days=int(evaluation.get("eval_window_days") or eval_window_days),
                engine_version=str(evaluation.get("engine_version") or engine_version),
                eval_status=str(evaluation.get("eval_status") or "error"),
                evaluated_at=datetime.now(),
                operation_advice=evaluation.get("operation_advice"),
                ranking_score=ranking_score,
                score_source=score_source,
                position_recommendation=evaluation.get("position_recommendation"),
                start_price=evaluation.get("start_price"),
                end_close=evaluation.get("end_close"),
                max_high=evaluation.get("max_high"),
                min_low=evaluation.get("min_low"),
                stock_return_pct=evaluation.get("stock_return_pct"),
                direction_expected=evaluation.get("direction_expected"),
                direction_correct=evaluation.get("direction_correct"),
                outcome=evaluation.get("outcome"),
                stop_loss=evaluation.get("stop_loss"),
                take_profit=evaluation.get("take_profit"),
                hit_stop_loss=evaluation.get("hit_stop_loss"),
                hit_take_profit=evaluation.get("hit_take_profit"),
                first_hit=evaluation.get("first_hit"),
                first_hit_date=evaluation.get("first_hit_date"),
                first_hit_trading_days=evaluation.get("first_hit_trading_days"),
                simulated_entry_price=evaluation.get("simulated_entry_price"),
                simulated_exit_price=evaluation.get("simulated_exit_price"),
                simulated_exit_reason=evaluation.get("simulated_exit_reason"),
                simulated_return_pct=evaluation.get("simulated_return_pct"),
            )
        except Exception as exc:
            logger.error(f"回测失败: {analysis.code}#{analysis.id}: {exc}")
            return BacktestResult(
                analysis_history_id=analysis.id,
                code=analysis.code,
                analysis_date=self._resolve_analysis_date(analysis),
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                eval_status="error",
                evaluated_at=datetime.now(),
                operation_advice=analysis.operation_advice,
                ranking_score=ranking_score,
                score_source=score_source,
            )

    @staticmethod
    def _scan_sort_key(item: Dict[str, Any]) -> Tuple[float, float, int, int]:
        avg_simulated_return_pct = item.get("avg_simulated_return_pct")
        win_rate_pct = item.get("win_rate_pct")
        completed_count = item.get("completed_count") or 0
        candidate_count = item.get("candidate_count") or 0
        return (
            float(avg_simulated_return_pct) if avg_simulated_return_pct is not None else float("-inf"),
            float(win_rate_pct) if win_rate_pct is not None else float("-inf"),
            int(completed_count),
            int(candidate_count),
        )

    @classmethod
    def _select_best_scan(cls, scans: Sequence[Dict[str, Any]], *, metric: str) -> Optional[Dict[str, Any]]:
        valid_scans = [scan for scan in scans if scan.get(metric) is not None]
        if not valid_scans:
            return None
        return max(
            valid_scans,
            key=lambda item: (
                float(item.get(metric)),
                cls._scan_sort_key(item),
            ),
        )

    @classmethod
    def _build_scan_conclusion(
        cls,
        *,
        scans: Sequence[Dict[str, Any]],
        best_by_return: Optional[Dict[str, Any]],
        best_by_win_rate: Optional[Dict[str, Any]],
        local_data_only: bool,
    ) -> Dict[str, Any]:
        completed_scans = [scan for scan in scans if (scan.get("completed_count") or 0) > 0]
        if not scans:
            return {
                "status": "no_candidates",
                "summary_text": "没有可比较的参数组合。",
                "recommended_scan": None,
                "secondary_scan": None,
            }

        if not completed_scans:
            if local_data_only:
                summary_text = "本地已有行情数据不足，当前只能得到数据不足结论，暂时无法推荐参数组合。"
            else:
                summary_text = "当前参数组合都未形成有效回测结果，先检查行情补数链路后再比较参数。"
            return {
                "status": "insufficient_data",
                "summary_text": summary_text,
                "recommended_scan": None,
                "secondary_scan": None,
            }

        recommended_scan = best_by_return or completed_scans[0]
        secondary_scan = None
        if best_by_win_rate and not cls._same_scan(best_by_win_rate, recommended_scan):
            secondary_scan = best_by_win_rate

        recommendation_parts = [
            f"优先考虑持有 {recommended_scan['eval_window_days']} 天",
            f"分数阈值 {cls._format_scan_value(recommended_scan.get('score_threshold'))}",
            f"前 N {cls._format_scan_value(recommended_scan.get('top_n'))}",
        ]
        metrics_parts = []
        if recommended_scan.get("avg_simulated_return_pct") is not None:
            metrics_parts.append(f"模拟收益 {recommended_scan['avg_simulated_return_pct']:.2f}%")
        if recommended_scan.get("win_rate_pct") is not None:
            metrics_parts.append(f"胜率 {recommended_scan['win_rate_pct']:.2f}%")
        if recommended_scan.get("completed_count") is not None:
            metrics_parts.append(f"有效样本 {recommended_scan['completed_count']}")

        summary_text = "，".join(recommendation_parts)
        if metrics_parts:
            summary_text = f"{summary_text}；{', '.join(metrics_parts)}。"
        else:
            summary_text = f"{summary_text}。"

        if secondary_scan is not None:
            summary_text += (
                f" 另一个偏稳的组合是持有 {secondary_scan['eval_window_days']} 天，"
                f"分数阈值 {cls._format_scan_value(secondary_scan.get('score_threshold'))}，"
                f"前 N {cls._format_scan_value(secondary_scan.get('top_n'))}。"
            )

        return {
            "status": "ok",
            "summary_text": summary_text,
            "recommended_scan": recommended_scan,
            "secondary_scan": secondary_scan,
        }

    @staticmethod
    def _same_scan(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
        return (
            left.get("eval_window_days") == right.get("eval_window_days")
            and left.get("score_threshold") == right.get("score_threshold")
            and left.get("top_n") == right.get("top_n")
        )

    @staticmethod
    def _format_scan_value(value: Optional[Any]) -> str:
        if value is None:
            return "不限"
        return str(value)

    def _try_fill_daily_data(self, *, code: str, analysis_date: date, eval_window_days: int) -> None:
        try:
            from data_provider.base import DataFetcherManager

            # fetch a window that covers start + forward bars
            end_date = analysis_date + timedelta(days=max(eval_window_days * 2, 30))
            manager = DataFetcherManager()
            df, source = manager.get_daily_data(
                stock_code=code,
                start_date=analysis_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                days=eval_window_days * 2,
            )
            if df is None or df.empty:
                return
            self.db.save_daily_data(df, code=code, data_source=source)
        except Exception as exc:
            logger.warning(f"补全日线数据失败({code}): {exc}")

    def _recompute_summaries(self, *, touched_codes: List[str], eval_window_days: int, engine_version: str) -> None:
        with self.db.get_session() as session:
            # overall
            overall_rows = session.execute(
                select(BacktestResult).where(
                    and_(
                        BacktestResult.eval_window_days == eval_window_days,
                        BacktestResult.engine_version == engine_version,
                    )
                )
            ).scalars().all()
            overall_data = BacktestEngine.compute_summary(
                results=overall_rows,
                scope="overall",
                code=OVERALL_SENTINEL_CODE,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
            )
            overall_summary = self._build_summary_model(overall_data)
            self.repo.upsert_summary(overall_summary)

            for code in touched_codes:
                rows = session.execute(
                    select(BacktestResult).where(
                        and_(
                            BacktestResult.code == code,
                            BacktestResult.eval_window_days == eval_window_days,
                            BacktestResult.engine_version == engine_version,
                        )
                    )
                ).scalars().all()
                data = BacktestEngine.compute_summary(
                    results=rows,
                    scope="stock",
                    code=code,
                    eval_window_days=eval_window_days,
                    engine_version=engine_version,
                )
                summary = self._build_summary_model(data)
                self.repo.upsert_summary(summary)

    @staticmethod
    def _build_summary_model(summary_data: Dict[str, Any]) -> BacktestSummary:
        return BacktestSummary(
            scope=summary_data.get("scope"),
            code=summary_data.get("code"),
            eval_window_days=summary_data.get("eval_window_days"),
            engine_version=summary_data.get("engine_version"),
            computed_at=datetime.now(),
            total_evaluations=summary_data.get("total_evaluations") or 0,
            completed_count=summary_data.get("completed_count") or 0,
            insufficient_count=summary_data.get("insufficient_count") or 0,
            long_count=summary_data.get("long_count") or 0,
            cash_count=summary_data.get("cash_count") or 0,
            win_count=summary_data.get("win_count") or 0,
            loss_count=summary_data.get("loss_count") or 0,
            neutral_count=summary_data.get("neutral_count") or 0,
            direction_accuracy_pct=summary_data.get("direction_accuracy_pct"),
            win_rate_pct=summary_data.get("win_rate_pct"),
            neutral_rate_pct=summary_data.get("neutral_rate_pct"),
            avg_stock_return_pct=summary_data.get("avg_stock_return_pct"),
            avg_simulated_return_pct=summary_data.get("avg_simulated_return_pct"),
            stop_loss_trigger_rate=summary_data.get("stop_loss_trigger_rate"),
            take_profit_trigger_rate=summary_data.get("take_profit_trigger_rate"),
            ambiguous_rate=summary_data.get("ambiguous_rate"),
            avg_days_to_first_hit=summary_data.get("avg_days_to_first_hit"),
            advice_breakdown_json=json.dumps(summary_data.get("advice_breakdown") or {}, ensure_ascii=False),
            diagnostics_json=json.dumps(summary_data.get("diagnostics") or {}, ensure_ascii=False),
        )

    @staticmethod
    def _result_to_dict(
        row: BacktestResult,
        stock_name: Optional[str] = None,
        trend_prediction: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "analysis_history_id": row.analysis_history_id,
            "code": row.code,
            "stock_name": stock_name,
            "analysis_date": row.analysis_date.isoformat() if row.analysis_date else None,
            "eval_window_days": row.eval_window_days,
            "engine_version": row.engine_version,
            "eval_status": row.eval_status,
            "evaluated_at": row.evaluated_at.isoformat() if row.evaluated_at else None,
            "operation_advice": row.operation_advice,
            "ranking_score": row.ranking_score,
            "score_source": row.score_source,
            "trend_prediction": trend_prediction,
            "position_recommendation": row.position_recommendation,
            "start_price": row.start_price,
            "end_close": row.end_close,
            "max_high": row.max_high,
            "min_low": row.min_low,
            "stock_return_pct": row.stock_return_pct,
            "actual_return_pct": row.stock_return_pct,
            "actual_movement": BacktestService._actual_movement_from_return(row.stock_return_pct),
            "direction_expected": row.direction_expected,
            "direction_correct": row.direction_correct,
            "outcome": row.outcome,
            "stop_loss": row.stop_loss,
            "take_profit": row.take_profit,
            "hit_stop_loss": row.hit_stop_loss,
            "hit_take_profit": row.hit_take_profit,
            "first_hit": row.first_hit,
            "first_hit_date": row.first_hit_date.isoformat() if row.first_hit_date else None,
            "first_hit_trading_days": row.first_hit_trading_days,
            "simulated_entry_price": row.simulated_entry_price,
            "simulated_exit_price": row.simulated_exit_price,
            "simulated_exit_reason": row.simulated_exit_reason,
            "simulated_return_pct": row.simulated_return_pct,
        }

    @staticmethod
    def _summary_to_dict(row: BacktestSummary) -> Dict[str, Any]:
        return {
            "scope": row.scope,
            "code": None if row.code == OVERALL_SENTINEL_CODE else row.code,
            "eval_window_days": row.eval_window_days,
            "engine_version": row.engine_version,
            "computed_at": row.computed_at.isoformat() if row.computed_at else None,
            "total_evaluations": row.total_evaluations,
            "completed_count": row.completed_count,
            "insufficient_count": row.insufficient_count,
            "long_count": row.long_count,
            "cash_count": row.cash_count,
            "win_count": row.win_count,
            "loss_count": row.loss_count,
            "neutral_count": row.neutral_count,
            "direction_accuracy_pct": row.direction_accuracy_pct,
            "win_rate_pct": row.win_rate_pct,
            "neutral_rate_pct": row.neutral_rate_pct,
            "avg_stock_return_pct": row.avg_stock_return_pct,
            "avg_simulated_return_pct": row.avg_simulated_return_pct,
            "stop_loss_trigger_rate": row.stop_loss_trigger_rate,
            "take_profit_trigger_rate": row.take_profit_trigger_rate,
            "ambiguous_rate": row.ambiguous_rate,
            "avg_days_to_first_hit": row.avg_days_to_first_hit,
            "advice_breakdown": json.loads(row.advice_breakdown_json) if row.advice_breakdown_json else {},
            "diagnostics": json.loads(row.diagnostics_json) if row.diagnostics_json else {},
        }

    @staticmethod
    def _normalize_learning_summary(summary: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Normalize summary metrics to the ratio-based shape expected by Agent memory."""
        if summary is None:
            return None

        normalized = dict(summary)
        normalized["win_rate"] = BacktestService._pct_to_ratio(summary.get("win_rate_pct"), default=0.5)
        normalized["direction_accuracy"] = BacktestService._pct_to_ratio(
            summary.get("direction_accuracy_pct"),
            default=0.5,
        )

        avg_return_pct = summary.get("avg_simulated_return_pct")
        if avg_return_pct is None:
            avg_return_pct = summary.get("avg_stock_return_pct")
        normalized["avg_return"] = BacktestService._pct_to_ratio(avg_return_pct, default=0.0)
        return normalized

    @staticmethod
    def _pct_to_ratio(value: Optional[float], default: float = 0.0) -> float:
        try:
            return float(value) / 100.0
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _actual_movement_from_return(value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        try:
            actual_return = float(value)
        except (TypeError, ValueError):
            return None
        if actual_return > 0:
            return "up"
        if actual_return < 0:
            return "down"
        return "flat"

    @staticmethod
    def _build_dynamic_summary(
        *,
        rows: List[BacktestResult],
        scope: str,
        code: Optional[str],
        eval_window_days: Optional[int],
        engine_version: str,
        max_rows: Optional[int] = None,
    ) -> Dict[str, Any]:
        filtered_rows = [row for row in rows if getattr(row, "engine_version", None) == engine_version]
        if eval_window_days is not None:
            summary_window_days = int(eval_window_days)
        else:
            window_values = sorted({
                int(row.eval_window_days)
                for row in filtered_rows
                if getattr(row, "eval_window_days", None) is not None
            })
            if len(window_values) > 1:
                logger.warning(
                    "Multiple eval_window_days values found for dynamic summary; using %s for engine_version=%s, scope=%s, code=%s",
                    window_values[0],
                    engine_version,
                    scope,
                    code,
                )
            if window_values:
                summary_window_days = window_values[0]
            else:
                summary_window_days = int(getattr(get_config(), "backtest_eval_window_days", 10))

        filtered_rows = [
            row for row in filtered_rows if getattr(row, "eval_window_days", None) == summary_window_days
        ]

        if max_rows is not None and len(filtered_rows) > max_rows:
            raise ValueError(
                "Date-filtered summary matches too many rows; narrow the analysis date range or stock code."
            )

        summary = BacktestEngine.compute_summary(
            results=filtered_rows,
            scope=scope,
            code=code,
            eval_window_days=summary_window_days,
            engine_version=engine_version,
        )
        summary["code"] = None if summary.get("code") == OVERALL_SENTINEL_CODE else summary.get("code")
        summary["computed_at"] = datetime.now().isoformat()
        return summary
