# -*- coding: utf-8 -*-
"""Backtest API schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class BacktestRunRequest(BaseModel):
    code: Optional[str] = Field(None, description="仅回测指定股票")
    force: bool = Field(False, description="强制重新计算")
    eval_window_days: Optional[int] = Field(None, ge=1, le=120, description="评估窗口（交易日数）")
    min_age_days: Optional[int] = Field(None, ge=0, le=365, description="分析记录最小天龄（0=不限）")
    limit: int = Field(200, ge=1, le=2000, description="最多处理的分析记录数")
    score_threshold: Optional[float] = Field(None, description="仅回测排序分数不低于该值的候选")
    top_n: Optional[int] = Field(None, ge=1, le=500, description="仅取排序后的前 N 条候选")


class BacktestRunResponse(BaseModel):
    candidate_count: int = Field(..., description="排序筛选后的候选记录数")
    processed: int = Field(..., description="候选记录数")
    saved: int = Field(..., description="写入回测结果数")
    completed: int = Field(..., description="完成回测数")
    insufficient: int = Field(..., description="数据不足数")
    errors: int = Field(..., description="错误数")


class BacktestScanRequest(BaseModel):
    code: Optional[str] = Field(None, description="仅扫描指定股票")
    min_age_days: Optional[int] = Field(None, ge=0, le=365, description="分析记录最小天龄（0=不限）")
    limit: int = Field(200, ge=1, le=2000, description="最多扫描的分析记录数")
    local_data_only: bool = Field(False, description="仅使用本地已有行情数据，不尝试联网补数")
    eval_window_days_options: List[int] = Field(default_factory=lambda: [10], description="待比较的持有天数列表")
    score_threshold_options: List[Optional[float]] = Field(
        default_factory=lambda: [None, 60.0, 70.0],
        description="待比较的分数阈值列表，可含 null 表示不过滤",
    )
    top_n_options: List[Optional[int]] = Field(
        default_factory=lambda: [None, 3, 5],
        description="待比较的前 N 名列表，可含 null 表示不过滤",
    )


class BacktestScanItem(BaseModel):
    eval_window_days: int
    score_threshold: Optional[float] = None
    top_n: Optional[int] = None
    candidate_count: int
    completed_count: int
    insufficient_count: int
    win_count: int
    loss_count: int
    neutral_count: int
    win_rate_pct: Optional[float] = None
    direction_accuracy_pct: Optional[float] = None
    avg_stock_return_pct: Optional[float] = None
    avg_simulated_return_pct: Optional[float] = None
    stop_loss_trigger_rate: Optional[float] = None
    take_profit_trigger_rate: Optional[float] = None
    advice_breakdown: Dict[str, Any] = Field(default_factory=dict)
    diagnostics: Dict[str, Any] = Field(default_factory=dict)


class BacktestScanConclusion(BaseModel):
    status: str
    summary_text: str
    recommended_scan: Optional[BacktestScanItem] = None
    secondary_scan: Optional[BacktestScanItem] = None


class BacktestScanResponse(BaseModel):
    raw_candidate_count: int
    ranked_candidate_count: int
    local_data_only: bool = False
    best_by_return: Optional[BacktestScanItem] = None
    best_by_win_rate: Optional[BacktestScanItem] = None
    conclusion: Optional[BacktestScanConclusion] = None
    scans: List[BacktestScanItem] = Field(default_factory=list)


class BacktestResultItem(BaseModel):
    analysis_history_id: int
    code: str
    stock_name: Optional[str] = None
    analysis_date: Optional[str] = None
    eval_window_days: int
    engine_version: str
    eval_status: str
    evaluated_at: Optional[str] = None
    operation_advice: Optional[str] = None
    ranking_score: Optional[float] = None
    score_source: Optional[str] = None
    trend_prediction: Optional[str] = None
    position_recommendation: Optional[str] = None
    start_price: Optional[float] = None
    end_close: Optional[float] = None
    max_high: Optional[float] = None
    min_low: Optional[float] = None
    stock_return_pct: Optional[float] = None
    actual_return_pct: Optional[float] = None
    actual_movement: Optional[str] = None
    direction_expected: Optional[str] = None
    direction_correct: Optional[bool] = None
    outcome: Optional[str] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    hit_stop_loss: Optional[bool] = None
    hit_take_profit: Optional[bool] = None
    first_hit: Optional[str] = None
    first_hit_date: Optional[str] = None
    first_hit_trading_days: Optional[int] = None
    simulated_entry_price: Optional[float] = None
    simulated_exit_price: Optional[float] = None
    simulated_exit_reason: Optional[str] = None
    simulated_return_pct: Optional[float] = None


class BacktestResultsResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[BacktestResultItem] = Field(default_factory=list)


class PerformanceMetrics(BaseModel):
    scope: str
    code: Optional[str] = None
    eval_window_days: int
    engine_version: str
    computed_at: Optional[str] = None

    total_evaluations: int
    completed_count: int
    insufficient_count: int
    long_count: int
    cash_count: int
    win_count: int
    loss_count: int
    neutral_count: int

    direction_accuracy_pct: Optional[float] = None
    win_rate_pct: Optional[float] = None
    neutral_rate_pct: Optional[float] = None
    avg_stock_return_pct: Optional[float] = None
    avg_simulated_return_pct: Optional[float] = None

    stop_loss_trigger_rate: Optional[float] = None
    take_profit_trigger_rate: Optional[float] = None
    ambiguous_rate: Optional[float] = None
    avg_days_to_first_hit: Optional[float] = None

    advice_breakdown: Dict[str, Any] = Field(default_factory=dict)
    diagnostics: Dict[str, Any] = Field(default_factory=dict)


class ShadowRuleCount(BaseModel):
    rule: str
    count: int


class ShadowScorecardSection(BaseModel):
    status: str
    json_path: Optional[str] = None
    report_path: Optional[str] = None
    generated_at: Optional[str] = None
    primary_window: Optional[int] = None
    min_samples: Optional[int] = None
    daily_meta: Dict[str, Any] = Field(default_factory=dict)
    candidates: List[Dict[str, Any]] = Field(default_factory=list)
    all_rows: List[Dict[str, Any]] = Field(default_factory=list)


class ShadowLedgerSection(BaseModel):
    status: str
    ledger_path: Optional[str] = None
    summary_path: Optional[str] = None
    total_count: int = 0
    open_count: int = 0
    settled_count: int = 0
    rule_counts: List[ShadowRuleCount] = Field(default_factory=list)
    entries: List[Dict[str, Any]] = Field(default_factory=list)


class IntradayReplaySignalTypeCount(BaseModel):
    signal_type: str
    count: int = 0
    labeled_count: int = 0
    effective_count: int = 0
    effective_rate_pct: Optional[float] = None
    avg_primary_return_pct: Optional[float] = None


class IntradayReplaySection(BaseModel):
    status: str
    ledger_path: Optional[str] = None
    total_count: int = 0
    labeled_count: int = 0
    pending_count: int = 0
    effective_count: int = 0
    effective_rate_pct: Optional[float] = None
    avg_primary_return_pct: Optional[float] = None
    avg_mfe_pct: Optional[float] = None
    avg_mae_pct: Optional[float] = None
    signal_type_counts: List[IntradayReplaySignalTypeCount] = Field(default_factory=list)
    entries: List[Dict[str, Any]] = Field(default_factory=list)


class ShadowDashboardResponse(BaseModel):
    status: str
    generated_at: str
    backtest_dir: str
    scorecard: ShadowScorecardSection
    ledger: ShadowLedgerSection
    intraday_replay: IntradayReplaySection
