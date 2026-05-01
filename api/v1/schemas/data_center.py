# -*- coding: utf-8 -*-
"""Data center response schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DataCenterSourceItem(BaseModel):
    name: str = Field(..., description="数据来源名称")
    count: int = Field(..., description="记录数量")


class DataCenterDatabaseInfo(BaseModel):
    path: Optional[str] = Field(None, description="本地数据库路径")
    exists: bool = Field(False, description="文件是否存在")
    size_bytes: int = Field(0, description="文件大小，字节")
    size_label: str = Field("0 B", description="可读文件大小")


class DataCenterFileInfo(BaseModel):
    key: str
    label: str
    path: str
    exists: bool
    file_count: int
    size_bytes: int
    size_label: str


class DataCenterRecommendation(BaseModel):
    level: str = Field(..., description="info/warning/success")
    title: str
    description: str


class DataCenterOverviewResponse(BaseModel):
    generated_at: str
    database: DataCenterDatabaseInfo
    market_data: Dict[str, Any]
    analysis: Dict[str, Any]
    backtests: Dict[str, Any]
    portfolio: Dict[str, Any]
    news: Dict[str, Any]
    fundamentals: Dict[str, Any]
    files: List[DataCenterFileInfo]
    maintenance: Dict[str, Any] = Field(default_factory=dict)
    ai_routing: Dict[str, Any] = Field(default_factory=dict)
    recommendations: List[DataCenterRecommendation]
    warnings: List[str] = Field(default_factory=list)


class DataCenterPortfolioBacktestRequest(BaseModel):
    force: bool = Field(False, description="是否强制重算已存在的回测结果")
    eval_window_days: Optional[int] = Field(None, ge=1, le=120, description="评估窗口天数")
    min_age_days: Optional[int] = Field(None, ge=0, le=3650, description="只回测至少多少天前的分析")
    limit_per_symbol: int = Field(50, ge=1, le=500, description="每只持仓最多处理多少条历史分析")


class DataCenterMarketDataRefreshRequest(BaseModel):
    force: bool = Field(False, description="是否强制按完整回看窗口刷新")
    lookback_days: Optional[int] = Field(None, ge=30, le=3650, description="首次补齐回看天数")
    refresh_overlap_days: Optional[int] = Field(None, ge=0, le=365, description="已有数据时回刷最近几天")
    max_symbols: Optional[int] = Field(None, ge=1, le=1000, description="本次最多处理多少只标的")


class DataCenterMarketDataRefreshResponse(BaseModel):
    generated_at: str
    status: str
    lookback_days: int
    refresh_overlap_days: int
    max_symbols: int
    force: bool
    end_date: str
    totals: Dict[str, int]
    items: List[Dict[str, Any]]
    ledger_path: Optional[str] = None


class DataCenterPortfolioReviewRequest(BaseModel):
    run_backtests: bool = Field(True, description="生成复盘前是否先执行持仓回测")
    send_notification: Optional[bool] = Field(None, description="是否发送通知，空值使用系统配置")


class DataCenterPortfolioReviewResponse(BaseModel):
    generated_at: str
    report_date: str
    status: str
    json_path: Optional[str] = None
    markdown_path: Optional[str] = None
    notification_sent: bool = False
    portfolio: Dict[str, Any] = Field(default_factory=dict)
    market_data: Dict[str, Any] = Field(default_factory=dict)
    backtest: Optional[Dict[str, Any]] = None
    radar: Dict[str, Any] = Field(default_factory=dict)
    ai_routing: Dict[str, Any] = Field(default_factory=dict)


class DataCenterCleanupRequest(BaseModel):
    dry_run: bool = Field(False, description="只预览不删除")


class DataCenterCleanupResponse(BaseModel):
    generated_at: str
    status: str
    dry_run: bool
    log_retention_days: int
    cache_retention_days: int
    totals: Dict[str, int]
    items: List[Dict[str, Any]]
    ledger_path: Optional[str] = None


class DataCenterPortfolioBacktestItem(BaseModel):
    code: str
    status: str
    message: str
    candidate_count: int
    processed: int
    saved: int
    completed: int
    insufficient: int
    errors: int
    summary: Optional[Dict[str, Any]] = None


class DataCenterPortfolioBacktestResponse(BaseModel):
    generated_at: str
    holding_count: int
    processed_symbols: int
    totals: Dict[str, int]
    items: List[DataCenterPortfolioBacktestItem]


class DataCenterPortfolioRiskRadarItem(BaseModel):
    code: str
    quantity: float
    market_value_base: float
    updated_at: Optional[str] = None
    tone: str
    label: str
    title: str
    message: str
    total_evaluations: int
    completed_count: int
    insufficient_count: int
    win_rate_pct: Optional[float] = None
    avg_simulated_return_pct: Optional[float] = None
    summary: Optional[Dict[str, Any]] = None


class DataCenterPortfolioRiskRadarResponse(BaseModel):
    generated_at: str
    holding_count: int
    items: List[DataCenterPortfolioRiskRadarItem]
